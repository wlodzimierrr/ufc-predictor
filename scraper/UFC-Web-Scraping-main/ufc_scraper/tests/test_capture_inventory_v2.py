"""Synthetic offline inventory/selection tests; never populate repository data."""

from contextlib import contextmanager
import fcntl
import hashlib
import importlib.abc
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import socket
import sys
import uuid

import pytest

from ufc_scraper import capture_inventory_v2 as audit
from ufc_scraper import raw_capture_v2 as capture


URL = "http://www.ufcstats.com/fight-details/synthetic"
COMPLETED = "http://www.ufcstats.com/statistics/events/completed?page=all"
UPCOMING = "http://www.ufcstats.com/statistics/events/upcoming"
SECRET = "SYNTHETIC-SECRET-NO-EXPORT"
ROOT = Path(__file__).resolve().parents[4]


def forbidden(*args, **kwargs):
    raise AssertionError("Network/database/write access forbidden in inventory checks")


class NoDatabase(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {"warehouse.db", "psycopg", "psycopg2"}:
            forbidden()


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    for name in ("create_connection", "getaddrinfo", "gethostbyname", "gethostbyname_ex", "gethostbyaddr"):
        monkeypatch.setattr(socket, name, forbidden)
    for name in ("connect", "connect_ex", "send", "sendall", "sendto", "sendmsg"):
        if hasattr(socket.socket, name):
            monkeypatch.setattr(socket.socket, name, forbidden)
    guard = NoDatabase()
    sys.meta_path.insert(0, guard)
    yield
    sys.meta_path.remove(guard)


@pytest.fixture
def store(tmp_path):
    return capture.CaptureStoreV2(tmp_path / "v2")


def pin(store, relative):
    return hashlib.sha256((store.root / relative).read_bytes()).hexdigest()


def mutate(path, content):
    path.chmod(0o600)
    path.write_bytes(content)
    path.chmod(0o444)


def rows(store):
    return audit.inventory(store.root)["observations"]


def cli():
    path = ROOT / "tools/audit_raw_capture_v2.py"
    spec = importlib.util.spec_from_file_location("inventory_cli", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def snapshot(root):
    # Access times are governed by the mount's read policy; all write metadata
    # and exact bytes are compared. Symlinks are inventoried without following.
    result = {}
    for path in [root, *sorted(root.rglob("*"))]:
        st = path.lstat()
        result[str(path.relative_to(root))] = (
            st.st_ino, st.st_mode, st.st_size, st.st_mtime_ns, st.st_ctime_ns,
            os.readlink(path) if path.is_symlink() else
            hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None)
    return result


@contextmanager
def no_writes(monkeypatch):
    with monkeypatch.context() as guard:
        original_open = os.open
        original_flock = fcntl.flock

        def readonly_open(path, flags, *args, **kwargs):
            assert not flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)
            return original_open(path, flags, *args, **kwargs)

        def readonly_lock(fd, operation):
            assert operation == fcntl.LOCK_SH | fcntl.LOCK_NB
            return original_flock(fd, operation)

        guard.setattr(os, "open", readonly_open)
        guard.setattr(fcntl, "flock", readonly_lock)
        for name in ("mkdir", "chmod", "fchmod", "fsync", "unlink", "link", "rename", "replace",
                     "truncate", "ftruncate", "write", "utime", "rmdir"):
            guard.setattr(os, name, forbidden)
        yield


def test_shared_object_versions_listing_urls_and_jobs_are_separate(store):
    a = store.capture_response(URL, b"shared", 200)
    b = store.capture_response(URL, b"shared", 200)
    c = store.capture_response(URL, b"changed", 200)
    other = capture.CaptureStoreV2(store.root)
    d = other.capture_response(COMPLETED, b"shared", 200)
    e = other.capture_response(UPCOMING, b"shared", 200)
    result = audit.inventory(store.root)
    assert result["status"] == "INVENTORIED"
    assert result["consistency"] == "COOPERATING_WRITERS_LOCKED"
    assert result["observation_count"] == 5
    assert result["counts"]["VERIFIED_SUCCEEDED"] == 5
    assert [r["receipt_path"] for r in result["observations"]] == sorted([a, b, c, d, e])
    assert len({r["verified_body"]["sha256"] for r in result["observations"]}) == 2
    assert {r["metadata"]["source_url"] for r in result["observations"]} == {URL, COMPLETED, UPCOMING}
    for row in result["observations"]:
        assert row["receipt_sha256"] == pin(store, row["receipt_path"])
        assert row["receipt_integrity"] == "MEASURED_ONLY"
        assert row["job_run_id"] == row["metadata"]["job_run_id"]
        assert row["observation_id"] == row["metadata"]["observation_id"]
        assert row["metadata"]["observed_at"] <= row["metadata"]["receipt_prepared_at"]
    assert result["receipt_pin_authority"] == "NOT_ESTABLISHED_BY_INVENTORY"
    assert result["prospective_evidence"] == "BLOCKED"
    assert result["historical_comparison"] == "STILL_BLOCKED"


@pytest.mark.parametrize("status,body,cached,expected", [
    (200, b"exact", False, "VERIFIED_SUCCEEDED"),
    (204, b"", False, "VERIFIED_SUCCEEDED"),
    (404, b"", False, "VERIFIED_FAILED"),
    (503, b"failed body", False, "VERIFIED_FAILED"),
    (301, b"redirect", False, "VERIFIED_FAILED"),
    (200, b"Checking your browser /__c", False, "VERIFIED_FAILED"),
    (200, b"cache", True, "VERIFIED_CACHED"),
    (503, b"failed cache", True, "VERIFIED_CACHED"),
])
def test_success_failure_and_cache_are_orthogonal(store, status, body, cached, expected):
    path = store.capture_response(URL, body, status, from_cache=cached)
    row = rows(store)[0]
    assert row["state"] == expected
    assert row["publication"] == "COMPLETE_VERIFIED"
    assert row["metadata"]["from_cache"] is cached
    assert row["verified_body"]["bytes"] == len(body)
    assert row["body_availability"] == "VERIFIED_BYTES"
    assert audit.select_capture(store.root, path, pin(store, path), forensic=True).body == body
    result = audit.inspect_receipt(store.root, path, pin(store, path))
    assert result["status"] == ("VERIFIED_CAPTURE_BYTES" if status in {200, 204} and
                                row["metadata"]["disposition"] == "SUCCEEDED" and not cached else "REFUSED")
    forensic = audit.inspect_receipt(store.root, path, pin(store, path), forensic=True)
    assert forensic["mode"] == "FORENSIC"
    assert forensic["observation"]["state"] == expected
    assert "body" not in forensic["observation"]["metadata"]


def test_empty_response_is_not_absent_exception_body(store):
    store.capture_response(URL, b"", 200)
    path = store.capture_exception(URL, TimeoutError(SECRET))
    result = rows(store)
    empty = next(r for r in result if r["metadata"]["response_present"])
    absent = next(r for r in result if not r["metadata"]["response_present"])
    assert empty["verified_body"]["sha256"] == hashlib.sha256(b"").hexdigest()
    assert absent["verified_body"] is None
    assert absent["body_availability"] == "ABSENT_EXCEPTION_BODY"
    assert absent["metadata"]["exception_category"] == "timeout"
    assert absent["state"] == "VERIFIED_FAILED"
    assert SECRET not in json.dumps(result)
    assert audit.select_capture(store.root, path, pin(store, path), forensic=True).body is None
    assert audit.inspect_receipt(store.root, path, pin(store, path))["status"] == "REFUSED"


@pytest.mark.parametrize("damage", ["reservation", "pending_only", "pending_stage", "receipt_stage", "object_stage"])
def test_incomplete_reservations_staging_and_expected_bytes(store, damage):
    path = store.capture_response(URL, b"must not claim pending bytes", 200)
    directory = (store.root / path).parent
    if damage == "reservation":
        (directory / "receipt.json").unlink()
        (directory / "pending.json").unlink()
    elif damage in {"pending_only", "pending_stage"}:
        (directory / "receipt.json").unlink()
    if damage in {"pending_stage", "receipt_stage"}:
        (directory / ".synthetic.partial").write_bytes(b"unfinished")
    if damage == "object_stage":
        object_path = store.root / json.loads((store.root / path).read_bytes())["body"]["path"]
        object_path.with_name(".synthetic.partial").write_bytes(b"unfinished")
    result = audit.inventory(store.root)
    row = result["observations"][0]
    if damage == "object_stage":
        # Preserve the resolver's accepted semantics: only observation staging
        # invalidates that receipt. Object staging is still explicitly reported.
        assert row["state"] == "VERIFIED_SUCCEEDED"
    else:
        assert row["state"] == "INCOMPLETE"
        assert row["verified_body"] is None
        assert row["metadata"] is None
        assert row["body_availability"] == "UNVERIFIED"
        assert audit.inspect_receipt(store.root, path, "0" * 64, forensic=True)["status"] == "REFUSED"
    assert len(result["staging_markers"]) == (1 if damage.endswith("stage") else 0)


@pytest.mark.parametrize("damage,expected", [("missing", "MISSING_OBJECT"),
                                            ("changed", "CORRUPT_OBJECT"), ("truncated", "CORRUPT_OBJECT")])
def test_referenced_object_failures_do_not_omit_observations(store, damage, expected):
    paths = [store.capture_response(URL, b"shared", 200) for _ in range(2)]
    reference = json.loads((store.root / paths[0]).read_bytes())["body"]
    object_path = store.root / reference["path"]
    if damage == "missing":
        object_path.unlink()
    else:
        mutate(object_path, b"xxxxxx" if damage == "changed" else b"x")
    result = audit.inventory(store.root)
    assert result["observation_count"] == 2
    assert result["counts"][expected] == 2
    assert all(r["verified_body"] is None and r["metadata"] is None for r in result["observations"])
    assert audit.inspect_receipt(store.root, paths[0], pin(store, paths[0]), forensic=True)["status"] == "REFUSED"


@pytest.mark.parametrize("damage", ["invalid_json", "duplicate_key", "field", "identity", "clock", "pending",
                                   "missing_pending", "unsafe_url", "false_challenge", "body_binding"])
def test_receipt_tampering_is_visible_and_never_exports_unverified_metadata(store, damage):
    path = store.capture_response(URL, b"exact", 200)
    receipt = store.root / path
    value = json.loads(receipt.read_bytes())
    if damage == "missing_pending":
        receipt.with_name("pending.json").unlink()
    elif damage == "pending":
        mutate(receipt.with_name("pending.json"), b"{}")
    elif damage == "invalid_json":
        mutate(receipt, ("invalid " + SECRET).encode())
    elif damage == "duplicate_key":
        mutate(receipt, receipt.read_bytes().replace(b'{', b'{"schema":"duplicate",', 1))
    else:
        if damage == "field":
            value["unknown"] = SECRET
        elif damage == "identity":
            value["observation_id"] = str(uuid.uuid4())
        elif damage == "clock":
            value["observed_at"] = "1990-01-01T00:00:00+00:00"
        elif damage == "unsafe_url":
            value["source_url"] = "https://user:" + SECRET + "@ufcstats.com/fight-details/synthetic"
        elif damage == "false_challenge":
            value.update(access_challenge=True, disposition="FAILED", failure_reasons=["access_challenge"])
        elif damage == "body_binding":
            value["body"]["path"] = "../outside"
        mutate(receipt, capture._json_bytes(value))
        if damage == "false_challenge":
            pending = {k: v for k, v in value.items() if k != "receipt_prepared_at"}
            pending["publication_state"] = "INCOMPLETE"
            mutate(receipt.with_name("pending.json"), capture._json_bytes(pending))
    result = audit.inventory(store.root)
    row = result["observations"][0]
    assert row["state"] == "INVALID_RECEIPT"
    assert row["receipt_sha256"] == pin(store, path)
    assert row["metadata"] is None and row["verified_body"] is None
    assert SECRET not in json.dumps(result)
    assert audit.inspect_receipt(store.root, path, pin(store, path), forensic=True)["status"] == "REFUSED"


@pytest.mark.parametrize("expected", [None, "", "ABC", "f" * 64, "0" * 64, "A" * 64])
def test_explicit_trusted_pin_is_required_and_must_match(store, expected):
    path = store.capture_response(URL, b"exact", 200)
    assert audit.inspect_receipt(store.root, path, expected)["status"] == "REFUSED"


def test_exact_selection_keeps_old_versions_and_never_selects_by_outcome(store):
    first = store.capture_response(URL, b"first", 200)
    second = store.capture_response(URL, b"second", 200)
    failed = store.capture_response(URL, b"last failed", 503)
    assert audit.select_capture(store.root, first, pin(store, first)).body == b"first"
    assert audit.select_capture(store.root, second, pin(store, second)).body == b"second"
    assert audit.inspect_receipt(store.root, failed, pin(store, failed))["status"] == "REFUSED"
    assert audit.inspect_receipt(store.root, first, pin(store, second))["status"] == "REFUSED"


@pytest.mark.parametrize("path", [URL, "latest", "receipt.json", "/observations/job/obs/receipt.json",
                                 "observations/../obs/receipt.json", "observations/job/obs/receipt.json",
                                 "observations//obs/receipt.json"])
def test_selection_refuses_noncanonical_and_implicit_paths(store, path):
    store.capture_response(URL, b"exact", 200)
    assert audit.inspect_receipt(store.root, path, "0" * 64, forensic=True)["status"] == "REFUSED"


@pytest.mark.parametrize("where", ["root", "ancestor", "lock", "observations", "job", "observation",
                                  "receipt", "pending", "objects", "shard", "body"])
def test_symlinks_refused_without_following_or_changing_targets(store, tmp_path, where):
    path = store.capture_response(URL, b"exact", 200)
    reference = json.loads((store.root / path).read_bytes())["body"]["path"]
    locations = {"root": store.root, "lock": store.root / ".writer.lock",
                 "observations": store.root / "observations",
                 "job": (store.root / path).parents[1], "observation": (store.root / path).parent,
                 "receipt": store.root / path, "pending": (store.root / path).with_name("pending.json"),
                 "objects": store.root / "objects", "shard": (store.root / reference).parent,
                 "body": store.root / reference}
    if where == "ancestor":
        parent = tmp_path / "link-parent"
        parent.symlink_to(tmp_path, target_is_directory=True)
        candidate = parent / "v2"
        target = tmp_path
    else:
        location = locations[where]
        target = location.with_name(location.name + "-target")
        location.rename(target)
        location.symlink_to(target, target_is_directory=target.is_dir())
        candidate = store.root
    before = snapshot(target) if target.is_dir() else target.read_bytes()
    if target.is_file():
        os.utime(target, ns=(1_000_000_000, target.stat().st_mtime_ns))
        target_atime = target.stat().st_atime_ns
    result = audit.inventory(candidate)
    assert result["status"] == "REFUSED" or result["namespace_issues"]
    assert not any(r["verification"] == "VERIFIED" for r in result["observations"])
    assert audit.inspect_receipt(candidate, path, "0" * 64, forensic=True)["status"] == "REFUSED"
    if target.is_file():
        assert target.stat().st_atime_ns == target_atime
    assert (snapshot(target) if target.is_dir() else target.read_bytes()) == before


@pytest.mark.parametrize("kind", ["missing", "empty", "empty_observations", "file", "parent_traversal", "null_byte"])
def test_absent_empty_and_unsafe_roots_never_created(tmp_path, kind, monkeypatch):
    root = tmp_path / "namespace"
    if kind in {"empty", "empty_observations"}:
        root.mkdir()
    if kind == "empty_observations":
        (root / "observations").mkdir()
    if kind == "file":
        root.write_bytes(b"not a directory")
    candidate = root if kind != "parent_traversal" else tmp_path / ".." / "unsafe"
    if kind == "null_byte":
        candidate = tmp_path / "unsafe\0name"
    with no_writes(monkeypatch):
        result = audit.inventory(candidate)
    assert result["observation_count"] == 0
    assert result["status"] == ("REFUSED" if kind in {"file", "parent_traversal", "null_byte"} else "NO_OBSERVATIONS")
    assert not (root / ".writer.lock").exists()
    assert root.exists() is (kind in {"empty", "empty_observations", "file"})


def test_noncanonical_reservation_and_nondirectory_entry_not_omitted(store):
    store.capture_response(URL, b"exact", 200)
    job = store.root / "observations" / store.job_run_id
    (job / "noncanonical").mkdir()
    (job / str(uuid.uuid4())).write_bytes(b"not a directory")
    (store.root / "observations" / "bad-parent").write_bytes(b"not a job")
    result = audit.inventory(store.root)
    assert result["observation_count"] == 3
    assert result["counts"]["INVALID_IDENTITY"] == 1
    assert result["counts"]["UNSAFE_PATH"] == 1
    assert any(i["path"] == "observations/bad-parent" for i in result["namespace_issues"])


def test_missing_writer_lock_reports_uncoordinated_and_refuses_selection(store):
    path = store.capture_response(URL, b"exact", 200)
    expected = pin(store, path)
    (store.root / ".writer.lock").unlink()
    assert audit.inventory(store.root)["status"] == "UNSTABLE_INVENTORY"
    assert audit.inspect_receipt(store.root, path, expected, forensic=True)["reason"] == "UNSTABLE_INVENTORY"
    assert not (store.root / ".writer.lock").exists()


@pytest.mark.parametrize("where", ["lock", "receipt", "pending", "body"])
def test_nonregular_references_are_refused_without_blocking(store, where):
    path = store.capture_response(URL, b"exact", 200)
    expected = pin(store, path)
    body = json.loads((store.root / path).read_bytes())["body"]["path"]
    target = {"lock": store.root / ".writer.lock", "receipt": store.root / path,
              "pending": (store.root / path).with_name("pending.json"), "body": store.root / body}[where]
    target.unlink()
    os.mkfifo(target)
    result = audit.inventory(store.root)
    assert result["status"] == "REFUSED" or result["namespace_issues"]
    assert audit.inspect_receipt(store.root, path, expected, forensic=True)["status"] == "REFUSED"


def test_unrecognized_entries_and_empty_invalid_job_are_explicit(store):
    path = store.capture_response(URL, b"exact", 200)
    (store.root / "observations" / "invalid-empty-job").mkdir()
    (store.root / path).with_name("extra.json").write_bytes(b"{}")
    (store.root / "unexpected").write_bytes(b"unrecognized")
    result = audit.inventory(store.root)
    assert result["status"] == "INVENTORIED_WITH_ISSUES"
    assert {i["reason"] for i in result["namespace_issues"]} >= {
        "INVALID_JOB_IDENTITY", "UNRECOGNIZED_OBSERVATION_ENTRY", "UNRECOGNIZED_NAMESPACE_ENTRY"}


def test_unreadable_directory_scan_refuses_complete_inventory(store, monkeypatch):
    store.capture_response(URL, b"exact", 200)
    original = audit._snapshot

    def unreadable(fd):
        nodes, errors = original(fd)
        errors.append({"path": "observations", "reason": "DIRECTORY_UNREADABLE"})
        return nodes, errors

    monkeypatch.setattr(audit, "_snapshot", unreadable)
    assert audit.inventory(store.root)["status"] == "UNSTABLE_INVENTORY"


def test_deterministic_cli_stdout_exact_selection_and_no_source_writes(store, monkeypatch, capsys):
    path = store.capture_response(URL, b"<html>" + SECRET.encode() + b"</html>", 200)
    store.capture_exception(URL, RuntimeError(SECRET))
    expected = pin(store, path)
    before = snapshot(store.root)
    command = cli()
    with no_writes(monkeypatch):
        one = audit.inventory(store.root)
        two = audit.inventory(store.root)
        assert one == two
        assert command.main(["inventory", "--root", str(store.root)]) == 0
        first = capsys.readouterr()
        assert command.main(["inventory", "--root", str(store.root)]) == 0
        second = capsys.readouterr()
        assert first == second
        assert json.loads(first.out) == one
        assert command.main(["inspect", "--root", str(store.root), "--receipt", path,
                             "--receipt-sha256", expected]) == 0
        inspection = capsys.readouterr()
        assert json.loads(inspection.out)["status"] == "VERIFIED_CAPTURE_BYTES"
        assert SECRET not in first.out + inspection.out
        assert first.err == inspection.err == ""
    assert snapshot(store.root) == before


def test_cli_requires_pin_and_has_no_output_file_option(store, capsys):
    command = cli()
    for args in (["inspect", "--root", str(store.root), "--receipt", "latest"],
                 ["inventory", "--root", str(store.root), "--output", str(store.root / "report.json")]):
        with pytest.raises(SystemExit) as error:
            command.main(args)
        assert error.value.code == 2
        capsys.readouterr()
    assert not store.root.exists()


def _paused_writer(root, ready, release):
    original = capture._publish

    def paused(parent, name, content):
        original(parent, name, content)
        if name == "pending.json":
            ready.send(True)
            assert release.recv() is True

    capture._publish = paused
    capture.CaptureStoreV2(Path(root)).capture_response(URL, b"concurrently published", 200)


def test_real_concurrent_publication_is_unstable_until_writer_completes(store):
    path = store.capture_response(URL, b"old", 200)
    expected = pin(store, path)
    context = multiprocessing.get_context("fork")
    ready_read, ready_write = context.Pipe(duplex=False)
    release_read, release_write = context.Pipe(duplex=False)
    writer = context.Process(target=_paused_writer, args=(str(store.root), ready_write, release_read))
    writer.start()
    try:
        assert ready_read.poll(10) and ready_read.recv() is True
        result = audit.inventory(store.root)
        assert result["status"] == "UNSTABLE_INVENTORY"
        assert result["consistency"] == "WRITER_ACTIVE"
        assert result["observation_count"] == 2
        assert result["counts"]["INCOMPLETE"] == 1
        assert audit.inspect_receipt(store.root, path, expected)["reason"] == "UNSTABLE_INVENTORY"
        release_write.send(True)
        writer.join(10)
        assert writer.exitcode == 0
        final = audit.inventory(store.root)
        assert final["status"] == "INVENTORIED"
        assert final["counts"]["VERIFIED_SUCCEEDED"] == 2
    finally:
        if writer.is_alive():
            writer.terminate()
            writer.join(5)
        for pipe in (ready_read, ready_write, release_read, release_write):
            pipe.close()


@pytest.mark.parametrize("mutation", ["new_reservation", "receipt", "lock_replaced", "root_replaced"])
def test_detectable_noncooperating_change_invalidates_inventory_and_selection(store, monkeypatch, mutation):
    path = store.capture_response(URL, b"exact", 200)
    expected = pin(store, path)
    original = capture.CaptureStoreV2.resolve_receipt
    changed = False

    def changing(self, *args, **kwargs):
        nonlocal changed
        result = original(self, *args, **kwargs)
        if not changed:
            changed = True
            if mutation == "new_reservation":
                (store.root / "observations" / store.job_run_id / str(uuid.uuid4())).mkdir()
            elif mutation == "receipt":
                mutate(store.root / path, b"tampered")
            elif mutation == "lock_replaced":
                lock = store.root / ".writer.lock"
                lock.unlink()
                lock.write_bytes(b"")
            else:
                store.root.rename(store.root.with_name(store.root.name + "-detached"))
                store.root.mkdir()
        return result

    monkeypatch.setattr(capture.CaptureStoreV2, "resolve_receipt", changing)
    # Both inventory and selection must fail the post-read check. Restore a
    # clean separate fixture between the operations, never repair a real root.
    result = audit.inventory(store.root)
    assert result["status"] == "UNSTABLE_INVENTORY"
    second = capture.CaptureStoreV2(store.root.with_name("second"))
    second_path = second.capture_response(URL, b"exact", 200)
    second_pin = pin(second, second_path)
    store = second
    path = second_path
    changed = False
    assert audit.inspect_receipt(store.root, path, second_pin)["reason"] == "UNSTABLE_INVENTORY"
    assert expected


def test_root_creation_between_absence_probes_reports_instability(tmp_path, monkeypatch):
    root = tmp_path / "v2"
    original = audit._directory
    calls = 0

    @contextmanager
    def appearing(path, *, create):
        nonlocal calls
        calls += 1
        if calls == 2:
            root.mkdir()
        with original(path, create=create) as fd:
            yield fd

    monkeypatch.setattr(audit, "_directory", appearing)
    assert audit.inventory(root)["status"] == "UNSTABLE_INVENTORY"
    assert not (root / ".writer.lock").exists()


def test_guards_really_refuse_network_and_database():
    with pytest.raises(AssertionError):
        socket.create_connection(("synthetic.invalid", 443))
    with pytest.raises(AssertionError):
        socket.getaddrinfo("synthetic.invalid", 443)
    with pytest.raises(AssertionError):
        __import__("warehouse.db")
