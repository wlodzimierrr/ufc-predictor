"""Synthetic-only capture verification; no crawler or warehouse connection."""

import hashlib
import importlib.abc
import json
import multiprocessing
import os
from pathlib import Path
import socket
import sys
from types import SimpleNamespace
import uuid

import pytest
from scrapy.http import HtmlResponse, Request
from scrapy.core.downloader.middleware import DownloaderMiddlewareManager
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.settings import Settings
from scrapy.utils.misc import load_object

from ufc_scraper import raw_capture_v2 as capture
from ufc_scraper import settings


URL = "http://www.ufcstats.com/fight-details/synthetic"
COMPLETED = "http://www.ufcstats.com/statistics/events/completed?page=all"
UPCOMING = "http://www.ufcstats.com/statistics/events/upcoming"
CHALLENGE = b'<p>Checking your browser</p><script src="/__c"></script>'
SECRET = "SYNTHETIC-secret-never-export"


def forbidden(*args, **kwargs):
    raise AssertionError("Network/real warehouse access forbidden in capture tests")


class NoWarehouse(importlib.abc.MetaPathFinder):
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
    guard = NoWarehouse()
    sys.meta_path.insert(0, guard)
    yield
    sys.meta_path.remove(guard)


@pytest.fixture
def store(tmp_path):
    return capture.CaptureStoreV2(tmp_path / "capture-v2")


def digest(body):
    return hashlib.sha256(body).hexdigest()


def receipts(store):
    return sorted(store.root.glob("observations/*/*/receipt.json"))


def metadata(store, relative):
    return json.loads((store.root / relative).read_bytes())


def mutate(path, content):
    path.chmod(0o600)
    path.write_bytes(content)
    path.chmod(0o444)


def rewrite_receipt(store, relative, change, *, pending_too=False):
    path = store.root / relative
    value = json.loads(path.read_bytes())
    change(value)
    mutate(path, capture._json_bytes(value))
    if pending_too:
        pending = {k: v for k, v in value.items() if k != "receipt_prepared_at"}
        pending["publication_state"] = "INCOMPLETE"
        mutate(path.with_name("pending.json"), capture._json_bytes(pending))


def test_repeated_identical_observations_share_bytes_but_preserve_provenance(store):
    before = capture._utc_now()
    body = b"\x00\xff\r\nexact response bytes"
    first = store.capture_response(URL, body, 200)
    second = store.capture_response(URL, body, 200)
    after = capture._utc_now()
    resolved = [store.resolve_receipt(path) for path in (first, second)]
    assert first != second
    assert len(list(store.root.glob("objects/sha256/*/*.body"))) == 1
    assert resolved[0].receipt["body"] == resolved[1].receipt["body"]
    for item in resolved:
        value = item.receipt
        assert item.body == body
        assert value["body"]["bytes"] == len(body)
        assert value["body"]["sha256"] == digest(body)
        assert value["source_url"] == value["request_url"] == URL
        assert value["job_run_id"] == store.job_run_id
        assert before <= value["observed_at"] <= value["receipt_prepared_at"] <= after
        assert value["request_started_at"] is None
        assert value["observed_at"].endswith("+00:00")
        assert value["disposition"] == "SUCCEEDED"
        assert value["schema"] == capture.SCHEMA
        assert (store.root / value["body"]["path"]).stat().st_mode & 0o222 == 0
    assert len(receipts(store)) == 2


def test_changed_bodies_keep_both_versions_and_distinct_jobs(store):
    first = store.capture_response(URL, b"first version", 200)
    old_bytes = (store.root / first).read_bytes()
    other_job = capture.CaptureStoreV2(store.root)
    second = other_job.capture_response(URL, b"changed version", 200)
    assert store.resolve_receipt(first).body == b"first version"
    assert store.resolve_receipt(second).body == b"changed version"
    assert (store.root / first).read_bytes() == old_bytes
    assert metadata(store, first)["job_run_id"] != metadata(store, second)["job_run_id"]
    assert len(list(store.root.glob("objects/sha256/*/*.body"))) == 2


def test_same_day_completed_and_upcoming_urls_never_collide(store):
    first = store.capture_response(COMPLETED, b"completed", 200)
    second = store.capture_response(UPCOMING, b"upcoming", 200)
    a, b = store.resolve_receipt(first), store.resolve_receipt(second)
    assert a.receipt["observed_at"][:10] == b.receipt["observed_at"][:10]
    assert a.receipt["source_url"] == COMPLETED
    assert b.receipt["source_url"] == UPCOMING
    assert [r.receipt["listing_kind"] for r in (a, b)] == ["completed", "upcoming"]
    assert (a.body, b.body) == (b"completed", b"upcoming")


@pytest.mark.parametrize("status,body,reasons", [
    (404, b"missing exact bytes", ["http_error"]),
    (503, b"service unavailable", ["http_error"]),
    (200, CHALLENGE, ["access_challenge"]),
    (403, CHALLENGE, ["http_error", "access_challenge"]),
    (301, b"unfollowed redirect", ["non_success_http_status"]),
])
def test_failed_response_bodies_preserved_and_refused_as_success(store, status, body, reasons):
    path = store.capture_response(URL, body, status)
    result = store.resolve_receipt(path, require_success=False)
    assert result.body == body
    assert result.receipt["body"]["sha256"] == digest(body)
    assert result.receipt["http_status"] == status
    assert result.receipt["response_present"] is True
    assert result.receipt["disposition"] == "FAILED"
    assert result.receipt["failure_reasons"] == reasons
    with pytest.raises(capture.CaptureError, match="FAILED"):
        store.resolve_receipt(path)


def test_empty_http_body_is_distinct_from_absent_exception_body(store):
    empty = store.capture_response(URL, b"", 200)
    empty_error = store.capture_response(URL, b"", 500)
    absent = store.capture_exception(URL, TimeoutError(SECRET))
    for path in (empty, empty_error):
        result = store.resolve_receipt(path, require_success=False)
        assert result.body == b""
        assert result.receipt["body"]["bytes"] == 0
        assert result.receipt["body"]["sha256"] == digest(b"")
        assert result.receipt["response_present"] is True
    result = store.resolve_receipt(absent, require_success=False)
    assert result.body is None
    assert result.receipt["body"] is None
    assert result.receipt["http_status"] is None
    assert result.receipt["response_present"] is False
    assert result.receipt["exception_category"] == "timeout"
    assert result.receipt["failure_reasons"] == ["request_exception"]
    assert len(list(store.root.glob("objects/sha256/*/*.body"))) == 1


@pytest.mark.parametrize("exception,category", [
    (TimeoutError(SECRET), "timeout"), (ConnectionError(SECRET), "connection"),
    (RuntimeError(SECRET), "other"),
    (type(SECRET, (Exception,), {})(SECRET), "other"),
])
def test_secret_safe_exception_metadata(store, exception, category):
    path = store.capture_exception(URL, exception)
    value = store.resolve_receipt(path, require_success=False).receipt
    assert value["exception_category"] == category
    for file in store.root.rglob("*"):
        if file.is_file():
            assert SECRET.encode() not in file.read_bytes()


def test_exception_str_and_repr_are_never_called(store):
    class Unprintable(Exception):
        __str__ = forbidden
        __repr__ = forbidden

    path = store.capture_exception(URL, Unprintable(SECRET))
    assert store.resolve_receipt(path, require_success=False).receipt["exception_category"] == "other"


@pytest.mark.parametrize("url", [
    "http://user:SYNTHETIC-secret-never-export@ufcstats.com/fight-details/1",
    URL + "?token=" + SECRET, URL + "#" + SECRET, URL + "?authorization=" + SECRET,
    "https://ufcstats.com.evil.invalid/fight-details/1", URL + "\n" + SECRET,
])
def test_unsafe_or_credential_urls_are_refused_without_export(store, url):
    with pytest.raises(capture.CaptureError) as caught:
        store.capture_exception(url, RuntimeError(SECRET))
    assert SECRET not in str(caught.value)
    assert not store.root.exists()


def _worker(root, job, observation, body, connection):
    try:
        store = capture.CaptureStoreV2(Path(root), job_run_id=job)
        path = store.capture_response(URL, body, 200, observation_id=observation)
        connection.send(("ok", path))
    except capture.DuplicateObservationError:
        connection.send(("duplicate", None))
    except Exception:
        connection.send(("error", None))
    finally:
        connection.close()


def concurrent_writes(root, job, observations, bodies):
    # fork inherits process-wide offline guards; OS pipes carry only outcomes.
    ctx = multiprocessing.get_context("fork")
    children, readers = [], []
    try:
        for observation, body in zip(observations, bodies):
            reader, writer = ctx.Pipe(duplex=False)
            process = ctx.Process(target=_worker, args=(str(root), job, observation, body, writer))
            process.start()
            writer.close()
            children.append(process)
            readers.append(reader)
        results = []
        for reader in readers:
            assert reader.poll(10), "Writer did not complete"
            results.append(reader.recv())
        for process in children:
            process.join(10)
            assert process.exitcode == 0
        return results
    finally:
        for reader in readers:
            reader.close()
        for process in children:
            if process.is_alive():
                process.terminate()
                process.join(10)


def test_concurrent_process_writers_share_object_and_keep_every_receipt(store):
    results = concurrent_writes(store.root, store.job_run_id,
                                [str(uuid.uuid4()) for _ in range(8)], [b"same bytes"] * 8)
    assert [status for status, _ in results] == ["ok"] * 8
    assert len(receipts(store)) == 8
    assert len(list(store.root.glob("objects/sha256/*/*.body"))) == 1
    for _, path in results:
        assert store.resolve_receipt(path).body == b"same bytes"


def test_concurrent_repeated_identity_accepts_exactly_one_observation(store):
    oid = str(uuid.uuid4())
    results = concurrent_writes(store.root, store.job_run_id, [oid] * 6,
                                [f"version {i}".encode() for i in range(6)])
    assert sorted(status for status, _ in results) == ["duplicate"] * 5 + ["ok"]
    assert len(receipts(store)) == 1
    path = next(path for status, path in results if status == "ok")
    assert store.resolve_receipt(path).body.startswith(b"version ")


def test_repeated_identity_never_overwrites_a_receipt(store):
    oid = str(uuid.uuid4())
    path = store.capture_response(URL, b"original", 200, observation_id=oid)
    before = (store.root / path).read_bytes()
    with pytest.raises(capture.DuplicateObservationError):
        store.capture_response(URL, b"replacement", 200, observation_id=oid)
    assert (store.root / path).read_bytes() == before
    assert store.resolve_receipt(path).body == b"original"
    assert len(list(store.root.glob("objects/sha256/*/*.body"))) == 1


@pytest.mark.parametrize("target", ["pending.json", ".body", "receipt.json"])
def test_interrupted_atomic_publication_leaves_detectable_state(store, monkeypatch, target):
    real_link = capture.os.link
    oid = str(uuid.uuid4())

    def interrupted(src, dst, **kwargs):
        if dst.endswith(target):
            raise OSError("Synthetic interruption before exclusive link")
        return real_link(src, dst, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(capture.os, "link", interrupted)
        with pytest.raises(capture.CaptureIntegrityError, match="publication"):
            store.capture_response(URL, b"preserve interrupted bytes", 200, observation_id=oid)
    directory = store.root / "observations" / store.job_run_id / oid
    assert directory.is_dir()
    assert not (directory / "receipt.json").exists()
    assert list(store.root.rglob("*.partial"))
    with pytest.raises(capture.DuplicateObservationError):
        store.capture_response(URL, b"retry", 200, observation_id=oid)
    relative = f"observations/{store.job_run_id}/{oid}/receipt.json"
    with pytest.raises(capture.CaptureIntegrityError):
        store.resolve_receipt(relative)
    if target == "receipt.json":
        pending = json.loads((directory / "pending.json").read_bytes())
        assert (store.root / pending["body"]["path"]).read_bytes() == b"preserve interrupted bytes"


def test_failure_after_receipt_link_is_detected_by_leftover_stage(store, monkeypatch):
    real_unlink = capture.os.unlink

    def interrupted(name, **kwargs):
        if name.startswith(".receipt.json."):
            raise OSError("Synthetic interruption after link")
        return real_unlink(name, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(capture.os, "unlink", interrupted)
        with pytest.raises(capture.CaptureIntegrityError):
            store.capture_response(URL, b"body durable before receipt", 200)
    assert len(receipts(store)) == 1
    relative = str(receipts(store)[0].relative_to(store.root))
    with pytest.raises(capture.CaptureIntegrityError, match="unfinished"):
        store.resolve_receipt(relative)


def test_body_fsync_failure_never_publishes_a_completed_receipt(store, monkeypatch):
    real_fsync = capture.os.fsync

    def interrupted(fd):
        name = os.readlink(f"/proc/self/fd/{fd}")
        if ".body." in name and name.endswith(".partial"):
            raise OSError("Synthetic fsync failure")
        return real_fsync(fd)

    monkeypatch.setattr(capture.os, "fsync", interrupted)
    with pytest.raises(capture.CaptureIntegrityError):
        store.capture_response(URL, b"not durable", 200)
    assert not receipts(store)
    assert list(store.root.glob("observations/*/*/pending.json"))
    assert list(store.root.rglob("*.partial"))


def test_receipt_directory_fsync_failure_keeps_visible_incomplete_marker(store, monkeypatch):
    real_fsync = capture.os.fsync

    def interrupted(fd):
        path = Path(os.readlink(f"/proc/self/fd/{fd}"))
        if path.is_dir() and (path / "receipt.json").exists():
            raise OSError("Synthetic receipt directory fsync failure")
        return real_fsync(fd)

    with monkeypatch.context() as patch:
        patch.setattr(capture.os, "fsync", interrupted)
        with pytest.raises(capture.CaptureIntegrityError):
            store.capture_response(URL, b"durable body", 200)
    path = receipts(store)[0]
    assert list(path.parent.glob("*.partial"))
    with pytest.raises(capture.CaptureIntegrityError, match="unfinished"):
        store.resolve_receipt(str(path.relative_to(store.root)))


def test_publication_verifies_body_before_receipt(store, monkeypatch):
    real_publish = capture._publish
    order = []

    def checked(parent, name, body):
        order.append(name)
        if name == "receipt.json":
            value = json.loads(body)
            assert (store.root / value["body"]["path"]).read_bytes() == b"verified first"
            assert digest((store.root / value["body"]["path"]).read_bytes()) == value["body"]["sha256"]
        real_publish(parent, name, body)

    monkeypatch.setattr(capture, "_publish", checked)
    path = store.capture_response(URL, b"verified first", 200)
    assert order[0] == "pending.json" and order[-1] == "receipt.json"
    assert store.resolve_receipt(path).body == b"verified first"


@pytest.mark.parametrize("tampered", [b"same size!", b"different length"])
def test_tampered_readonly_object_fails_resolution_and_reuse_without_replacement(store, tampered):
    path = store.capture_response(URL, b"original!", 200)
    body_path = store.root / metadata(store, path)["body"]["path"]
    mutate(body_path, tampered)  # Authorized writers can bypass read-only modes.
    with pytest.raises(capture.CaptureIntegrityError, match="hash/length"):
        store.resolve_receipt(path)
    with pytest.raises(capture.CaptureIntegrityError, match="Existing"):
        store.capture_response(URL, b"original!", 200)
    assert body_path.read_bytes() == tampered
    assert len(receipts(store)) == 1
    assert len(list(store.root.glob("observations/*/*/pending.json"))) == 2


def test_trusted_receipt_pin_and_reservation_detect_metadata_tampering(store):
    path = store.capture_response(URL, b"body", 200)
    pin = digest((store.root / path).read_bytes())
    assert store.resolve_receipt(path, expected_receipt_sha256=pin).body == b"body"
    rewrite_receipt(store, path, lambda r: r.update(source_url=URL + "-changed"))
    with pytest.raises(capture.CaptureIntegrityError, match="pin mismatch"):
        store.resolve_receipt(path, expected_receipt_sha256=pin)
    with pytest.raises(capture.CaptureIntegrityError, match="reservation"):
        store.resolve_receipt(path)


@pytest.mark.parametrize("change", [
    lambda r: r["body"].update(path="../../outside.body"),
    lambda r: r["body"].update(sha256="0" * 64),
    lambda r: r["body"].update(bytes=999),
    lambda r: r.update(schema="unknown-v3"),
    lambda r: r.update(job_run_id=str(uuid.uuid4())),
    lambda r: r.update(observed_at="2000-01-01T00:00:00"),
    lambda r: r.update(observed_at="2000-01-01T00:00:00+00:00"),
    lambda r: r.update(http_status=500),
    lambda r: r.update(authorization=SECRET),
])
def test_resolver_checks_schema_identities_clocks_hashes_and_disposition(store, change):
    path = store.capture_response(URL, b"body", 200)
    rewrite_receipt(store, path, change, pending_too=True)
    with pytest.raises(capture.CaptureIntegrityError):
        store.resolve_receipt(path)


def test_challenge_cannot_be_relabelled_success_by_metadata_only(store):
    path = store.capture_response(URL, CHALLENGE, 200)
    rewrite_receipt(store, path, lambda r: r.update(access_challenge=False, disposition="SUCCEEDED",
                                                  failure_reasons=[]), pending_too=True)
    with pytest.raises(capture.CaptureIntegrityError, match="challenge"):
        store.resolve_receipt(path)


@pytest.mark.parametrize("relative", ["../receipt.json", "/observations/a/b/receipt.json",
                                       "observations/../x/receipt.json", "observations/a/b/receipt.json",
                                       "observations\\a\\b\\receipt.json"])
def test_unsafe_receipt_paths_are_rejected(store, relative):
    with pytest.raises(capture.CaptureError):
        store.resolve_receipt(relative)
    assert not store.root.exists()


@pytest.mark.parametrize("component", ["root", "ancestor", "observations", "job", "object-directory",
                                        "object-file", "receipt-file", "pending-file", "lock"])
def test_symlink_paths_are_refused_without_touching_targets(store, tmp_path, component):
    path = store.capture_response(URL, b"body", 200)
    value = metadata(store, path)
    target = tmp_path / "untouched-target"
    target.write_bytes(b"do not change")
    if component in {"object-file", "receipt-file", "pending-file", "lock"}:
        candidate = {"object-file": store.root / value["body"]["path"],
                     "receipt-file": store.root / path,
                     "pending-file": (store.root / path).with_name("pending.json"),
                     "lock": store.root / ".writer.lock"}[component]
        candidate.unlink()
        candidate.symlink_to(target)
    else:
        candidate = {"root": store.root, "ancestor": store.root.parent,
                     "observations": store.root / "observations",
                     "job": store.root / "observations" / store.job_run_id,
                     "object-directory": store.root / "objects"}[component]
        original = candidate.with_name(candidate.name + "-original")
        candidate.rename(original)
        candidate.symlink_to(original, target_is_directory=True)
    if component != "lock":
        with pytest.raises(capture.CaptureIntegrityError):
            store.resolve_receipt(path)
    if component in {"receipt-file", "pending-file"}:
        # A new exclusive observation does not traverse another observation's
        # damaged files. Resolution of that older observation still refuses it.
        new_path = store.capture_response(URL, b"body", 200)
        assert store.resolve_receipt(new_path).body == b"body"
    else:
        with pytest.raises(capture.CaptureIntegrityError):
            store.capture_response(URL, b"body", 200)
    if component == "ancestor":
        # Undo the synthetic ancestor symlink before pytest's temp cleanup.
        candidate.unlink()
        original.rename(candidate)
    assert target.read_bytes() == b"do not change"


def test_existing_receipt_cannot_be_replaced_by_exclusive_publication(store):
    path = store.capture_response(URL, b"body", 200)
    file = store.root / path
    before = file.read_bytes()
    fd = os.open(file.parent, capture._DIR_FLAGS)
    try:
        with pytest.raises(FileExistsError):
            capture._publish(fd, "receipt.json", b"replacement")
    finally:
        os.close(fd)
    assert file.read_bytes() == before


def _crash_before_receipt(root, job, oid):
    real_publish = capture._publish

    def interrupted(parent, name, body):
        if name == "receipt.json":
            os._exit(73)
        real_publish(parent, name, body)

    capture._publish = interrupted
    capture.CaptureStoreV2(Path(root), job_run_id=job).capture_response(
        URL, b"durable body before process exit", 200, observation_id=oid)


def test_process_exit_preserves_incomplete_reservation_and_releases_lock(store):
    oid = str(uuid.uuid4())
    process = multiprocessing.get_context("fork").Process(
        target=_crash_before_receipt, args=(str(store.root), store.job_run_id, oid))
    process.start()
    process.join(10)
    assert process.exitcode == 73
    relative = f"observations/{store.job_run_id}/{oid}/receipt.json"
    pending = json.loads((store.root / relative).with_name("pending.json").read_bytes())
    assert pending["publication_state"] == "INCOMPLETE"
    assert (store.root / pending["body"]["path"]).read_bytes() == b"durable body before process exit"
    with pytest.raises(capture.CaptureIntegrityError):
        store.resolve_receipt(relative)
    with pytest.raises(capture.DuplicateObservationError):
        store.capture_response(URL, b"retry", 200, observation_id=oid)
    # A separately identified later observation can reuse verified durable
    # bytes; it does not complete, erase or claim the crashed observation.
    next_path = store.capture_response(URL, b"durable body before process exit", 200)
    assert store.resolve_receipt(next_path).body == b"durable body before process exit"
    assert not (store.root / relative).exists()


def test_missing_object_is_an_explicit_resolution_failure(store):
    path = store.capture_response(URL, b"body", 200)
    (store.root / metadata(store, path)["body"]["path"]).unlink()
    with pytest.raises(capture.CaptureIntegrityError, match="Missing"):
        store.resolve_receipt(path)


def test_clock_rollback_refuses_receipt_without_inventing_replacement_clock(store, monkeypatch):
    instants = iter(["2099-01-01T00:00:01+00:00", "2099-01-01T00:00:00+00:00"])
    monkeypatch.setattr(capture, "_utc_now", lambda: next(instants))
    with pytest.raises(capture.CaptureIntegrityError, match="clock"):
        store.capture_response(URL, b"clock rollback", 200)
    assert not receipts(store)
    assert len(list(store.root.glob("observations/*/*/pending.json"))) == 1


def test_invalid_observation_id_and_traversal_root_are_refused(store, tmp_path):
    with pytest.raises(capture.CaptureError):
        store.capture_response(URL, b"body", 200, observation_id="../escape")
    assert not store.root.exists()
    unsafe = capture.CaptureStoreV2(tmp_path / ".." / "escape")
    with pytest.raises(capture.CaptureError, match="Unsafe"):
        unsafe.capture_response(URL, b"body", 200)


def test_middleware_retains_request_urls_cache_flag_and_safe_headers(tmp_path):
    middleware = capture.ImmutableRawCaptureMiddlewareV2(tmp_path)
    request = Request(URL, cookies={"session": SECRET}, headers={"Authorization": SECRET, "Cookie": SECRET})
    before = dict(request.headers)
    assert middleware.process_request(request, None) is None
    response = HtmlResponse(URL + "-redirected", request=request, body=b"response", flags=["cached"])
    assert middleware.process_response(request, response, None) is response
    value = json.loads(receipts(middleware.store)[0].read_bytes())
    assert value["request_url"] == URL
    assert value["source_url"] == response.url
    assert value["from_cache"] is True
    assert value["request_started_at"] is not None
    assert dict(request.headers) == before
    assert request.cookies == {"session": SECRET}
    assert SECRET.encode() not in receipts(middleware.store)[0].read_bytes()
    assert SECRET.encode() not in receipts(middleware.store)[0].with_name("pending.json").read_bytes()


def test_middleware_challenge_retains_original_status_and_legacy_return_behavior(tmp_path):
    middleware = capture.ImmutableRawCaptureMiddlewareV2(tmp_path)
    request = Request(URL)
    response = HtmlResponse(URL, request=request, body=CHALLENGE, status=200)
    returned = middleware.process_response(request, response, None)
    assert returned.status == 503 and returned.body == CHALLENGE
    value = json.loads(receipts(middleware.store)[0].read_bytes())
    assert value["http_status"] == 200 and value["disposition"] == "FAILED"


def test_discovery_response_passes_through_without_capture(tmp_path):
    middleware = capture.ImmutableRawCaptureMiddlewareV2(tmp_path)
    request = Request("http://ufcstats.com/statistics/fighters?char=a&page=all")
    response = HtmlResponse(request.url, request=request, body=b"listing")
    assert middleware.process_response(request, response, None) is response
    assert not middleware.store.root.exists()


def test_middleware_exception_without_response_is_captured_secret_safely(tmp_path):
    middleware = capture.ImmutableRawCaptureMiddlewareV2(tmp_path)
    assert middleware.process_exception(Request(URL), RuntimeError(SECRET), None) is None
    assert SECRET.encode() not in receipts(middleware.store)[0].read_bytes()
    assert json.loads(receipts(middleware.store)[0].read_bytes())["body"] is None


@pytest.mark.parametrize("final", [False, True])
def test_real_middleware_chain_exposes_only_unretried_response(tmp_path, monkeypatch, final):
    middleware = capture.ImmutableRawCaptureMiddlewareV2(tmp_path)
    retry = RetryMiddleware(Settings())
    request = Request(URL)
    retry_request = request.copy()
    monkeypatch.setattr(retry, "_retry", lambda *args: None if final else retry_request)
    manager = DownloaderMiddlewareManager(middleware, retry)
    response = HtmlResponse(URL, request=request, body=b"synthetic 503", status=503)
    result = []
    # In-memory callback only: no engine, handler, scheduler or request occurs.
    manager.download(lambda request, spider: response, request, None).addCallback(result.append)
    assert result == [response if final else retry_request]
    assert len(receipts(middleware.store)) == (1 if final else 0)
    if final:
        value = json.loads(receipts(middleware.store)[0].read_bytes())
        assert value["http_status"] == 503 and value["disposition"] == "FAILED"


def test_challenge_synthetic_503_does_not_reenter_retry_chain(tmp_path, monkeypatch):
    middleware = capture.ImmutableRawCaptureMiddlewareV2(tmp_path)
    retry = RetryMiddleware(Settings())
    monkeypatch.setattr(retry, "_retry", forbidden)
    manager = DownloaderMiddlewareManager(middleware, retry)
    request = Request(URL)
    response = HtmlResponse(URL, request=request, body=CHALLENGE, status=200)
    result = []
    manager.download(lambda request, spider: response, request, None).addCallback(result.append)
    assert len(result) == 1 and result[0].status == 503
    assert len(receipts(middleware.store)) == 1


def test_configured_class_and_factory_are_v2_without_acquisition_or_writes():
    configured = Settings()
    configured.setmodule(settings)
    entries = configured.getwithbase("DOWNLOADER_MIDDLEWARES")
    dotted = "ufc_scraper.raw_capture_v2.ImmutableRawCaptureMiddlewareV2"
    assert entries[dotted] == 200
    assert "ufc_scraper.middlewares.RawCaptureMiddleware" not in entries
    assert entries["scrapy.downloadermiddlewares.retry.RetryMiddleware"] == 550
    assert entries["ufc_scraper.middlewares.BrowserSessionHeaderMiddleware"] == 100
    connected = []
    crawler = SimpleNamespace(signals=SimpleNamespace(connect=lambda *a, **kw: connected.append((a, kw))))
    instance = load_object(dotted).from_crawler(crawler)
    assert isinstance(instance, capture.ImmutableRawCaptureMiddlewareV2)
    assert instance.store.root == Path(capture.__file__).resolve().parents[4] / "data/raw/ufcstats_v2"
    assert len(connected) == 1


def test_v2_does_not_modify_legacy_files(tmp_path):
    legacy = {"raw/ufcstats/fights/existing.html": b"old exact body",
              "raw/ufcstats/event_listing/event_listing_20261003.html": b"old listing",
              "manifests/fetch_manifest.csv": b"old exact manifest\n"}
    for relative, body in legacy.items():
        file = tmp_path / relative
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(body)
    middleware = capture.ImmutableRawCaptureMiddlewareV2(tmp_path)
    for url in (URL, COMPLETED, UPCOMING):
        request = Request(url)
        middleware.process_response(request, HtmlResponse(url, request=request, body=b"new"), None)
    for relative, body in legacy.items():
        assert (tmp_path / relative).read_bytes() == body


def test_offline_guards_are_active():
    with pytest.raises(AssertionError, match="forbidden"):
        socket.create_connection(("synthetic.invalid", 443))
    with pytest.raises(AssertionError, match="forbidden"):
        socket.getaddrinfo("synthetic.invalid", 443)
    with pytest.raises(AssertionError, match="forbidden"):
        __import__("warehouse.db")
