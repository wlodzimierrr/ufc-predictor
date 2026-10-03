"""Read-only v2 inventory and explicitly pinned byte inspection.

Schema/body verification belongs exclusively to CaptureStoreV2. A shared lock
on the *existing*, read-only writer-lock descriptor coordinates local writers;
two no-follow namespace scans detect other visible changes. No lock is created.
See the Phase 5C.7 report for limits, including filesystem access-time policy.
"""

from collections import Counter
from contextlib import contextmanager
import fcntl
import hashlib
import os
from pathlib import Path
import stat

from ufc_scraper.raw_capture_v2 import (
    CaptureError, CaptureIntegrityError, CaptureStoreV2,
    _child_directory, _directory, _HASH, _identity, _read, _READ_FLAGS,
)


VERSION = "ufcstats_v2_observation_inventory_v1"
STATES = ("VERIFIED_SUCCEEDED", "VERIFIED_FAILED", "VERIFIED_CACHED",
          "INCOMPLETE", "INVALID_RECEIPT", "MISSING_OBJECT", "CORRUPT_OBJECT",
          "UNSAFE_PATH", "INVALID_IDENTITY")


class InventoryError(CaptureError):
    """A fixed, secret-safe refusal code, never arbitrary exception content."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


class _AuditStore(CaptureStoreV2):
    @staticmethod
    def _resolve_body(root, reference):
        # This hook only distinguishes failures. It delegates all body checks
        # unchanged, after the resolver has validated receipt and reservation.
        try:
            return CaptureStoreV2._resolve_body(root, reference)
        except FileNotFoundError:
            raise InventoryError("MISSING_OBJECT") from None
        except OSError:
            raise InventoryError("UNSAFE_PATH") from None
        except CaptureIntegrityError:
            raise InventoryError("CORRUPT_OBJECT") from None


def _fingerprint(value):
    return (value.st_dev, value.st_ino, value.st_mode, value.st_size,
            value.st_mtime_ns, value.st_ctime_ns, value.st_nlink)


def _snapshot(root):
    """Stat every namespace entry without following links or reading objects."""
    nodes = {"": _fingerprint(os.fstat(root))}
    errors = []

    def visit(fd, prefix):
        try:
            names = sorted(os.listdir(fd))
        except OSError:
            errors.append({"path": prefix, "reason": "DIRECTORY_UNREADABLE"})
            return
        for name in names:
            path = f"{prefix}/{name}" if prefix else name
            try:
                value = os.stat(name, dir_fd=fd, follow_symlinks=False)
                nodes[path] = _fingerprint(value)
                if stat.S_ISDIR(value.st_mode):
                    if path.count("/") >= 8:
                        errors.append({"path": path, "reason": "UNEXPECTED_DIRECTORY_DEPTH"})
                        continue
                    child = _child_directory(fd, name, create=False)
                    try:
                        if _fingerprint(os.fstat(child)) != nodes[path]:
                            raise OSError
                        visit(child, path)
                    finally:
                        os.close(child)
            except OSError:
                errors.append({"path": path, "reason": "ENTRY_CHANGED_OR_UNREADABLE"})

    visit(root, "")
    return nodes, errors


@contextmanager
def _view(path):
    with _directory(Path(path), create=False) as root:
        lock = None
        coordination = "NO_WRITER_LOCK"
        try:
            try:
                lock = os.open(".writer.lock", _READ_FLAGS, dir_fd=root)
                if not stat.S_ISREG(os.fstat(lock).st_mode):
                    raise InventoryError("UNSAFE_WRITER_LOCK")
                try:
                    fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    coordination = "COOPERATING_WRITERS_LOCKED"
                except BlockingIOError:
                    coordination = "WRITER_ACTIVE"
            except FileNotFoundError:
                pass
            except OSError:
                raise InventoryError("UNSAFE_WRITER_LOCK") from None
            yield root, coordination
        finally:
            if lock is not None:
                os.close(lock)


def _unchanged(path, before, after):
    if before[1] or after[1] or before != after:
        return False
    try:
        with _directory(Path(path), create=False) as root:
            return _fingerprint(os.fstat(root)) == after[0][""]
    except (OSError, CaptureError):
        return False


def _canonical(value):
    try:
        return _identity(value)
    except CaptureError:
        return None


def _read_at(root, path):
    """Read an enumerated path through anchored no-follow descriptors."""
    fd = os.dup(root)
    try:
        parts = path.split("/")
        for part in parts[:-1]:
            child = _child_directory(fd, part, create=False)
            os.close(fd)
            fd = child
        return _read(fd, parts[-1])
    finally:
        os.close(fd)


def _verified(row, resolved):
    receipt = resolved.receipt
    row.update(
        state=("VERIFIED_CACHED" if receipt["from_cache"] else
               "VERIFIED_SUCCEEDED" if receipt["disposition"] == "SUCCEEDED"
               else "VERIFIED_FAILED"),
        publication="COMPLETE_VERIFIED", verification="VERIFIED",
        metadata={k: v for k, v in receipt.items() if k != "body"},
        verified_body=receipt["body"],
        body_availability=("VERIFIED_BYTES" if resolved.body is not None
                           else "ABSENT_EXCEPTION_BODY"),
    )
    return row


def _observation(store, root, path, nodes):
    _, job, observation = path.split("/")
    receipt_path = path + "/receipt.json"
    row = {"observation_path": path, "job_run_id": _canonical(job),
           "observation_id": _canonical(observation), "receipt_path": receipt_path,
           "receipt_sha256": None, "receipt_integrity": "MEASURED_ONLY",
           "pending_sha256": None, "publication": "UNVERIFIED",
           "state": "INVALID_RECEIPT", "verification": "REFUSED",
           "metadata": None, "verified_body": None,
           "body_availability": "UNVERIFIED"}
    if row["job_run_id"] is None or row["observation_id"] is None:
        row["state"] = "INVALID_IDENTITY"
        return row
    if not stat.S_ISDIR(nodes[path][2]):
        row["state"] = "UNSAFE_PATH"
        return row
    for name, field in (("receipt.json", "receipt_sha256"), ("pending.json", "pending_sha256")):
        relative = path + "/" + name
        if relative in nodes:
            try:
                row[field] = hashlib.sha256(_read_at(root, relative)).hexdigest()
            except (OSError, CaptureError):
                row["state"] = "UNSAFE_PATH"
                return row
    staging = any(p.startswith(path + "/") and p.endswith(".partial") for p in nodes)
    if receipt_path not in nodes or staging:
        row.update(state="INCOMPLETE", publication="INCOMPLETE", verification="NOT_COMPLETED")
        return row
    try:
        # This pin binds this measurement to the returned receipt, not authority.
        resolved = store.resolve_receipt(receipt_path,
                                         expected_receipt_sha256=row["receipt_sha256"],
                                         require_success=False)
        return _verified(row, resolved)
    except InventoryError as error:
        row["state"] = error.code
    except CaptureError:
        pass
    return row


def _result(status, coordination, rows=(), issues=(), staging=()):
    counts = Counter(row["state"] for row in rows)
    return {"version": VERSION, "status": status, "consistency": coordination,
            "receipt_pin_authority": "NOT_ESTABLISHED_BY_INVENTORY",
            "prospective_evidence": "BLOCKED", "historical_comparison": "STILL_BLOCKED",
            "observation_count": len(rows), "counts": {s: counts[s] for s in STATES},
            "observations": list(rows), "namespace_issues": list(issues),
            "staging_markers": list(staging)}


def inventory(root):
    """Inventory an explicit root; never create it, choose a version or repair."""
    try:
        with _view(root) as (fd, coordination):
            before = _snapshot(fd)
            nodes, errors = before
            issues = list(errors)
            for path, value in sorted(nodes.items()):
                if stat.S_ISLNK(value[2]) or not (stat.S_ISDIR(value[2]) or stat.S_ISREG(value[2])):
                    issues.append({"path": path, "reason": "UNSAFE_PATH"})
                if path and path.split("/")[0] not in {".writer.lock", "observations", "objects"}:
                    issues.append({"path": path, "reason": "UNRECOGNIZED_NAMESPACE_ENTRY"})
                if path == "observations" or (path.startswith("observations/") and path.count("/") == 1):
                    if not stat.S_ISDIR(value[2]):
                        issues.append({"path": path, "reason": "OBSERVATION_PARENT_UNREADABLE"})
                    if path != "observations" and _canonical(path.split("/")[1]) is None:
                        issues.append({"path": path, "reason": "INVALID_JOB_IDENTITY"})
                if path.startswith("observations/") and path.count("/") >= 3:
                    if path.count("/") != 3 or (path.split("/")[-1] not in {"receipt.json", "pending.json"}
                                                and not path.endswith(".partial")):
                        issues.append({"path": path, "reason": "UNRECOGNIZED_OBSERVATION_ENTRY"})
            store = _AuditStore(Path(root))
            rows = [_observation(store, fd, p, nodes) for p in sorted(nodes)
                    if p.startswith("observations/") and p.count("/") == 2]
            after = _snapshot(fd)
            stable = _unchanged(root, before, after)
            status = "INVENTORIED" if rows or issues else "NO_OBSERVATIONS"
            if not stable or coordination == "WRITER_ACTIVE" or ((rows or issues) and coordination == "NO_WRITER_LOCK"):
                status = "UNSTABLE_INVENTORY"
            elif issues:
                status = "INVENTORIED_WITH_ISSUES"
            issues.extend(issue for issue in after[1] if issue not in issues)
            return _result(status, coordination, rows, sorted(issues, key=lambda i: (i["path"], i["reason"])),
                           sorted(p for p in nodes if p.endswith(".partial")))
    except FileNotFoundError:
        # A second no-follow probe distinguishes observed absence from creation.
        try:
            with _directory(Path(root), create=False):
                return _result("UNSTABLE_INVENTORY", "ROOT_APPEARED")
        except FileNotFoundError:
            return _result("NO_OBSERVATIONS", "ROOT_ABSENT_TWO_PROBES")
        except (OSError, CaptureError, ValueError, TypeError):
            return _result("UNSTABLE_INVENTORY", "ROOT_CHANGED")
    except (OSError, CaptureError, ValueError, TypeError):
        return _result("REFUSED", "UNSAFE_OR_UNREADABLE_ROOT_OR_LOCK")


def select_capture(root, relative_path, expected_receipt_sha256, *, forensic=False):
    """Return verified bytes for an exact caller-pinned receipt or refuse.

    Forensic mode permits failed/cached captures; it never qualifies evidence.
    No selection, even forensic, accepts an incomplete or uncoordinated view.
    """
    if not isinstance(expected_receipt_sha256, str) or not _HASH.fullmatch(expected_receipt_sha256):
        raise InventoryError("EXPLICIT_RECEIPT_PIN_REQUIRED")
    if not isinstance(relative_path, str) or type(forensic) is not bool:
        raise InventoryError("INVALID_SELECTION_ARGUMENT")
    try:
        with _view(root) as (fd, coordination):
            if coordination != "COOPERATING_WRITERS_LOCKED":
                raise InventoryError("UNSTABLE_INVENTORY")
            before = _snapshot(fd)
            resolved = _AuditStore(Path(root)).resolve_receipt(
                relative_path, expected_receipt_sha256=expected_receipt_sha256,
                require_success=not forensic)
            if not forensic and resolved.receipt["from_cache"]:
                raise InventoryError("CACHED_CAPTURE_REFUSED")
            if not _unchanged(root, before, _snapshot(fd)):
                raise InventoryError("UNSTABLE_INVENTORY")
            return resolved
    except InventoryError:
        raise
    except (CaptureError, OSError, ValueError, TypeError):
        raise InventoryError("RESOLUTION_REFUSED") from None


def inspect_receipt(root, relative_path, expected_receipt_sha256, *, forensic=False):
    """Compact CLI-facing selection result; body bytes are never serialized."""
    mode = "FORENSIC" if forensic else "USABLE_CAPTURE_BYTES"
    try:
        resolved = select_capture(root, relative_path, expected_receipt_sha256, forensic=forensic)
        row = _verified({"receipt_path": relative_path, "receipt_sha256": expected_receipt_sha256,
                         "receipt_integrity": "CALLER_SUPPLIED_PIN_MATCHED"}, resolved)
        return {"version": VERSION, "status": "VERIFIED_CAPTURE_BYTES", "mode": mode,
                "prospective_evidence": "BLOCKED", "historical_comparison": "STILL_BLOCKED",
                "observation": row}
    except InventoryError as error:
        return {"version": VERSION, "status": "REFUSED", "mode": mode, "reason": error.code,
                "prospective_evidence": "BLOCKED", "historical_comparison": "STILL_BLOCKED"}
