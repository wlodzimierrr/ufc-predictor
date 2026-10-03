"""POSIX immutable response capture; see the Phase 5C.6 report for schema v2.

No acquisition, legacy manifest writes, aliases, parser admission or recovery.
Locks coordinate cooperating local writers; hashes remain mandatory even for
read-only files. Receipt clocks describe this middleware boundary, not a new
upstream observation when Scrapy supplies a cached response.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from urllib.parse import parse_qsl, urlsplit
import uuid

from scrapy import signals

from ufc_scraper.middlewares import RawCaptureMiddleware


SCHEMA = "ufcstats_raw_capture_receipt_v2"
BOUNDARY = "scrapy_downloader_priority_200_v1"
_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
_HASH = re.compile(r"[0-9a-f]{64}\Z")


class CaptureError(RuntimeError):
    """A capture refused unsafe input or could not publish safely."""


class CaptureIntegrityError(CaptureError):
    """Existing immutable bytes or receipt metadata do not verify."""


class DuplicateObservationError(CaptureError):
    """An observation identity is already reserved, including incomplete ones."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _identity(value: str) -> str:
    try:
        if str(uuid.UUID(value)) == value:
            return value
    except (ValueError, TypeError, AttributeError):
        pass
    raise CaptureError("Expected a canonical UUID identity")


def _public_url(value: str) -> str:
    # Do not export userinfo, arbitrary query tokens or fragments. Exact public
    # URLs are retained, never redacted into misleading identity aliases.
    try:
        parts = urlsplit(value)
        valid = (
            isinstance(value, str) and value.isascii()
            and not any(ord(c) <= 32 or ord(c) == 127 for c in value)
            and parts.scheme in {"http", "https"}
            and parts.netloc in {"ufcstats.com", "www.ufcstats.com"}
            and not parts.fragment and not parts.username and not parts.password
            and re.fullmatch(r"/[A-Za-z0-9/_-]*", parts.path) is not None
        )
        for key, val in parse_qsl(parts.query, keep_blank_values=True, strict_parsing=True):
            valid = valid and (
                key == "page" and (val == "all" or re.fullmatch(r"[0-9]+", val))
                or key == "char" and re.fullmatch(r"[a-z]", val)
            )
        if valid:
            return value
    except (ValueError, TypeError, AttributeError):
        pass
    raise CaptureError("Capture requires a public UFCStats URL without credentials")


def _classification(url: str) -> tuple[str, str | None]:
    path = urlsplit(url).path
    for prefix, entity in (("/event-details/", "event"), ("/fight-details/", "fight"),
                           ("/fighter-details/", "fighter")):
        if path.startswith(prefix):
            return entity, None
    for listing in ("completed", "upcoming"):
        if path == f"/statistics/events/{listing}":
            return "event_listing", listing
    return "unknown", None


def _exception_category(exception: Exception) -> str:
    # No str/repr, arguments, traceback, headers or arbitrary class names.
    name = type(exception).__name__
    if name in {"TimeoutError", "TCPTimedOutError"}:
        return "timeout"
    if name == "DNSLookupError":
        return "dns"
    if name in {"ConnectionError", "ConnectionRefusedError", "ConnectionLost", "ConnectError"}:
        return "connection"
    return "other"


def _reasons(status: int | None, challenge: bool) -> list[str]:
    if status is None:
        return ["request_exception"]
    reasons = []
    if status >= 400:
        reasons.append("http_error")
    elif not 200 <= status < 300:
        reasons.append("non_success_http_status")
    if challenge:
        reasons.append("access_challenge")
    return reasons


def _json_bytes(value: dict) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False,
                       separators=(",", ":")) + "\n").encode("utf-8")


def _json_object(raw: bytes) -> dict:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate key")
            result[key] = value
        return result

    try:
        value = json.loads(raw, object_pairs_hook=unique)
        if isinstance(value, dict):
            return value
    except (ValueError, UnicodeError):
        pass
    raise CaptureIntegrityError("Invalid receipt JSON")


@contextmanager
def _directory(path: Path, *, create: bool):
    # Do not resolve away symlinks. Traverse even the namespace's ancestors
    # through anchored descriptors, refusing symlinks at every component.
    path = path.absolute()
    if ".." in path.parts:
        raise CaptureError("Unsafe capture root")
    fd = os.open(path.anchor, _DIR_FLAGS)
    try:
        for part in path.parts[1:]:
            child = _child_directory(fd, part, create=create)
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


def _child_directory(parent: int, name: str, *, create: bool, exclusive: bool = False) -> int:
    if create:
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent)
            os.fsync(parent)
        except FileExistsError:
            if exclusive:
                raise DuplicateObservationError("Observation already reserved") from None
    return os.open(name, _DIR_FLAGS, dir_fd=parent)


def _read(parent: int, name: str) -> bytes:
    fd = os.open(name, _READ_FLAGS, dir_fd=parent)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise CaptureIntegrityError("Capture reference is not a regular file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            return stream.read()
    finally:
        os.close(fd)


def _publish(parent: int, name: str, content: bytes) -> None:
    """Stage, fsync, link without replacement, fsync directory, retire stage.

    Failures deliberately leave the exclusive reservation and/or .partial
    staging files for inspection. No automatic retry/resume or garbage removal.
    """
    staged = f".{name}.{uuid.uuid4()}.partial"
    fd = os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                 0o600, dir_fd=parent)
    try:
        with os.fdopen(fd, "wb", closefd=False) as stream:
            stream.write(content)
            stream.flush()
            os.fchmod(fd, 0o444)
            os.fsync(fd)
    finally:
        os.close(fd)
    # Hard-link publication is atomic and fails if the destination exists.
    os.link(staged, name, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)
    os.fsync(parent)
    # The final link is durable before stage retirement. Keep retirement as
    # the last fallible operation: fsync/link/unlink failures retain a stage.
    # Retirement itself need not be durable; after a power loss its stage may
    # reappear, conservatively marking publication as unconfirmed to readers.
    os.unlink(staged, dir_fd=parent)


@dataclass(frozen=True)
class ResolvedCapture:
    receipt: dict
    body: bytes | None


class CaptureStoreV2:
    """One job's append-only observations in a dedicated namespace.

    Constructing this object performs no filesystem writes. Observation UUIDs
    are generated per boundary event, never derived from URL/date/body. Supplied
    UUIDs are exclusive reservations, not idempotent retries.
    """

    def __init__(self, root: Path, *, job_run_id: str | None = None):
        self.root = Path(root)
        self.job_run_id = _identity(job_run_id if job_run_id is not None else str(uuid.uuid4()))
        self.job_started_at = _utc_now()

    @contextmanager
    def _writer(self):
        with _directory(self.root, create=True) as root:
            fd = os.open(".writer.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
                         0o600, dir_fd=root)
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    raise CaptureIntegrityError("Invalid writer lock")
                fcntl.flock(fd, fcntl.LOCK_EX)
                os.fsync(root)
                yield root
            finally:
                os.close(fd)

    @staticmethod
    def _body_reference(body: bytes) -> dict:
        digest = hashlib.sha256(body).hexdigest()
        return {"path": f"objects/sha256/{digest[:2]}/{digest}.body",
                "sha256": digest, "bytes": len(body)}

    def capture_response(self, source_url: str, body: bytes, http_status: int, *,
                         observation_id: str | None = None, request_url: str | None = None,
                         request_started_at: str | None = None, from_cache: bool = False) -> str:
        if not isinstance(body, bytes) or type(http_status) is not int or not 100 <= http_status <= 599:
            raise CaptureError("Invalid response bytes or HTTP status")
        return self._capture(source_url, body, http_status, None, observation_id,
                             request_url, request_started_at, from_cache)

    def capture_exception(self, source_url: str, exception: Exception, *,
                          observation_id: str | None = None, request_started_at: str | None = None) -> str:
        return self._capture(source_url, None, None, _exception_category(exception),
                             observation_id, source_url, request_started_at, False)

    def _capture(self, url, body, status, category, observation_id, request_url, requested_at, cached):
        observed_at = _utc_now()
        url = _public_url(url)
        request_url = _public_url(request_url if request_url is not None else url)
        oid = _identity(observation_id if observation_id is not None else str(uuid.uuid4()))
        entity, listing = _classification(url)
        challenge = body is not None and RawCaptureMiddleware._is_browser_challenge(body)
        reasons = _reasons(status, challenge)
        pending = {"schema": SCHEMA, "boundary": BOUNDARY, "publication_state": "INCOMPLETE",
                   "job_run_id": self.job_run_id, "observation_id": oid,
                   "job_started_at": self.job_started_at, "observed_at": observed_at,
                   "request_started_at": requested_at, "request_url": request_url, "source_url": url,
                   "entity_type": entity, "listing_kind": listing, "http_status": status,
                   "response_present": body is not None,
                   "body": self._body_reference(body) if body is not None else None,
                   "disposition": "FAILED" if reasons else "SUCCEEDED", "failure_reasons": reasons,
                   "access_challenge": challenge, "exception_category": category, "from_cache": cached}
        self._validate_metadata({**pending, "publication_state": "COMPLETE", "receipt_prepared_at": observed_at})
        try:
            with self._writer() as root:
                observations = _child_directory(root, "observations", create=True)
                try:
                    job = _child_directory(observations, self.job_run_id, create=True)
                    try:
                        observation = _child_directory(job, oid, create=True, exclusive=True)
                    finally:
                        os.close(job)
                finally:
                    os.close(observations)
                try:
                    _publish(observation, "pending.json", _json_bytes(pending))
                    if body is not None:
                        self._store_body(root, pending["body"], body)
                    receipt = {**pending, "publication_state": "COMPLETE", "receipt_prepared_at": _utc_now()}
                    self._validate_metadata(receipt)
                    _publish(observation, "receipt.json", _json_bytes(receipt))
                finally:
                    os.close(observation)
        except OSError:
            raise CaptureIntegrityError("Capture filesystem/publication failure; inspect incomplete states") from None
        return f"observations/{self.job_run_id}/{oid}/receipt.json"

    @staticmethod
    def _store_body(root: int, reference: dict, body: bytes) -> None:
        objects = _child_directory(root, "objects", create=True)
        try:
            hashes = _child_directory(objects, "sha256", create=True)
            try:
                shard = _child_directory(hashes, reference["sha256"][:2], create=True)
            finally:
                os.close(hashes)
        finally:
            os.close(objects)
        try:
            name = reference["sha256"] + ".body"
            try:
                existing = _read(shard, name)
            except FileNotFoundError:
                _publish(shard, name, body)
                existing = _read(shard, name)
            if existing != body or hashlib.sha256(existing).hexdigest() != reference["sha256"]:
                raise CaptureIntegrityError("Existing content-addressed object failed integrity verification")
            # Reused objects need durability too, including a previously linked
            # object whose writer was interrupted before directory fsync.
            fd = os.open(name, _READ_FLAGS, dir_fd=shard)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.fsync(shard)
            if _read(shard, name) != body:
                raise CaptureIntegrityError("Body changed during durability verification")
        finally:
            os.close(shard)

    @staticmethod
    def _validate_metadata(receipt: dict) -> None:
        keys = {"schema", "boundary", "publication_state", "job_run_id", "observation_id", "job_started_at",
                "observed_at", "receipt_prepared_at", "request_started_at", "request_url", "source_url",
                "entity_type", "listing_kind", "http_status", "response_present", "body", "disposition",
                "failure_reasons", "access_challenge", "exception_category", "from_cache"}

        def check(condition):
            if not condition:
                raise ValueError("Invalid metadata")

        try:
            check(set(receipt) == keys)
            check(receipt["schema"] == SCHEMA and receipt["boundary"] == BOUNDARY)
            check(receipt["publication_state"] == "COMPLETE")
            _identity(receipt["job_run_id"])
            _identity(receipt["observation_id"])
            _public_url(receipt["source_url"])
            _public_url(receipt["request_url"])
            check((receipt["entity_type"], receipt["listing_kind"]) == _classification(receipt["source_url"]))
            for name in ("response_present", "access_challenge", "from_cache"):
                check(type(receipt[name]) is bool)
            status = receipt["http_status"]
            if receipt["response_present"]:
                check(type(status) is int and 100 <= status <= 599)
                check(receipt["exception_category"] is None)
                body = receipt["body"]
                check(set(body) == {"path", "sha256", "bytes"})
                check(_HASH.fullmatch(body["sha256"]))
                check(body["path"] == f'objects/sha256/{body["sha256"][:2]}/{body["sha256"]}.body')
                check(type(body["bytes"]) is int and body["bytes"] >= 0)
            else:
                check(status is None and receipt["body"] is None)
                check(receipt["exception_category"] in {"timeout", "dns", "connection", "other"})
                check(not receipt["access_challenge"] and not receipt["from_cache"])
                check(receipt["request_url"] == receipt["source_url"])
            reasons = _reasons(status, receipt["access_challenge"])
            check(receipt["failure_reasons"] == reasons)
            check(receipt["disposition"] == ("FAILED" if reasons else "SUCCEEDED"))
            clocks = [receipt[name] for name in ("job_started_at", "observed_at", "receipt_prepared_at")]
            if receipt["request_started_at"] is not None:
                clocks.insert(1, receipt["request_started_at"])
            instants = [datetime.fromisoformat(value) for value in clocks]
            check(all(t.tzinfo is not None and t.utcoffset().total_seconds() == 0 for t in instants))
            check(instants == sorted(instants))
        except (ValueError, TypeError, KeyError, AttributeError, CaptureError):
            raise CaptureIntegrityError("Receipt schema, disposition or clock verification failed") from None

    def resolve_receipt(self, relative_path: str, *, expected_receipt_sha256: str | None = None,
                        require_success: bool = True) -> ResolvedCapture:
        """Read-only offline resolution; failed responses require forensic opt-in.

        An independently trusted receipt pin is optional for byte resolution,
        necessary for externally anchored receipt integrity. Hashes alone do not
        certify origin, semantic evidence, coverage or freshness.
        """
        parts = relative_path.split("/")
        if len(parts) != 4 or parts[0] != "observations" or parts[3] != "receipt.json":
            raise CaptureError("Unsafe receipt path")
        _identity(parts[1])
        _identity(parts[2])
        try:
            with _directory(self.root, create=False) as root:
                observations = _child_directory(root, "observations", create=False)
                try:
                    job = _child_directory(observations, parts[1], create=False)
                    try:
                        observation = _child_directory(job, parts[2], create=False)
                    finally:
                        os.close(job)
                finally:
                    os.close(observations)
                try:
                    if any(name.endswith(".partial") for name in os.listdir(observation)):
                        raise CaptureIntegrityError("Observation publication has unfinished staging files")
                    raw = _read(observation, "receipt.json")
                    pending = _json_object(_read(observation, "pending.json"))
                finally:
                    os.close(observation)
                if expected_receipt_sha256 is not None and (
                    not _HASH.fullmatch(expected_receipt_sha256)
                    or hashlib.sha256(raw).hexdigest() != expected_receipt_sha256
                ):
                    raise CaptureIntegrityError("Receipt pin mismatch")
                receipt = _json_object(raw)
                self._validate_metadata(receipt)
                if (receipt["job_run_id"], receipt["observation_id"]) != (parts[1], parts[2]):
                    raise CaptureIntegrityError("Receipt identity/path mismatch")
                expected_pending = {k: v for k, v in receipt.items() if k != "receipt_prepared_at"}
                expected_pending["publication_state"] = "INCOMPLETE"
                if pending != expected_pending:
                    raise CaptureIntegrityError("Receipt/reservation mismatch")
                body = None
                if receipt["body"] is not None:
                    body = self._resolve_body(root, receipt["body"])
                    if receipt["access_challenge"] != RawCaptureMiddleware._is_browser_challenge(body):
                        raise CaptureIntegrityError("Access-challenge disposition mismatch")
                if require_success and receipt["disposition"] != "SUCCEEDED":
                    raise CaptureError("FAILED observation cannot be used as successful source evidence")
                return ResolvedCapture(receipt, body)
        except OSError:
            raise CaptureIntegrityError("Missing, unsafe or incomplete capture reference") from None

    @staticmethod
    def _resolve_body(root: int, reference: dict) -> bytes:
        fd = os.dup(root)
        try:
            parts = reference["path"].split("/")
            for part in parts[:-1]:
                child = _child_directory(fd, part, create=False)
                os.close(fd)
                fd = child
            body = _read(fd, parts[-1])
        finally:
            os.close(fd)
        if len(body) != reference["bytes"] or hashlib.sha256(body).hexdigest() != reference["sha256"]:
            raise CaptureIntegrityError("Referenced body hash/length mismatch")
        return body


class ImmutableRawCaptureMiddlewareV2:
    """Final-response capture at priority 200; acquisition behavior is unchanged."""

    _REQUEST_CLOCK = "_raw_capture_v2_request_started_at"

    def __init__(self, data_dir: Path):
        self.store = CaptureStoreV2(Path(data_dir) / "raw" / "ufcstats_v2")

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls(Path(__file__).resolve().parents[4] / "data")
        crawler.signals.connect(instance._spider_opened, signal=signals.spider_opened)
        return instance

    def _spider_opened(self, spider):
        spider.logger.info("ImmutableRawCaptureMiddlewareV2 active | job_run_id=%s", self.store.job_run_id)

    def process_request(self, request, spider):
        request.meta[self._REQUEST_CLOCK] = _utc_now()
        return None

    def process_response(self, request, response, spider):
        # Preserve the legacy response coverage: detail and event-listing pages.
        # A-Z discovery responses remain outside this capture boundary.
        if _classification(response.url)[0] == "unknown":
            return response
        self.store.capture_response(response.url, response.body, response.status,
                                    request_url=request.url,
                                    request_started_at=request.meta.get(self._REQUEST_CLOCK),
                                    from_cache="cached" in response.flags)
        # Retain the legacy synthetic 503 challenge disposition after Retry has
        # already run; no retry, header or browser-session behavior is added.
        if response.status < 400 and RawCaptureMiddleware._is_browser_challenge(response.body):
            return response.replace(status=503)
        return response

    def process_exception(self, request, exception, spider):
        self.store.capture_exception(request.url, exception,
                                     request_started_at=request.meta.get(self._REQUEST_CLOCK))
        return None
