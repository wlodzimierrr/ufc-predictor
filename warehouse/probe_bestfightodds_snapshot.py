"""Capture one explicitly requested BestFightOdds raw HTML snapshot.

This probe is intentionally narrow: it accepts exactly one BestFightOdds event,
fighter, archive, or home page URL target, writes raw HTML plus metadata, and
does not parse or load odds rows.

Usage:
    python3 warehouse/probe_bestfightodds_snapshot.py --dry-run --event-id ufc-330-4237
    python3 warehouse/probe_bestfightodds_snapshot.py --url https://www.bestfightodds.com/events/ufc-330-4237
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = REPO_ROOT / "data" / "odds" / "raw" / "bestfightodds"
BESTFIGHTODDS_BASE_URL = "https://www.bestfightodds.com"
DEFAULT_USER_AGENT = (
    "ufc-data BestFightOdds snapshot probe/0.1 "
    "(documentation-only bounded research; contact before broad crawl)"
)
MAX_REQUEST_CAP = 1
MIN_REQUEST_INTERVAL_SECONDS = 2.0
ALLOWED_HOSTS = {"bestfightodds.com", "www.bestfightodds.com"}
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]*$")


@dataclass(frozen=True)
class ProbeTarget:
    """One BestFightOdds page selected by an explicit user input."""

    url: str
    target_type: str
    identifier: str


@dataclass(frozen=True)
class FetchResponse:
    """Raw response fields needed for snapshot metadata."""

    final_url: str
    http_status: int
    body: bytes


@dataclass(frozen=True)
class SnapshotResult:
    """Result of a dry-run or completed raw snapshot probe."""

    dry_run: bool
    url: str
    target_type: str
    html_path: Path | None
    metadata_path: Path | None
    http_status: int | None
    content_sha256: str | None
    bytes_written: int


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture one bounded BestFightOdds raw HTML snapshot."
    )
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--url", help="Explicit BestFightOdds page URL.")
    target_group.add_argument("--event-id", help="BestFightOdds event identifier, for example ufc-330-4237.")
    target_group.add_argument("--fighter-id", help="BestFightOdds fighter identifier, for example islam-makhachev-5541.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--dry-run", action="store_true", help="Report the intended fetch without network access.")
    parser.add_argument("--request-cap", type=int, default=MAX_REQUEST_CAP)
    parser.add_argument("--request-interval-seconds", type=float, default=MIN_REQUEST_INTERVAL_SECONDS)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--retry-backoff-seconds", type=float, default=2.0)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    return parser


def probe_bestfightodds_snapshot(
    *,
    url: str | None = None,
    event_id: str | None = None,
    fighter_id: str | None = None,
    raw_dir: Path = DEFAULT_RAW_DIR,
    dry_run: bool = False,
    request_cap: int = MAX_REQUEST_CAP,
    request_interval_seconds: float = MIN_REQUEST_INTERVAL_SECONDS,
    timeout_seconds: float = 15.0,
    max_retries: int = 1,
    retry_backoff_seconds: float = 2.0,
    user_agent: str = DEFAULT_USER_AGENT,
    fetcher: Callable[[str, str, float, int, float], FetchResponse] | None = None,
    fetched_at: datetime | None = None,
) -> SnapshotResult:
    """Capture one raw BestFightOdds page, or report it in dry-run mode."""
    target = resolve_target(url=url, event_id=event_id, fighter_id=fighter_id)
    _validate_request_policy(
        request_cap=request_cap,
        request_interval_seconds=request_interval_seconds,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )

    if dry_run:
        return SnapshotResult(
            dry_run=True,
            url=target.url,
            target_type=target.target_type,
            html_path=None,
            metadata_path=None,
            http_status=None,
            content_sha256=None,
            bytes_written=0,
        )

    active_fetcher = fetcher or fetch_url
    response = active_fetcher(
        target.url,
        user_agent,
        timeout_seconds,
        max_retries,
        retry_backoff_seconds,
    )
    timestamp = fetched_at or datetime.now(timezone.utc)
    return write_snapshot(
        raw_dir=raw_dir,
        target=target,
        response=response,
        fetched_at=timestamp,
        user_agent=user_agent,
        request_cap=request_cap,
        request_interval_seconds=request_interval_seconds,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )


def resolve_target(
    *,
    url: str | None = None,
    event_id: str | None = None,
    fighter_id: str | None = None,
) -> ProbeTarget:
    """Resolve exactly one explicit target into a validated BestFightOdds URL."""
    provided = [value is not None for value in (url, event_id, fighter_id)].count(True)
    if provided != 1:
        raise ValueError("provide exactly one of url, event_id, or fighter_id")

    if event_id is not None:
        identifier = _validated_identifier(event_id, "event_id")
        target_url = f"{BESTFIGHTODDS_BASE_URL}/events/{identifier}"
        return ProbeTarget(target_url, "event", identifier)
    if fighter_id is not None:
        identifier = _validated_identifier(fighter_id, "fighter_id")
        target_url = f"{BESTFIGHTODDS_BASE_URL}/fighters/{identifier}"
        return ProbeTarget(target_url, "fighter", identifier)

    assert url is not None
    normalized_url, target_type, identifier = _validated_url(url)
    return ProbeTarget(normalized_url, target_type, identifier)


def fetch_url(
    url: str,
    user_agent: str,
    timeout_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
) -> FetchResponse:
    """Fetch one URL with simple retry/backoff around transient failures."""
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                return FetchResponse(
                    final_url=response.geturl(),
                    http_status=response.status,
                    body=response.read(),
                )
        except urllib.error.HTTPError as exc:
            body = exc.read()
            return FetchResponse(final_url=exc.geturl(), http_status=exc.code, body=body)
        except urllib.error.URLError as exc:
            last_error = exc
            if attempt >= max_retries:
                break
            time.sleep(retry_backoff_seconds)

    assert last_error is not None
    raise RuntimeError(f"failed to fetch {url}: {last_error}") from last_error


def write_snapshot(
    *,
    raw_dir: Path,
    target: ProbeTarget,
    response: FetchResponse,
    fetched_at: datetime,
    user_agent: str,
    request_cap: int,
    request_interval_seconds: float,
    timeout_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
) -> SnapshotResult:
    """Write raw HTML and adjacent JSON metadata for one response."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    fetched_at_utc = _utc_datetime(fetched_at)
    timestamp = fetched_at_utc.strftime("%Y%m%dT%H%M%SZ")
    content_sha256 = hashlib.sha256(response.body).hexdigest()
    stem = f"{timestamp}_{target.target_type}_{_safe_filename(target.identifier)}_{content_sha256[:12]}"
    html_path = raw_dir / f"{stem}.html"
    metadata_path = raw_dir / f"{stem}.metadata.json"

    html_path.write_bytes(response.body)
    metadata = {
        "source": "bestfightodds_raw_snapshot_probe",
        "fetched_at": fetched_at_utc.isoformat(),
        "target_type": target.target_type,
        "target_identifier": target.identifier,
        "source_url": target.url,
        "final_url": response.final_url,
        "http_status": response.http_status,
        "content_sha256": content_sha256,
        "content_bytes": len(response.body),
        "html_path": str(html_path),
        "request_cap": request_cap,
        "request_interval_seconds": request_interval_seconds,
        "timeout_seconds": timeout_seconds,
        "max_retries": max_retries,
        "retry_backoff_seconds": retry_backoff_seconds,
        "user_agent": user_agent,
        "parsing_performed": False,
        "loaded_into_fight_odds": False,
    }
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return SnapshotResult(
        dry_run=False,
        url=target.url,
        target_type=target.target_type,
        html_path=html_path,
        metadata_path=metadata_path,
        http_status=response.http_status,
        content_sha256=content_sha256,
        bytes_written=len(response.body),
    )


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        result = probe_bestfightodds_snapshot(
            url=args.url,
            event_id=args.event_id,
            fighter_id=args.fighter_id,
            raw_dir=args.raw_dir,
            dry_run=args.dry_run,
            request_cap=args.request_cap,
            request_interval_seconds=args.request_interval_seconds,
            timeout_seconds=args.timeout_seconds,
            max_retries=args.max_retries,
            retry_backoff_seconds=args.retry_backoff_seconds,
            user_agent=args.user_agent,
        )
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    if result.dry_run:
        print("Dry run: no network request made.")
        print(f"Would fetch: {result.url}")
        print(f"Target type: {result.target_type}")
        print(f"Would write raw snapshot under: {args.raw_dir}")
        return 0

    print(f"Fetched: {result.url}")
    print(f"HTTP status: {result.http_status}")
    print(f"Bytes written: {result.bytes_written}")
    print(f"SHA-256: {result.content_sha256}")
    print(f"Wrote HTML: {result.html_path}")
    print(f"Wrote metadata: {result.metadata_path}")
    return 0


def _validate_request_policy(
    *,
    request_cap: int,
    request_interval_seconds: float,
    timeout_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
) -> None:
    if request_cap != MAX_REQUEST_CAP:
        raise ValueError("request_cap is hard-limited to 1 page for this ticket")
    if request_interval_seconds < MIN_REQUEST_INTERVAL_SECONDS:
        raise ValueError("request_interval_seconds must be at least 2.0")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if max_retries < 0:
        raise ValueError("max_retries must be non-negative")
    if retry_backoff_seconds < 0:
        raise ValueError("retry_backoff_seconds must be non-negative")


def _validated_identifier(value: str, field_name: str) -> str:
    text = value.strip()
    if not IDENTIFIER_RE.fullmatch(text):
        raise ValueError(f"invalid {field_name}: {value}")
    return text


def _validated_url(value: str) -> tuple[str, str, str]:
    parsed = urllib.parse.urlparse(value.strip())
    if parsed.scheme != "https":
        raise ValueError("BestFightOdds URL must use https")
    if parsed.netloc.lower() not in ALLOWED_HOSTS:
        raise ValueError("URL host must be bestfightodds.com")
    if parsed.fragment:
        raise ValueError("URL fragments are not supported")
    if parsed.params:
        raise ValueError("URL params are not supported")

    path = parsed.path or "/"
    if path == "/":
        target_type = "home"
        identifier = "home"
    elif path == "/archive":
        target_type = "archive"
        identifier = "archive"
    elif path.startswith("/events/"):
        target_type = "event"
        identifier = _validated_identifier(path.rsplit("/", 1)[-1], "event_id")
    elif path.startswith("/fighters/"):
        target_type = "fighter"
        identifier = _validated_identifier(path.rsplit("/", 1)[-1], "fighter_id")
    else:
        raise ValueError("URL path must be /, /archive, /events/<id>, or /fighters/<id>")

    normalized = urllib.parse.urlunparse(
        ("https", parsed.netloc.lower(), path, "", parsed.query, "")
    )
    return normalized, target_type, identifier


def _safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-") or "snapshot"


def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


if __name__ == "__main__":
    raise SystemExit(main())

