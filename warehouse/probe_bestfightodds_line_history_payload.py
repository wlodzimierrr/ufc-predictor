"""Capture one explicitly reviewed BestFightOdds line-history payload.

This probe is intentionally narrower than a crawler. It stores one same-host
payload response plus metadata and does not parse, normalize, schedule, or load
odds rows.

Usage:
    python3 warehouse/probe_bestfightodds_line_history_payload.py --dry-run \
        --url 'https://www.bestfightodds.com/js/bfo.min.js?v=0.4.10'
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

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = REPO_ROOT / "data" / "odds" / "raw" / "bestfightodds" / "payloads"
DEFAULT_USER_AGENT = (
    "ufc-data BestFightOdds line-history payload probe/0.1 "
    "(single reviewed request; contact before broad crawl)"
)
MAX_REQUEST_CAP = 1
MIN_REQUEST_INTERVAL_SECONDS = 2.0
ALLOWED_HOSTS = {"bestfightodds.com", "www.bestfightodds.com"}
ALLOWED_JS_ASSET = "/js/bfo.min.js"
ALLOWED_DETAIL_PREFIXES = ("/api/",)


@dataclass(frozen=True)
class PayloadTarget:
    """One explicitly reviewed BestFightOdds payload target."""

    url: str
    target_type: str
    identifier: str


@dataclass(frozen=True)
class FetchResponse:
    """Raw response fields needed for payload metadata."""

    final_url: str
    http_status: int
    body: bytes
    content_type: str


@dataclass(frozen=True)
class PayloadProbeResult:
    """Result of a dry-run or completed payload probe."""

    dry_run: bool
    url: str
    target_type: str
    payload_path: Path | None
    metadata_path: Path | None
    http_status: int | None
    content_sha256: str | None
    bytes_written: int


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture one bounded BestFightOdds line-history payload response."
    )
    parser.add_argument("--url", required=True, help="Explicit reviewed BestFightOdds payload URL.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--dry-run", action="store_true", help="Report the intended fetch without network access.")
    parser.add_argument("--request-cap", type=int, default=MAX_REQUEST_CAP)
    parser.add_argument("--request-interval-seconds", type=float, default=MIN_REQUEST_INTERVAL_SECONDS)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--retry-backoff-seconds", type=float, default=2.0)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    return parser


def probe_bestfightodds_line_history_payload(
    *,
    url: str,
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
) -> PayloadProbeResult:
    """Capture one raw BestFightOdds payload, or report it in dry-run mode."""
    target = resolve_payload_target(url)
    _validate_request_policy(
        request_cap=request_cap,
        request_interval_seconds=request_interval_seconds,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )

    if dry_run:
        return PayloadProbeResult(
            dry_run=True,
            url=target.url,
            target_type=target.target_type,
            payload_path=None,
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
    return write_payload(
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


def resolve_payload_target(url: str) -> PayloadTarget:
    """Resolve one reviewed URL into a validated BestFightOdds payload target."""
    parsed = urllib.parse.urlparse(url.strip())
    if parsed.scheme != "https":
        raise ValueError("BestFightOdds payload URL must use https")
    if parsed.netloc.lower() not in ALLOWED_HOSTS:
        raise ValueError("payload URL host must be bestfightodds.com")
    if parsed.fragment:
        raise ValueError("URL fragments are not supported")
    if parsed.params:
        raise ValueError("URL params are not supported")

    path = parsed.path or "/"
    if path == ALLOWED_JS_ASSET:
        target_type = "js_asset"
        identifier = path.rsplit("/", 1)[-1]
    elif path.startswith(ALLOWED_DETAIL_PREFIXES):
        target_type = "detail_payload"
        identifier = f"{path.strip('/')}_{hashlib.sha256(parsed.query.encode('utf-8')).hexdigest()[:12]}"
    else:
        raise ValueError("payload URL path must be /js/bfo.min.js or an explicitly reviewed /api/ path")

    normalized = urllib.parse.urlunparse(
        ("https", parsed.netloc.lower(), path, "", parsed.query, "")
    )
    return PayloadTarget(normalized, target_type, identifier)


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
                    content_type=response.headers.get("Content-Type", ""),
                )
        except urllib.error.HTTPError as exc:
            return FetchResponse(
                final_url=exc.geturl(),
                http_status=exc.code,
                body=exc.read(),
                content_type=exc.headers.get("Content-Type", ""),
            )
        except urllib.error.URLError as exc:
            last_error = exc
            if attempt >= max_retries:
                break
            time.sleep(retry_backoff_seconds)

    assert last_error is not None
    raise RuntimeError(f"failed to fetch {url}: {last_error}") from last_error


def write_payload(
    *,
    raw_dir: Path,
    target: PayloadTarget,
    response: FetchResponse,
    fetched_at: datetime,
    user_agent: str,
    request_cap: int,
    request_interval_seconds: float,
    timeout_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
) -> PayloadProbeResult:
    """Write raw payload bytes and adjacent JSON metadata for one response."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    fetched_at_utc = _utc_datetime(fetched_at)
    timestamp = fetched_at_utc.strftime("%Y%m%dT%H%M%SZ")
    content_sha256 = hashlib.sha256(response.body).hexdigest()
    extension = _payload_extension(target, response.content_type)
    stem = f"{timestamp}_{target.target_type}_{_safe_filename(target.identifier)}_{content_sha256[:12]}"
    payload_path = raw_dir / f"{stem}{extension}"
    metadata_path = raw_dir / f"{stem}.metadata.json"

    payload_path.write_bytes(response.body)
    metadata = {
        "source": "bestfightodds_line_history_payload_probe",
        "fetched_at": fetched_at_utc.isoformat(),
        "target_type": target.target_type,
        "target_identifier": target.identifier,
        "source_url": target.url,
        "final_url": response.final_url,
        "http_status": response.http_status,
        "content_type": response.content_type,
        "content_sha256": content_sha256,
        "content_bytes": len(response.body),
        "payload_path": str(payload_path),
        "request_cap": request_cap,
        "request_interval_seconds": request_interval_seconds,
        "timeout_seconds": timeout_seconds,
        "max_retries": max_retries,
        "retry_backoff_seconds": retry_backoff_seconds,
        "user_agent": user_agent,
        "parsing_performed": False,
        "loaded_into_fight_odds": False,
        "scheduled_or_continuous_fetch": False,
    }
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return PayloadProbeResult(
        dry_run=False,
        url=target.url,
        target_type=target.target_type,
        payload_path=payload_path,
        metadata_path=metadata_path,
        http_status=response.http_status,
        content_sha256=content_sha256,
        bytes_written=len(response.body),
    )


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        result = probe_bestfightodds_line_history_payload(
            url=args.url,
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
        print(f"Would write raw payload under: {args.raw_dir}")
        return 0

    print(f"Fetched: {result.url}")
    print(f"HTTP status: {result.http_status}")
    print(f"Bytes written: {result.bytes_written}")
    print(f"SHA-256: {result.content_sha256}")
    print(f"Wrote payload: {result.payload_path}")
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
        raise ValueError("request_cap is hard-limited to 1 payload for this ticket")
    if request_interval_seconds < MIN_REQUEST_INTERVAL_SECONDS:
        raise ValueError("request_interval_seconds must be at least 2.0")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if max_retries < 0:
        raise ValueError("max_retries must be non-negative")
    if retry_backoff_seconds < 0:
        raise ValueError("retry_backoff_seconds must be non-negative")


def _payload_extension(target: PayloadTarget, content_type: str) -> str:
    if target.target_type == "js_asset":
        return ".js"
    lowered = content_type.lower()
    if "json" in lowered:
        return ".json"
    if "html" in lowered:
        return ".html"
    return ".payload"


def _safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-") or "payload"


def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


if __name__ == "__main__":
    raise SystemExit(main())
