"""Run explicitly bounded BestFightOdds captures from a target manifest.

This is the expansion layer for BFO Mean market-benchmark coverage. It does not
discover site targets. Every page or payload must be listed explicitly in the
manifest, and raw capture still delegates to the one-request probes.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.probe_bestfightodds_line_history_payload import (
    DEFAULT_USER_AGENT as DEFAULT_PAYLOAD_USER_AGENT,
    PayloadProbeResult,
    probe_bestfightodds_line_history_payload,
    resolve_payload_target,
)
from warehouse.probe_bestfightodds_snapshot import (
    DEFAULT_USER_AGENT as DEFAULT_SNAPSHOT_USER_AGENT,
    SnapshotResult,
    probe_bestfightodds_snapshot,
    resolve_target as resolve_snapshot_target,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = REPO_ROOT / "data" / "odds" / "raw" / "bestfightodds"
DEFAULT_PAYLOAD_DIR = DEFAULT_RAW_DIR / "payloads"
DEFAULT_MANIFEST_OUTPUT = DEFAULT_RAW_DIR / "manifests" / "bfo_mean_capture_results.csv"
DEFAULT_USER_AGENT = (
    "ufc-data BestFightOdds manifest capture/0.1 "
    "(explicit targets only; contact before broad crawl)"
)
DEFAULT_REQUEST_CAP = 1
MAX_REQUEST_CAP = 25
MIN_REQUEST_INTERVAL_SECONDS = 2.0
TARGET_KINDS = {"snapshot", "payload"}
PURPOSES = {"historical_backfill", "upcoming_snapshot"}

MANIFEST_COLUMNS = [
    "target_id",
    "purpose",
    "local_event_id",
    "local_fight_id",
    "target_kind",
    "url",
    "event_id",
    "fighter_id",
    "notes",
]

RESULT_COLUMNS = [
    *MANIFEST_COLUMNS,
    "result_status",
    "planned_url",
    "target_type",
    "dry_run",
    "http_status",
    "content_sha256",
    "bytes_written",
    "artifact_path",
    "metadata_path",
    "recorded_at",
    "error",
]


@dataclass(frozen=True)
class ManifestTarget:
    """One explicit BFO target from a capture manifest."""

    row_number: int
    target_id: str
    purpose: str
    local_event_id: str
    local_fight_id: str
    target_kind: str
    url: str
    event_id: str
    fighter_id: str
    notes: str
    planned_url: str
    target_type: str


@dataclass(frozen=True)
class ManifestCaptureResult:
    """Completed or dry-run manifest capture rows."""

    targets_read: int
    targets_run: int
    dry_run: bool
    result_rows: tuple[dict[str, str], ...]
    output_path: Path


def build_arg_parser() -> argparse.ArgumentParser:
    """Return the manifest capture CLI parser."""
    parser = argparse.ArgumentParser(
        description="Capture explicit BestFightOdds page/payload targets from a manifest."
    )
    parser.add_argument("--manifest", type=Path, required=True, help="CSV manifest of explicit BFO targets.")
    parser.add_argument("--output", type=Path, default=DEFAULT_MANIFEST_OUTPUT, help="Capture result manifest CSV.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR, help="Raw HTML output directory.")
    parser.add_argument("--payload-dir", type=Path, default=DEFAULT_PAYLOAD_DIR, help="Raw payload output directory.")
    parser.add_argument("--dry-run", action="store_true", help="Validate/report targets without network access.")
    parser.add_argument("--request-cap", type=int, default=DEFAULT_REQUEST_CAP, help="Maximum manifest targets to fetch.")
    parser.add_argument("--request-interval-seconds", type=float, default=MIN_REQUEST_INTERVAL_SECONDS)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--retry-backoff-seconds", type=float, default=2.0)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    return parser


def capture_bestfightodds_manifest(
    *,
    manifest: Path,
    output: Path = DEFAULT_MANIFEST_OUTPUT,
    raw_dir: Path = DEFAULT_RAW_DIR,
    payload_dir: Path = DEFAULT_PAYLOAD_DIR,
    dry_run: bool = False,
    request_cap: int = DEFAULT_REQUEST_CAP,
    request_interval_seconds: float = MIN_REQUEST_INTERVAL_SECONDS,
    timeout_seconds: float = 15.0,
    max_retries: int = 1,
    retry_backoff_seconds: float = 2.0,
    user_agent: str = DEFAULT_USER_AGENT,
    snapshot_probe: Callable[..., SnapshotResult] = probe_bestfightodds_snapshot,
    payload_probe: Callable[..., PayloadProbeResult] = probe_bestfightodds_line_history_payload,
    sleeper: Callable[[float], object] = time.sleep,
    recorded_at: datetime | None = None,
) -> ManifestCaptureResult:
    """Capture or dry-run all explicit targets in ``manifest``."""
    targets = read_manifest_targets(manifest)
    _validate_request_policy(
        target_count=len(targets),
        request_cap=request_cap,
        request_interval_seconds=request_interval_seconds,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )

    rows: list[dict[str, str]] = []
    timestamp = _utc_datetime(recorded_at or datetime.now(timezone.utc)).isoformat()
    for index, target in enumerate(targets):
        if index > 0 and not dry_run:
            sleeper(request_interval_seconds)
        try:
            result = _capture_target(
                target,
                raw_dir=raw_dir,
                payload_dir=payload_dir,
                dry_run=dry_run,
                request_interval_seconds=request_interval_seconds,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                retry_backoff_seconds=retry_backoff_seconds,
                user_agent=user_agent,
                snapshot_probe=snapshot_probe,
                payload_probe=payload_probe,
            )
            rows.append(_result_row(target, result, recorded_at=timestamp))
        except Exception as exc:  # pragma: no cover - defensive row reporting.
            rows.append(_error_row(target, str(exc), recorded_at=timestamp, dry_run=dry_run))

    _write_csv(output, RESULT_COLUMNS, rows)
    return ManifestCaptureResult(
        targets_read=len(targets),
        targets_run=len(rows),
        dry_run=dry_run,
        result_rows=tuple(rows),
        output_path=output,
    )


def read_manifest_targets(path: Path) -> tuple[ManifestTarget, ...]:
    """Read and validate explicit BFO manifest targets."""
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = [column for column in MANIFEST_COLUMNS if column not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"manifest missing columns: {', '.join(missing)}")
        rows = [
            _manifest_target(row, row_number=index)
            for index, row in enumerate(reader, start=2)
            if _enabled(row)
        ]
    if not rows:
        raise ValueError("manifest has no enabled targets")
    return tuple(rows)


def _manifest_target(row: dict[str, str], *, row_number: int) -> ManifestTarget:
    target_id = _required(row, "target_id", row_number)
    purpose = _required(row, "purpose", row_number)
    if purpose not in PURPOSES:
        raise ValueError(f"row {row_number}: purpose must be one of {sorted(PURPOSES)}")
    local_event_id = _text(row.get("local_event_id"))
    local_fight_id = _text(row.get("local_fight_id"))
    if not local_event_id and not local_fight_id:
        raise ValueError(f"row {row_number}: local_event_id or local_fight_id is required")
    target_kind = _required(row, "target_kind", row_number)
    if target_kind not in TARGET_KINDS:
        raise ValueError(f"row {row_number}: target_kind must be one of {sorted(TARGET_KINDS)}")

    url = _text(row.get("url"))
    event_id = _text(row.get("event_id"))
    fighter_id = _text(row.get("fighter_id"))
    if target_kind == "snapshot":
        planned = resolve_snapshot_target(url=url or None, event_id=event_id or None, fighter_id=fighter_id or None)
    else:
        if event_id or fighter_id:
            raise ValueError(f"row {row_number}: payload targets accept url only")
        if not url:
            raise ValueError(f"row {row_number}: payload target requires url")
        planned = resolve_payload_target(url)

    return ManifestTarget(
        row_number=row_number,
        target_id=target_id,
        purpose=purpose,
        local_event_id=local_event_id,
        local_fight_id=local_fight_id,
        target_kind=target_kind,
        url=url,
        event_id=event_id,
        fighter_id=fighter_id,
        notes=_text(row.get("notes")),
        planned_url=planned.url,
        target_type=planned.target_type,
    )


def _capture_target(
    target: ManifestTarget,
    *,
    raw_dir: Path,
    payload_dir: Path,
    dry_run: bool,
    request_interval_seconds: float,
    timeout_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
    user_agent: str,
    snapshot_probe: Callable[..., SnapshotResult],
    payload_probe: Callable[..., PayloadProbeResult],
) -> SnapshotResult | PayloadProbeResult:
    if dry_run:
        if target.target_kind == "snapshot":
            return SnapshotResult(
                dry_run=True,
                url=target.planned_url,
                target_type=target.target_type,
                html_path=None,
                metadata_path=None,
                http_status=None,
                content_sha256=None,
                bytes_written=0,
            )
        return PayloadProbeResult(
            dry_run=True,
            url=target.planned_url,
            target_type=target.target_type,
            payload_path=None,
            metadata_path=None,
            http_status=None,
            content_sha256=None,
            bytes_written=0,
        )
    if target.target_kind == "snapshot":
        return snapshot_probe(
            url=target.url or None,
            event_id=target.event_id or None,
            fighter_id=target.fighter_id or None,
            raw_dir=raw_dir,
            dry_run=dry_run,
            request_cap=1,
            request_interval_seconds=request_interval_seconds,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            user_agent=user_agent or DEFAULT_SNAPSHOT_USER_AGENT,
        )
    return payload_probe(
        url=target.url,
        raw_dir=payload_dir,
        dry_run=dry_run,
        request_cap=1,
        request_interval_seconds=request_interval_seconds,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        user_agent=user_agent or DEFAULT_PAYLOAD_USER_AGENT,
    )


def _result_row(
    target: ManifestTarget,
    result: SnapshotResult | PayloadProbeResult,
    *,
    recorded_at: str,
) -> dict[str, str]:
    artifact_path = getattr(result, "html_path", None) or getattr(result, "payload_path", None)
    return {
        **_target_row(target),
        "result_status": "dry_run" if result.dry_run else "fetched",
        "planned_url": result.url,
        "target_type": result.target_type,
        "dry_run": str(result.dry_run).lower(),
        "http_status": "" if result.http_status is None else str(result.http_status),
        "content_sha256": result.content_sha256 or "",
        "bytes_written": str(result.bytes_written),
        "artifact_path": "" if artifact_path is None else str(artifact_path),
        "metadata_path": "" if result.metadata_path is None else str(result.metadata_path),
        "recorded_at": recorded_at,
        "error": "",
    }


def _error_row(
    target: ManifestTarget,
    error: str,
    *,
    recorded_at: str,
    dry_run: bool,
) -> dict[str, str]:
    return {
        **_target_row(target),
        "result_status": "error",
        "planned_url": target.planned_url,
        "target_type": target.target_type,
        "dry_run": str(dry_run).lower(),
        "http_status": "",
        "content_sha256": "",
        "bytes_written": "0",
        "artifact_path": "",
        "metadata_path": "",
        "recorded_at": recorded_at,
        "error": error,
    }


def _target_row(target: ManifestTarget) -> dict[str, str]:
    return {
        "target_id": target.target_id,
        "purpose": target.purpose,
        "local_event_id": target.local_event_id,
        "local_fight_id": target.local_fight_id,
        "target_kind": target.target_kind,
        "url": target.url,
        "event_id": target.event_id,
        "fighter_id": target.fighter_id,
        "notes": target.notes,
    }


def _validate_request_policy(
    *,
    target_count: int,
    request_cap: int,
    request_interval_seconds: float,
    timeout_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
) -> None:
    if request_cap < 1:
        raise ValueError("request_cap must be positive")
    if request_cap > MAX_REQUEST_CAP:
        raise ValueError(f"request_cap must not exceed {MAX_REQUEST_CAP}")
    if target_count > request_cap:
        raise ValueError(f"manifest has {target_count} targets, exceeding request_cap={request_cap}")
    if request_interval_seconds < MIN_REQUEST_INTERVAL_SECONDS:
        raise ValueError("request_interval_seconds must be at least 2.0")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if max_retries < 0:
        raise ValueError("max_retries must be non-negative")
    if retry_backoff_seconds < 0:
        raise ValueError("retry_backoff_seconds must be non-negative")


def _write_csv(path: Path, columns: list[str], rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _enabled(row: dict[str, str]) -> bool:
    value = _text(row.get("enabled"))
    return value not in {"0", "false", "no", "n"}


def _required(row: dict[str, str], key: str, row_number: int) -> str:
    value = _text(row.get(key))
    if not value:
        raise ValueError(f"row {row_number}: missing {key}")
    return value


def _text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = build_arg_parser().parse_args(argv)
    try:
        result = capture_bestfightodds_manifest(
            manifest=args.manifest,
            output=args.output,
            raw_dir=args.raw_dir,
            payload_dir=args.payload_dir,
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

    label = "Dry-run" if result.dry_run else "Captured"
    print(f"{label} BestFightOdds manifest targets: {result.targets_run}")
    print(f"Wrote manifest result: {result.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
