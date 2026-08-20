"""Tests for explicit BestFightOdds manifest capture."""

from __future__ import annotations

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from warehouse.capture_bestfightodds_manifest import (
    MANIFEST_COLUMNS,
    capture_bestfightodds_manifest,
    read_manifest_targets,
)
from warehouse.probe_bestfightodds_line_history_payload import PayloadProbeResult
from warehouse.probe_bestfightodds_snapshot import SnapshotResult


def test_read_manifest_targets_accepts_explicit_snapshot_and_payload(tmp_path):
    manifest = tmp_path / "targets.csv"
    _write_manifest(manifest, [
        {
            "target_id": "event-1",
            "purpose": "historical_backfill",
            "local_event_id": "local-event",
            "local_fight_id": "",
            "target_kind": "snapshot",
            "url": "",
            "event_id": "ufc-330-4237",
            "fighter_id": "",
            "notes": "event page",
        },
        {
            "target_id": "payload-1",
            "purpose": "historical_backfill",
            "local_event_id": "local-event",
            "local_fight_id": "local-fight",
            "target_kind": "payload",
            "url": "https://www.bestfightodds.com/api/ggd?m=43741&p=1",
            "event_id": "",
            "fighter_id": "",
            "notes": "line history",
        },
    ])

    targets = read_manifest_targets(manifest)

    assert len(targets) == 2
    assert targets[0].planned_url == "https://www.bestfightodds.com/events/ufc-330-4237"
    assert targets[0].target_type == "event"
    assert targets[1].planned_url == "https://www.bestfightodds.com/api/ggd?m=43741&p=1"
    assert targets[1].target_type == "detail_payload"


def test_read_manifest_targets_rejects_missing_local_target(tmp_path):
    manifest = tmp_path / "targets.csv"
    _write_manifest(manifest, [{
        "target_id": "bad",
        "purpose": "historical_backfill",
        "local_event_id": "",
        "local_fight_id": "",
        "target_kind": "snapshot",
        "url": "https://www.bestfightodds.com/events/ufc-330-4237",
        "event_id": "",
        "fighter_id": "",
        "notes": "",
    }])

    with pytest.raises(ValueError, match="local_event_id or local_fight_id"):
        read_manifest_targets(manifest)


def test_manifest_capture_refuses_targets_above_request_cap(tmp_path):
    manifest = tmp_path / "targets.csv"
    _write_manifest(manifest, [
        _snapshot_row("event-1"),
        _snapshot_row("event-2", event_id="ufc-sacramento-4320"),
    ])

    with pytest.raises(ValueError, match="exceeding request_cap=1"):
        capture_bestfightodds_manifest(
            manifest=manifest,
            output=tmp_path / "out.csv",
            dry_run=True,
            request_cap=1,
        )


def test_manifest_dry_run_writes_result_without_calling_probes(tmp_path):
    manifest = tmp_path / "targets.csv"
    output = tmp_path / "out.csv"
    _write_manifest(manifest, [_snapshot_row("event-1")])

    def fail_snapshot_probe(**kwargs):
        raise AssertionError("dry-run should not fetch")

    result = capture_bestfightodds_manifest(
        manifest=manifest,
        output=output,
        dry_run=True,
        snapshot_probe=fail_snapshot_probe,
        recorded_at=datetime(2026, 8, 19, 12, tzinfo=timezone.utc),
    )

    assert result.dry_run is True
    assert result.targets_run == 1
    assert output.exists()
    rows = _read_csv(output)
    assert rows[0]["result_status"] == "dry_run"
    assert rows[0]["planned_url"] == "https://www.bestfightodds.com/events/ufc-330-4237"
    assert rows[0]["artifact_path"] == ""


def test_manifest_capture_uses_injected_probes_and_delay(tmp_path):
    manifest = tmp_path / "targets.csv"
    output = tmp_path / "out.csv"
    _write_manifest(manifest, [
        _snapshot_row("event-1"),
        {
            "target_id": "payload-1",
            "purpose": "historical_backfill",
            "local_event_id": "local-event",
            "local_fight_id": "local-fight",
            "target_kind": "payload",
            "url": "https://www.bestfightodds.com/api/ggd?m=43741&p=1",
            "event_id": "",
            "fighter_id": "",
            "notes": "line history",
        },
    ])
    calls = []
    sleeps = []

    def fake_snapshot_probe(**kwargs):
        calls.append(("snapshot", kwargs["url"], kwargs["event_id"], kwargs["dry_run"]))
        return SnapshotResult(
            dry_run=False,
            url="https://www.bestfightodds.com/events/ufc-330-4237",
            target_type="event",
            html_path=tmp_path / "event.html",
            metadata_path=tmp_path / "event.metadata.json",
            http_status=200,
            content_sha256="abc",
            bytes_written=12,
        )

    def fake_payload_probe(**kwargs):
        calls.append(("payload", kwargs["url"], kwargs["dry_run"]))
        return PayloadProbeResult(
            dry_run=False,
            url="https://www.bestfightodds.com/api/ggd?m=43741&p=1",
            target_type="detail_payload",
            payload_path=tmp_path / "payload.html",
            metadata_path=tmp_path / "payload.metadata.json",
            http_status=200,
            content_sha256="def",
            bytes_written=34,
        )

    result = capture_bestfightodds_manifest(
        manifest=manifest,
        output=output,
        dry_run=False,
        request_cap=2,
        request_interval_seconds=2.5,
        snapshot_probe=fake_snapshot_probe,
        payload_probe=fake_payload_probe,
        sleeper=sleeps.append,
        recorded_at=datetime(2026, 8, 19, 12, tzinfo=timezone.utc),
    )

    assert result.targets_run == 2
    assert calls == [
        ("snapshot", None, "ufc-330-4237", False),
        ("payload", "https://www.bestfightodds.com/api/ggd?m=43741&p=1", False),
    ]
    assert sleeps == [2.5]
    rows = _read_csv(output)
    assert [row["result_status"] for row in rows] == ["fetched", "fetched"]
    assert rows[0]["artifact_path"].endswith("event.html")
    assert rows[1]["artifact_path"].endswith("payload.html")


def _snapshot_row(target_id: str, *, event_id: str = "ufc-330-4237") -> dict[str, str]:
    return {
        "target_id": target_id,
        "purpose": "historical_backfill",
        "local_event_id": "local-event",
        "local_fight_id": "",
        "target_kind": "snapshot",
        "url": "",
        "event_id": event_id,
        "fighter_id": "",
        "notes": "event page",
    }


def _write_manifest(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))
