"""Load events from data/events.csv into the warehouse events table.

Joins with data/manifests/events_manifest.csv to pick up event_status.
Re-runnable: uses upsert, so running twice is a no-op.

Usage:
    python warehouse/load_events.py
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.db import get_connection, upsert
from warehouse.transform import transform_event

REPO_ROOT = Path(__file__).resolve().parent.parent
EVENTS_CSV = REPO_ROOT / "data" / "events.csv"
MANIFEST_CSV = REPO_ROOT / "data" / "manifests" / "events_manifest.csv"


def _load_manifest(path: Path) -> dict[str, str]:
    """Return {event_id: event_status} from the manifest. Empty dict if missing."""
    if not path.exists():
        print(f"  warn  manifest not found at {path}, event_status will default to 'completed'")
        return {}
    with path.open(newline="", encoding="utf-8") as f:
        return {row["event_id"]: row["event_status"] for row in csv.DictReader(f)}


def load_events() -> None:
    manifest = _load_manifest(MANIFEST_CSV)

    rows = []
    with EVENTS_CSV.open(newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            raw["event_status"] = manifest.get(raw["event_id"], "completed")
            rows.append(transform_event(raw))

    print(f"  read  {len(rows)} rows from {EVENTS_CSV.name}")

    conn = get_connection()
    try:
        with conn:
            n = upsert(conn, "events", rows, pk_columns=["event_id"])
        print(f"  done  {n} rows upserted into events")
    finally:
        conn.close()


if __name__ == "__main__":
    load_events()
