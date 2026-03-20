"""Load fights from data/fights.csv into the warehouse fights table.

Skips any row whose event_id is not present in the events table (logs a
warning rather than crashing — handles partial data sets gracefully).
Re-runnable: uses upsert.

Usage:
    python warehouse/load_fights.py
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.db import get_connection, upsert
from warehouse.transform import transform_fight

REPO_ROOT = Path(__file__).resolve().parent.parent
FIGHTS_CSV = REPO_ROOT / "data" / "fights.csv"


def _known_event_ids(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT event_id FROM events")
        return {str(row[0]) for row in cur.fetchall()}


def load_fights() -> None:
    conn = get_connection()
    try:
        known_events = _known_event_ids(conn)

        rows = []
        skipped = 0
        with FIGHTS_CSV.open(newline="", encoding="utf-8") as f:
            for raw in csv.DictReader(f):
                if raw["event_id"] not in known_events:
                    print(f"  warn  unknown event_id {raw['event_id']} for fight {raw['fight_id']} — skipping")
                    skipped += 1
                    continue
                rows.append(transform_fight(raw))

        print(f"  read  {len(rows)} rows from {FIGHTS_CSV.name} ({skipped} skipped — unknown event_id)")

        with conn:
            n = upsert(conn, "fights", rows, pk_columns=["fight_id"])
        print(f"  done  {n} rows upserted into fights")
    finally:
        conn.close()


if __name__ == "__main__":
    load_fights()
