"""Load fight stats from CSV into the warehouse stats tables.

Reads:
  data/fight_stats.csv          → fight_stats_aggregate
  data/fight_stats_by_round.csv → fight_stats_by_round

Skips rows whose fight_id is not present in the fights table (logs a warning).
Re-runnable: uses upsert.

Usage:
    python warehouse/load_fight_stats.py
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.db import get_connection, upsert
from warehouse.transform import transform_fight_stat

REPO_ROOT = Path(__file__).resolve().parent.parent
STATS_CSV = REPO_ROOT / "data" / "fight_stats.csv"
STATS_BY_ROUND_CSV = REPO_ROOT / "data" / "fight_stats_by_round.csv"


def _known_fight_ids(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT fight_id FROM fights")
        return {str(row[0]) for row in cur.fetchall()}


def _load_csv(path: Path, known_fights: set[str], by_round: bool) -> tuple[list[dict], int]:
    rows = []
    skipped = 0
    with path.open(newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            if raw["fight_id"] not in known_fights:
                skipped += 1
                continue
            rows.append(transform_fight_stat(raw, by_round=by_round))
    return rows, skipped


def load_fight_stats() -> None:
    conn = get_connection()
    try:
        known_fights = _known_fight_ids(conn)

        # --- aggregate ---
        rows, skipped = _load_csv(STATS_CSV, known_fights, by_round=False)
        print(f"  read  {len(rows)} rows from {STATS_CSV.name} ({skipped} skipped — unknown fight_id)")
        with conn:
            n = upsert(conn, "fight_stats_aggregate", rows, pk_columns=["fight_stat_id"])
        print(f"  done  {n} rows upserted into fight_stats_aggregate")

        # --- by round ---
        rows, skipped = _load_csv(STATS_BY_ROUND_CSV, known_fights, by_round=True)
        print(f"  read  {len(rows)} rows from {STATS_BY_ROUND_CSV.name} ({skipped} skipped — unknown fight_id)")
        with conn:
            n = upsert(conn, "fight_stats_by_round", rows, pk_columns=["fight_stat_by_round_id"])
        print(f"  done  {n} rows upserted into fight_stats_by_round")
    finally:
        conn.close()


if __name__ == "__main__":
    load_fight_stats()
