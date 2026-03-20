"""Load fighters from data/fighters.csv into the warehouse fighters table.

Re-runnable: uses upsert.

Usage:
    python warehouse/load_fighters.py
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.db import get_connection, upsert
from warehouse.transform import transform_fighter

REPO_ROOT = Path(__file__).resolve().parent.parent
FIGHTERS_CSV = REPO_ROOT / "data" / "fighters.csv"


def load_fighters() -> None:
    rows = []
    with FIGHTERS_CSV.open(newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            rows.append(transform_fighter(raw))

    print(f"  read  {len(rows)} rows from {FIGHTERS_CSV.name}")

    conn = get_connection()
    try:
        with conn:
            n = upsert(conn, "fighters", rows, pk_columns=["fighter_id"])
        print(f"  done  {n} rows upserted into fighters")
    finally:
        conn.close()


if __name__ == "__main__":
    load_fighters()
