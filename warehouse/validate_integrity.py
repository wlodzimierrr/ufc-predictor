"""Post-load integrity checks for the UFC data warehouse.

Prints PASS/FAIL per check; exits non-zero if any hard (FK) check fails.

Usage:
    python warehouse/validate_integrity.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.db import get_connection

# ---------------------------------------------------------------------------
# Expected row counts (derived from CSV line counts minus header)
# Ranges allow for intentional skips (e.g. stats rows with unknown fight_id)
# ---------------------------------------------------------------------------
EXPECTED = {
    "events":                {"min": 760,   "max": 800},
    "fighters":              {"min": 4400,  "max": 4600},
    "fights":                {"min": 8500,  "max": 8700},
    "fight_stats_aggregate": {"min": 17000, "max": 17200},
    "fight_stats_by_round":  {"min": 40000, "max": 41000},
}

_PASS = "PASS"
_FAIL = "FAIL"
_INFO = "INFO"


def _fmt(status: str, msg: str) -> str:
    return f"  [{status}]  {msg}"


def run_checks(conn) -> bool:
    """Run all checks. Returns True if all hard checks pass."""
    failures = 0

    with conn.cursor() as cur:

        # ── Row counts ────────────────────────────────────────────────────
        print("\nRow counts")
        for table, bounds in EXPECTED.items():
            cur.execute(f"SELECT count(*) FROM {table}")
            n = cur.fetchone()[0]
            ok = bounds["min"] <= n <= bounds["max"]
            status = _PASS if ok else _FAIL
            if not ok:
                failures += 1
            print(_fmt(status, f"{table}: {n:,} rows (expected {bounds['min']:,}–{bounds['max']:,})"))

        # ── FK: fights → events ───────────────────────────────────────────
        print("\nFK completeness")
        cur.execute("""
            SELECT count(*) FROM fights f
            WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.event_id = f.event_id)
        """)
        n = cur.fetchone()[0]
        status = _PASS if n == 0 else _FAIL
        if n > 0:
            failures += 1
        print(_fmt(status, f"fights → events: {n} orphaned rows"))

        # ── FK: fights → fighters (fighter_1_id) ─────────────────────────
        cur.execute("""
            SELECT count(*) FROM fights f
            WHERE NOT EXISTS (SELECT 1 FROM fighters fi WHERE fi.fighter_id = f.fighter_1_id)
        """)
        n = cur.fetchone()[0]
        status = _PASS if n == 0 else _FAIL
        if n > 0:
            failures += 1
        print(_fmt(status, f"fights.fighter_1_id → fighters: {n} orphaned rows"))

        # ── FK: fights → fighters (fighter_2_id) ─────────────────────────
        cur.execute("""
            SELECT count(*) FROM fights f
            WHERE NOT EXISTS (SELECT 1 FROM fighters fi WHERE fi.fighter_id = f.fighter_2_id)
        """)
        n = cur.fetchone()[0]
        status = _PASS if n == 0 else _FAIL
        if n > 0:
            failures += 1
        print(_fmt(status, f"fights.fighter_2_id → fighters: {n} orphaned rows"))

        # ── FK: fight_stats_aggregate → fights ────────────────────────────
        cur.execute("""
            SELECT count(*) FROM fight_stats_aggregate s
            WHERE NOT EXISTS (SELECT 1 FROM fights f WHERE f.fight_id = s.fight_id)
        """)
        n = cur.fetchone()[0]
        status = _PASS if n == 0 else _FAIL
        if n > 0:
            failures += 1
        print(_fmt(status, f"fight_stats_aggregate → fights: {n} orphaned rows"))

        # ── FK: fight_stats_by_round → fights ─────────────────────────────
        cur.execute("""
            SELECT count(*) FROM fight_stats_by_round s
            WHERE NOT EXISTS (SELECT 1 FROM fights f WHERE f.fight_id = s.fight_id)
        """)
        n = cur.fetchone()[0]
        status = _PASS if n == 0 else _FAIL
        if n > 0:
            failures += 1
        print(_fmt(status, f"fight_stats_by_round → fights: {n} orphaned rows"))

        # ── FK: fight_stats_aggregate → fighters ──────────────────────────
        cur.execute("""
            SELECT count(*) FROM fight_stats_aggregate s
            WHERE NOT EXISTS (SELECT 1 FROM fighters fi WHERE fi.fighter_id = s.fighter_id)
        """)
        n = cur.fetchone()[0]
        status = _PASS if n == 0 else _FAIL
        if n > 0:
            failures += 1
        print(_fmt(status, f"fight_stats_aggregate → fighters: {n} orphaned rows"))

        # ── Informational: fights with no stats ───────────────────────────
        print("\nInformational")
        cur.execute("""
            SELECT count(*) FROM fights f
            WHERE NOT EXISTS (
                SELECT 1 FROM fight_stats_aggregate s WHERE s.fight_id = f.fight_id
            )
        """)
        n = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM fights")
        total_fights = cur.fetchone()[0]
        pct = n / total_fights * 100 if total_fights else 0
        print(_fmt(_INFO, f"fights with no stats: {n} / {total_fights:,} ({pct:.1f}%) — expected ~0–2%"))

        # ── Informational: aggregate vs by-round ratio ────────────────────
        cur.execute("SELECT count(*) FROM fight_stats_aggregate")
        agg = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM fight_stats_by_round")
        byr = cur.fetchone()[0]
        ratio = byr / agg if agg else 0
        print(_fmt(_INFO, f"by-round / aggregate ratio: {ratio:.1f}× (expected 3–5×)"))

        # ── Informational: result_type distribution ───────────────────────
        cur.execute("SELECT result_type, count(*) FROM fights GROUP BY result_type ORDER BY count(*) DESC")
        rows = cur.fetchall()
        dist = ", ".join(f"{rt}: {n:,}" for rt, n in rows)
        print(_fmt(_INFO, f"result_type distribution — {dist}"))

    return failures == 0


def main() -> None:
    conn = get_connection()
    try:
        passed = run_checks(conn)
    finally:
        conn.close()

    if passed:
        print("\nAll checks passed.")
    else:
        print("\nOne or more checks FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
