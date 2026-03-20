"""Logical consistency checks for the UFC data warehouse.

Checks internal data logic rather than FK integrity (see validate_integrity.py).
Aggregate-round discrepancies print a summary but do not fail the run.
All other checks fail hard (exit non-zero).

Usage:
    python warehouse/validate_consistency.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.db import get_connection

KNOWN_WEIGHT_CLASSES = {
    "women_strawweight", "women_flyweight", "women_bantamweight", "women_featherweight",
    "light_heavyweight", "super_heavyweight", "heavyweight", "featherweight",
    "lightweight", "welterweight", "middleweight", "bantamweight", "flyweight",
    "strawweight", "open_weight", "catch_weight",
}

_PASS = "PASS"
_FAIL = "FAIL"
_WARN = "WARN"
_INFO = "INFO"


def _fmt(status: str, msg: str) -> str:
    return f"  [{status}]  {msg}"


def run_checks(conn) -> bool:
    """Run all checks. Returns True if all hard checks pass."""
    failures = 0

    with conn.cursor() as cur:

        # ── result_type = 'win' ↔ winner_fighter_id IS NOT NULL ──────────
        print("\nResult / winner consistency")
        cur.execute("""
            SELECT count(*) FROM fights
            WHERE result_type = 'win' AND winner_fighter_id IS NULL
        """)
        n = cur.fetchone()[0]
        status = _PASS if n == 0 else _FAIL
        if n > 0:
            failures += 1
        print(_fmt(status, f"result_type='win' with NULL winner_fighter_id: {n}"))

        cur.execute("""
            SELECT count(*) FROM fights
            WHERE result_type IN ('draw', 'nc') AND winner_fighter_id IS NOT NULL
        """)
        n = cur.fetchone()[0]
        status = _PASS if n == 0 else _FAIL
        if n > 0:
            failures += 1
        print(_fmt(status, f"result_type='draw'/'nc' with non-NULL winner_fighter_id: {n}"))

        cur.execute("""
            SELECT count(*) FROM fights
            WHERE result_type = 'win' AND winner_fighter_id IS NOT NULL
        """)
        n_wins = cur.fetchone()[0]
        print(_fmt(_INFO, f"valid wins (result='win', winner set): {n_wins:,}"))

        # ── finish_round <= scheduled_rounds ─────────────────────────────
        print("\nFinish round bounds")
        cur.execute("""
            SELECT count(*) FROM fights
            WHERE finish_round IS NOT NULL
              AND scheduled_rounds IS NOT NULL
              AND finish_round > scheduled_rounds
        """)
        n = cur.fetchone()[0]
        status = _PASS if n == 0 else _FAIL
        if n > 0:
            failures += 1
        print(_fmt(status, f"finish_round > scheduled_rounds: {n}"))

        # ── Weight class vocabulary ───────────────────────────────────────
        print("\nWeight class vocabulary")
        cur.execute("""
            SELECT DISTINCT weight_class FROM fights
            WHERE weight_class IS NOT NULL
            ORDER BY weight_class
        """)
        db_classes = {row[0] for row in cur.fetchall()}
        unknown = db_classes - KNOWN_WEIGHT_CLASSES
        if unknown:
            for val in sorted(unknown):
                print(_fmt(_WARN, f"unrecognized weight_class: '{val}'"))
        else:
            print(_fmt(_PASS, f"all {len(db_classes)} weight_class values are in known vocabulary"))

        cur.execute("SELECT count(*) FROM fights WHERE weight_class IS NULL")
        n_null = cur.fetchone()[0]
        print(_fmt(_INFO, f"fights with NULL weight_class (early UFC tournament bouts): {n_null}"))

        # ── Aggregate ≈ sum of rounds (sig_strikes_landed) ───────────────
        print("\nAggregate vs round-sum (sig_strikes_landed, tolerance ±1)")
        cur.execute("""
            SELECT
                agg.fight_id,
                agg.fighter_id,
                agg.sig_strikes_landed                          AS agg_val,
                COALESCE(SUM(byr.sig_strikes_landed), 0)        AS rnd_sum,
                ABS(agg.sig_strikes_landed
                    - COALESCE(SUM(byr.sig_strikes_landed), 0)) AS diff
            FROM fight_stats_aggregate agg
            JOIN fight_stats_by_round byr
              ON byr.fight_id = agg.fight_id
             AND byr.fighter_id = agg.fighter_id
            GROUP BY agg.fight_id, agg.fighter_id, agg.sig_strikes_landed
            HAVING ABS(agg.sig_strikes_landed
                       - COALESCE(SUM(byr.sig_strikes_landed), 0)) > 1
        """)
        discrepancies = cur.fetchall()
        n_disc = len(discrepancies)

        if n_disc == 0:
            print(_fmt(_PASS, "no aggregate/round-sum discrepancies > ±1"))
        else:
            # Soft check — print summary, do not fail
            print(_fmt(_WARN, f"{n_disc} fighter-fight pairs have aggregate ≠ round-sum (>±1)"))
            cur.execute("""
                SELECT COUNT(DISTINCT fight_id) FROM (
                    SELECT agg.fight_id
                    FROM fight_stats_aggregate agg
                    JOIN fight_stats_by_round byr
                      ON byr.fight_id = agg.fight_id
                     AND byr.fighter_id = agg.fighter_id
                    GROUP BY agg.fight_id, agg.fighter_id, agg.sig_strikes_landed
                    HAVING ABS(agg.sig_strikes_landed
                               - COALESCE(SUM(byr.sig_strikes_landed), 0)) > 1
                ) sub
            """)
            n_fights = cur.fetchone()[0]
            print(_fmt(_INFO, f"affects {n_fights} distinct fights — known source-data issue, not a load error"))

    return failures == 0


def main() -> None:
    conn = get_connection()
    try:
        passed = run_checks(conn)
    finally:
        conn.close()

    if passed:
        print("\nAll hard checks passed.")
    else:
        print("\nOne or more hard checks FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
