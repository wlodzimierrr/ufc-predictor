"""Leakage prevention tests — integration tests against the homelab DB.

Proves that no feature uses data from the target fight or any fight after
the cutoff date. These are the safety net for the entire feature pipeline.

Usage:
    python -m pytest features/tests/test_leakage.py -v
    # or: make test_leakage
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from warehouse.db import get_connection


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def conn():
    c = get_connection()
    yield c
    c.close()


@pytest.fixture(scope="module")
def cur(conn):
    with conn.cursor() as cur:
        yield cur


# ── Temporal exclusion ────────────────────────────────────────────────────────

class TestTemporalExclusion:
    """For every snapshot, career_fights must equal the count of that fighter's
    fights strictly before the snapshot's as_of_date."""

    def test_career_fights_matches_prior_count(self, cur):
        """Sample 200 snapshots and verify career_fights = count of prior fights."""
        cur.execute("""
            SELECT fs.fighter_id, fs.fight_id, fs.as_of_date, fs.career_fights
            FROM fighter_snapshots fs
            ORDER BY random()
            LIMIT 200
        """)
        rows = cur.fetchall()
        assert len(rows) > 0, "No snapshots found — run make build_features first"

        mismatches = []
        for fighter_id, fight_id, as_of_date, career_fights in rows:
            cur.execute("""
                SELECT count(*) FROM fights f
                JOIN events e ON e.event_id = f.event_id
                WHERE (f.fighter_1_id = %s OR f.fighter_2_id = %s)
                  AND e.event_date < %s
            """, (fighter_id, fighter_id, as_of_date))
            actual_prior = cur.fetchone()[0]
            if career_fights != actual_prior:
                mismatches.append(
                    f"fighter={fighter_id} fight={fight_id} "
                    f"as_of={as_of_date} snapshot={career_fights} actual={actual_prior}"
                )

        assert mismatches == [], (
            f"{len(mismatches)} snapshots have career_fights != count of prior fights:\n"
            + "\n".join(mismatches[:10])
        )


# ── Target fight exclusion ────────────────────────────────────────────────────

class TestTargetFightExclusion:
    """The target fight's own stats must never be included in the snapshot.
    If a fighter had N prior fights, the snapshot's career_fights must be N,
    not N+1."""

    def test_target_fight_not_counted(self, cur):
        """For 200 sampled snapshots, career_fights must NOT include the target fight."""
        cur.execute("""
            SELECT fs.fighter_id, fs.fight_id, fs.as_of_date, fs.career_fights
            FROM fighter_snapshots fs
            ORDER BY random()
            LIMIT 200
        """)
        rows = cur.fetchall()

        for fighter_id, fight_id, as_of_date, career_fights in rows:
            # Count fights strictly before (excludes same-date fights = target fight)
            cur.execute("""
                SELECT count(*) FROM fights f
                JOIN events e ON e.event_id = f.event_id
                WHERE (f.fighter_1_id = %s OR f.fighter_2_id = %s)
                  AND e.event_date < %s
            """, (fighter_id, fighter_id, as_of_date))
            prior = cur.fetchone()[0]

            # Count fights on or after (includes target fight)
            cur.execute("""
                SELECT count(*) FROM fights f
                JOIN events e ON e.event_id = f.event_id
                WHERE (f.fighter_1_id = %s OR f.fighter_2_id = %s)
                  AND e.event_date >= %s
            """, (fighter_id, fighter_id, as_of_date))
            on_or_after = cur.fetchone()[0]

            assert on_or_after >= 1, (
                f"Fighter {fighter_id} should have at least the target fight on/after {as_of_date}"
            )
            assert career_fights == prior, (
                f"Fighter {fighter_id} fight {fight_id}: "
                f"career_fights={career_fights} but prior count={prior} "
                f"(target fight may be leaking)"
            )


# ── Monotonic history ─────────────────────────────────────────────────────────

class TestMonotonicHistory:
    """For any fighter with multiple fights, career_fights must be strictly
    non-decreasing and must increase between distinct event dates.

    Early UFC events had single-night tournaments where one fighter could
    have 2-4 fights on the same date. All same-date fights share the same
    career_fights count (since cutoff is strict <). When the date advances,
    career_fights must jump by the number of same-date fights on the previous date.
    """

    def test_career_fights_strictly_increases_across_dates(self, cur):
        """Pick 50 fighters with 5+ fights and check monotonicity."""
        cur.execute("""
            SELECT fighter_id, count(*) as n
            FROM fighter_snapshots
            GROUP BY fighter_id
            HAVING count(*) >= 5
            ORDER BY random()
            LIMIT 50
        """)
        fighters = cur.fetchall()
        assert len(fighters) > 0

        violations = []
        for (fighter_id, _) in fighters:
            cur.execute("""
                SELECT fs.as_of_date, fs.career_fights
                FROM fighter_snapshots fs
                WHERE fs.fighter_id = %s
                ORDER BY fs.as_of_date, fs.career_fights
            """, (fighter_id,))
            snapshots = cur.fetchall()

            # Group by date
            from itertools import groupby
            groups = []
            for date_val, rows in groupby(snapshots, key=lambda r: r[0]):
                rows_list = list(rows)
                cf = rows_list[0][1]
                count_on_date = len(rows_list)
                # All same-date snapshots must have the same career_fights
                for r in rows_list:
                    if r[1] != cf:
                        violations.append(
                            f"fighter={fighter_id} date={date_val}: "
                            f"same-date career_fights mismatch {cf} vs {r[1]}"
                        )
                groups.append((date_val, cf, count_on_date))

            # Between distinct dates, career_fights must increase
            for i in range(1, len(groups)):
                prev_date, prev_cf, prev_count = groups[i - 1]
                curr_date, curr_cf, _ = groups[i]
                expected = prev_cf + prev_count
                if curr_cf != expected:
                    violations.append(
                        f"fighter={fighter_id} {prev_date}→{curr_date}: "
                        f"career_fights {prev_cf}→{curr_cf} "
                        f"(expected {expected}, had {prev_count} fights on {prev_date})"
                    )

        assert violations == [], (
            f"{len(violations)} monotonicity violations:\n"
            + "\n".join(violations[:10])
        )


# ── Elo causality ────────────────────────────────────────────────────────────

class TestEloCausality:
    """A fighter's Elo must only reflect outcomes of prior fights.
    For debut fighters, Elo must be the initial value (1500)."""

    def test_debut_elo_is_initial(self, cur):
        """Fighters whose first-ever fight is the only fight on that date
        must have elo_rating=1500 in that snapshot.

        Fighters with same-date tournament bouts may have non-1500 Elo in their
        second same-date fight (Elo updates within-event are intentional and not
        leakage — the prior bout on the same card has already happened).
        """
        # Find fighters whose earliest date has exactly 1 snapshot (no tournament)
        cur.execute("""
            WITH first_dates AS (
                SELECT fighter_id, min(as_of_date) AS first_date
                FROM fighter_snapshots
                GROUP BY fighter_id
            ),
            first_date_counts AS (
                SELECT fd.fighter_id, fd.first_date, count(*) AS n
                FROM first_dates fd
                JOIN fighter_snapshots fs
                  ON fs.fighter_id = fd.fighter_id AND fs.as_of_date = fd.first_date
                GROUP BY fd.fighter_id, fd.first_date
            )
            SELECT fs.fighter_id, fs.elo_rating
            FROM first_date_counts fdc
            JOIN fighter_snapshots fs
              ON fs.fighter_id = fdc.fighter_id AND fs.as_of_date = fdc.first_date
            WHERE fdc.n = 1
              AND fs.elo_rating != 1500
        """)
        bad = cur.fetchall()
        assert len(bad) == 0, (
            f"{len(bad)} fighters with single first-date fight have non-1500 Elo:\n"
            + "\n".join(f"  {r[0]}: elo={r[1]}" for r in bad[:10])
        )

    def test_elo_does_not_use_future_fights(self, cur):
        """A fighter's Elo must be non-decreasing through wins and non-increasing
        through losses, only reflecting prior outcomes.

        Specifically: for fighters with fights on distinct dates (no same-date
        ambiguity), check that Elo after a win is >= Elo before, and Elo after
        a loss is <= Elo before.
        """
        # Pick 30 fighters with 3+ fights on distinct dates
        cur.execute("""
            SELECT fighter_id FROM fighter_snapshots
            GROUP BY fighter_id
            HAVING count(*) >= 3 AND count(*) = count(DISTINCT as_of_date)
            ORDER BY random() LIMIT 30
        """)
        fighters = [r[0] for r in cur.fetchall()]
        assert len(fighters) > 0

        violations = []
        for fid in fighters:
            cur.execute("""
                SELECT fs.fight_id, fs.as_of_date, fs.elo_rating
                FROM fighter_snapshots fs
                WHERE fs.fighter_id = %s
                ORDER BY fs.as_of_date
            """, (fid,))
            snapshots = cur.fetchall()

            # First fight: Elo must be 1500
            if float(snapshots[0][2]) != 1500.0:
                violations.append(
                    f"fighter={fid}: first Elo={snapshots[0][2]}, expected 1500"
                )

            # For each pair of consecutive fights, check direction
            for i in range(1, len(snapshots)):
                prev_fid, prev_date, prev_elo = snapshots[i - 1]
                curr_fid, curr_date, curr_elo = snapshots[i]

                # Look up what happened in the previous fight
                cur.execute("""
                    SELECT f.result_type, f.winner_fighter_id
                    FROM fights f WHERE f.fight_id = %s
                """, (prev_fid,))
                result_type, winner_id = cur.fetchone()

                if result_type == "win" and str(winner_id) == str(fid):
                    # Won → Elo should increase
                    if float(curr_elo) < float(prev_elo):
                        violations.append(
                            f"fighter={fid} {prev_date}→{curr_date}: "
                            f"won but Elo dropped {prev_elo}→{curr_elo}"
                        )
                elif result_type == "win":
                    # Lost → Elo should decrease
                    if float(curr_elo) > float(prev_elo):
                        violations.append(
                            f"fighter={fid} {prev_date}→{curr_date}: "
                            f"lost but Elo rose {prev_elo}→{curr_elo}"
                        )

        assert violations == [], (
            f"{len(violations)} Elo causality violations:\n"
            + "\n".join(violations[:10])
        )


# ── Label isolation ──────────────────────────────────────────────────────────

class TestLabelIsolation:
    """bout_features.label must be derived purely from fights.result_type and
    fights.winner_fighter_id — not from any feature column."""

    def test_label_matches_fight_outcome(self, cur):
        """Every bout_features.label must match the fights table outcome."""
        cur.execute("""
            SELECT
                bf.fight_id,
                bf.label,
                bf.fighter_1_id,
                bf.fighter_2_id,
                f.result_type,
                f.winner_fighter_id
            FROM bout_features bf
            JOIN fights f ON f.fight_id = bf.fight_id
        """)
        rows = cur.fetchall()
        assert len(rows) > 0

        mismatches = []
        for fight_id, label, f1_id, f2_id, result_type, winner_id in rows:
            if result_type == "win":
                if str(winner_id) == str(f1_id):
                    expected = 1
                elif str(winner_id) == str(f2_id):
                    expected = 0
                else:
                    expected = None
            else:
                expected = None

            if label != expected:
                mismatches.append(
                    f"fight={fight_id} label={label} expected={expected} "
                    f"result_type={result_type} winner={winner_id}"
                )

        assert mismatches == [], (
            f"{len(mismatches)} label mismatches:\n"
            + "\n".join(mismatches[:10])
        )

    def test_draws_and_nc_have_null_label(self, cur):
        """Draws and no-contests must have label=NULL."""
        cur.execute("""
            SELECT count(*) FROM bout_features bf
            JOIN fights f ON f.fight_id = bf.fight_id
            WHERE f.result_type IN ('draw', 'nc')
              AND bf.label IS NOT NULL
        """)
        n = cur.fetchone()[0]
        assert n == 0, f"{n} draw/NC bouts have non-NULL label"
