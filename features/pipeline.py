"""Full feature build pipeline.

Loads all warehouse data, computes Elos, builds fighter snapshots and bout
feature rows for every fight, then persists to the database.

Usage:
    python features/pipeline.py
    # or: make build_features
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from features.data_loader import load_all_data
from features.history import build_fighter_index, get_history
from features.elo import compute_all_elos, get_fighter_elo_features
from features.snapshot import build_fighter_snapshot
from features.bout import build_bout_features
from warehouse.db import get_connection, upsert


# ── Column mapping ────────────────────────────────────────────────────────────
# The feature modules produce keys that don't always match the DDL column names.
# These mapping functions translate module output → exact DDL columns.


def _snapshot_to_row(snap: dict) -> dict:
    """Map a build_fighter_snapshot() output to the fighter_snapshots DDL schema."""
    now = datetime.now(timezone.utc)

    def _g(key, default=None):
        return snap.get(key, default)

    # Rolling helper: module uses last{N}_{metric}, DDL uses {metric}_last{N}
    def _roll(metric_module: str, metric_ddl: str, n: int):
        return _g(f"last{n}_{metric_module}")

    def _win_rate_last(n: int):
        wins = _g(f"last{n}_wins")
        # has_{n}_fights tells us if we had enough fights
        available = _g(f"has_{n}_fights", False)
        total = _g("total_fights", 0)
        window = min(n, total) if total else 0
        if wins is not None and window > 0:
            return wins / window
        return None

    row = {
        "fighter_id": snap["fighter_id"],
        "fight_id": snap["fight_id"],
        "as_of_date": snap["as_of_date"],
        "feature_version": snap.get("feature_version", 1),

        # ── Career ─────────────────────────────────────────────────────────
        "career_fights": _g("total_fights", 0),
        "career_wins": _g("wins", 0),
        "career_losses": _g("losses", 0),
        "career_draws": _g("draws", 0),
        "career_nc": _g("no_contests", 0),
        "career_win_rate": _g("win_rate"),
        "career_finish_rate": _g("finish_rate"),
        "career_ko_tko_wins": _g("ko_tko_wins", 0),
        "career_sub_wins": _g("sub_wins", 0),
        "career_dec_wins": _g("dec_wins", 0),
        "career_ko_tko_losses": _g("ko_tko_losses", 0),
        "career_sub_losses": _g("sub_losses", 0),
        "career_title_fights": _g("title_fights", 0),
        "career_title_wins": _g("title_wins", 0),
        "career_minutes": (_g("total_cage_time_seconds", 0) or 0) / 60.0,
        "career_sig_strikes_landed_pm": _g("career_sig_strikes_landed_per_min"),
        "career_sig_strikes_absorbed_pm": _g("career_sig_strikes_absorbed_per_min"),
        "career_sig_strike_accuracy": _g("career_sig_strike_accuracy"),
        "career_sig_strike_defense": _g("career_sig_strike_defense"),
        "career_takedown_accuracy": _g("career_takedown_accuracy"),
        "career_takedown_defense": _g("career_takedown_defense"),
        "career_sub_attempts_pm": _g("career_submissions_per_15min"),  # approx
        "career_control_rate": _g("career_control_time_per_fight"),  # per-fight stored
        "career_knockdowns_pm": _g("career_knockdowns_per_fight"),  # per-fight stored
    }

    # ── Rolling windows ────────────────────────────────────────────────────
    for n in (1, 3, 5):
        row.update({
            f"win_rate_last{n}":                _win_rate_last(n),
            f"finish_rate_last{n}":             _roll("finish_rate", "finish_rate", n),
            f"sig_strikes_landed_pm_last{n}":   _roll("sig_strikes_landed_per_min", "sig_strikes_landed_pm", n),
            f"sig_strikes_absorbed_pm_last{n}": _roll("sig_strikes_absorbed_per_min", "sig_strikes_absorbed_pm", n),
            f"sig_strike_accuracy_last{n}":     _roll("sig_strike_accuracy", "sig_strike_accuracy", n),
            f"sig_strike_defense_last{n}":      None,  # not computed by rolling module
            f"takedown_landed_pm_last{n}":      None,  # not computed by rolling module
            f"takedown_accuracy_last{n}":       _roll("takedown_accuracy", "takedown_accuracy", n),
            f"takedown_defense_last{n}":        None,  # not computed by rolling module
            f"control_rate_last{n}":            _roll("control_time_per_fight", "control_rate", n),
            f"knockdowns_pm_last{n}":           _roll("knockdowns_per_fight", "knockdowns_pm", n),
            f"knockdowns_absorbed_pm_last{n}":  None,  # not computed by rolling module
            f"sub_attempts_pm_last{n}":         None,  # not computed by rolling module
            f"avg_fight_time_last{n}":          None,  # not computed by rolling module
            f"streak_last{n}":                  None,  # not computed by rolling module
        })

    # ── Decay ──────────────────────────────────────────────────────────────
    row.update({
        "sig_strikes_landed_pm_decay": _g("decay_sig_strike_rate"),
        "sig_strikes_absorbed_pm_decay": None,  # not computed
        "sig_strike_accuracy_decay": _g("decay_sig_strike_accuracy"),
        "sig_strike_defense_decay": None,
        "takedown_landed_pm_decay": _g("decay_takedown_rate"),
        "takedown_accuracy_decay": _g("decay_takedown_accuracy"),
        "takedown_defense_decay": None,
        "control_rate_decay": _g("decay_control_time_per_fight"),
        "knockdowns_pm_decay": _g("decay_knockdowns_per_fight"),
        "win_rate_decay": _g("decay_win_rate"),
    })

    # ── Physical / demographic / activity ──────────────────────────────────
    row.update({
        "age": _g("age_at_fight"),
        "age_squared": _g("age_squared"),
        "height_cm": _g("height_cm"),
        "reach_cm": _g("reach_cm"),
        "reach_to_height": _g("reach_to_height_ratio"),
        "is_orthodox": _g("stance") == "orthodox",
        "is_southpaw": _g("stance") == "southpaw",
        "days_since_last_fight": _g("days_since_last_fight"),
        "is_long_layoff": bool(_g("is_long_layoff", False)),
        "is_short_notice": False,  # not computed (proxy not reliable enough)
        "is_debut": bool(_g("is_debut", False)),
        "age_missing": bool(_g("dob_missing", False)),
        "height_reach_missing": bool(_g("height_missing", False) or _g("reach_missing", False)),
    })

    # ── Elo / opponent ─────────────────────────────────────────────────────
    row.update({
        "elo_rating": _g("pre_fight_elo", 1500),
        "elo_opponent": _g("opponent_pre_fight_elo"),
        "elo_diff": (
            (_g("pre_fight_elo") or 1500) - (_g("opponent_pre_fight_elo") or 1500)
            if _g("opponent_pre_fight_elo") is not None else None
        ),
        "opp_avg_elo": _g("avg_opponent_elo"),
        "opp_adj_sig_strike_accuracy": _g("opp_adjusted_sig_strike_rate"),
    })

    row["computed_at"] = now
    return row


def _bout_to_row(fight: dict, snap_a: dict, snap_b: dict) -> dict:
    """Build a bout_features row from a fight dict and two snapshot dicts.

    Uses the raw snapshot dicts (module output) to compute the full set of
    DDL-compatible difference and ratio features.
    """
    now = datetime.now(timezone.utc)

    def _diff(key_a, key_b=None):
        va = snap_a.get(key_a)
        vb = snap_b.get(key_b or key_a)
        return va - vb if va is not None and vb is not None else None

    def _ratio(key_a, key_b=None):
        va = snap_a.get(key_a)
        vb = snap_b.get(key_b or key_a)
        if va is None or vb is None:
            return None
        total = va + vb
        return va / total if total else None

    # Label
    winner = fight.get("winner_fighter_id")
    result_type = fight.get("result_type")
    if result_type == "win" and winner == fight["fighter_1_id"]:
        label = 1
    elif result_type == "win" and winner == fight["fighter_2_id"]:
        label = 0
    else:
        label = None

    # Matchup flags
    stance_a = snap_a.get("stance")
    stance_b = snap_b.get("stance")
    is_orth_vs_south = (
        (stance_a == "orthodox" and stance_b == "southpaw")
        or (stance_a == "southpaw" and stance_b == "orthodox")
    ) if stance_a and stance_b else False

    both_debuting = (
        bool(snap_a.get("is_debut")) and bool(snap_b.get("is_debut"))
    )

    return {
        "fight_id": fight["fight_id"],
        "fighter_1_id": fight["fighter_1_id"],
        "fighter_2_id": fight["fighter_2_id"],
        "event_date": fight.get("event_date"),
        "weight_class": fight.get("weight_class"),
        "is_title_fight": bool(fight.get("is_title_fight")),
        "scheduled_rounds": fight.get("scheduled_rounds"),
        "label": label,
        "feature_version": 1,

        # Differences (fighter_1 - fighter_2)
        "diff_elo": _diff("pre_fight_elo"),
        "diff_career_wins": _diff("wins"),
        "diff_career_fights": _diff("total_fights"),
        "diff_career_win_rate": _diff("win_rate"),
        "diff_career_finish_rate": _diff("finish_rate"),
        "diff_career_sig_strikes_landed_pm": _diff("career_sig_strikes_landed_per_min"),
        "diff_career_sig_strike_accuracy": _diff("career_sig_strike_accuracy"),
        "diff_career_takedown_accuracy": _diff("career_takedown_accuracy"),
        "diff_career_control_rate": _diff("career_control_time_per_fight"),
        "diff_age": _diff("age_at_fight"),
        "diff_height_cm": _diff("height_cm"),
        "diff_reach_cm": _diff("reach_cm"),
        "diff_days_since_last_fight": _diff("days_since_last_fight"),
        "diff_win_rate_last3": (
            _diff_rolling_win_rate(snap_a, snap_b, 3)
        ),
        "diff_sig_strikes_landed_pm_last3": _diff("last3_sig_strikes_landed_per_min"),
        "diff_takedown_accuracy_last3": _diff("last3_takedown_accuracy"),
        "diff_control_rate_last3": _diff("last3_control_time_per_fight"),
        "diff_sig_strikes_landed_pm_decay": _diff("decay_sig_strike_rate"),
        "diff_win_rate_decay": _diff("decay_win_rate"),
        "diff_opp_avg_elo": _diff("avg_opponent_elo"),

        # Ratios (fighter_1 / (fighter_1 + fighter_2))
        "ratio_career_wins": _ratio("wins"),
        "ratio_career_fights": _ratio("total_fights"),
        "ratio_career_sig_strikes_landed_pm": _ratio("career_sig_strikes_landed_per_min"),
        "ratio_career_control_rate": _ratio("career_control_time_per_fight"),
        "ratio_elo": _ratio("pre_fight_elo"),

        # Matchup
        "is_orthodox_vs_southpaw": is_orth_vs_south,
        "both_debuting": both_debuting,

        "computed_at": now,
    }


def _diff_rolling_win_rate(snap_a: dict, snap_b: dict, n: int):
    """Compute diff of rolling win rate (wins/window_size)."""
    wins_a = snap_a.get(f"last{n}_wins")
    wins_b = snap_b.get(f"last{n}_wins")
    total_a = snap_a.get("total_fights", 0)
    total_b = snap_b.get("total_fights", 0)
    if wins_a is None or wins_b is None:
        return None
    wa = min(n, total_a) if total_a else 0
    wb = min(n, total_b) if total_b else 0
    rate_a = wins_a / wa if wa else None
    rate_b = wins_b / wb if wb else None
    if rate_a is None or rate_b is None:
        return None
    return rate_a - rate_b


# ── Pipeline ──────────────────────────────────────────────────────────────────

def build_all_features(conn) -> tuple[int, int]:
    """Run the full feature build pipeline.

    1. Load all warehouse data into memory.
    2. Build fighter index.
    3. Compute all Elos in one chronological pass.
    4. For each fight (date-ordered), build both fighter snapshots + bout row.
    5. Upsert all fighter_snapshots and bout_features rows.

    Returns:
        (snapshot_count, bout_count) — rows written.
    """
    print("Loading warehouse data ...", flush=True)
    data = load_all_data(conn)
    print(f"  events={len(data.events):,}  fighters={len(data.fighters):,}  "
          f"fights={len(data.fights):,}  stats={len(data.fight_stats):,}")

    print("Building fighter index ...", flush=True)
    fighter_index = build_fighter_index(data)
    print(f"  indexed {len(fighter_index):,} fighters")

    print("Computing Elo ratings ...", flush=True)
    elos = compute_all_elos(data.fights)
    print(f"  rated {len(elos):,} fights")

    # Sort fights by event_date for chronological processing
    sorted_fights = sorted(data.fights, key=lambda f: f["event_date"])

    snapshot_rows: list[dict] = []
    bout_rows: list[dict] = []

    print("Building features ...", flush=True)
    for i, fight in enumerate(sorted_fights, 1):
        fight_id = fight["fight_id"]
        f1_id = fight["fighter_1_id"]
        f2_id = fight["fighter_2_id"]
        cutoff = fight["event_date"]

        f1_row = data.fighter_by_id.get(f1_id, {})
        f2_row = data.fighter_by_id.get(f2_id, {})

        h1 = get_history(fighter_index, f1_id, cutoff)
        h2 = get_history(fighter_index, f2_id, cutoff)

        snap_1 = build_fighter_snapshot(
            f1_row, h1, cutoff, elos, fighter_index,
            fighter_id=f1_id, fight_id=fight_id,
        )
        snap_2 = build_fighter_snapshot(
            f2_row, h2, cutoff, elos, fighter_index,
            fighter_id=f2_id, fight_id=fight_id,
        )

        snapshot_rows.append(_snapshot_to_row(snap_1))
        snapshot_rows.append(_snapshot_to_row(snap_2))
        bout_rows.append(_bout_to_row(fight, snap_1, snap_2))

        if i % 1000 == 0:
            print(f"  processed {i:,} / {len(sorted_fights):,} fights", flush=True)

    print(f"  done — {len(snapshot_rows):,} snapshots, {len(bout_rows):,} bout rows")

    # ── Persist ────────────────────────────────────────────────────────────
    print("Upserting fighter_snapshots ...", flush=True)
    n_snap = upsert(conn, "fighter_snapshots", snapshot_rows,
                    pk_columns=["fighter_id", "fight_id"])
    conn.commit()
    print(f"  {n_snap:,} rows")

    print("Upserting bout_features ...", flush=True)
    n_bout = upsert(conn, "bout_features", bout_rows,
                    pk_columns=["fight_id"])
    conn.commit()
    print(f"  {n_bout:,} rows")

    return n_snap, n_bout


def main() -> None:
    conn = get_connection()
    try:
        n_snap, n_bout = build_all_features(conn)
        print(f"\nPipeline complete: {n_snap:,} snapshots, {n_bout:,} bout rows.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
