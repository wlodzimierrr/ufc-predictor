"""Bout feature row builder.

Merges two fighter snapshots into a single model-ready row with difference,
ratio, and matchup features.

Note: The pipeline uses pipeline.py:_bout_to_row() for DB persistence.
This module provides the same logic with module-level column names for
testing and standalone use.

Usage:
    row = build_bout_features(fight, snapshot_a, snapshot_b)
"""

from __future__ import annotations

from datetime import datetime, timezone


FEATURE_VERSION = 2

_WEIGHT_CLASS_RANK: dict[str, int] = {
    "strawweight": 1,
    "flyweight": 2,
    "bantamweight": 3,
    "featherweight": 4,
    "lightweight": 5,
    "welterweight": 6,
    "middleweight": 7,
    "light_heavyweight": 8,
    "heavyweight": 9,
    "women_strawweight": 1,
    "women_flyweight": 2,
    "women_bantamweight": 3,
    "women_featherweight": 4,
}


def _safe_diff(a, b):
    """a - b, or None if either is None."""
    if a is None or b is None:
        return None
    return a - b


def _safe_ratio(a, b):
    """a / (a + b), bounded [0, 1]. None if both are None or sum is 0."""
    if a is None or b is None:
        return None
    total = a + b
    if total == 0:
        return None
    return a / total


def build_bout_features(fight: dict, snapshot_a: dict, snapshot_b: dict) -> dict:
    """Build a model-ready bout feature row from two fighter snapshots.

    Fighter A = fighter_1, Fighter B = fighter_2 (matching fights table ordering).
    Difference features are always A - B. Ratio features are A / (A + B).

    Args:
        fight:       Fight dict with fight_id, event_date, weight_class, etc.
        snapshot_a:  Fighter 1's snapshot (output of build_fighter_snapshot).
        snapshot_b:  Fighter 2's snapshot (output of build_fighter_snapshot).

    Returns:
        Flat dict with metadata, differences, ratios, matchup flags, and label.
    """
    a = snapshot_a
    b = snapshot_b

    # ── Label ─────────────────────────────────────────────────────────────
    winner = fight.get("winner_fighter_id")
    result_type = fight.get("result_type")
    if result_type == "win" and winner == fight["fighter_1_id"]:
        label = 1
    elif result_type == "win" and winner == fight["fighter_2_id"]:
        label = 0
    else:
        label = None  # draw or NC

    # ── Matchup flags ─────────────────────────────────────────────────────
    stance_a = a.get("stance")
    stance_b = b.get("stance")
    is_orth_vs_south = (
        (stance_a == "orthodox" and stance_b == "southpaw")
        or (stance_a == "southpaw" and stance_b == "orthodox")
    ) if stance_a and stance_b else False

    both_debuting = (
        bool(a.get("is_debut")) and bool(b.get("is_debut"))
    )

    # Weight class rank
    wc = fight.get("weight_class")
    weight_class_rank = _WEIGHT_CLASS_RANK.get(wc) if wc else None

    return {
        # Metadata
        "fight_id": fight["fight_id"],
        "event_date": fight.get("event_date"),
        "weight_class": fight.get("weight_class"),
        "is_title_fight": bool(fight.get("is_title_fight")),
        "scheduled_rounds": fight.get("scheduled_rounds"),
        "fighter_1_id": fight["fighter_1_id"],
        "fighter_2_id": fight["fighter_2_id"],

        # ── v1 differences (A - B) ───────────────────────────────────────
        "diff_elo": _safe_diff(a.get("pre_fight_elo"), b.get("pre_fight_elo")),
        "diff_career_wins": _safe_diff(a.get("wins"), b.get("wins")),
        "diff_career_fights": _safe_diff(a.get("total_fights"), b.get("total_fights")),
        "diff_career_win_rate": _safe_diff(a.get("win_rate"), b.get("win_rate")),
        "diff_career_finish_rate": _safe_diff(a.get("finish_rate"), b.get("finish_rate")),
        "diff_career_sig_strikes_landed_pm": _safe_diff(
            a.get("career_sig_strikes_landed_per_min"),
            b.get("career_sig_strikes_landed_per_min"),
        ),
        "diff_career_sig_strike_accuracy": _safe_diff(
            a.get("career_sig_strike_accuracy"),
            b.get("career_sig_strike_accuracy"),
        ),
        "diff_career_takedown_accuracy": _safe_diff(
            a.get("career_takedown_accuracy"),
            b.get("career_takedown_accuracy"),
        ),
        "diff_career_control_rate": _safe_diff(
            a.get("career_control_time_per_fight"),
            b.get("career_control_time_per_fight"),
        ),
        "diff_age": _safe_diff(a.get("age_at_fight"), b.get("age_at_fight")),
        "diff_height_cm": _safe_diff(a.get("height_cm"), b.get("height_cm")),
        "diff_reach_cm": _safe_diff(a.get("reach_cm"), b.get("reach_cm")),
        "diff_days_since_last_fight": _safe_diff(
            a.get("days_since_last_fight"), b.get("days_since_last_fight"),
        ),

        # ── v2 wired diffs ───────────────────────────────────────────────
        "diff_career_ko_rate": _safe_diff(a.get("ko_tko_win_rate"), b.get("ko_tko_win_rate")),
        "diff_career_sub_rate": _safe_diff(a.get("sub_win_rate"), b.get("sub_win_rate")),
        "diff_career_decision_rate": _safe_diff(a.get("dec_win_rate"), b.get("dec_win_rate")),
        "diff_career_sig_strikes_absorbed_pm": _safe_diff(
            a.get("career_sig_strikes_absorbed_per_min"),
            b.get("career_sig_strikes_absorbed_per_min"),
        ),
        "diff_career_sig_strike_defense": _safe_diff(
            a.get("career_sig_strike_defense"),
            b.get("career_sig_strike_defense"),
        ),
        "diff_career_takedown_defense": _safe_diff(
            a.get("career_takedown_defense"),
            b.get("career_takedown_defense"),
        ),
        "diff_title_fight_count": _safe_diff(a.get("title_fights"), b.get("title_fights")),
        "diff_five_round_fights": _safe_diff(
            a.get("five_round_experience"), b.get("five_round_experience"),
        ),
        "diff_reach_height_ratio": _safe_diff(
            a.get("reach_to_height_ratio"), b.get("reach_to_height_ratio"),
        ),
        "diff_fights_per_year_last3": _safe_diff(
            a.get("fights_per_year_last3"), b.get("fights_per_year_last3"),
        ),

        # ── v2 trend diffs ───────────────────────────────────────────────
        "diff_slope_sig_strikes_last5": _safe_diff(
            a.get("slope_sig_strikes_last5"), b.get("slope_sig_strikes_last5"),
        ),
        "diff_slope_td_accuracy_last5": _safe_diff(
            a.get("slope_td_accuracy_last5"), b.get("slope_td_accuracy_last5"),
        ),
        "diff_slope_control_rate_last5": _safe_diff(
            a.get("slope_control_rate_last5"), b.get("slope_control_rate_last5"),
        ),
        "diff_std_sig_strikes_last5": _safe_diff(
            a.get("std_sig_strikes_last5"), b.get("std_sig_strikes_last5"),
        ),
        "diff_std_td_accuracy_last5": _safe_diff(
            a.get("std_td_accuracy_last5"), b.get("std_td_accuracy_last5"),
        ),

        # ── v1 ratios (A / (A + B)) ─────────────────────────────────────
        "ratio_career_wins": _safe_ratio(a.get("wins"), b.get("wins")),
        "ratio_career_fights": _safe_ratio(a.get("total_fights"), b.get("total_fights")),
        "ratio_career_sig_strikes_landed_pm": _safe_ratio(
            a.get("career_sig_strikes_landed_per_min"),
            b.get("career_sig_strikes_landed_per_min"),
        ),
        "ratio_career_control_rate": _safe_ratio(
            a.get("career_control_time_per_fight"),
            b.get("career_control_time_per_fight"),
        ),
        "ratio_elo": _safe_ratio(a.get("pre_fight_elo"), b.get("pre_fight_elo")),

        # ── Matchup / metadata ───────────────────────────────────────────
        "is_orthodox_vs_southpaw": is_orth_vs_south,
        "both_debuting": both_debuting,
        "f1_is_southpaw": bool(stance_a == "southpaw") if stance_a else False,
        "f2_is_southpaw": bool(stance_b == "southpaw") if stance_b else False,
        "weight_class_rank": weight_class_rank,

        # Label
        "label": label,

        # Versioning
        "feature_version": FEATURE_VERSION,
        "computed_at": datetime.now(timezone.utc),
    }
