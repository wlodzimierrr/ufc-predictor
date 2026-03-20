"""Bout feature row builder.

Merges two fighter snapshots into a single model-ready row with difference,
ratio, and matchup features.

Usage:
    row = build_bout_features(fight, snapshot_a, snapshot_b)
"""

from __future__ import annotations

from datetime import datetime, timezone


FEATURE_VERSION = 1


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
    Difference features are always A − B. Ratio features are A / (A + B).

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
    if stance_a and stance_b:
        pair = sorted([stance_a, stance_b])
        stance_matchup = f"{pair[0]}_vs_{pair[1]}"
    else:
        stance_matchup = None

    reach_a = a.get("reach_cm")
    reach_b = b.get("reach_cm")
    is_reach_advantage_a = (
        reach_a > reach_b if reach_a is not None and reach_b is not None else None
    )

    exp_a = a.get("ufc_fight_count", 0) or 0
    exp_b = b.get("ufc_fight_count", 0) or 0
    is_experience_advantage_a = exp_a > exp_b

    return {
        # Metadata
        "fight_id": fight["fight_id"],
        "event_date": fight.get("event_date"),
        "weight_class": fight.get("weight_class"),
        "is_title_fight": bool(fight.get("is_title_fight")),
        "scheduled_rounds": fight.get("scheduled_rounds"),
        "fighter_1_id": fight["fighter_1_id"],
        "fighter_2_id": fight["fighter_2_id"],

        # Difference features (A − B)
        "age_diff": _safe_diff(a.get("age_at_fight"), b.get("age_at_fight")),
        "height_diff": _safe_diff(a.get("height_cm"), b.get("height_cm")),
        "reach_diff": _safe_diff(a.get("reach_cm"), b.get("reach_cm")),
        "elo_diff": _safe_diff(a.get("pre_fight_elo"), b.get("pre_fight_elo")),
        "win_rate_diff": _safe_diff(a.get("win_rate"), b.get("win_rate")),
        "career_sig_strike_rate_diff": _safe_diff(
            a.get("career_sig_strikes_landed_per_min"),
            b.get("career_sig_strikes_landed_per_min"),
        ),
        "career_takedown_rate_diff": _safe_diff(
            a.get("career_takedown_accuracy"),
            b.get("career_takedown_accuracy"),
        ),
        "career_control_time_diff": _safe_diff(
            a.get("career_control_time_per_fight"),
            b.get("career_control_time_per_fight"),
        ),
        "experience_diff": _safe_diff(
            a.get("ufc_fight_count"),
            b.get("ufc_fight_count"),
        ),

        # Ratio features (A / (A + B))
        "experience_ratio": _safe_ratio(
            a.get("ufc_fight_count"),
            b.get("ufc_fight_count"),
        ),
        "win_rate_ratio": _safe_ratio(
            a.get("win_rate"),
            b.get("win_rate"),
        ),
        "sig_strike_accuracy_ratio": _safe_ratio(
            a.get("career_sig_strike_accuracy"),
            b.get("career_sig_strike_accuracy"),
        ),
        "takedown_accuracy_ratio": _safe_ratio(
            a.get("career_takedown_accuracy"),
            b.get("career_takedown_accuracy"),
        ),

        # Matchup flags
        "stance_matchup": stance_matchup,
        "is_reach_advantage_a": is_reach_advantage_a,
        "is_experience_advantage_a": is_experience_advantage_a,

        # Label
        "label": label,

        # Versioning
        "feature_version": FEATURE_VERSION,
        "computed_at": datetime.now(timezone.utc),
    }
