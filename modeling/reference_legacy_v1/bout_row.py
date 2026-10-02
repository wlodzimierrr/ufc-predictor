"""Pure legacy bout mapping, extracted without production writers."""
from datetime import datetime, timezone

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

    # Weight class rank
    wc = fight.get("weight_class")
    weight_class_rank = _WEIGHT_CLASS_RANK.get(wc) if wc else None

    return {
        "fight_id": fight["fight_id"],
        "fighter_1_id": fight["fighter_1_id"],
        "fighter_2_id": fight["fighter_2_id"],
        "event_date": fight.get("event_date"),
        "weight_class": fight.get("weight_class"),
        "is_title_fight": bool(fight.get("is_title_fight")),
        "scheduled_rounds": fight.get("scheduled_rounds"),
        "label": label,
        "feature_version": 2,

        # ── v1 differences (fighter_1 − fighter_2) ────────────────────────
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

        # ── v2 wired differences (already in snapshot, newly surfaced) ────
        "diff_career_ko_rate": _diff("ko_tko_win_rate"),
        "diff_career_sub_rate": _diff("sub_win_rate"),
        "diff_career_decision_rate": _diff("dec_win_rate"),
        "diff_career_sig_strikes_absorbed_pm": _diff("career_sig_strikes_absorbed_per_min"),
        "diff_career_sig_strike_defense": _diff("career_sig_strike_defense"),
        "diff_career_takedown_defense": _diff("career_takedown_defense"),
        "diff_title_fight_count": _diff("title_fights"),
        "diff_five_round_fights": _diff("five_round_experience"),
        "diff_reach_height_ratio": _diff("reach_to_height_ratio"),
        "diff_fights_per_year_last3": _diff("fights_per_year_last3"),

        # ── v2 trend differences ──────────────────────────────────────────
        "diff_slope_sig_strikes_last5": _diff("slope_sig_strikes_last5"),
        "diff_slope_td_accuracy_last5": _diff("slope_td_accuracy_last5"),
        "diff_slope_control_rate_last5": _diff("slope_control_rate_last5"),
        "diff_std_sig_strikes_last5": _diff("std_sig_strikes_last5"),
        "diff_std_td_accuracy_last5": _diff("std_td_accuracy_last5"),

        # ── v1 ratios (fighter_1 / (fighter_1 + fighter_2)) ───────────────
        "ratio_career_wins": _ratio("wins"),
        "ratio_career_fights": _ratio("total_fights"),
        "ratio_career_sig_strikes_landed_pm": _ratio("career_sig_strikes_landed_per_min"),
        "ratio_career_control_rate": _ratio("career_control_time_per_fight"),
        "ratio_elo": _ratio("pre_fight_elo"),

        # ── v2 matchup / metadata ────────────────────────────────────────
        "is_orthodox_vs_southpaw": is_orth_vs_south,
        "both_debuting": both_debuting,
        "f1_is_southpaw": bool(stance_a == "southpaw") if stance_a else False,
        "f2_is_southpaw": bool(stance_b == "southpaw") if stance_b else False,
        "weight_class_rank": weight_class_rank,

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
