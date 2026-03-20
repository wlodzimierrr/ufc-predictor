"""Opponent-adjusted feature computation.

Adjusts a fighter's key stats relative to their opponents' baseline allowed
rates. A fighter who lands 5 sig strikes/min against opponents who typically
allow only 3/min is performing above expectation (ratio ≈ 1.67).

For each prior fight the opponent's career "allowed rate" is computed from
the opponent's own history before that fight's date. The fighter's per-fight
rate is divided by the opponent's allowed rate, and these ratios are averaged.

Usage:
    feats = compute_opponent_adjusted(
        history, fighter_index, elos=elos,
    )
"""

from __future__ import annotations

from datetime import date

from features.history import FightHistory, get_history


_DECISION = {"decision"}


def _fight_duration_seconds(h: FightHistory) -> int | None:
    if h.finish_method in _DECISION:
        return h.scheduled_rounds * 300 if h.scheduled_rounds is not None else None
    if h.finish_round is not None and h.finish_time_seconds is not None:
        return (h.finish_round - 1) * 300 + h.finish_time_seconds
    if h.scheduled_rounds is not None:
        return h.scheduled_rounds * 300
    return None


def _opp_career_absorbed(
    opp_history: list[FightHistory],
) -> dict:
    """Compute an opponent's career average stats-absorbed rates.

    Returns dict with sig_absorbed_pm, td_absorbed_pm, ctrl_absorbed_per_fight
    (all float or None).
    """
    total_sig = 0
    total_td = 0
    total_ctrl = 0
    total_seconds = 0
    n_fights = len(opp_history)

    for h in opp_history:
        os_ = h.opponent_stats  # what the opponent's opponents landed ON them
        if os_ is not None:
            total_sig += os_.get("sig_strikes_landed", 0) or 0
            total_td += os_.get("takedowns_landed", 0) or 0
            total_ctrl += os_.get("control_time_seconds", 0) or 0

        dur = _fight_duration_seconds(h)
        if dur is not None:
            total_seconds += dur

    cage_min = total_seconds / 60.0

    return {
        "sig_absorbed_pm": total_sig / cage_min if cage_min else None,
        "td_absorbed_pm": total_td / cage_min if cage_min else None,
        "ctrl_absorbed_per_fight": total_ctrl / n_fights if n_fights else None,
    }


def compute_opponent_adjusted(
    history: list[FightHistory],
    fighter_index: dict[str, list[FightHistory]],
    elos: dict[str, dict[str, float]] | None = None,
) -> dict:
    """Compute opponent-adjusted features for one fighter.

    For each prior fight, looks up the opponent's career stats before that
    fight's date. Computes the fighter's per-fight rate divided by the
    opponent's career allowed rate, then averages those ratios (simple mean).

    Args:
        history:        Fighter's prior fights sorted oldest-first.
        fighter_index:  Full index from build_fighter_index() for opponent lookups.
        elos:           Output of compute_all_elos() for opponent Elo lookups.
                        If None, avg_opponent_elo is None.

    Returns:
        dict with 5 opponent-adjusted features. All None for empty history.
    """
    if not history:
        return {
            "opp_adjusted_sig_strike_rate": None,
            "opp_adjusted_takedown_rate": None,
            "opp_adjusted_control_rate": None,
            "avg_opponent_elo": None,
            "avg_opponent_win_rate": None,
        }

    sig_ratios: list[float] = []
    td_ratios: list[float] = []
    ctrl_ratios: list[float] = []
    opp_elos: list[float] = []
    opp_win_rates: list[float] = []

    for h in history:
        opp_id = h.opponent_id
        fight_date = h.event_date

        # Opponent's history before this fight
        opp_history = get_history(fighter_index, opp_id, fight_date)

        # ── Opponent win rate ────────────────────────────────────────────
        if opp_history:
            opp_wins = sum(1 for oh in opp_history if oh.won)
            opp_win_rates.append(opp_wins / len(opp_history))

        # ── Opponent Elo ─────────────────────────────────────────────────
        if elos is not None:
            fight_elos = elos.get(h.fight_id, {})
            opp_elo = fight_elos.get(opp_id)
            if opp_elo is not None:
                opp_elos.append(opp_elo)

        # ── Adjusted rates ───────────────────────────────────────────────
        opp_allowed = _opp_career_absorbed(opp_history) if opp_history else {}
        fs = h.fighter_stats
        dur = _fight_duration_seconds(h)

        if fs is not None and dur:
            cage_min = dur / 60.0

            # Sig strike rate adjustment
            fighter_sig_pm = (fs.get("sig_strikes_landed", 0) or 0) / cage_min
            opp_sig_allowed = opp_allowed.get("sig_absorbed_pm")
            if opp_sig_allowed and opp_sig_allowed > 0:
                sig_ratios.append(fighter_sig_pm / opp_sig_allowed)

            # Takedown rate adjustment
            fighter_td_pm = (fs.get("takedowns_landed", 0) or 0) / cage_min
            opp_td_allowed = opp_allowed.get("td_absorbed_pm")
            if opp_td_allowed and opp_td_allowed > 0:
                td_ratios.append(fighter_td_pm / opp_td_allowed)

            # Control rate adjustment
            fighter_ctrl = (fs.get("control_time_seconds", 0) or 0)
            opp_ctrl_allowed = opp_allowed.get("ctrl_absorbed_per_fight")
            if opp_ctrl_allowed and opp_ctrl_allowed > 0:
                ctrl_ratios.append(fighter_ctrl / opp_ctrl_allowed)

    return {
        "opp_adjusted_sig_strike_rate": (
            sum(sig_ratios) / len(sig_ratios) if sig_ratios else None
        ),
        "opp_adjusted_takedown_rate": (
            sum(td_ratios) / len(td_ratios) if td_ratios else None
        ),
        "opp_adjusted_control_rate": (
            sum(ctrl_ratios) / len(ctrl_ratios) if ctrl_ratios else None
        ),
        "avg_opponent_elo": (
            sum(opp_elos) / len(opp_elos) if opp_elos else None
        ),
        "avg_opponent_win_rate": (
            sum(opp_win_rates) / len(opp_win_rates) if opp_win_rates else None
        ),
    }
