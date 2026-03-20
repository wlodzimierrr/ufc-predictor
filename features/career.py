"""Career aggregate feature computation.

Computes cumulative pre-fight statistics from a fighter's full prior history.
All inputs must already be filtered to fights strictly before the cutoff date
(use features.history.get_history).

Usage:
    history = get_history(index, fighter_id, cutoff_date)
    feats = compute_career_features(history)
"""

from __future__ import annotations

from features.history import FightHistory


# Finish methods that count as a finish (not decision)
_KO_TKO = {"ko_tko", "doctor_stoppage"}
_SUBMISSION = {"submission"}
_DECISION = {"decision"}


def _fight_duration_seconds(h: FightHistory) -> int | None:
    """Elapsed fight time in seconds.

    For stoppages: (finish_round - 1) * 300 + finish_time_seconds
    For decisions: scheduled_rounds * 300
    Returns None if the necessary fields are missing.
    """
    if h.finish_method in _DECISION:
        if h.scheduled_rounds is None:
            return None
        return h.scheduled_rounds * 300

    # Stoppage or other finish
    if h.finish_round is not None and h.finish_time_seconds is not None:
        return (h.finish_round - 1) * 300 + h.finish_time_seconds

    # Fall back to scheduled rounds if available (e.g., overturned results)
    if h.scheduled_rounds is not None:
        return h.scheduled_rounds * 300

    return None


def _safe_rate(numerator: float | int, denominator: float | int) -> float | None:
    """Return numerator / denominator, or None if denominator is zero."""
    if not denominator:
        return None
    return numerator / denominator


def compute_career_features(history: list[FightHistory]) -> dict:
    """Compute cumulative career features from a fighter's pre-fight history.

    All features represent information available strictly before the target fight.
    Per-minute rates use total_cage_time_seconds / 60 as the denominator.

    Args:
        history: list of FightHistory sorted oldest-first (output of get_history).
                 May be empty for a debuting fighter.

    Returns:
        dict with all career feature keys. Rates are None when the denominator
        is zero. Counts are always int (0 for a debut).
    """
    # ── Accumulators ──────────────────────────────────────────────────────────
    total_fights = len(history)
    wins = losses = draws = nc = 0
    ko_tko_wins = sub_wins = dec_wins = 0
    ko_tko_losses = sub_losses = dec_losses = 0
    title_fights = title_wins = 0
    total_cage_time = 0  # seconds; None entries skipped

    # Striking (from fighter_stats)
    sig_landed = sig_attempted = 0
    opp_sig_landed = opp_sig_attempted = 0

    # Grappling
    td_landed = td_attempted = 0
    opp_td_landed = opp_td_attempted = 0
    sub_attempts = 0
    knockdowns = 0
    control_seconds = 0

    for h in history:
        # ── Outcome ───────────────────────────────────────────────────────────
        if h.result_type == "win":
            if h.won:
                wins += 1
                fm = h.finish_method
                if fm in _KO_TKO:
                    ko_tko_wins += 1
                elif fm in _SUBMISSION:
                    sub_wins += 1
                elif fm in _DECISION:
                    dec_wins += 1
            else:
                losses += 1
                fm = h.finish_method
                if fm in _KO_TKO:
                    ko_tko_losses += 1
                elif fm in _SUBMISSION:
                    sub_losses += 1
                elif fm in _DECISION:
                    dec_losses += 1
        elif h.result_type == "draw":
            draws += 1
        elif h.result_type == "nc":
            nc += 1

        # ── Title ─────────────────────────────────────────────────────────────
        if h.is_title_fight:
            title_fights += 1
            if h.won:
                title_wins += 1

        # ── Fight duration ────────────────────────────────────────────────────
        dur = _fight_duration_seconds(h)
        if dur is not None:
            total_cage_time += dur

        # ── Per-fight striking & grappling ────────────────────────────────────
        fs = h.fighter_stats
        os_ = h.opponent_stats

        if fs is not None:
            sig_landed += fs.get("sig_strikes_landed", 0) or 0
            sig_attempted += fs.get("sig_strikes_attempted", 0) or 0
            td_landed += fs.get("takedowns_landed", 0) or 0
            td_attempted += fs.get("takedowns_attempted", 0) or 0
            sub_attempts += fs.get("submissions_attempted", 0) or 0
            knockdowns += fs.get("knockdowns", 0) or 0
            control_seconds += fs.get("control_time_seconds", 0) or 0

        if os_ is not None:
            opp_sig_landed += os_.get("sig_strikes_landed", 0) or 0
            opp_sig_attempted += os_.get("sig_strikes_attempted", 0) or 0
            opp_td_landed += os_.get("takedowns_landed", 0) or 0
            opp_td_attempted += os_.get("takedowns_attempted", 0) or 0

    # ── Streaks ───────────────────────────────────────────────────────────────
    current_win_streak = 0
    for h in reversed(history):
        if h.won:
            current_win_streak += 1
        else:
            break

    current_lose_streak = 0
    for h in reversed(history):
        if h.result_type == "win" and not h.won:
            current_lose_streak += 1
        else:
            break

    # ── Derived rates ─────────────────────────────────────────────────────────
    cage_minutes = total_cage_time / 60.0 if total_cage_time else 0.0

    return {
        # Record
        "total_fights": total_fights,
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "no_contests": nc,
        "win_rate": _safe_rate(wins, total_fights),

        # Finish profile — counts
        "ko_tko_wins": ko_tko_wins,
        "sub_wins": sub_wins,
        "dec_wins": dec_wins,
        "ko_tko_losses": ko_tko_losses,
        "sub_losses": sub_losses,
        "dec_losses": dec_losses,

        # Finish profile — rates (fraction of total fights)
        "ko_tko_win_rate": _safe_rate(ko_tko_wins, total_fights),
        "sub_win_rate": _safe_rate(sub_wins, total_fights),
        "dec_win_rate": _safe_rate(dec_wins, total_fights),
        "finish_rate": _safe_rate(ko_tko_wins + sub_wins, wins),

        # Title
        "title_fights": title_fights,
        "title_wins": title_wins,

        # Time
        "total_cage_time_seconds": total_cage_time,
        "avg_fight_time_seconds": _safe_rate(total_cage_time, total_fights),

        # Streaks
        "current_win_streak": current_win_streak,
        "current_lose_streak": current_lose_streak,

        # Striking — per-minute (uses total cage time as denominator)
        "career_sig_strikes_landed_per_min": _safe_rate(sig_landed, cage_minutes),
        "career_sig_strikes_absorbed_per_min": _safe_rate(opp_sig_landed, cage_minutes),
        "career_sig_strike_accuracy": _safe_rate(sig_landed, sig_attempted),
        "career_sig_strike_defense": (
            1.0 - opp_sig_landed / opp_sig_attempted
            if opp_sig_attempted else None
        ),

        # Grappling
        "career_takedown_accuracy": _safe_rate(td_landed, td_attempted),
        "career_takedown_defense": (
            1.0 - opp_td_landed / opp_td_attempted
            if opp_td_attempted else None
        ),
        "career_takedowns_per_15min": _safe_rate(td_landed, cage_minutes / 15.0),
        "career_submissions_per_15min": _safe_rate(sub_attempts, cage_minutes / 15.0),

        # Dominance
        "career_knockdowns_per_fight": _safe_rate(knockdowns, total_fights),
        "career_control_time_per_fight": _safe_rate(control_seconds, total_fights),
    }
