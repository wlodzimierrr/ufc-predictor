"""Rolling window feature computation.

Computes statistics over the most recent N fights (N = 1, 3, 5 by default).
History must already be filtered to fights strictly before the cutoff date
(use features.history.get_history) and sorted oldest-first.

Usage:
    history = get_history(index, fighter_id, cutoff_date)
    feats = compute_rolling_features(history)
"""

from __future__ import annotations

from features.history import FightHistory


_KO_TKO = {"ko_tko", "doctor_stoppage"}
_SUBMISSION = {"submission"}
_DECISION = {"decision"}

_DEFAULT_WINDOWS = (1, 3, 5)


def _fight_duration_seconds(h: FightHistory) -> int | None:
    """Elapsed fight time in seconds."""
    if h.finish_method in _DECISION:
        if h.scheduled_rounds is None:
            return None
        return h.scheduled_rounds * 300
    if h.finish_round is not None and h.finish_time_seconds is not None:
        return (h.finish_round - 1) * 300 + h.finish_time_seconds
    if h.scheduled_rounds is not None:
        return h.scheduled_rounds * 300
    return None


def _safe_rate(num: float | int, den: float | int) -> float | None:
    return None if not den else num / den


def _window_features(window: list[FightHistory]) -> dict:
    """Compute features for a single window slice."""
    n = len(window)
    if n == 0:
        return {
            "wins": 0,
            "sig_strikes_landed_per_min": None,
            "sig_strikes_absorbed_per_min": None,
            "sig_strike_accuracy": None,
            "takedown_accuracy": None,
            "control_time_per_fight": None,
            "knockdowns_per_fight": None,
            "finish_rate": None,
        }

    wins = finishes = 0
    sig_landed = sig_attempted = 0
    opp_sig_landed = opp_sig_attempted = 0
    td_landed = td_attempted = 0
    control_seconds = knockdowns = 0
    total_seconds = 0

    for h in window:
        if h.won:
            wins += 1
            if h.finish_method in _KO_TKO or h.finish_method in _SUBMISSION:
                finishes += 1

        dur = _fight_duration_seconds(h)
        if dur is not None:
            total_seconds += dur

        fs = h.fighter_stats
        if fs is not None:
            sig_landed += fs.get("sig_strikes_landed", 0) or 0
            sig_attempted += fs.get("sig_strikes_attempted", 0) or 0
            td_landed += fs.get("takedowns_landed", 0) or 0
            td_attempted += fs.get("takedowns_attempted", 0) or 0
            control_seconds += fs.get("control_time_seconds", 0) or 0
            knockdowns += fs.get("knockdowns", 0) or 0

        os_ = h.opponent_stats
        if os_ is not None:
            opp_sig_landed += os_.get("sig_strikes_landed", 0) or 0
            opp_sig_attempted += os_.get("sig_strikes_attempted", 0) or 0

    cage_minutes = total_seconds / 60.0

    return {
        "wins": wins,
        "sig_strikes_landed_per_min": _safe_rate(sig_landed, cage_minutes),
        "sig_strikes_absorbed_per_min": _safe_rate(opp_sig_landed, cage_minutes),
        "sig_strike_accuracy": _safe_rate(sig_landed, sig_attempted),
        "takedown_accuracy": _safe_rate(td_landed, td_attempted),
        "control_time_per_fight": _safe_rate(control_seconds, n),
        "knockdowns_per_fight": _safe_rate(knockdowns, n),
        "finish_rate": _safe_rate(finishes, n),
    }


def compute_rolling_features(
    history: list[FightHistory],
    windows: tuple[int, ...] = _DEFAULT_WINDOWS,
) -> dict:
    """Compute rolling-window features for the given windows.

    For each window size N the most recent N fights (history[-N:]) are used.
    If the fighter has fewer than N prior fights, all available fights are used
    and `has_{N}_fights` is set to False.

    Args:
        history:  FightHistory list sorted oldest-first (output of get_history).
        windows:  window sizes to compute, default (1, 3, 5).

    Returns:
        dict with keys `last{N}_{metric}` and `has_{N}_fights` for each N.
    """
    result: dict = {}
    n_available = len(history)

    for n in windows:
        window = history[-n:] if n_available >= n else history[:]
        feats = _window_features(window)

        result[f"has_{n}_fights"] = n_available >= n
        for metric, value in feats.items():
            result[f"last{n}_{metric}"] = value

    return result
