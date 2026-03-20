"""Exponentially decayed feature computation.

Computes recency-weighted versions of key statistics using a half-life decay
function. Fights from half_life_days ago contribute half the weight of today's
fights; fights from 2 * half_life_days ago contribute a quarter, etc.

Usage:
    history = get_history(index, fighter_id, cutoff_date)
    feats = compute_decayed_features(history, cutoff_date)
"""

from __future__ import annotations

from datetime import date

from features.history import FightHistory


_KO_TKO = {"ko_tko", "doctor_stoppage"}
_SUBMISSION = {"submission"}
_DECISION = {"decision"}


def _fight_duration_seconds(h: FightHistory) -> int | None:
    if h.finish_method in _DECISION:
        return h.scheduled_rounds * 300 if h.scheduled_rounds is not None else None
    if h.finish_round is not None and h.finish_time_seconds is not None:
        return (h.finish_round - 1) * 300 + h.finish_time_seconds
    if h.scheduled_rounds is not None:
        return h.scheduled_rounds * 300
    return None


def _weight(event_date: date, cutoff_date: date, half_life_days: float) -> float:
    """Decay weight: 2^(−days_since_fight / half_life_days)."""
    days = (cutoff_date - event_date).days
    return 2.0 ** (-days / half_life_days)


def _weighted_avg(pairs: list[tuple[float, float | None]]) -> float | None:
    """Weighted average of (weight, value) pairs, skipping None values.

    Returns None if no valid (non-None) values exist.
    """
    total_w = total_wv = 0.0
    for w, v in pairs:
        if v is not None:
            total_w += w
            total_wv += w * v
    return total_wv / total_w if total_w else None


def compute_decayed_features(
    history: list[FightHistory],
    cutoff_date: date,
    half_life_days: float = 365.0,
) -> dict:
    """Compute exponentially decayed feature averages.

    Each metric is a weighted mean of per-fight values:
        result = sum(w_i * value_i) / sum(w_i)
    where w_i = 2^(-(cutoff_date - event_date_i).days / half_life_days).

    Fights where a specific metric cannot be computed (missing stats or zero
    fight duration) are excluded from that metric's weighted sum only.

    Args:
        history:        FightHistory sorted oldest-first (output of get_history).
        cutoff_date:    Event date of the target fight (used to compute days_since).
        half_life_days: Weight halves every this many days. Default 365.

    Returns:
        dict with 8 decay feature keys. All are None for an empty history.
    """
    if not history:
        return {
            "decay_sig_strike_rate": None,
            "decay_sig_strike_accuracy": None,
            "decay_takedown_rate": None,
            "decay_takedown_accuracy": None,
            "decay_control_time_per_fight": None,
            "decay_knockdowns_per_fight": None,
            "decay_win_rate": None,
            "decay_finish_rate": None,
        }

    # Accumulate (weight, per_fight_value) pairs per metric
    sig_rate_pairs: list[tuple[float, float | None]] = []
    sig_acc_pairs: list[tuple[float, float | None]] = []
    td_rate_pairs: list[tuple[float, float | None]] = []
    td_acc_pairs: list[tuple[float, float | None]] = []
    ctrl_pairs: list[tuple[float, float | None]] = []
    kd_pairs: list[tuple[float, float | None]] = []
    win_pairs: list[tuple[float, float]] = []
    finish_pairs: list[tuple[float, float]] = []

    for h in history:
        w = _weight(h.event_date, cutoff_date, half_life_days)

        # Outcome metrics (always computable)
        win_pairs.append((w, 1.0 if h.won else 0.0))
        is_finish = h.finish_method in _KO_TKO or h.finish_method in _SUBMISSION
        finish_pairs.append((w, 1.0 if (h.won and is_finish) else 0.0))

        # Stats-dependent metrics
        fs = h.fighter_stats
        dur = _fight_duration_seconds(h)
        cage_min = dur / 60.0 if dur else None

        if fs is not None:
            sl = fs.get("sig_strikes_landed", 0) or 0
            sa = fs.get("sig_strikes_attempted", 0) or 0
            tl = fs.get("takedowns_landed", 0) or 0
            ta = fs.get("takedowns_attempted", 0) or 0
            ctrl = fs.get("control_time_seconds", 0) or 0
            kd = fs.get("knockdowns", 0) or 0

            sig_rate_pairs.append((w, sl / cage_min if cage_min else None))
            sig_acc_pairs.append((w, sl / sa if sa else None))
            td_rate_pairs.append((w, (tl / cage_min * 15) if cage_min else None))
            td_acc_pairs.append((w, tl / ta if ta else None))
            ctrl_pairs.append((w, float(ctrl)))
            kd_pairs.append((w, float(kd)))
        else:
            sig_rate_pairs.append((w, None))
            sig_acc_pairs.append((w, None))
            td_rate_pairs.append((w, None))
            td_acc_pairs.append((w, None))
            ctrl_pairs.append((w, None))
            kd_pairs.append((w, None))

    return {
        "decay_sig_strike_rate": _weighted_avg(sig_rate_pairs),
        "decay_sig_strike_accuracy": _weighted_avg(sig_acc_pairs),
        "decay_takedown_rate": _weighted_avg(td_rate_pairs),
        "decay_takedown_accuracy": _weighted_avg(td_acc_pairs),
        "decay_control_time_per_fight": _weighted_avg(ctrl_pairs),
        "decay_knockdowns_per_fight": _weighted_avg(kd_pairs),
        "decay_win_rate": _weighted_avg(win_pairs),
        "decay_finish_rate": _weighted_avg(finish_pairs),
    }
