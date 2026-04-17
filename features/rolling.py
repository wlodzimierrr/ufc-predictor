"""Rolling window feature computation.

Computes statistics over the most recent N fights (N = 1, 3, 5 by default),
plus trend features (OLS slope, std) over the last 5 fights.
History must already be filtered to fights strictly before the cutoff date
(use features.history.get_history) and sorted oldest-first.

Usage:
    history = get_history(index, fighter_id, cutoff_date)
    feats = compute_rolling_features(history)
"""

from __future__ import annotations

from datetime import date

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


def _ols_slope(values: list[float | None]) -> float | None:
    """OLS slope of values over fight index (0, 1, 2, ...).

    Skips None entries. Returns None if fewer than 2 valid data points.
    Uses the closed-form formula: slope = (n*sum(xy) - sum(x)*sum(y)) / (n*sum(x^2) - sum(x)^2).
    """
    pairs = [(i, v) for i, v in enumerate(values) if v is not None]
    n = len(pairs)
    if n < 2:
        return None
    sx = sy = sxy = sx2 = 0.0
    for x, y in pairs:
        sx += x
        sy += y
        sxy += x * y
        sx2 += x * x
    denom = n * sx2 - sx * sx
    if denom == 0:
        return 0.0
    return (n * sxy - sx * sy) / denom


def _std(values: list[float | None]) -> float | None:
    """Population standard deviation, skipping Nones. None if < 2 valid."""
    valid = [v for v in values if v is not None]
    if len(valid) < 2:
        return None
    mean = sum(valid) / len(valid)
    return (sum((v - mean) ** 2 for v in valid) / len(valid)) ** 0.5


def _per_fight_metrics(window: list[FightHistory]) -> dict[str, list[float | None]]:
    """Extract per-fight metric vectors for trend computation."""
    sig_rates: list[float | None] = []
    td_accs: list[float | None] = []
    ctrl_rates: list[float | None] = []

    for h in window:
        fs = h.fighter_stats
        dur = _fight_duration_seconds(h)
        cage_min = dur / 60.0 if dur else None

        if fs is not None:
            sl = fs.get("sig_strikes_landed", 0) or 0
            tl = fs.get("takedowns_landed", 0) or 0
            ta = fs.get("takedowns_attempted", 0) or 0
            ctrl = fs.get("control_time_seconds", 0) or 0

            sig_rates.append(sl / cage_min if cage_min else None)
            td_accs.append(tl / ta if ta else None)
            ctrl_rates.append(float(ctrl))
        else:
            sig_rates.append(None)
            td_accs.append(None)
            ctrl_rates.append(None)

    return {
        "sig_strikes_pm": sig_rates,
        "td_accuracy": td_accs,
        "control_rate": ctrl_rates,
    }


def _compute_trend_features(history: list[FightHistory], n: int = 5) -> dict:
    """Compute slope and std trend features over the last N fights.

    Returns dict with keys like slope_sig_strikes_last5, std_sig_strikes_last5, etc.
    All values are None if fewer than 2 fights are available.
    """
    window = history[-n:] if len(history) >= n else history[:]
    metrics = _per_fight_metrics(window)

    return {
        f"slope_sig_strikes_last{n}": _ols_slope(metrics["sig_strikes_pm"]),
        f"slope_td_accuracy_last{n}": _ols_slope(metrics["td_accuracy"]),
        f"slope_control_rate_last{n}": _ols_slope(metrics["control_rate"]),
        f"std_sig_strikes_last{n}": _std(metrics["sig_strikes_pm"]),
        f"std_td_accuracy_last{n}": _std(metrics["td_accuracy"]),
    }


def _compute_fights_per_year_last3(
    history: list[FightHistory],
    cutoff_date: date,
) -> float | None:
    """Bouts per calendar year over the last 3 fights window.

    Uses the time span from the earliest of the last 3 fights to the cutoff date.
    Returns None if fewer than 2 fights.
    """
    if len(history) < 2:
        return None
    window = history[-3:] if len(history) >= 3 else history[:]
    earliest = window[0].event_date
    span_days = (cutoff_date - earliest).days
    if span_days <= 0:
        return None
    span_years = span_days / 365.25
    return len(window) / span_years


def compute_rolling_features(
    history: list[FightHistory],
    windows: tuple[int, ...] = _DEFAULT_WINDOWS,
    cutoff_date: date | None = None,
) -> dict:
    """Compute rolling-window features for the given windows.

    For each window size N the most recent N fights (history[-N:]) are used.
    If the fighter has fewer than N prior fights, all available fights are used
    and `has_{N}_fights` is set to False.

    Also computes trend features (slope/std over last 5) and fights_per_year_last3.

    Args:
        history:      FightHistory list sorted oldest-first (output of get_history).
        windows:      window sizes to compute, default (1, 3, 5).
        cutoff_date:  event date of the target fight (needed for fights_per_year_last3).

    Returns:
        dict with keys `last{N}_{metric}`, `has_{N}_fights`, trend features,
        and fights_per_year_last3.
    """
    result: dict = {}
    n_available = len(history)

    for n in windows:
        window = history[-n:] if n_available >= n else history[:]
        feats = _window_features(window)

        result[f"has_{n}_fights"] = n_available >= n
        for metric, value in feats.items():
            result[f"last{n}_{metric}"] = value

    # Trend features (slope and volatility over last 5 fights)
    result.update(_compute_trend_features(history, n=5))

    # Activity rate over last 3 fights
    result["fights_per_year_last3"] = (
        _compute_fights_per_year_last3(history, cutoff_date)
        if cutoff_date is not None else None
    )

    return result
