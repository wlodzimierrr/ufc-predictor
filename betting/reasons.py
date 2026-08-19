"""Standard reason codes for betting decisions and reports."""

from __future__ import annotations

from enum import StrEnum
from typing import Iterable

REASON_CODE_SEPARATOR = "|"


class ReasonCode(StrEnum):
    """Machine-readable reason codes for bet/pass/cap decisions."""

    MISSING_ODDS = "missing_odds"
    STALE_ODDS = "stale_odds"
    AMBIGUOUS_ODDS = "ambiguous_odds"
    INVALID_ODDS = "invalid_odds"
    MISSING_PREDICTION = "missing_prediction"
    TOSS_UP_TIER = "toss_up_tier"
    EDGE_BELOW_THRESHOLD = "edge_below_threshold"
    EV_BELOW_THRESHOLD = "ev_below_threshold"
    KELLY_NON_POSITIVE = "kelly_non_positive"
    SINGLE_BET_CAP_ZERO = "single_bet_cap_zero"
    EVENT_EXPOSURE_CAP_REACHED = "event_exposure_cap_reached"
    FIGHT_BET_CAP_APPLIED = "fight_bet_cap_applied"
    DRAWDOWN_PROTECTION = "drawdown_protection"
    NON_WIN_OUTCOME = "non_win_outcome"
    UNRESOLVED_OUTCOME = "unresolved_outcome"
    FIGHTER_REPLACEMENT = "fighter_replacement"
    POSITIVE_EDGE = "positive_edge"
    POSITIVE_EV = "positive_ev"
    FRACTIONAL_KELLY = "fractional_kelly"
    TIER_CAP_APPLIED = "tier_cap_applied"
    SINGLE_BET_CAP_APPLIED = "single_bet_cap_applied"
    EVENT_CAP_APPLIED = "event_cap_applied"


PASS_REASON_CODES: tuple[ReasonCode, ...] = (
    ReasonCode.MISSING_ODDS,
    ReasonCode.STALE_ODDS,
    ReasonCode.AMBIGUOUS_ODDS,
    ReasonCode.INVALID_ODDS,
    ReasonCode.MISSING_PREDICTION,
    ReasonCode.TOSS_UP_TIER,
    ReasonCode.EDGE_BELOW_THRESHOLD,
    ReasonCode.EV_BELOW_THRESHOLD,
    ReasonCode.KELLY_NON_POSITIVE,
    ReasonCode.SINGLE_BET_CAP_ZERO,
    ReasonCode.EVENT_EXPOSURE_CAP_REACHED,
    ReasonCode.FIGHT_BET_CAP_APPLIED,
    ReasonCode.DRAWDOWN_PROTECTION,
    ReasonCode.NON_WIN_OUTCOME,
    ReasonCode.UNRESOLVED_OUTCOME,
    ReasonCode.FIGHTER_REPLACEMENT,
)

BET_CAP_REASON_CODES: tuple[ReasonCode, ...] = (
    ReasonCode.POSITIVE_EDGE,
    ReasonCode.POSITIVE_EV,
    ReasonCode.FRACTIONAL_KELLY,
    ReasonCode.TIER_CAP_APPLIED,
    ReasonCode.SINGLE_BET_CAP_APPLIED,
    ReasonCode.EVENT_CAP_APPLIED,
)

ALL_REASON_CODES: tuple[ReasonCode, ...] = PASS_REASON_CODES + BET_CAP_REASON_CODES


def normalize_reason_code(code: ReasonCode | str) -> ReasonCode:
    """Return a known reason code enum from a string or enum value."""
    try:
        return ReasonCode(code)
    except ValueError as exc:
        raise ValueError(f"unknown betting reason code: {code}") from exc


def normalize_reason_codes(codes: Iterable[ReasonCode | str]) -> tuple[ReasonCode, ...]:
    """Normalize and de-duplicate reason codes while preserving first-seen order."""
    normalized: list[ReasonCode] = []
    seen: set[ReasonCode] = set()

    for code in codes:
        reason_code = normalize_reason_code(code)
        if reason_code not in seen:
            normalized.append(reason_code)
            seen.add(reason_code)

    return tuple(normalized)


def format_reason_codes(codes: Iterable[ReasonCode | str]) -> str:
    """Format reason codes for report output as a pipe-delimited string."""
    return REASON_CODE_SEPARATOR.join(code.value for code in normalize_reason_codes(codes))


def parse_reason_codes(value: str) -> tuple[ReasonCode, ...]:
    """Parse pipe-delimited report reason codes."""
    if not value:
        return ()
    return normalize_reason_codes(value.split(REASON_CODE_SEPARATOR))


def threshold_reason_codes(
    *,
    edge: float,
    ev_per_unit: float,
    min_edge: float,
    min_ev: float,
) -> tuple[ReasonCode, ...]:
    """Return value-threshold reason codes for a candidate betting decision."""
    return (
        ReasonCode.POSITIVE_EDGE if edge >= min_edge else ReasonCode.EDGE_BELOW_THRESHOLD,
        ReasonCode.POSITIVE_EV if ev_per_unit >= min_ev else ReasonCode.EV_BELOW_THRESHOLD,
    )


__all__ = [
    "ALL_REASON_CODES",
    "BET_CAP_REASON_CODES",
    "PASS_REASON_CODES",
    "REASON_CODE_SEPARATOR",
    "ReasonCode",
    "format_reason_codes",
    "normalize_reason_code",
    "normalize_reason_codes",
    "parse_reason_codes",
    "threshold_reason_codes",
]
