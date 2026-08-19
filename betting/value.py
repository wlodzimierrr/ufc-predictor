"""Value and expected-value decision helpers."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Mapping

from betting.config import BettingConfig, default_config
from betting.reasons import ReasonCode


@dataclass(frozen=True)
class ValueEvaluation:
    """Computed model-vs-market value metrics for one fighter side."""

    decision: str
    model_probability: Decimal | None
    market_implied_probability: Decimal | None
    no_vig_market_probability: Decimal | None
    offered_decimal_odds: Decimal | None
    edge: Decimal | None
    ev_per_unit: Decimal | None
    ev_percent: Decimal | None
    reason_codes: tuple[ReasonCode, ...]

    @property
    def passes_thresholds(self) -> bool:
        return self.decision == "bet"


def evaluate_value(
    row: Mapping[str, object] | object,
    *,
    config: BettingConfig | None = None,
) -> ValueEvaluation:
    """Compute edge and EV for a joined recommendation input row."""
    config = config or default_config()

    try:
        model_probability = _required_probability(row, "model_probability")
        market_implied = _required_probability(row, "market_implied_probability")
        no_vig_market = _required_probability(row, "no_vig_market_probability")
        offered_decimal_odds = _required_decimal_odds(row)
    except ValueError:
        return ValueEvaluation(
            decision="pass",
            model_probability=_optional_decimal(row, "model_probability"),
            market_implied_probability=_optional_decimal(row, "market_implied_probability"),
            no_vig_market_probability=_optional_decimal(row, "no_vig_market_probability"),
            offered_decimal_odds=_optional_offered_decimal_odds(row),
            edge=None,
            ev_per_unit=None,
            ev_percent=None,
            reason_codes=(ReasonCode.INVALID_ODDS,),
        )

    edge = model_probability - no_vig_market
    net_decimal = offered_decimal_odds - Decimal("1")
    ev_per_unit = model_probability * net_decimal - (Decimal("1") - model_probability)
    reason_codes = _threshold_reason_codes(
        edge=edge,
        ev_per_unit=ev_per_unit,
        min_edge=Decimal(str(config.risk.min_edge)),
        min_ev=Decimal(str(config.risk.min_ev)),
    )
    decision = (
        "bet"
        if reason_codes == (ReasonCode.POSITIVE_EDGE, ReasonCode.POSITIVE_EV)
        else "pass"
    )

    return ValueEvaluation(
        decision=decision,
        model_probability=model_probability,
        market_implied_probability=market_implied,
        no_vig_market_probability=no_vig_market,
        offered_decimal_odds=offered_decimal_odds,
        edge=edge,
        ev_per_unit=ev_per_unit,
        ev_percent=ev_per_unit,
        reason_codes=reason_codes,
    )


def _threshold_reason_codes(
    *,
    edge: Decimal,
    ev_per_unit: Decimal,
    min_edge: Decimal,
    min_ev: Decimal,
) -> tuple[ReasonCode, ReasonCode]:
    return (
        ReasonCode.POSITIVE_EDGE if edge >= min_edge else ReasonCode.EDGE_BELOW_THRESHOLD,
        ReasonCode.POSITIVE_EV if ev_per_unit >= min_ev else ReasonCode.EV_BELOW_THRESHOLD,
    )


def _required_probability(row: Mapping[str, object] | object, key: str) -> Decimal:
    value = _optional_decimal(row, key)
    if value is None:
        raise ValueError(f"missing {key}")
    if not Decimal("0") <= value <= Decimal("1"):
        raise ValueError(f"{key} must be between 0 and 1")
    return value


def _required_decimal_odds(row: Mapping[str, object] | object) -> Decimal:
    value = _optional_offered_decimal_odds(row)
    if value is None:
        raise ValueError("missing offered decimal odds")
    if value <= Decimal("1"):
        raise ValueError("offered decimal odds must be greater than 1")
    return value


def _optional_offered_decimal_odds(row: Mapping[str, object] | object) -> Decimal | None:
    for key in ("normalized_decimal_odds", "offered_decimal_odds", "decimal_odds"):
        value = _optional_decimal(row, key)
        if value is not None:
            return value
    return None


def _optional_decimal(row: Mapping[str, object] | object, key: str) -> Decimal | None:
    value = row.get(key) if isinstance(row, Mapping) else getattr(row, key, None)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


__all__ = [
    "ValueEvaluation",
    "evaluate_value",
]
