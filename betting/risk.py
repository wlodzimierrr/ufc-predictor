"""Risk management and staking helpers."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from betting.config import BettingConfig, default_config
from betting.reasons import ReasonCode
from betting.recommend import BetDecision


@dataclass(frozen=True)
class KellyStake:
    """Pure Kelly staking result before confidence/exposure caps."""

    decision: str
    full_kelly_fraction: Decimal
    fractional_kelly_fraction: Decimal
    stake_amount: Decimal | None
    reason_codes: tuple[ReasonCode, ...]


@dataclass(frozen=True)
class StakedBetDecision:
    """Final staking result after Kelly sizing and exposure caps."""

    decision: str
    event_id: str | None
    fight_id: str | None
    recommended_fighter_id: str | None
    recommended_fighter_name: str | None
    reason_codes: tuple[ReasonCode, ...]
    event_name: str | None = None
    event_date: object | None = None
    bookmaker: str | None = None
    market: str | None = None
    line_type: str | None = None
    odds_timestamp: object | None = None
    scored_at: object | None = None
    evaluated_fighter_id: str | None = None
    evaluated_fighter_name: str | None = None
    opponent_fighter_id: str | None = None
    opponent_fighter_name: str | None = None
    confidence_tier: str | None = None
    model_probability: Decimal | None = None
    market_implied_probability: Decimal | None = None
    no_vig_market_probability: Decimal | None = None
    offered_decimal_odds: Decimal | None = None
    edge: Decimal | None = None
    ev_per_unit: Decimal | None = None
    actual_winner_fighter_id: str | None = None
    actual_winner_name: str | None = None
    result_type: str | None = None
    resolved: bool = False
    uncapped_kelly_fraction: Decimal = Decimal("0")
    fractional_kelly_fraction: Decimal = Decimal("0")
    final_stake_fraction: Decimal = Decimal("0")
    stake_amount: Decimal | None = None
    drawdown_protection_enabled: bool = False
    drawdown_protection_fired: bool = False
    current_drawdown: Decimal | None = None


@dataclass(frozen=True)
class StakingResult:
    """Collection of final staking decisions."""

    decisions: tuple[StakedBetDecision, ...]


def full_kelly_fraction(
    *,
    decimal_odds: Decimal | int | float | str,
    model_probability: Decimal | int | float | str,
) -> Decimal:
    """Return the full Kelly fraction for a decimal-odds bet."""
    odds = _decimal(decimal_odds, "decimal_odds")
    probability = _probability(model_probability, "model_probability")
    if odds <= Decimal("1"):
        raise ValueError("decimal_odds must be greater than 1")

    b = odds - Decimal("1")
    q = Decimal("1") - probability
    return (b * probability - q) / b


def fractional_kelly_stake(
    *,
    decimal_odds: Decimal | int | float | str,
    model_probability: Decimal | int | float | str,
    config: BettingConfig | None = None,
    bankroll: Decimal | int | float | str | None = None,
) -> KellyStake:
    """Convert a probability and price into a fractional Kelly stake."""
    config = config or default_config()
    full_kelly = full_kelly_fraction(
        decimal_odds=decimal_odds,
        model_probability=model_probability,
    )
    if full_kelly <= Decimal("0"):
        return KellyStake(
            decision="pass",
            full_kelly_fraction=full_kelly,
            fractional_kelly_fraction=Decimal("0"),
            stake_amount=Decimal("0") if bankroll is not None else None,
            reason_codes=(ReasonCode.KELLY_NON_POSITIVE,),
        )

    fractional = full_kelly * Decimal(str(config.risk.kelly_fraction))
    return KellyStake(
        decision="bet",
        full_kelly_fraction=full_kelly,
        fractional_kelly_fraction=fractional,
        stake_amount=_stake_amount(fractional, bankroll),
        reason_codes=(ReasonCode.FRACTIONAL_KELLY,),
    )


def apply_staking_caps(
    decisions: list[BetDecision] | tuple[BetDecision, ...],
    *,
    config: BettingConfig | None = None,
    bankroll: Decimal | int | float | str | None = None,
    current_drawdown: Decimal | int | float | str | None = None,
) -> StakingResult:
    """Apply Kelly sizing, drawdown protection, tier caps, and exposure caps."""
    config = config or default_config()
    bankroll_decimal = _optional_positive_decimal(bankroll, "bankroll")
    drawdown_value = _optional_positive_decimal(current_drawdown, "current_drawdown")
    drawdown_enabled = config.risk.drawdown_protection_threshold is not None
    drawdown_fired = (
        drawdown_enabled
        and drawdown_value is not None
        and drawdown_value >= Decimal(str(config.risk.drawdown_protection_threshold))
    )
    output: list[StakedBetDecision] = []
    event_exposure: dict[str, Decimal] = {}

    for decision in sorted(decisions, key=_allocation_key):
        if decision.decision != "bet":
            output.append(_zero_stake_decision(
                decision,
                drawdown_protection_enabled=drawdown_enabled,
                drawdown_protection_fired=drawdown_fired,
                current_drawdown=drawdown_value,
            ))
            continue

        kelly = fractional_kelly_stake(
            decimal_odds=decision.offered_decimal_odds,
            model_probability=decision.model_probability,
            config=config,
            bankroll=bankroll_decimal,
        )
        reason_codes = _append_reason_codes(decision.reason_codes, kelly.reason_codes)
        if kelly.decision != "bet":
            output.append(_zero_stake_decision(
                decision,
                reason_codes=reason_codes,
                uncapped_kelly_fraction=kelly.full_kelly_fraction,
                drawdown_protection_enabled=drawdown_enabled,
                drawdown_protection_fired=drawdown_fired,
                current_drawdown=drawdown_value,
            ))
            continue

        if drawdown_fired:
            output.append(_zero_stake_decision(
                decision,
                reason_codes=_append_reason_codes(
                    reason_codes,
                    (ReasonCode.DRAWDOWN_PROTECTION,),
                ),
                uncapped_kelly_fraction=kelly.full_kelly_fraction,
                fractional_kelly_fraction=kelly.fractional_kelly_fraction,
                drawdown_protection_enabled=drawdown_enabled,
                drawdown_protection_fired=True,
                current_drawdown=drawdown_value,
            ))
            continue

        tier_cap = _tier_cap(decision.confidence_tier, config)
        capped_fraction = kelly.fractional_kelly_fraction
        if capped_fraction > tier_cap:
            capped_fraction = tier_cap
            reason_codes = _append_reason_codes(reason_codes, (ReasonCode.TIER_CAP_APPLIED,))

        single_cap = Decimal(str(config.risk.max_single_bet_fraction))
        if capped_fraction > single_cap:
            capped_fraction = single_cap
            reason_codes = _append_reason_codes(
                reason_codes,
                (ReasonCode.SINGLE_BET_CAP_APPLIED,),
            )

        if capped_fraction <= Decimal("0"):
            pass_reasons = (ReasonCode.SINGLE_BET_CAP_ZERO,)
            if decision.confidence_tier == "toss-up":
                pass_reasons = (ReasonCode.TOSS_UP_TIER,) + pass_reasons
            output.append(_zero_stake_decision(
                decision,
                reason_codes=_append_reason_codes(reason_codes, pass_reasons),
                uncapped_kelly_fraction=kelly.full_kelly_fraction,
                fractional_kelly_fraction=kelly.fractional_kelly_fraction,
                drawdown_protection_enabled=drawdown_enabled,
                drawdown_protection_fired=drawdown_fired,
                current_drawdown=drawdown_value,
            ))
            continue

        event_key = decision.event_id or decision.fight_id or "__unknown_event__"
        max_event_fraction = Decimal(str(config.risk.max_event_fraction))
        used_fraction = event_exposure.get(event_key, Decimal("0"))
        remaining_fraction = max_event_fraction - used_fraction
        if remaining_fraction <= Decimal("0"):
            output.append(_zero_stake_decision(
                decision,
                reason_codes=_append_reason_codes(
                    reason_codes,
                    (ReasonCode.EVENT_EXPOSURE_CAP_REACHED,),
                ),
                uncapped_kelly_fraction=kelly.full_kelly_fraction,
                fractional_kelly_fraction=kelly.fractional_kelly_fraction,
                drawdown_protection_enabled=drawdown_enabled,
                drawdown_protection_fired=drawdown_fired,
                current_drawdown=drawdown_value,
            ))
            continue
        if capped_fraction > remaining_fraction:
            capped_fraction = remaining_fraction
            reason_codes = _append_reason_codes(reason_codes, (ReasonCode.EVENT_CAP_APPLIED,))

        event_exposure[event_key] = used_fraction + capped_fraction
        output.append(StakedBetDecision(
            decision="bet",
            event_id=decision.event_id,
            fight_id=decision.fight_id,
            recommended_fighter_id=decision.recommended_fighter_id,
            recommended_fighter_name=decision.recommended_fighter_name,
            reason_codes=reason_codes,
            event_name=decision.event_name,
            event_date=decision.event_date,
            bookmaker=decision.bookmaker,
            market=decision.market,
            line_type=decision.line_type,
            odds_timestamp=decision.odds_timestamp,
            scored_at=decision.scored_at,
            evaluated_fighter_id=decision.evaluated_fighter_id,
            evaluated_fighter_name=decision.evaluated_fighter_name,
            opponent_fighter_id=decision.opponent_fighter_id,
            opponent_fighter_name=decision.opponent_fighter_name,
            confidence_tier=decision.confidence_tier,
            model_probability=decision.model_probability,
            market_implied_probability=decision.market_implied_probability,
            no_vig_market_probability=decision.no_vig_market_probability,
            offered_decimal_odds=decision.offered_decimal_odds,
            edge=decision.edge,
            ev_per_unit=decision.ev_per_unit,
            actual_winner_fighter_id=decision.actual_winner_fighter_id,
            actual_winner_name=decision.actual_winner_name,
            result_type=decision.result_type,
            resolved=decision.resolved,
            uncapped_kelly_fraction=kelly.full_kelly_fraction,
            fractional_kelly_fraction=kelly.fractional_kelly_fraction,
            final_stake_fraction=capped_fraction,
            stake_amount=_stake_amount(capped_fraction, bankroll_decimal),
            drawdown_protection_enabled=drawdown_enabled,
            drawdown_protection_fired=drawdown_fired,
            current_drawdown=drawdown_value,
        ))

    return StakingResult(decisions=tuple(output))


def _allocation_key(decision: BetDecision) -> tuple:
    if decision.decision != "bet":
        return (1, Decimal("0"), Decimal("0"), "", "", "")
    return (
        0,
        -(decision.ev_per_unit or Decimal("0")),
        -(decision.edge or Decimal("0")),
        decision.odds_timestamp or "",
        decision.fight_id or "",
        decision.recommended_fighter_id or "",
    )


def _tier_cap(confidence_tier: str | None, config: BettingConfig) -> Decimal:
    if confidence_tier == "high":
        return Decimal(str(config.risk.high_tier_cap))
    if confidence_tier == "medium":
        return Decimal(str(config.risk.medium_tier_cap))
    if confidence_tier == "toss-up":
        return Decimal(str(config.risk.toss_up_tier_cap))
    return Decimal("0")


def _zero_stake_decision(
    decision: BetDecision,
    *,
    reason_codes: tuple[ReasonCode, ...] | None = None,
    uncapped_kelly_fraction: Decimal = Decimal("0"),
    fractional_kelly_fraction: Decimal = Decimal("0"),
    drawdown_protection_enabled: bool = False,
    drawdown_protection_fired: bool = False,
    current_drawdown: Decimal | None = None,
) -> StakedBetDecision:
    return StakedBetDecision(
        decision="pass",
        event_id=decision.event_id,
        fight_id=decision.fight_id,
        recommended_fighter_id=None,
        recommended_fighter_name=None,
        reason_codes=reason_codes or decision.reason_codes,
        event_name=decision.event_name,
        event_date=decision.event_date,
        bookmaker=decision.bookmaker,
        market=decision.market,
        line_type=decision.line_type,
        odds_timestamp=decision.odds_timestamp,
        scored_at=decision.scored_at,
        evaluated_fighter_id=decision.evaluated_fighter_id,
        evaluated_fighter_name=decision.evaluated_fighter_name,
        opponent_fighter_id=decision.opponent_fighter_id,
        opponent_fighter_name=decision.opponent_fighter_name,
        confidence_tier=decision.confidence_tier,
        model_probability=decision.model_probability,
        market_implied_probability=decision.market_implied_probability,
        no_vig_market_probability=decision.no_vig_market_probability,
        offered_decimal_odds=decision.offered_decimal_odds,
        edge=decision.edge,
        ev_per_unit=decision.ev_per_unit,
        actual_winner_fighter_id=decision.actual_winner_fighter_id,
        actual_winner_name=decision.actual_winner_name,
        result_type=decision.result_type,
        resolved=decision.resolved,
        uncapped_kelly_fraction=uncapped_kelly_fraction,
        fractional_kelly_fraction=fractional_kelly_fraction,
        final_stake_fraction=Decimal("0"),
        stake_amount=Decimal("0"),
        drawdown_protection_enabled=drawdown_protection_enabled,
        drawdown_protection_fired=drawdown_protection_fired,
        current_drawdown=current_drawdown,
    )


def _stake_amount(
    stake_fraction: Decimal,
    bankroll: Decimal | int | float | str | None,
) -> Decimal | None:
    bankroll_decimal = _optional_positive_decimal(bankroll, "bankroll")
    if bankroll_decimal is None:
        return None
    return stake_fraction * bankroll_decimal


def _append_reason_codes(
    existing: tuple[ReasonCode, ...],
    additions: tuple[ReasonCode, ...],
) -> tuple[ReasonCode, ...]:
    output = list(existing)
    for reason_code in additions:
        if reason_code not in output:
            output.append(reason_code)
    return tuple(output)


def _probability(value: Decimal | int | float | str | None, label: str) -> Decimal:
    probability = _decimal(value, label)
    if not Decimal("0") <= probability <= Decimal("1"):
        raise ValueError(f"{label} must be between 0 and 1")
    return probability


def _optional_positive_decimal(
    value: Decimal | int | float | str | None,
    label: str,
) -> Decimal | None:
    if value is None:
        return None
    decimal_value = _decimal(value, label)
    if decimal_value < Decimal("0"):
        raise ValueError(f"{label} must be non-negative")
    return decimal_value


def _decimal(value: Decimal | int | float | str | None, label: str) -> Decimal:
    if value is None:
        raise ValueError(f"{label} is required")
    try:
        return Decimal(str(value).strip())
    except InvalidOperation as exc:
        raise ValueError(f"invalid {label}: {value}") from exc


__all__ = [
    "KellyStake",
    "StakedBetDecision",
    "StakingResult",
    "apply_staking_caps",
    "fractional_kelly_stake",
    "full_kelly_fraction",
]
