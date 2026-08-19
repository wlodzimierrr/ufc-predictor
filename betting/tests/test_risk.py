"""Tests for Kelly staking and exposure caps."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from betting.config import BettingConfig, RiskConfig
from betting.reasons import ReasonCode
from betting.recommend import BetDecision
from betting.risk import (
    apply_staking_caps,
    fractional_kelly_stake,
    full_kelly_fraction,
)


def _bet(**overrides) -> BetDecision:
    row = {
        "decision": "bet",
        "event_id": "event-1",
        "fight_id": "fight-1",
        "recommended_fighter_id": "fighter-a",
        "recommended_fighter_name": "Fighter A",
        "reason_codes": (ReasonCode.POSITIVE_EDGE, ReasonCode.POSITIVE_EV),
        "bookmaker": "TestBook",
        "market": "moneyline",
        "line_type": "current",
        "odds_timestamp": datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
        "evaluated_fighter_id": "fighter-a",
        "evaluated_fighter_name": "Fighter A",
        "confidence_tier": "high",
        "model_probability": Decimal("0.55"),
        "offered_decimal_odds": Decimal("2.00"),
        "edge": Decimal("0.05"),
        "ev_per_unit": Decimal("0.10"),
    }
    row.update(overrides)
    return BetDecision(**row)


def _config(**risk_overrides) -> BettingConfig:
    return BettingConfig(risk=RiskConfig(**risk_overrides))


def test_full_kelly_fraction_positive():
    assert full_kelly_fraction(
        decimal_odds="2.00",
        model_probability="0.55",
    ) == Decimal("0.10")


def test_fractional_kelly_uses_default_quarter_kelly():
    stake = fractional_kelly_stake(
        decimal_odds="2.00",
        model_probability="0.55",
        bankroll="1000",
    )

    assert stake.decision == "bet"
    assert stake.full_kelly_fraction == Decimal("0.10")
    assert stake.fractional_kelly_fraction == Decimal("0.0250")
    assert stake.stake_amount == Decimal("25.0000")
    assert stake.reason_codes == (ReasonCode.FRACTIONAL_KELLY,)


def test_zero_kelly_produces_pass():
    stake = fractional_kelly_stake(decimal_odds="2.00", model_probability="0.50")

    assert stake.decision == "pass"
    assert stake.full_kelly_fraction == Decimal("0.00")
    assert stake.fractional_kelly_fraction == Decimal("0")
    assert stake.reason_codes == (ReasonCode.KELLY_NON_POSITIVE,)


def test_negative_kelly_produces_pass():
    stake = fractional_kelly_stake(decimal_odds="2.00", model_probability="0.45")

    assert stake.decision == "pass"
    assert stake.full_kelly_fraction == Decimal("-0.10")
    assert stake.fractional_kelly_fraction == Decimal("0")
    assert stake.reason_codes == (ReasonCode.KELLY_NON_POSITIVE,)


def test_medium_tier_cap_is_smaller_than_high_tier_cap():
    high = apply_staking_caps([_bet(confidence_tier="high")]).decisions[0]
    medium = apply_staking_caps([
        _bet(confidence_tier="medium"),
    ]).decisions[0]

    assert high.final_stake_fraction == Decimal("0.02")
    assert medium.final_stake_fraction == Decimal("0.01")
    assert ReasonCode.TIER_CAP_APPLIED in medium.reason_codes


def test_toss_up_cap_is_zero_and_results_in_no_bet():
    result = apply_staking_caps([
        _bet(confidence_tier="toss-up", model_probability=Decimal("0.70")),
    ])

    decision = result.decisions[0]
    assert decision.decision == "pass"
    assert decision.final_stake_fraction == Decimal("0")
    assert decision.uncapped_kelly_fraction > Decimal("0")
    assert decision.fractional_kelly_fraction > Decimal("0")
    assert ReasonCode.TOSS_UP_TIER in decision.reason_codes
    assert ReasonCode.SINGLE_BET_CAP_ZERO in decision.reason_codes


def test_single_bet_cap_is_enforced_after_tier_cap():
    result = apply_staking_caps(
        [_bet(model_probability=Decimal("0.70"))],
        config=_config(high_tier_cap=0.05, max_single_bet_fraction=0.02),
    )

    decision = result.decisions[0]
    assert decision.uncapped_kelly_fraction == Decimal("0.40")
    assert decision.fractional_kelly_fraction == Decimal("0.1000")
    assert decision.final_stake_fraction == Decimal("0.02")
    assert decision.reason_codes[-2:] == (
        ReasonCode.TIER_CAP_APPLIED,
        ReasonCode.SINGLE_BET_CAP_APPLIED,
    )


def test_event_exposure_cap_is_applied_cumulatively():
    result = apply_staking_caps(
        [
            _bet(fight_id="fight-1", recommended_fighter_id="a", ev_per_unit=Decimal("0.30")),
            _bet(fight_id="fight-2", recommended_fighter_id="b", ev_per_unit=Decimal("0.20")),
            _bet(fight_id="fight-3", recommended_fighter_id="c", ev_per_unit=Decimal("0.10")),
            _bet(fight_id="fight-4", recommended_fighter_id="d", ev_per_unit=Decimal("0.05")),
        ]
    )

    assert [decision.fight_id for decision in result.decisions] == [
        "fight-1",
        "fight-2",
        "fight-3",
        "fight-4",
    ]
    assert [decision.final_stake_fraction for decision in result.decisions] == [
        Decimal("0.02"),
        Decimal("0.02"),
        Decimal("0.02"),
        Decimal("0"),
    ]
    assert result.decisions[-1].decision == "pass"
    assert ReasonCode.EVENT_EXPOSURE_CAP_REACHED in result.decisions[-1].reason_codes


def test_event_cap_can_reduce_later_bet_to_remaining_exposure():
    result = apply_staking_caps(
        [
            _bet(fight_id="fight-1", recommended_fighter_id="a", ev_per_unit=Decimal("0.30")),
            _bet(fight_id="fight-2", recommended_fighter_id="b", ev_per_unit=Decimal("0.20")),
        ],
        config=_config(max_event_fraction=0.03),
    )

    assert [decision.final_stake_fraction for decision in result.decisions] == [
        Decimal("0.02"),
        Decimal("0.01"),
    ]
    assert ReasonCode.EVENT_CAP_APPLIED in result.decisions[1].reason_codes


def test_drawdown_protection_is_disabled_by_default():
    result = apply_staking_caps([_bet()], current_drawdown=Decimal("0.99"))

    decision = result.decisions[0]
    assert decision.decision == "bet"
    assert decision.drawdown_protection_enabled is False
    assert decision.drawdown_protection_fired is False
    assert decision.current_drawdown == Decimal("0.99")
    assert ReasonCode.DRAWDOWN_PROTECTION not in decision.reason_codes


def test_enabled_drawdown_protection_does_not_infer_current_drawdown():
    result = apply_staking_caps(
        [_bet()],
        config=_config(drawdown_protection_threshold=0.20),
    )

    decision = result.decisions[0]
    assert decision.decision == "bet"
    assert decision.drawdown_protection_enabled is True
    assert decision.drawdown_protection_fired is False
    assert decision.current_drawdown is None
    assert ReasonCode.DRAWDOWN_PROTECTION not in decision.reason_codes


def test_enabled_drawdown_protection_allows_bets_below_threshold():
    result = apply_staking_caps(
        [_bet()],
        config=_config(drawdown_protection_threshold=0.20),
        current_drawdown=Decimal("0.10"),
    )

    decision = result.decisions[0]
    assert decision.decision == "bet"
    assert decision.drawdown_protection_enabled is True
    assert decision.drawdown_protection_fired is False
    assert decision.current_drawdown == Decimal("0.10")
    assert ReasonCode.DRAWDOWN_PROTECTION not in decision.reason_codes


def test_drawdown_protection_passes_qualifying_bets_at_threshold():
    result = apply_staking_caps(
        [_bet()],
        config=_config(drawdown_protection_threshold=0.20),
        current_drawdown=Decimal("0.20"),
    )

    decision = result.decisions[0]
    assert decision.decision == "pass"
    assert decision.uncapped_kelly_fraction == Decimal("0.10")
    assert decision.fractional_kelly_fraction == Decimal("0.0250")
    assert decision.final_stake_fraction == Decimal("0")
    assert decision.drawdown_protection_enabled is True
    assert decision.drawdown_protection_fired is True
    assert decision.current_drawdown == Decimal("0.20")
    assert ReasonCode.DRAWDOWN_PROTECTION in decision.reason_codes


def test_invalid_decimal_odds_rejected_for_kelly():
    with pytest.raises(ValueError):
        full_kelly_fraction(decimal_odds="1.0", model_probability="0.55")
