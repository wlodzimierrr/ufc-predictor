"""Tests for betting value and expected-value calculations."""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from betting.reasons import ReasonCode
from betting.value import evaluate_value


def _row(**overrides) -> dict[str, object]:
    row = {
        "model_probability": "0.60",
        "market_implied_probability": "0.4761904761904761904761904762",
        "no_vig_market_probability": "0.52",
        "normalized_decimal_odds": "2.10",
    }
    row.update(overrides)
    return row


def test_positive_ev_and_edge_produce_bet_decision():
    evaluation = evaluate_value(_row())

    assert evaluation.decision == "bet"
    assert evaluation.model_probability == Decimal("0.60")
    assert evaluation.market_implied_probability == Decimal("0.4761904761904761904761904762")
    assert evaluation.no_vig_market_probability == Decimal("0.52")
    assert evaluation.edge == Decimal("0.08")
    assert evaluation.ev_per_unit == Decimal("0.260")
    assert evaluation.ev_percent == evaluation.ev_per_unit
    assert evaluation.reason_codes == (ReasonCode.POSITIVE_EDGE, ReasonCode.POSITIVE_EV)


def test_negative_ev_produces_pass_decision():
    evaluation = evaluate_value(_row(
        model_probability="0.50",
        no_vig_market_probability="0.47",
        normalized_decimal_odds="1.50",
    ))

    assert evaluation.decision == "pass"
    assert evaluation.edge == Decimal("0.03")
    assert evaluation.ev_per_unit == Decimal("-0.250")
    assert evaluation.reason_codes == (ReasonCode.POSITIVE_EDGE, ReasonCode.EV_BELOW_THRESHOLD)


def test_positive_edge_with_insufficient_ev_produces_pass():
    evaluation = evaluate_value(_row(
        model_probability="0.55",
        no_vig_market_probability="0.51",
        normalized_decimal_odds="1.80",
    ))

    assert evaluation.decision == "pass"
    assert evaluation.edge == Decimal("0.04")
    assert evaluation.ev_per_unit == Decimal("-0.010")
    assert evaluation.reason_codes == (ReasonCode.POSITIVE_EDGE, ReasonCode.EV_BELOW_THRESHOLD)


def test_sufficient_ev_with_insufficient_edge_produces_pass():
    evaluation = evaluate_value(_row(
        model_probability="0.55",
        no_vig_market_probability="0.53",
        normalized_decimal_odds="2.10",
    ))

    assert evaluation.decision == "pass"
    assert evaluation.edge == Decimal("0.02")
    assert evaluation.ev_per_unit == Decimal("0.155")
    assert evaluation.reason_codes == (ReasonCode.EDGE_BELOW_THRESHOLD, ReasonCode.POSITIVE_EV)


def test_missing_odds_produce_pass():
    evaluation = evaluate_value(_row(normalized_decimal_odds=None))

    assert evaluation.decision == "pass"
    assert evaluation.edge is None
    assert evaluation.ev_per_unit is None
    assert evaluation.reason_codes == (ReasonCode.INVALID_ODDS,)


def test_invalid_odds_produce_pass():
    evaluation = evaluate_value(_row(normalized_decimal_odds="1.0"))

    assert evaluation.decision == "pass"
    assert evaluation.reason_codes == (ReasonCode.INVALID_ODDS,)
