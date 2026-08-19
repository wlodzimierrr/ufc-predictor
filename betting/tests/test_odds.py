"""Tests for betting odds conversion helpers."""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from betting.odds import (
    american_to_decimal_odds,
    calculate_no_vig_probabilities,
    decimal_to_american_odds,
    implied_probability,
    normalize_decimal_odds,
    parse_optional_american_odds,
    parse_optional_decimal,
    validate_decimal_odds,
)


def test_positive_american_odds_convert_to_decimal_odds():
    assert american_to_decimal_odds("+150") == Decimal("2.5")


def test_negative_american_odds_convert_to_decimal_odds():
    assert american_to_decimal_odds("-200") == Decimal("1.5")


def test_decimal_odds_convert_to_implied_probability():
    assert implied_probability("2.50") == Decimal("0.4")


def test_decimal_to_american_for_underdog_price():
    assert decimal_to_american_odds("2.50") == Decimal("150")


def test_decimal_to_american_for_favorite_price():
    assert decimal_to_american_odds("1.50") == Decimal("-200")


def test_decimal_to_american_does_not_round_non_integer_price():
    assert decimal_to_american_odds("1.91") != Decimal("-110")


def test_normalize_decimal_prefers_supplied_decimal_odds():
    assert normalize_decimal_odds(american_odds="+150", decimal_odds="2.40") == Decimal("2.40")


def test_parse_optional_american_odds_handles_numeric_strings():
    assert parse_optional_american_odds(" +150 ") == 150
    assert parse_optional_american_odds("") is None


def test_parse_optional_decimal_handles_numeric_strings():
    assert parse_optional_decimal(" 2.50 ") == Decimal("2.50")
    assert parse_optional_decimal("") is None


def test_invalid_zero_american_odds_is_rejected():
    with pytest.raises(ValueError, match="cannot be zero"):
        american_to_decimal_odds(0)


def test_non_integer_american_odds_is_rejected():
    with pytest.raises(ValueError, match="must be an integer"):
        american_to_decimal_odds("150.5")


def test_invalid_decimal_odds_is_rejected():
    with pytest.raises(ValueError, match="greater than 1.0"):
        validate_decimal_odds("1.0")


def test_missing_odds_are_rejected():
    with pytest.raises(ValueError, match="either American or decimal odds"):
        normalize_decimal_odds()


def _side(fighter_id: str, opponent_id: str, american_odds: str) -> dict[str, object]:
    return {
        "fighter_id": fighter_id,
        "opponent_fighter_id": opponent_id,
        "american_odds": american_odds,
        "bookmaker": "TestBook",
        "market": "moneyline",
        "line_type": "current",
        "odds_timestamp": "2026-07-31T12:00:00+00:00",
    }


def test_no_vig_balanced_market_probabilities():
    result = calculate_no_vig_probabilities([
        _side("fighter-a", "fighter-b", "+100"),
        _side("fighter-b", "fighter-a", "+100"),
    ])

    assert result.valid
    assert result.reason is None
    assert [row.no_vig_implied_probability for row in result.rows] == [
        Decimal("0.5"),
        Decimal("0.5"),
    ]
    assert all(row.overround == Decimal("1.0") for row in result.rows)


def test_no_vig_overround_market_probabilities_sum_to_one():
    result = calculate_no_vig_probabilities([
        _side("fighter-a", "fighter-b", "-120"),
        _side("fighter-b", "fighter-a", "+100"),
    ])

    assert result.valid
    assert len(result.rows) == 2
    probability_sum = sum(row.no_vig_implied_probability for row in result.rows)
    assert probability_sum == pytest.approx(Decimal("1.0"))
    assert result.rows[0].overround > Decimal("1.0")


def test_no_vig_preserves_market_metadata():
    result = calculate_no_vig_probabilities([
        _side("fighter-a", "fighter-b", "-120"),
        _side("fighter-b", "fighter-a", "+100"),
    ])

    assert result.valid
    row = result.rows[0]
    assert row.bookmaker == "TestBook"
    assert row.market == "moneyline"
    assert row.line_type == "current"
    assert row.odds_timestamp == "2026-07-31T12:00:00+00:00"


def test_no_vig_missing_side_is_invalid():
    result = calculate_no_vig_probabilities([
        _side("fighter-a", "fighter-b", "-120"),
    ])

    assert not result.valid
    assert result.reason == "missing_side"
    assert result.rows == ()


def test_no_vig_duplicate_side_is_invalid():
    result = calculate_no_vig_probabilities([
        _side("fighter-a", "fighter-b", "-120"),
        _side("fighter-a", "fighter-b", "+100"),
    ])

    assert not result.valid
    assert result.reason == "duplicate_side"


def test_no_vig_more_than_two_sides_is_invalid():
    result = calculate_no_vig_probabilities([
        _side("fighter-a", "fighter-b", "-120"),
        _side("fighter-b", "fighter-a", "+100"),
        _side("fighter-c", "fighter-a", "+200"),
    ])

    assert not result.valid
    assert result.reason == "too_many_sides"


def test_no_vig_non_reciprocal_sides_are_invalid():
    result = calculate_no_vig_probabilities([
        _side("fighter-a", "fighter-b", "-120"),
        _side("fighter-b", "fighter-c", "+100"),
    ])

    assert not result.valid
    assert result.reason == "non_reciprocal_sides"


def test_no_vig_mismatched_metadata_is_invalid():
    side_a = _side("fighter-a", "fighter-b", "-120")
    side_b = _side("fighter-b", "fighter-a", "+100")
    side_b["bookmaker"] = "OtherBook"

    result = calculate_no_vig_probabilities([side_a, side_b])

    assert not result.valid
    assert result.reason == "mismatched_bookmaker"
