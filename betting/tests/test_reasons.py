"""Tests for centralized betting reason codes."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from betting.config import default_config
from betting.reasons import (
    BET_CAP_REASON_CODES,
    PASS_REASON_CODES,
    ReasonCode,
    format_reason_codes,
    normalize_reason_code,
    normalize_reason_codes,
    parse_reason_codes,
    threshold_reason_codes,
)


def test_pass_reason_codes_include_required_vocabulary():
    assert {code.value for code in PASS_REASON_CODES} == {
        "missing_odds",
        "stale_odds",
        "ambiguous_odds",
        "invalid_odds",
        "missing_prediction",
        "toss_up_tier",
        "edge_below_threshold",
        "ev_below_threshold",
        "kelly_non_positive",
        "single_bet_cap_zero",
        "event_exposure_cap_reached",
        "fight_bet_cap_applied",
        "drawdown_protection",
        "non_win_outcome",
        "unresolved_outcome",
        "fighter_replacement",
    }


def test_bet_and_cap_reason_codes_include_required_vocabulary():
    assert {code.value for code in BET_CAP_REASON_CODES} == {
        "positive_edge",
        "positive_ev",
        "fractional_kelly",
        "tier_cap_applied",
        "single_bet_cap_applied",
        "event_cap_applied",
    }


def test_format_reason_codes_is_pipe_delimited_for_reports():
    report_value = format_reason_codes([
        ReasonCode.POSITIVE_EDGE,
        "positive_ev",
        ReasonCode.FRACTIONAL_KELLY,
    ])

    assert report_value == "positive_edge|positive_ev|fractional_kelly"


def test_normalize_reason_codes_deduplicates_in_first_seen_order():
    codes = normalize_reason_codes([
        "positive_edge",
        ReasonCode.POSITIVE_EDGE,
        "single_bet_cap_applied",
    ])

    assert codes == (
        ReasonCode.POSITIVE_EDGE,
        ReasonCode.SINGLE_BET_CAP_APPLIED,
    )


def test_parse_reason_codes_round_trips_report_string():
    parsed = parse_reason_codes("missing_odds|stale_odds")

    assert parsed == (ReasonCode.MISSING_ODDS, ReasonCode.STALE_ODDS)


def test_unknown_reason_code_is_rejected():
    with pytest.raises(ValueError):
        normalize_reason_code("trust_me_bro")


def test_positive_value_scenario_gets_positive_reason_codes():
    risk = default_config().risk

    codes = threshold_reason_codes(
        edge=0.04,
        ev_per_unit=0.02,
        min_edge=risk.min_edge,
        min_ev=risk.min_ev,
    )

    assert codes == (ReasonCode.POSITIVE_EDGE, ReasonCode.POSITIVE_EV)


def test_low_edge_scenario_gets_threshold_pass_reason_code():
    risk = default_config().risk

    codes = threshold_reason_codes(
        edge=0.02,
        ev_per_unit=0.02,
        min_edge=risk.min_edge,
        min_ev=risk.min_ev,
    )

    assert codes == (ReasonCode.EDGE_BELOW_THRESHOLD, ReasonCode.POSITIVE_EV)


def test_low_ev_scenario_gets_threshold_pass_reason_code():
    risk = default_config().risk

    codes = threshold_reason_codes(
        edge=0.04,
        ev_per_unit=0.00,
        min_edge=risk.min_edge,
        min_ev=risk.min_ev,
    )

    assert codes == (ReasonCode.POSITIVE_EDGE, ReasonCode.EV_BELOW_THRESHOLD)
