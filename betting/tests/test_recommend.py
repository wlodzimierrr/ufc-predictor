"""Tests for joining current predictions to betting odds."""

from __future__ import annotations

import sys
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from betting.reasons import ReasonCode
from betting.recommend import (
    JoinIssue,
    RecommendationInputResult,
    apply_bet_pass_policy,
    build_recommendation_inputs,
)


AS_OF = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


def _prediction(**overrides) -> dict[str, object]:
    row = {
        "event_id": "event-1",
        "event_name": "UFC Test Card",
        "event_date": "2026-08-02",
        "fight_id": "fight-1",
        "fighter_1_id": "fighter-a",
        "fighter_2_id": "fighter-b",
        "fighter_1_name": "Fighter A",
        "fighter_2_name": "Fighter B",
        "predicted_prob_f1": "0.68",
        "calibrated_prob_f1": "0.70",
        "confidence_tier": "high",
        "is_uncertain": False,
        "model_name": "test-model",
        "model_artifact": "models/test/model.pkl",
        "scored_at": "2026-08-01T10:00:00+00:00",
    }
    row.update(overrides)
    return row


def _odds_side(
    fighter_id: str,
    opponent_id: str,
    *,
    timestamp: str = "2026-08-01T11:00:00+00:00",
    bookmaker: str = "TestBook",
    line_type: str = "current",
) -> dict[str, object]:
    if fighter_id == "fighter-a":
        return {
            "fight_id": "fight-1",
            "event_id": "event-1",
            "fighter_id": fighter_id,
            "opponent_fighter_id": opponent_id,
            "bookmaker": bookmaker,
            "market": "moneyline",
            "line_type": line_type,
            "odds_timestamp": timestamp,
            "american_odds": -120,
            "decimal_odds": None,
            "normalized_decimal_odds": "1.833333333333333333333333333",
            "implied_probability": "0.5454545454545454545454545455",
            "overround": "1.045454545454545454545454546",
            "no_vig_implied_probability": "0.5217391304347826086956521737",
        }
    return {
        "fight_id": "fight-1",
        "event_id": "event-1",
        "fighter_id": fighter_id,
        "opponent_fighter_id": opponent_id,
        "bookmaker": bookmaker,
        "market": "moneyline",
        "line_type": line_type,
        "odds_timestamp": timestamp,
        "american_odds": 100,
        "decimal_odds": None,
        "normalized_decimal_odds": "2.0",
        "implied_probability": "0.5",
        "overround": "1.045454545454545454545454546",
        "no_vig_implied_probability": "0.4782608695652173913043478261",
    }


def _valid_odds(**overrides) -> list[dict[str, object]]:
    side_a = _odds_side("fighter-a", "fighter-b", **overrides)
    side_b = _odds_side("fighter-b", "fighter-a", **overrides)
    return [side_a, side_b]


def test_join_assigns_fighter_side_model_probabilities():
    result = build_recommendation_inputs([_prediction()], _valid_odds(), as_of=AS_OF)

    assert result.issues == ()
    assert len(result.rows) == 2
    by_fighter = {row.fighter_id: row for row in result.rows}
    assert by_fighter["fighter-a"].fighter_slot == "fighter_1"
    assert by_fighter["fighter-a"].model_probability == Decimal("0.70")
    assert by_fighter["fighter-b"].fighter_slot == "fighter_2"
    assert by_fighter["fighter-b"].model_probability == Decimal("0.30")


def test_join_preserves_prediction_and_market_fields():
    result = build_recommendation_inputs([_prediction()], _valid_odds(), as_of=AS_OF)

    row = result.rows[0]
    assert row.event_name == "UFC Test Card"
    assert row.predicted_prob_f1 == Decimal("0.68")
    assert row.calibrated_prob_f1 == Decimal("0.70")
    assert row.confidence_tier == "high"
    assert row.bookmaker == "TestBook"
    assert row.line_type == "current"
    assert row.market == "moneyline"
    assert row.market_implied_probability > Decimal("0")
    assert row.no_vig_market_probability > Decimal("0")


def test_join_can_filter_by_bookmaker_and_line_type():
    odds = _valid_odds(bookmaker="KeepBook") + _valid_odds(bookmaker="OtherBook")

    result = build_recommendation_inputs(
        [_prediction()],
        odds,
        as_of=AS_OF,
        bookmaker="KeepBook",
        line_type="current",
    )

    assert len(result.rows) == 2
    assert {row.bookmaker for row in result.rows} == {"KeepBook"}


def test_join_excludes_stale_odds_with_issue():
    result = build_recommendation_inputs(
        [_prediction()],
        _valid_odds(timestamp="2026-07-29T11:00:00+00:00"),
        as_of=AS_OF,
    )

    assert result.rows == ()
    assert len(result.issues) == 1
    assert result.issues[0].reason_code == ReasonCode.STALE_ODDS


def test_join_marks_ambiguous_odds_when_side_is_missing():
    result = build_recommendation_inputs(
        [_prediction()],
        [_odds_side("fighter-a", "fighter-b")],
        as_of=AS_OF,
    )

    assert result.rows == ()
    assert len(result.issues) == 1
    assert result.issues[0].reason_code == ReasonCode.AMBIGUOUS_ODDS


def test_join_marks_ambiguous_odds_when_fighters_do_not_match_prediction():
    result = build_recommendation_inputs(
        [_prediction()],
        [
            _odds_side("fighter-a", "fighter-c"),
            _odds_side("fighter-c", "fighter-a"),
        ],
        as_of=AS_OF,
    )

    assert result.rows == ()
    assert len(result.issues) == 1
    assert result.issues[0].reason_code == ReasonCode.AMBIGUOUS_ODDS


def test_join_reports_missing_odds_for_prediction_without_market_rows():
    result = build_recommendation_inputs([_prediction()], [], as_of=AS_OF)

    assert result.rows == ()
    assert len(result.issues) == 1
    assert result.issues[0].reason_code == ReasonCode.MISSING_ODDS


def test_policy_recommends_high_tier_positive_value_candidate():
    inputs = build_recommendation_inputs([_prediction()], _valid_odds(), as_of=AS_OF)

    result = apply_bet_pass_policy(inputs)

    bets = [decision for decision in result.decisions if decision.decision == "bet"]
    assert len(bets) == 1
    assert bets[0].recommended_fighter_id == "fighter-a"
    assert bets[0].recommended_fighter_name == "Fighter A"
    assert bets[0].reason_codes == (ReasonCode.POSITIVE_EDGE, ReasonCode.POSITIVE_EV)


def test_policy_allows_medium_tier_positive_value_candidate():
    inputs = build_recommendation_inputs(
        [_prediction(confidence_tier="medium")],
        _valid_odds(),
        as_of=AS_OF,
    )

    result = apply_bet_pass_policy(inputs)

    bets = [decision for decision in result.decisions if decision.decision == "bet"]
    assert len(bets) == 1
    assert bets[0].recommended_fighter_id == "fighter-a"


def test_policy_toss_up_tier_always_passes_even_with_positive_value():
    inputs = build_recommendation_inputs(
        [_prediction(confidence_tier="toss-up")],
        _valid_odds(),
        as_of=AS_OF,
    )

    result = apply_bet_pass_policy(inputs)

    assert all(decision.decision == "pass" for decision in result.decisions)
    assert {decision.reason_codes for decision in result.decisions} == {
        (ReasonCode.TOSS_UP_TIER,),
    }


def test_policy_missing_stale_and_ambiguous_odds_always_pass():
    issues = (
        JoinIssue("fight-1", ReasonCode.MISSING_ODDS, "missing"),
        JoinIssue("fight-2", ReasonCode.STALE_ODDS, "stale"),
        JoinIssue("fight-3", ReasonCode.AMBIGUOUS_ODDS, "ambiguous"),
    )
    inputs = RecommendationInputResult(rows=(), issues=issues)

    result = apply_bet_pass_policy(inputs)

    assert [decision.decision for decision in result.decisions] == ["pass", "pass", "pass"]
    assert [decision.reason_codes for decision in result.decisions] == [
        (ReasonCode.MISSING_ODDS,),
        (ReasonCode.STALE_ODDS,),
        (ReasonCode.AMBIGUOUS_ODDS,),
    ]


def test_policy_invalid_odds_always_pass():
    inputs = build_recommendation_inputs([_prediction()], _valid_odds(), as_of=AS_OF)
    invalid_row = replace(inputs.rows[0], normalized_decimal_odds=Decimal("1.0"))

    result = apply_bet_pass_policy(RecommendationInputResult(rows=(invalid_row,), issues=()))

    assert len(result.decisions) == 1
    assert result.decisions[0].decision == "pass"
    assert result.decisions[0].recommended_fighter_id is None
    assert result.decisions[0].reason_codes == (ReasonCode.INVALID_ODDS,)


def test_policy_edge_below_threshold_pass_reason():
    inputs = build_recommendation_inputs(
        [_prediction(calibrated_prob_f1="0.54")],
        _valid_odds(),
        as_of=AS_OF,
    )

    result = apply_bet_pass_policy(inputs)

    by_fighter = {decision.evaluated_fighter_id: decision for decision in result.decisions}
    assert by_fighter["fighter-a"].decision == "pass"
    assert ReasonCode.EDGE_BELOW_THRESHOLD in by_fighter["fighter-a"].reason_codes


def test_policy_ev_below_threshold_pass_reason():
    inputs = build_recommendation_inputs([_prediction()], _valid_odds(), as_of=AS_OF)
    low_price_row = replace(inputs.rows[0], normalized_decimal_odds=Decimal("1.05"))

    result = apply_bet_pass_policy(RecommendationInputResult(rows=(low_price_row,), issues=()))

    assert result.decisions[0].decision == "pass"
    assert result.decisions[0].reason_codes == (
        ReasonCode.POSITIVE_EDGE,
        ReasonCode.EV_BELOW_THRESHOLD,
    )


def test_policy_never_recommends_both_sides_of_one_market():
    inputs = build_recommendation_inputs([_prediction()], _valid_odds(), as_of=AS_OF)
    conflicting_rows = tuple(
        replace(
            row,
            normalized_decimal_odds=Decimal("4.00"),
            no_vig_market_probability=Decimal("0.10"),
        )
        for row in inputs.rows
    )

    result = apply_bet_pass_policy(
        RecommendationInputResult(rows=conflicting_rows, issues=()),
    )

    bets = [decision for decision in result.decisions if decision.decision == "bet"]
    assert len(bets) == 1
    assert len(result.decisions) == 2
    assert any(
        decision.decision == "pass"
        and decision.reason_codes == (ReasonCode.AMBIGUOUS_ODDS,)
        for decision in result.decisions
    )
