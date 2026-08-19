"""Tests for leakage-safe historical betting dataset construction."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from betting import backtest
from betting.backtest import (
    BACKTEST_DETAIL_EVALUATED,
    BACKTEST_SUMMARY_COLUMNS,
    LINE_POLICY_CLOSING,
    LINE_POLICY_OPENING,
    build_backtest_report_rows,
    build_historical_betting_dataset,
    filter_historical_odds_rows,
    filter_historical_prediction_rows,
    generate_backtest_reports,
    simulate_betting_backtest,
)
from betting.config import BettingConfig
from betting.reasons import ReasonCode


def _prediction(**overrides) -> dict[str, object]:
    row = {
        "event_id": "event-1",
        "event_name": "UFC Historical Card",
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
        "actual_label": 1,
        "actual_winner_name": "Fighter A",
        "result_type": "win",
        "resolved": True,
    }
    row.update(overrides)
    return row


def _odds_side(
    fighter_id: str,
    opponent_id: str,
    *,
    timestamp: str,
    fight_id: str = "fight-1",
    event_id: str = "event-1",
    bookmaker: str = "TestBook",
    line_type: str = "current",
) -> dict[str, object]:
    if fighter_id == "fighter-a":
        return {
            "fight_id": fight_id,
            "event_id": event_id,
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
        "fight_id": fight_id,
        "event_id": event_id,
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


def _valid_odds(
    timestamp: str,
    line_type: str = "current",
    *,
    fight_id: str = "fight-1",
    event_id: str = "event-1",
) -> list[dict[str, object]]:
    return [
        _odds_side(
            "fighter-a",
            "fighter-b",
            timestamp=timestamp,
            line_type=line_type,
            fight_id=fight_id,
            event_id=event_id,
        ),
        _odds_side(
            "fighter-b",
            "fighter-a",
            timestamp=timestamp,
            line_type=line_type,
            fight_id=fight_id,
            event_id=event_id,
        ),
    ]


def test_future_odds_are_excluded_from_historical_dataset():
    result = build_historical_betting_dataset(
        [_prediction()],
        _valid_odds("2026-08-01T09:00:00+00:00")
        + _valid_odds("2026-08-01T11:00:00+00:00")
        + _valid_odds("2026-08-02T01:00:00+00:00"),
    )

    assert result.issues == ()
    assert len(result.rows) == 2
    assert {row.decision for row in result.rows} == {"evaluate"}
    assert {row.odds_timestamp for row in result.rows} == {
        datetime(2026, 8, 1, 9, 0, tzinfo=timezone.utc),
    }
    assert all(row.odds_timestamp <= row.scored_at for row in result.rows)
    assert all(row.odds_timestamp.date() < row.event_date for row in result.rows)


def test_future_predictions_are_excluded_from_historical_dataset():
    result = build_historical_betting_dataset(
        [_prediction(scored_at="2026-08-02T00:00:00+00:00")],
        _valid_odds("2026-08-01T09:00:00+00:00"),
    )

    assert result.rows == ()
    assert len(result.issues) == 1
    assert result.issues[0].reason_code == ReasonCode.MISSING_PREDICTION
    assert "on or after event date" in result.issues[0].detail


def test_historical_backtest_regression_excludes_future_predictions_and_odds():
    result = build_historical_betting_dataset(
        [
            _prediction(scored_at="2026-08-01T10:00:00+00:00"),
            _prediction(scored_at="2026-08-02T00:00:00+00:00"),
        ],
        _valid_odds("2026-08-01T09:00:00+00:00")
        + _valid_odds("2026-08-02T01:00:00+00:00"),
    )

    assert len(result.issues) == 1
    assert result.issues[0].reason_code == ReasonCode.MISSING_PREDICTION
    assert len(result.rows) == 2
    assert {row.decision for row in result.rows} == {"evaluate"}
    assert {row.scored_at for row in result.rows} == {
        datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc),
    }
    assert {row.odds_timestamp for row in result.rows} == {
        datetime(2026, 8, 1, 9, 0, tzinfo=timezone.utc),
    }

    report = build_backtest_report_rows(
        simulate_betting_backtest(result, starting_bankroll="1000"),
        dataset=result,
    )
    assert {row["scored_at"] for row in report.fight_rows} == {
        "2026-08-01T10:00:00+00:00",
    }
    assert {row["odds_timestamp"] for row in report.fight_rows} == {
        "2026-08-01T09:00:00+00:00",
    }


def test_missing_historical_odds_produces_pass_row():
    result = build_historical_betting_dataset([_prediction()], [])

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.decision == "pass"
    assert row.reason_codes == (ReasonCode.MISSING_ODDS,)
    assert row.scored_at == datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc)
    assert row.odds_timestamp is None


def test_future_only_historical_odds_produce_missing_odds_pass_not_bet():
    result = build_historical_betting_dataset(
        [_prediction()],
        _valid_odds("2026-08-02T01:00:00+00:00"),
    )

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.decision == "pass"
    assert row.reason_codes == (ReasonCode.MISSING_ODDS,)

    simulation = simulate_betting_backtest(result, starting_bankroll="1000")
    assert {bet.decision for bet in simulation.bets} == {"pass"}
    assert {bet.bet_result for bet in simulation.bets} == {"no_bet"}


def test_one_sided_historical_market_produces_ambiguous_pass_row():
    result = build_historical_betting_dataset(
        [_prediction()],
        [_odds_side("fighter-a", "fighter-b", timestamp="2026-08-01T09:00:00+00:00")],
    )

    assert len(result.rows) == 1
    assert result.rows[0].decision == "pass"
    assert result.rows[0].reason_codes == (ReasonCode.AMBIGUOUS_ODDS,)


def test_line_policy_can_select_opening_or_closing_lines():
    odds = (
        _valid_odds("2026-08-01T08:00:00+00:00", line_type="opening")
        + _valid_odds("2026-08-01T09:00:00+00:00", line_type="opening")
        + _valid_odds("2026-08-01T08:00:00+00:00", line_type="closing")
        + _valid_odds("2026-08-01T09:00:00+00:00", line_type="closing")
    )

    opening = build_historical_betting_dataset(
        [_prediction()],
        odds,
        line_policy=LINE_POLICY_OPENING,
    )
    closing = build_historical_betting_dataset(
        [_prediction()],
        odds,
        line_policy=LINE_POLICY_CLOSING,
    )

    assert {row.odds_timestamp for row in opening.rows} == {
        datetime(2026, 8, 1, 8, 0, tzinfo=timezone.utc),
    }
    assert {row.odds_timestamp for row in closing.rows} == {
        datetime(2026, 8, 1, 9, 0, tzinfo=timezone.utc),
    }
    assert all(row.model_probability in {Decimal("0.70"), Decimal("0.30")} for row in opening.rows)


def test_backtest_settlement_uses_explicit_win_loss_formulas():
    winning_dataset = build_historical_betting_dataset(
        [_prediction(actual_label=1, actual_winner_name="Fighter A")],
        _valid_odds("2026-08-01T09:00:00+00:00"),
    )
    losing_dataset = build_historical_betting_dataset(
        [_prediction(actual_label=0, actual_winner_name="Fighter B")],
        _valid_odds("2026-08-01T09:00:00+00:00"),
    )

    winning_result = simulate_betting_backtest(winning_dataset, starting_bankroll="1000")
    losing_result = simulate_betting_backtest(losing_dataset, starting_bankroll="1000")

    winning_bet = next(row for row in winning_result.bets if row.decision == "bet")
    losing_bet = next(row for row in losing_result.bets if row.decision == "bet")
    assert winning_bet.bet_result == "win"
    assert winning_bet.profit_loss_amount == (
        winning_bet.stake_amount * (winning_bet.offered_decimal_odds - Decimal("1"))
    )
    assert losing_bet.bet_result == "loss"
    assert losing_bet.profit_loss_amount == -losing_bet.stake_amount


def test_same_card_bets_use_bankroll_available_before_event():
    dataset = build_historical_betting_dataset(
        [
            _prediction(fight_id="fight-1"),
            _prediction(fight_id="fight-2"),
        ],
        _valid_odds("2026-08-01T09:00:00+00:00", fight_id="fight-1")
        + _valid_odds("2026-08-01T09:00:00+00:00", fight_id="fight-2"),
    )

    result = simulate_betting_backtest(dataset, starting_bankroll="1000")

    bets = [row for row in result.bets if row.decision == "bet"]
    assert len(bets) == 2
    assert {row.bankroll_before_event for row in bets} == {Decimal("1000")}
    assert [row.stake_amount for row in bets] == [Decimal("20.00"), Decimal("20.00")]
    assert result.events[0].total_staked == Decimal("40.00")


def test_max_one_bet_per_fight_keeps_best_candidate_across_books():
    dataset = build_historical_betting_dataset(
        [_prediction()],
        [
            _odds_side(
                "fighter-a",
                "fighter-b",
                timestamp="2026-08-01T09:00:00+00:00",
                bookmaker="BookA",
            ),
            _odds_side(
                "fighter-b",
                "fighter-a",
                timestamp="2026-08-01T09:00:00+00:00",
                bookmaker="BookA",
            ),
            _odds_side(
                "fighter-a",
                "fighter-b",
                timestamp="2026-08-01T09:00:00+00:00",
                bookmaker="BookB",
            ),
            _odds_side(
                "fighter-b",
                "fighter-a",
                timestamp="2026-08-01T09:00:00+00:00",
                bookmaker="BookB",
            ),
        ],
    )

    default_result = simulate_betting_backtest(dataset, starting_bankroll="1000")
    capped_result = simulate_betting_backtest(
        dataset,
        starting_bankroll="1000",
        max_one_bet_per_fight=True,
    )

    assert sum(1 for row in default_result.bets if row.decision == "bet") == 2
    assert sum(1 for row in capped_result.bets if row.decision == "bet") == 1
    capped_passes = [
        row
        for row in capped_result.bets
        if row.reason_codes == (ReasonCode.FIGHT_BET_CAP_APPLIED,)
    ]
    assert len(capped_passes) == 1
    assert capped_passes[0].stake_amount == Decimal("0")


def test_backtest_processes_events_in_ascending_event_date():
    dataset = build_historical_betting_dataset(
        [
            _prediction(
                event_id="event-later",
                event_name="Later Card",
                event_date="2026-08-09",
                fight_id="fight-later",
                scored_at="2026-08-08T10:00:00+00:00",
            ),
            _prediction(
                event_id="event-earlier",
                event_name="Earlier Card",
                event_date="2026-08-02",
                fight_id="fight-earlier",
                actual_label=0,
                actual_winner_name="Fighter B",
            ),
        ],
        _valid_odds(
            "2026-08-01T09:00:00+00:00",
            fight_id="fight-earlier",
            event_id="event-earlier",
        )
        + _valid_odds(
            "2026-08-08T09:00:00+00:00",
            fight_id="fight-later",
            event_id="event-later",
        ),
    )

    result = simulate_betting_backtest(dataset, starting_bankroll="1000")

    assert [event.event_id for event in result.events] == ["event-earlier", "event-later"]
    assert result.events[1].bankroll_before_event == result.events[0].bankroll_after_event


def test_backtest_computes_peak_bankroll_drawdown_and_max_drawdown():
    dataset = build_historical_betting_dataset(
        [
            _prediction(
                event_id="event-1",
                event_date="2026-08-02",
                fight_id="fight-1",
                actual_label=0,
                actual_winner_name="Fighter B",
            ),
            _prediction(
                event_id="event-2",
                event_date="2026-08-09",
                fight_id="fight-2",
                scored_at="2026-08-08T10:00:00+00:00",
                actual_label=0,
                actual_winner_name="Fighter B",
            ),
        ],
        _valid_odds("2026-08-01T09:00:00+00:00", fight_id="fight-1", event_id="event-1")
        + _valid_odds("2026-08-08T09:00:00+00:00", fight_id="fight-2", event_id="event-2"),
    )

    result = simulate_betting_backtest(dataset, starting_bankroll="1000")

    assert [event.bankroll_after_event for event in result.events] == [
        Decimal("980.00"),
        Decimal("960.4000"),
    ]
    assert result.peak_bankroll == Decimal("1000")
    assert result.max_drawdown == Decimal("0.0396")
    assert result.events[-1].drawdown == Decimal("0.0396")


def test_backtest_summary_columns_snapshot():
    assert BACKTEST_SUMMARY_COLUMNS == [
        "summary_type",
        "group",
        "total_bets",
        "wins",
        "losses",
        "pushes",
        "total_staked",
        "profit_loss",
        "roi",
        "hit_rate",
        "average_odds",
        "max_drawdown",
        "starting_bankroll",
        "ending_bankroll",
        "odds_policy",
        "require_odds_before_prediction",
        "max_one_bet_per_fight",
        "kelly_fraction",
        "min_edge",
        "min_ev",
        "max_single_bet_fraction",
        "max_event_fraction",
        "medium_tier_cap",
        "high_tier_cap",
        "toss_up_tier_cap",
        "drawdown_protection_threshold",
    ]


def test_backtest_summary_aggregates_roi_by_tier_edge_bucket_and_event():
    dataset = build_historical_betting_dataset(
        [
            _prediction(
                event_id="event-high",
                event_name="High Edge Card",
                event_date="2026-08-02",
                fight_id="fight-high",
                confidence_tier="high",
                actual_label=1,
                actual_winner_name="Fighter A",
            ),
            _prediction(
                event_id="event-medium",
                event_name="Medium Edge Card",
                event_date="2026-08-09",
                fight_id="fight-medium",
                scored_at="2026-08-08T10:00:00+00:00",
                calibrated_prob_f1="0.56",
                confidence_tier="medium",
                actual_label=0,
                actual_winner_name="Fighter B",
            ),
        ],
        _valid_odds("2026-08-01T09:00:00+00:00", fight_id="fight-high", event_id="event-high")
        + _valid_odds("2026-08-08T09:00:00+00:00", fight_id="fight-medium", event_id="event-medium"),
    )
    simulation = simulate_betting_backtest(dataset, starting_bankroll="1000")

    rows = build_backtest_report_rows(
        simulation,
        dataset=dataset,
        line_policy=dataset.line_policy,
        require_odds_before_prediction=dataset.require_odds_before_prediction,
    ).summary_rows
    by_key = {(row["summary_type"], row["group"]): row for row in rows}

    assert by_key[("confidence_tier", "high")]["total_bets"] == "1"
    assert Decimal(by_key[("confidence_tier", "high")]["roi"]) > Decimal("0.83")
    assert by_key[("confidence_tier", "medium")]["total_bets"] == "1"
    assert by_key[("confidence_tier", "medium")]["roi"] == "-1"
    assert by_key[("edge_bucket", "10%+")]["total_bets"] == "1"
    assert by_key[("edge_bucket", "3-5%")]["total_bets"] == "1"
    assert by_key[("event", "High Edge Card")]["wins"] == "1"
    assert by_key[("event", "Medium Edge Card")]["losses"] == "1"
    assert by_key[("overall", "all")]["odds_policy"] == "latest_current"
    assert by_key[("overall", "all")]["starting_bankroll"] == "1000"


def test_generate_backtest_reports_writes_configured_csvs(tmp_path):
    dataset = build_historical_betting_dataset(
        [_prediction()],
        _valid_odds("2026-08-01T09:00:00+00:00"),
    )
    config = BettingConfig(report_dir=str(tmp_path))

    result = generate_backtest_reports(dataset, config=config)

    assert result.fights_path == tmp_path / "betting_backtest_fights.csv"
    assert result.events_path == tmp_path / "betting_backtest_events.csv"
    assert result.summary_path == tmp_path / "betting_backtest_summary.csv"
    assert result.fights_path.exists()
    assert result.events_path.exists()
    assert result.summary_path.exists()
    assert result.rows.fight_rows
    assert result.rows.event_rows
    assert result.rows.summary_rows


def test_backtest_fight_report_can_use_evaluated_detail_mode():
    dataset = build_historical_betting_dataset(
        [_prediction()],
        _valid_odds("2026-08-01T09:00:00+00:00"),
    )
    simulation = simulate_betting_backtest(dataset, starting_bankroll="1000")

    rows = build_backtest_report_rows(
        simulation,
        dataset=dataset,
        detail_mode=BACKTEST_DETAIL_EVALUATED,
    ).fight_rows

    assert len(rows) == len(dataset.rows)
    assert {row["detail_mode"] for row in rows} == {"evaluated"}
    assert {row["decision"] for row in rows} == {"evaluate"}


def test_backtest_cli_smoke_writes_reports_from_fixture_rows(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(
        backtest,
        "fetch_pre_event_prediction_rows",
        lambda conn: [
            _prediction(),
            _prediction(
                event_id="event-later",
                event_date="2026-09-01",
                fight_id="fight-later",
                scored_at="2026-08-31T10:00:00+00:00",
            ),
        ],
    )
    monkeypatch.setattr(
        backtest,
        "fetch_historical_no_vig_odds_rows",
        lambda conn: _valid_odds("2026-08-01T09:00:00+00:00")
        + _valid_odds(
            "2026-08-31T09:00:00+00:00",
            fight_id="fight-later",
            event_id="event-later",
        ),
    )

    exit_code = backtest.main(
        [
            "--start-date",
            "2026-08-01",
            "--end-date",
            "2026-08-02",
            "--bookmaker",
            "TestBook",
            "--line-type",
            "current",
            "--odds-policy",
            "latest-before-prediction",
            "--initial-bankroll",
            "1000",
            "--kelly-fraction",
            "0.25",
            "--report-dir",
            str(tmp_path),
        ],
        conn=object(),
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Total bets: 1" in output
    assert "Profit/Loss:" in output
    assert (tmp_path / "betting_backtest_fights.csv").exists()
    assert (tmp_path / "betting_backtest_events.csv").exists()
    assert (tmp_path / "betting_backtest_summary.csv").exists()


def test_historical_backtest_filters_prediction_dates_and_odds_metadata():
    predictions = [
        _prediction(event_id="before", event_date="2026-07-01", fight_id="fight-before"),
        _prediction(event_id="inside", event_date="2026-08-01", fight_id="fight-inside"),
        _prediction(event_id="after", event_date="2026-09-01", fight_id="fight-after"),
    ]
    odds = [
        _odds_side("fighter-a", "fighter-b", timestamp="2026-08-01T09:00:00+00:00"),
        _odds_side(
            "fighter-a",
            "fighter-b",
            timestamp="2026-08-01T09:00:00+00:00",
            bookmaker="OtherBook",
        ),
        _odds_side(
            "fighter-a",
            "fighter-b",
            timestamp="2026-08-01T09:00:00+00:00",
            line_type="opening",
        ),
    ]

    filtered_predictions = filter_historical_prediction_rows(
        predictions,
        start_date="2026-08-01",
        end_date="2026-08-31",
    )
    filtered_odds = filter_historical_odds_rows(
        odds,
        bookmaker="TestBook",
        line_type="current",
    )

    assert [row["event_id"] for row in filtered_predictions] == ["inside"]
    assert len(filtered_odds) == 1
    assert filtered_odds[0]["bookmaker"] == "TestBook"
    assert filtered_odds[0]["line_type"] == "current"
