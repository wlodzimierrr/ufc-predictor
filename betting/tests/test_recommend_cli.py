"""Smoke tests for the current betting recommendation CLI."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from betting import recommend


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
            "odds_timestamp": "2026-08-01T11:00:00+00:00",
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
        "odds_timestamp": "2026-08-01T11:00:00+00:00",
        "american_odds": 100,
        "decimal_odds": None,
        "normalized_decimal_odds": "2.0",
        "implied_probability": "0.5",
        "overround": "1.045454545454545454545454546",
        "no_vig_implied_probability": "0.4782608695652173913043478261",
    }


def _valid_odds(bookmaker: str = "TestBook", line_type: str = "current") -> list[dict[str, object]]:
    return [
        _odds_side("fighter-a", "fighter-b", bookmaker=bookmaker, line_type=line_type),
        _odds_side("fighter-b", "fighter-a", bookmaker=bookmaker, line_type=line_type),
    ]


def test_filter_prediction_rows_selects_next_event():
    rows = [
        _prediction(event_id="event-later", event_name="Later Card", event_date="2026-08-09"),
        _prediction(event_id="event-next", event_name="Next Card", event_date="2026-08-02"),
        _prediction(
            event_id="event-next",
            event_name="Next Card",
            event_date="2026-08-02",
            fight_id="fight-2",
        ),
    ]

    filtered = recommend.filter_prediction_rows(rows, next_event=True)

    assert [row["fight_id"] for row in filtered] == ["fight-1", "fight-2"]
    assert {row["event_name"] for row in filtered} == {"Next Card"}


def test_current_betting_cli_smoke_writes_reports(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(
        recommend,
        "fetch_current_prediction_rows",
        lambda conn: [_prediction()],
    )
    monkeypatch.setattr(
        recommend,
        "fetch_no_vig_odds_rows",
        lambda conn, bookmaker=None, line_type="current": _valid_odds(
            bookmaker=bookmaker or "TestBook",
            line_type=line_type,
        ),
    )

    code = recommend.main(
        [
            "--next",
            "--bookmaker",
            "TestBook",
            "--line-type",
            "current",
            "--bankroll",
            "1000",
            "--as-of",
            "2026-08-01T12:00:00+00:00",
            "--report-dir",
            str(tmp_path),
        ],
        conn=object(),
    )

    assert code == 0
    output = capsys.readouterr().out
    assert "Bets recommended: 1" in output
    assert "Total stake: 20.00" in output
    assert "Top pass reasons:" in output

    recommendation_rows = list(csv.DictReader((tmp_path / "betting_recommendations.csv").open()))
    summary_rows = list(csv.DictReader((tmp_path / "betting_event_summary.csv").open()))

    bet_rows = [row for row in recommendation_rows if row["decision"] == "bet"]
    assert len(bet_rows) == 1
    assert bet_rows[0]["recommended_fighter_id"] == "fighter-a"
    assert bet_rows[0]["final_stake_fraction"] == "0.02"
    assert bet_rows[0]["stake_amount"] == "20.00"
    assert "fractional_kelly" in bet_rows[0]["reason_codes"]

    assert summary_rows == [
        {
            "event_id": "event-1",
            "event_name": "UFC Test Card",
            "event_date": "2026-08-02",
            "bets_recommended": "1",
            "total_stake_fraction": "0.02",
            "total_stake_amount": "20.00",
            "event_exposure_fraction": "0.02",
            "pass_count": "1",
            "top_pass_reasons": "edge_below_threshold=1, ev_below_threshold=1",
            "drawdown_protection_enabled": "false",
            "drawdown_protection_fired": "false",
        }
    ]
