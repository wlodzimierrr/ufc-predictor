"""Tests for the BestFightOdds Mean market-benchmark CSV runner."""

from __future__ import annotations

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from betting.bfo_mean_market_backtest import (
    DEFAULT_BOOKMAKER,
    DEFAULT_SOURCE,
    _prepare_bfo_no_vig_odds_rows,
    load_bfo_mean_market_benchmark_dataset,
)


def test_bfo_no_vig_rows_require_mean_source_and_two_sides():
    rows = [
        _odds_side("fighter-a", "fighter-b", "2.5"),
        _odds_side("fighter-b", "fighter-a", "1.6"),
        _odds_side(
            "fighter-a",
            "fighter-b",
            "2.4",
            source="manual_review_only",
        ),
    ]

    prepared, counters = _prepare_bfo_no_vig_odds_rows(rows)

    assert counters["bfo_mean_rows"] == 3
    assert counters["skipped_bfo_wrong_source"] == 1
    assert counters["bfo_market_groups"] == 1
    assert len(prepared) == 2
    assert {row["no_vig_implied_probability"] for row in prepared}


def test_market_benchmark_dataset_joins_bfo_predictions_and_selects_latest_before_prediction(tmp_path):
    predictions_path = tmp_path / "predictions.csv"
    odds_path = tmp_path / "fight_odds.csv"
    fights_path = tmp_path / "fights.csv"
    fighters_path = tmp_path / "fighters.csv"

    _write_csv(predictions_path, [
        {
            "event_id": "event-1",
            "event_name": "UFC Benchmark",
            "event_date": "2026-08-15",
            "fight_id": "fight-1",
            "fighter_1_name": "Fighter A",
            "fighter_2_name": "Fighter B",
            "weight_class": "welterweight",
            "scored_at": "2026-08-09 11:17:50+00:00",
            "predicted_prob_f1": "0.70",
            "calibrated_prob_f1": "0.70",
            "confidence_tier": "high",
            "is_uncertain": "False",
            "actual_label": "1.0",
            "actual_winner_name": "Fighter A",
            "resolved": "True",
            "model_name": "test-model",
            "model_artifact": "models/test",
        },
        {
            "event_id": "event-2",
            "event_name": "Other UFC",
            "event_date": "2026-08-15",
            "fight_id": "fight-without-bfo",
            "fighter_1_name": "Other A",
            "fighter_2_name": "Other B",
            "weight_class": "lightweight",
            "scored_at": "2026-08-09 11:17:50+00:00",
            "predicted_prob_f1": "0.70",
            "calibrated_prob_f1": "0.70",
            "confidence_tier": "high",
            "is_uncertain": "False",
            "actual_label": "1.0",
            "actual_winner_name": "Other A",
            "resolved": "True",
            "model_name": "test-model",
            "model_artifact": "models/test",
        },
    ])
    _write_csv(fights_path, [{
        "scraped_at": "2026-08-16 00:00:00 UTC",
        "fight_id": "fight-1",
        "event_id": "event-1",
        "fighter_1_id": "fighter-a",
        "fighter_2_id": "fighter-b",
    }])
    _write_csv(fighters_path, [
        {
            "scraped_at": "2026-08-16 00:00:00 UTC",
            "fighter_id": "fighter-a",
            "full_name": "Fighter A",
        },
        {
            "scraped_at": "2026-08-16 00:00:00 UTC",
            "fighter_id": "fighter-b",
            "full_name": "Fighter B",
        },
    ])
    _write_csv(odds_path, [
        _odds_side("fighter-a", "fighter-b", "2.5", timestamp="2026-08-09T03:00:00+00:00"),
        _odds_side("fighter-b", "fighter-a", "1.6", timestamp="2026-08-09T03:00:00+00:00"),
        _odds_side("fighter-a", "fighter-b", "2.6", timestamp="2026-08-09T12:00:00+00:00"),
        _odds_side("fighter-b", "fighter-a", "1.55", timestamp="2026-08-09T12:00:00+00:00"),
    ])

    dataset, counters = load_bfo_mean_market_benchmark_dataset(
        predictions_path=predictions_path,
        odds_path=odds_path,
        fights_path=fights_path,
        fighters_path=fighters_path,
    )

    assert counters["saved_prediction_rows_read"] == 2
    assert counters["prediction_rows_prepared"] == 1
    assert counters["prediction_rows_after_filters"] == 1
    assert counters["dataset_issues"] == 0
    assert len(dataset.rows) == 2
    assert {row.fight_id for row in dataset.rows} == {"fight-1"}
    assert {row.odds_timestamp for row in dataset.rows} == {
        datetime(2026, 8, 9, 3, tzinfo=timezone.utc),
    }
    assert {row.actual_winner_fighter_id for row in dataset.rows} == {"fighter-a"}


def _odds_side(
    fighter_id: str,
    opponent_id: str,
    decimal_odds: str,
    *,
    source: str = DEFAULT_SOURCE,
    timestamp: str = "2026-08-09T03:00:00+00:00",
) -> dict[str, str]:
    return {
        "fight_id": "fight-1",
        "event_id": "event-1",
        "event_date": "2026-08-15",
        "fighter_id": fighter_id,
        "fighter_name": "Fighter A" if fighter_id == "fighter-a" else "Fighter B",
        "opponent_fighter_id": opponent_id,
        "bookmaker": DEFAULT_BOOKMAKER,
        "market": "moneyline",
        "line_type": "current",
        "odds_timestamp": timestamp,
        "american_odds": "",
        "decimal_odds": decimal_odds,
        "source": source,
        "source_url": "https://www.bestfightodds.com/api/ggd?m=1&p=1",
        "imported_at": "2026-08-19T20:59:51+00:00",
    }


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    columns = list(rows[0])
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
