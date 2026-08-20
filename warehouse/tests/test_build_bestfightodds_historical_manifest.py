"""Tests for the BFO historical review manifest builder."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from betting.bfo_mean_market_backtest import DEFAULT_BOOKMAKER, DEFAULT_SOURCE
from warehouse.build_bestfightodds_historical_manifest import (
    REVIEW_MANIFEST_COLUMNS,
    build_bestfightodds_historical_review_manifest,
)


def test_build_historical_manifest_selects_completed_predictions_without_bfo_rows(tmp_path):
    predictions = tmp_path / "predictions.csv"
    odds = tmp_path / "fight_odds.csv"
    fights = tmp_path / "fights.csv"
    output = tmp_path / "review.csv"
    _write_csv(predictions, [
        _prediction("fight-good", "event-good", event_date="2026-08-01"),
        _prediction("fight-existing", "event-existing", event_date="2026-08-02"),
        _prediction("fight-upcoming", "event-upcoming", event_date="2026-08-21"),
        _prediction("fight-unresolved", "event-unresolved", event_date="2026-08-03", resolved="False"),
    ])
    _write_csv(odds, [_bfo_odds("fight-existing", "event-existing")])
    _write_csv(fights, [
        _fight("fight-good", "event-good"),
        _fight("fight-existing", "event-existing"),
        _fight("fight-upcoming", "event-upcoming", event_status="upcoming"),
        _fight("fight-unresolved", "event-unresolved"),
    ])

    result = build_bestfightodds_historical_review_manifest(
        predictions_path=predictions,
        odds_path=odds,
        fights_path=fights,
        output_path=output,
        as_of_date="2026-08-20",
    )

    assert result.counters["prediction_rows_read"] == 4
    assert result.counters["skipped_existing_bfo_mean"] == 1
    assert result.counters["skipped_not_past_event"] == 1
    assert result.counters["skipped_unresolved_prediction"] == 1
    assert result.counters["manifest_rows_written"] == 1
    rows = _read_csv(output)
    assert len(rows) == 1
    assert rows[0]["local_fight_id"] == "fight-good"
    assert rows[0]["target_status"] == "needs_manual_bfo_lookup"
    assert rows[0]["purpose"] == "historical_backfill"
    assert rows[0]["bfo_payload_p1_url"] == ""
    assert rows[0]["capture_manifest_ready"] == "false"
    assert "Review BFO event/fighter pages manually" in rows[0]["manual_lookup_notes"]


def test_historical_manifest_prioritizes_recent_completed_fights_deterministically(tmp_path):
    predictions = tmp_path / "predictions.csv"
    odds = tmp_path / "fight_odds.csv"
    fights = tmp_path / "fights.csv"
    output = tmp_path / "review.csv"
    _write_csv(predictions, [
        _prediction("fight-old", "event-old", event_date="2026-06-01", scored_at="2026-05-30 10:00:00+00:00"),
        _prediction("fight-newer", "event-newer", event_date="2026-08-01", scored_at="2026-07-30 10:00:00+00:00"),
        _prediction("fight-newest", "event-newest", event_date="2026-08-02", scored_at="2026-07-31 10:00:00+00:00"),
    ])
    _write_csv(odds, [])
    _write_csv(fights, [
        _fight("fight-old", "event-old"),
        _fight("fight-newer", "event-newer"),
        _fight("fight-newest", "event-newest"),
    ])

    result = build_bestfightodds_historical_review_manifest(
        predictions_path=predictions,
        odds_path=odds,
        fights_path=fights,
        output_path=output,
        max_fights=2,
        as_of_date="2026-08-20",
    )

    assert result.counters["candidate_fights"] == 3
    assert [row["local_fight_id"] for row in result.rows] == ["fight-newest", "fight-newer"]
    assert [row["review_priority"] for row in result.rows] == ["1", "2"]


def test_historical_manifest_rejects_non_positive_max_fights(tmp_path):
    with pytest.raises(ValueError, match="max_fights"):
        build_bestfightodds_historical_review_manifest(
            predictions_path=tmp_path / "missing_predictions.csv",
            odds_path=tmp_path / "missing_odds.csv",
            fights_path=tmp_path / "missing_fights.csv",
            output_path=tmp_path / "review.csv",
            max_fights=0,
        )


def _prediction(
    fight_id: str,
    event_id: str,
    *,
    event_date: str,
    scored_at: str = "2026-07-30 10:00:00+00:00",
    resolved: str = "True",
) -> dict[str, str]:
    return {
        "event_id": event_id,
        "event_name": f"UFC {event_id}",
        "event_date": event_date,
        "fight_id": fight_id,
        "fighter_1_name": "Fighter A",
        "fighter_2_name": "Fighter B",
        "scored_at": scored_at,
        "calibrated_prob_f1": "0.60",
        "actual_winner_name": "Fighter A",
        "resolved": resolved,
        "confidence_tier": "medium",
    }


def _fight(fight_id: str, event_id: str, *, event_status: str = "completed") -> dict[str, str]:
    return {
        "scraped_at": "2026-08-18 00:00:00 UTC",
        "fight_id": fight_id,
        "event_id": event_id,
        "fighter_1_id": f"{fight_id}-fighter-a",
        "fighter_2_id": f"{fight_id}-fighter-b",
        "event_status": event_status,
    }


def _bfo_odds(fight_id: str, event_id: str) -> dict[str, str]:
    return {
        "fight_id": fight_id,
        "event_id": event_id,
        "event_date": "2026-08-02",
        "fighter_id": f"{fight_id}-fighter-a",
        "fighter_name": "Fighter A",
        "opponent_fighter_id": f"{fight_id}-fighter-b",
        "bookmaker": DEFAULT_BOOKMAKER,
        "market": "moneyline",
        "line_type": "current",
        "odds_timestamp": "2026-07-30T10:00:00+00:00",
        "american_odds": "",
        "decimal_odds": "2.0",
        "source": DEFAULT_SOURCE,
        "source_url": "https://www.bestfightodds.com/api/ggd?m=1&p=1",
        "imported_at": "2026-08-19T20:59:51+00:00",
    }


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    columns = list(rows[0]) if rows else REVIEW_MANIFEST_COLUMNS
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))
