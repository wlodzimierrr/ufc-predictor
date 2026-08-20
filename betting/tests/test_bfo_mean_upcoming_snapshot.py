"""Tests for the BestFightOdds Mean upcoming snapshot report."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from betting.config import default_config
from betting.bfo_mean_market_backtest import DEFAULT_BOOKMAKER, DEFAULT_SOURCE
from betting.bfo_mean_upcoming_snapshot import (
    BENCHMARK_LABEL,
    build_upcoming_snapshot_rows,
)


def test_upcoming_snapshot_selects_latest_two_sided_odds_before_as_of(tmp_path):
    predictions_path = tmp_path / "pre_event_prediction_fights.csv"
    odds_path = tmp_path / "fight_odds.csv"
    fights_path = tmp_path / "fights.csv"
    fighters_path = tmp_path / "fighters.csv"

    _write_csv(predictions_path, [
        {
            "event_id": "event-upcoming",
            "event_name": "UFC Upcoming",
            "event_date": "2026-08-22",
            "fight_id": "fight-upcoming",
            "fighter_1_name": "Fighter A",
            "fighter_2_name": "Fighter B",
            "scored_at": "2026-08-16 20:25:39+00:00",
            "calibrated_prob_f1": "0.60",
            "actual_label": "",
        },
    ])
    _write_csv(fights_path, [{
        "scraped_at": "2026-08-18 00:00:00 UTC",
        "fight_id": "fight-upcoming",
        "event_id": "event-upcoming",
        "fighter_1_id": "fighter-a",
        "fighter_2_id": "fighter-b",
    }])
    _write_csv(fighters_path, [
        {
            "scraped_at": "2026-08-18 00:00:00 UTC",
            "fighter_id": "fighter-a",
            "full_name": "Fighter A",
        },
        {
            "scraped_at": "2026-08-18 00:00:00 UTC",
            "fighter_id": "fighter-b",
            "full_name": "Fighter B",
        },
    ])
    _write_csv(odds_path, [
        _odds_side("fighter-a", "fighter-b", "2.00", timestamp="2026-08-19T10:00:00+00:00"),
        _odds_side("fighter-b", "fighter-a", "2.00", timestamp="2026-08-19T10:00:00+00:00"),
        _odds_side("fighter-a", "fighter-b", "1.50", timestamp="2026-08-19T13:00:00+00:00"),
        _odds_side("fighter-b", "fighter-a", "2.50", timestamp="2026-08-19T13:00:00+00:00"),
        _odds_side("fighter-a", "fighter-b", "1.40", timestamp="2026-08-19T18:00:00+00:00"),
        _odds_side("fighter-b", "fighter-a", "2.80", timestamp="2026-08-19T18:00:00+00:00"),
    ])

    rows = build_upcoming_snapshot_rows(
        predictions_path=predictions_path,
        odds_path=odds_path,
        fights_path=fights_path,
        fighters_path=fighters_path,
        as_of="2026-08-19T13:30:00+00:00",
    )

    assert len(rows) == 2
    assert {row["benchmark_label"] for row in rows} == {BENCHMARK_LABEL}
    assert {row["benchmark_freshness_status"] for row in rows} == {"fresh"}
    assert {row["benchmark_exclusion_reason"] for row in rows} == {""}
    assert {row["odds_timestamp"] for row in rows} == {"2026-08-19T13:00:00+00:00"}
    assert {row["odds_age_hours"] for row in rows} == {"0.500000"}
    assert {row["max_odds_age_hours"] for row in rows} == {"48"}
    assert {row["fighter_name"] for row in rows} == {"Fighter A", "Fighter B"}
    fighter_a = next(row for row in rows if row["fighter_id"] == "fighter-a")
    fighter_b = next(row for row in rows if row["fighter_id"] == "fighter-b")
    assert fighter_a["opponent_fighter_name"] == "Fighter B"
    assert fighter_b["opponent_fighter_name"] == "Fighter A"
    assert fighter_a["model_probability"] == "0.60"
    assert fighter_b["model_probability"] == "0.40"
    assert fighter_a["offered_decimal_odds"] == "1.50"


def test_upcoming_snapshot_flags_stale_but_keeps_benchmark_prices(tmp_path):
    predictions_path, odds_path, fights_path, fighters_path = _write_standard_inputs(
        tmp_path,
        odds_rows=[
            _odds_side("fighter-a", "fighter-b", "1.50", timestamp="2026-08-19T13:00:00+00:00"),
            _odds_side("fighter-b", "fighter-a", "2.50", timestamp="2026-08-19T13:00:00+00:00"),
        ],
    )

    rows = build_upcoming_snapshot_rows(
        predictions_path=predictions_path,
        odds_path=odds_path,
        fights_path=fights_path,
        fighters_path=fighters_path,
        as_of="2026-08-22T14:00:00+00:00",
        config=default_config().with_overrides({"max_odds_age_hours_current": 24}),
    )

    assert len(rows) == 2
    assert {row["benchmark_freshness_status"] for row in rows} == {"stale"}
    assert {row["benchmark_exclusion_reason"] for row in rows} == {"odds_age_exceeds_max"}
    assert {row["odds_timestamp"] for row in rows} == {"2026-08-19T13:00:00+00:00"}
    assert {row["max_odds_age_hours"] for row in rows} == {"24"}
    assert {row["offered_decimal_odds"] for row in rows} == {"1.50", "2.50"}


def test_upcoming_snapshot_preserves_rows_with_missing_future_and_one_sided_reasons(tmp_path):
    predictions_path = tmp_path / "pre_event_prediction_fights.csv"
    odds_path = tmp_path / "fight_odds.csv"
    fights_path = tmp_path / "fights.csv"
    fighters_path = tmp_path / "fighters.csv"
    _write_csv(predictions_path, [
        _prediction("fight-missing", "event-missing"),
        _prediction("fight-future", "event-future"),
        _prediction("fight-one-sided", "event-one-sided"),
    ])
    _write_csv(fights_path, [
        _fight("fight-missing", "event-missing"),
        _fight("fight-future", "event-future"),
        _fight("fight-one-sided", "event-one-sided"),
    ])
    _write_csv(fighters_path, [
        _fighter("fighter-a", "Fighter A"),
        _fighter("fighter-b", "Fighter B"),
    ])
    _write_csv(odds_path, [
        _odds_side(
            "fighter-a",
            "fighter-b",
            "1.50",
            fight_id="fight-future",
            event_id="event-future",
            timestamp="2026-08-21T13:00:00+00:00",
        ),
        _odds_side(
            "fighter-b",
            "fighter-a",
            "2.50",
            fight_id="fight-future",
            event_id="event-future",
            timestamp="2026-08-21T13:00:00+00:00",
        ),
        _odds_side(
            "fighter-a",
            "fighter-b",
            "1.80",
            fight_id="fight-one-sided",
            event_id="event-one-sided",
            timestamp="2026-08-19T13:00:00+00:00",
        ),
    ])

    rows = build_upcoming_snapshot_rows(
        predictions_path=predictions_path,
        odds_path=odds_path,
        fights_path=fights_path,
        fighters_path=fighters_path,
        as_of="2026-08-20T13:00:00+00:00",
    )

    assert len(rows) == 6
    reasons = {
        row["fight_id"]: row["benchmark_exclusion_reason"]
        for row in rows
    }
    assert reasons["fight-missing"] == "missing_bfo_rows"
    assert reasons["fight-future"] == "future_only_bfo_rows"
    assert reasons["fight-one-sided"] == "expected_two_bfo_sides_got_1"
    assert {row["offered_decimal_odds"] for row in rows} == {""}


def _write_standard_inputs(
    tmp_path: Path,
    *,
    odds_rows: list[dict[str, str]],
) -> tuple[Path, Path, Path, Path]:
    predictions_path = tmp_path / "pre_event_prediction_fights.csv"
    odds_path = tmp_path / "fight_odds.csv"
    fights_path = tmp_path / "fights.csv"
    fighters_path = tmp_path / "fighters.csv"
    _write_csv(predictions_path, [_prediction("fight-upcoming", "event-upcoming")])
    _write_csv(fights_path, [_fight("fight-upcoming", "event-upcoming")])
    _write_csv(fighters_path, [
        _fighter("fighter-a", "Fighter A"),
        _fighter("fighter-b", "Fighter B"),
    ])
    _write_csv(odds_path, odds_rows)
    return predictions_path, odds_path, fights_path, fighters_path


def _prediction(fight_id: str, event_id: str) -> dict[str, str]:
    return {
        "event_id": event_id,
        "event_name": "UFC Upcoming",
        "event_date": "2026-08-22",
        "fight_id": fight_id,
        "fighter_1_name": "Fighter A",
        "fighter_2_name": "Fighter B",
        "scored_at": "2026-08-16 20:25:39+00:00",
        "calibrated_prob_f1": "0.60",
        "actual_label": "",
    }


def _fight(fight_id: str, event_id: str) -> dict[str, str]:
    return {
        "scraped_at": "2026-08-18 00:00:00 UTC",
        "fight_id": fight_id,
        "event_id": event_id,
        "fighter_1_id": "fighter-a",
        "fighter_2_id": "fighter-b",
    }


def _fighter(fighter_id: str, full_name: str) -> dict[str, str]:
    return {
        "scraped_at": "2026-08-18 00:00:00 UTC",
        "fighter_id": fighter_id,
        "full_name": full_name,
    }


def _odds_side(
    fighter_id: str,
    opponent_id: str,
    decimal_odds: str,
    *,
    timestamp: str,
    fight_id: str = "fight-upcoming",
    event_id: str = "event-upcoming",
) -> dict[str, str]:
    return {
        "fight_id": fight_id,
        "event_id": event_id,
        "event_date": "2026-08-22",
        "fighter_id": fighter_id,
        "fighter_name": "Fighter A" if fighter_id == "fighter-a" else "Fighter B",
        "opponent_fighter_id": opponent_id,
        "bookmaker": DEFAULT_BOOKMAKER,
        "market": "moneyline",
        "line_type": "current",
        "odds_timestamp": timestamp,
        "american_odds": "",
        "decimal_odds": decimal_odds,
        "source": DEFAULT_SOURCE,
        "source_url": "https://www.bestfightodds.com/api/ggd?m=1&p=1",
        "imported_at": "2026-08-19T20:59:51+00:00",
    }


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    columns = list(rows[0])
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
