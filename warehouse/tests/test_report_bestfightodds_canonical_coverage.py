"""Tests for BestFightOdds canonical coverage QA reports."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from betting.bfo_mean_market_backtest import DEFAULT_BOOKMAKER, DEFAULT_SOURCE
from warehouse.report_bestfightodds_canonical_coverage import (
    COVERAGE_COLUMNS,
    write_bestfightodds_canonical_coverage,
)


def test_bfo_canonical_coverage_groups_rows_by_event_and_fight(tmp_path):
    odds = tmp_path / "fight_odds.csv"
    events = tmp_path / "events.csv"
    output = tmp_path / "coverage.csv"
    _write_csv(events, ["event_id", "name"], [{
        "event_id": "event-1",
        "name": "UFC Test Card",
    }])
    _write_csv(odds, _odds_columns(), [
        _odds_row("fighter-a", "Fighter A", "fighter-b", "2.0", "2026-08-01T10:00:00+00:00"),
        _odds_row("fighter-b", "Fighter B", "fighter-a", "1.8", "2026-08-01T10:00:00+00:00"),
        _odds_row("fighter-a", "Fighter A", "fighter-b", "2.1", "2026-08-01T12:00:00+00:00"),
        _odds_row("fighter-b", "Fighter B", "fighter-a", "1.7", "2026-08-01T12:00:00+00:00"),
        _odds_row("fighter-a", "Fighter A", "fighter-b", "2.2", "2026-08-01T12:00:00+00:00", bookmaker="Other"),
    ])

    result = write_bestfightodds_canonical_coverage(
        odds_path=odds,
        events_path=events,
        output_path=output,
    )

    assert result.counters["canonical_bfo_rows"] == 4
    assert result.counters["covered_fights"] == 1
    assert result.counters["covered_events"] == 1
    assert result.counters["two_sided_fights"] == 1
    rows = _read_csv(output)
    assert rows == [{
        "event_id": "event-1",
        "event_name": "UFC Test Card",
        "event_date": "2026-08-02",
        "fight_id": "fight-1",
        "fighter_names": "Fighter A vs. Fighter B",
        "side_count": "2",
        "row_count": "4",
        "market_group_count": "2",
        "first_odds_timestamp": "2026-08-01T10:00:00+00:00",
        "last_odds_timestamp": "2026-08-01T12:00:00+00:00",
        "has_two_sides": "true",
        "source_url_count": "2",
    }]


def _odds_columns() -> list[str]:
    return [
        "fight_id",
        "event_id",
        "event_date",
        "fighter_id",
        "fighter_name",
        "opponent_fighter_id",
        "bookmaker",
        "market",
        "line_type",
        "odds_timestamp",
        "american_odds",
        "decimal_odds",
        "source",
        "source_url",
        "imported_at",
    ]


def _odds_row(
    fighter_id: str,
    fighter_name: str,
    opponent_id: str,
    decimal_odds: str,
    odds_timestamp: str,
    *,
    bookmaker: str = DEFAULT_BOOKMAKER,
) -> dict[str, str]:
    return {
        "fight_id": "fight-1",
        "event_id": "event-1",
        "event_date": "2026-08-02",
        "fighter_id": fighter_id,
        "fighter_name": fighter_name,
        "opponent_fighter_id": opponent_id,
        "bookmaker": bookmaker,
        "market": "moneyline",
        "line_type": "current",
        "odds_timestamp": odds_timestamp,
        "american_odds": "",
        "decimal_odds": decimal_odds,
        "source": DEFAULT_SOURCE,
        "source_url": f"https://www.bestfightodds.com/api/ggd?m=1&p={'1' if fighter_id == 'fighter-a' else '2'}",
        "imported_at": "2026-08-20T10:00:00+00:00",
    }


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))
