"""Tests for promoting reviewed source odds into the canonical CSV."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from warehouse.adapt_kaggle_odds import CANONICAL_COLUMNS
from warehouse.promote_odds_source_to_canonical import promote_source_to_canonical


EVENT_ID = "11111111-1111-1111-1111-111111111111"
FIGHT_ID = "22222222-2222-2222-2222-222222222222"
FIGHTER_1_ID = "33333333-3333-3333-3333-333333333333"
FIGHTER_2_ID = "44444444-4444-4444-4444-444444444444"


def test_promote_source_rows_strips_extra_columns_and_preserves_existing(tmp_path):
    canonical_csv = tmp_path / "fight_odds.csv"
    source_csv = tmp_path / "source.csv"
    output_csv = tmp_path / "promoted.csv"
    events_csv, fights_csv, fighters_csv = _write_identity_csvs(tmp_path)
    existing = _odds_row(
        fighter_id=FIGHTER_2_ID,
        fighter_name="Fighter Two",
        opponent_id=FIGHTER_1_ID,
        odds_timestamp="2026-07-01T12:00:00+00:00",
        decimal_odds="1.75",
    )
    incoming = _odds_row(
        fighter_id=FIGHTER_1_ID,
        fighter_name="Fighter One",
        opponent_id=FIGHTER_2_ID,
        odds_timestamp="2026-07-01T12:00:00+00:00",
        decimal_odds="2.50",
    )
    _write_csv(canonical_csv, CANONICAL_COLUMNS, [existing])
    _write_csv(source_csv, [*CANONICAL_COLUMNS, "source_payload_sha256"], [
        {**incoming, "source_payload_sha256": "payload-sha"},
    ])

    summary = promote_source_to_canonical(
        source_csv=source_csv,
        canonical_csv=canonical_csv,
        output_csv=output_csv,
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
    )

    rows = _read_csv(output_csv)
    assert summary.existing_rows == 1
    assert summary.source_rows_read == 1
    assert summary.source_rows_valid == 1
    assert summary.appended_rows == 1
    assert summary.duplicate_rows == 0
    assert rows == [existing, incoming]
    assert list(rows[0].keys()) == CANONICAL_COLUMNS


def test_promote_source_rows_skips_existing_stable_key(tmp_path):
    canonical_csv = tmp_path / "fight_odds.csv"
    source_csv = tmp_path / "source.csv"
    events_csv, fights_csv, fighters_csv = _write_identity_csvs(tmp_path)
    row = _odds_row(
        fighter_id=FIGHTER_1_ID,
        fighter_name="Fighter One",
        opponent_id=FIGHTER_2_ID,
        odds_timestamp="2026-07-01T12:00:00+00:00",
        decimal_odds="2.50",
    )
    _write_csv(canonical_csv, CANONICAL_COLUMNS, [row])
    _write_csv(source_csv, CANONICAL_COLUMNS, [row])

    summary = promote_source_to_canonical(
        source_csv=source_csv,
        canonical_csv=canonical_csv,
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
    )

    rows = _read_csv(canonical_csv)
    assert summary.appended_rows == 0
    assert summary.duplicate_rows == 1
    assert rows == [row]


def test_promote_source_rows_rejects_invalid_source_before_writing(tmp_path):
    canonical_csv = tmp_path / "fight_odds.csv"
    source_csv = tmp_path / "source.csv"
    events_csv, fights_csv, fighters_csv = _write_identity_csvs(tmp_path)
    existing = _odds_row(
        fighter_id=FIGHTER_1_ID,
        fighter_name="Fighter One",
        opponent_id=FIGHTER_2_ID,
        odds_timestamp="2026-07-01T12:00:00+00:00",
        decimal_odds="2.50",
    )
    invalid = {**existing, "fighter_id": "99999999-9999-9999-9999-999999999999"}
    _write_csv(canonical_csv, CANONICAL_COLUMNS, [existing])
    _write_csv(source_csv, CANONICAL_COLUMNS, [invalid])

    with pytest.raises(ValueError, match="unknown fighter_id"):
        promote_source_to_canonical(
            source_csv=source_csv,
            canonical_csv=canonical_csv,
            events_csv=events_csv,
            fights_csv=fights_csv,
            fighters_csv=fighters_csv,
        )

    assert _read_csv(canonical_csv) == [existing]


def _odds_row(
    *,
    fighter_id: str,
    fighter_name: str,
    opponent_id: str,
    odds_timestamp: str,
    decimal_odds: str,
) -> dict[str, str]:
    return {
        "fight_id": FIGHT_ID,
        "event_id": EVENT_ID,
        "event_date": "2026-08-01",
        "fighter_id": fighter_id,
        "fighter_name": fighter_name,
        "opponent_fighter_id": opponent_id,
        "bookmaker": "BestFightOdds Mean",
        "market": "moneyline",
        "line_type": "current",
        "odds_timestamp": odds_timestamp,
        "american_odds": "",
        "decimal_odds": decimal_odds,
        "source": "bestfightodds_line_history_payload",
        "source_url": "https://www.bestfightodds.com/api/ggd?m=123&p=1",
        "imported_at": "2026-08-19T13:00:00+00:00",
    }


def _write_identity_csvs(tmp_path: Path) -> tuple[Path, Path, Path]:
    events_csv = tmp_path / "events.csv"
    fights_csv = tmp_path / "fights.csv"
    fighters_csv = tmp_path / "fighters.csv"
    _write_csv(events_csv, ["event_id", "name", "date_formatted"], [{
        "event_id": EVENT_ID,
        "name": "UFC Test Card",
        "date_formatted": "2026-08-01",
    }])
    _write_csv(fights_csv, ["fight_id", "event_id", "fighter_1_id", "fighter_2_id"], [{
        "fight_id": FIGHT_ID,
        "event_id": EVENT_ID,
        "fighter_1_id": FIGHTER_1_ID,
        "fighter_2_id": FIGHTER_2_ID,
    }])
    _write_csv(fighters_csv, ["fighter_id", "full_name"], [
        {"fighter_id": FIGHTER_1_ID, "full_name": "Fighter One"},
        {"fighter_id": FIGHTER_2_ID, "full_name": "Fighter Two"},
    ])
    return events_csv, fights_csv, fighters_csv


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))
