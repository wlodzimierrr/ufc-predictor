"""Tests for adapting BestFightOdds raw snapshots."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from warehouse.adapt_bestfightodds_snapshots import (
    BFO_SOURCE_COLUMNS,
    UNMATCHED_COLUMNS,
    BfoSnapshot,
    LocalFight,
    adapt_bestfightodds_rows,
    adapt_bestfightodds_snapshots,
    parse_snapshot_rows,
)
from warehouse.adapt_kaggle_odds import CANONICAL_COLUMNS
from warehouse.load_fight_odds import FightContext, OddsValidationContext, validate_odds_rows


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "bestfightodds"
EVENT_ID = "11111111-1111-1111-1111-111111111111"
FIGHT_ID = "22222222-2222-2222-2222-222222222222"
FIGHTER_1_ID = "33333333-3333-3333-3333-333333333333"
FIGHTER_2_ID = "44444444-4444-4444-4444-444444444444"


def _snapshot(filename: str) -> BfoSnapshot:
    path = FIXTURES / filename
    return BfoSnapshot(
        path=path,
        html_text=path.read_text(encoding="utf-8"),
        source_url="https://www.bestfightodds.com/fighters/fighter-one-1",
        fetched_at="2026-08-19T12:00:00+00:00",
        content_sha256="fixture-sha",
    )


def _local_fights(*, duplicate: bool = False):
    fight = LocalFight(
        event_id=EVENT_ID,
        event_name="UFC Test Card",
        event_date="2026-08-01",
        fight_id=FIGHT_ID,
        fighter_1_id=FIGHTER_1_ID,
        fighter_1_name="Fighter One",
        fighter_2_id=FIGHTER_2_ID,
        fighter_2_name="Fighter Two",
    )
    matches = [fight]
    if duplicate:
        matches.append(LocalFight(
            event_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            event_name="UFC Test Card Alternate",
            event_date="2026-08-01",
            fight_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            fighter_1_id=FIGHTER_1_ID,
            fighter_1_name="Fighter One",
            fighter_2_id=FIGHTER_2_ID,
            fighter_2_name="Fighter Two",
        ))
    return {
        ("2026-08-01", ("fighter one", "fighter two")): tuple(matches),
    }


def _validation_context() -> OddsValidationContext:
    return OddsValidationContext(
        event_ids={EVENT_ID},
        fighter_ids={FIGHTER_1_ID, FIGHTER_2_ID},
        fights={
            FIGHT_ID: FightContext(
                event_id=EVENT_ID,
                fighter_1_id=FIGHTER_1_ID,
                fighter_2_id=FIGHTER_2_ID,
            )
        },
    )


def test_parse_snapshot_extracts_open_close_moneyline_rows():
    rows = parse_snapshot_rows(_snapshot("fighter_history_timestamped.html"))

    assert len(rows) == 2
    assert rows[0].source_event_name == "UFC Test Card"
    assert rows[0].source_event_date == "Aug 1st 2026"
    assert rows[0].fighter_name == "Fighter One"
    assert rows[0].opponent_name == "Fighter Two"
    assert rows[0].market == "moneyline"
    assert rows[0].open_odds == "+150"
    assert rows[0].close_odds == "+145"
    assert rows[0].open_timestamp == "2026-07-01T12:00:00Z"
    assert rows[0].close_timestamp == "2026-08-01T00:00:00Z"


def test_adapter_maps_timestamped_open_close_rows_to_loadable_output():
    source_rows = parse_snapshot_rows(_snapshot("fighter_history_timestamped.html"))

    result = adapt_bestfightodds_rows(
        source_rows,
        _local_fights(),
        imported_at="2026-08-19T13:00:00+00:00",
    )

    assert result.source_rows_read == 2
    assert result.unmatched_rows == ()
    assert len(result.matched_rows) == 4
    assert {row["line_type"] for row in result.matched_rows} == {"opening", "closing"}
    assert {row["market"] for row in result.matched_rows} == {"moneyline"}
    assert {row["source"] for row in result.matched_rows} == {"bestfightodds_raw_snapshot"}
    assert {row["source_snapshot_sha256"] for row in result.matched_rows} == {"fixture-sha"}
    assert all(row["source_timestamp_quality"] == "observed_timestamp" for row in result.matched_rows)
    assert [row for row in result.matched_rows if row["fighter_id"] == FIGHTER_1_ID and row["line_type"] == "opening"][0]["american_odds"] == "150"
    assert [row for row in result.matched_rows if row["fighter_id"] == FIGHTER_2_ID and row["line_type"] == "closing"][0]["american_odds"] == "-155"

    loader_rows = [
        (index, row)
        for index, row in enumerate(result.matched_rows, start=2)
    ]
    validation = validate_odds_rows(loader_rows, _validation_context())
    assert validation.rejected == []
    assert validation.skipped == []
    assert len(validation.rows) == 4


def test_label_only_open_close_rows_are_review_only():
    source_rows = parse_snapshot_rows(_snapshot("fighter_history_label_only.html"))

    result = adapt_bestfightodds_rows(source_rows, _local_fights())

    assert result.matched_rows == ()
    assert len(result.unmatched_rows) == 1
    assert result.unmatched_rows[0]["rejection_reason"] == "missing_opening_observed_timestamp_open_close_label_only"
    assert result.unmatched_rows[0]["source_timestamp_quality"] == "open_close_label_only"
    assert result.unmatched_rows[0]["american_odds"] == "open=+150;close=+145"


def test_native_bestfightodds_table_rows_are_label_only_review_rows():
    source_rows = parse_snapshot_rows(_snapshot("fighter_history_native.html"))

    assert len(source_rows) == 2
    assert source_rows[0].source_event_name == "UFC Test Card"
    assert source_rows[0].source_event_date == "2026-08-01"
    assert source_rows[0].fighter_name == "Fighter One"
    assert source_rows[0].opponent_name == "Fighter Two"
    assert source_rows[0].open_odds == "+150"
    assert source_rows[0].close_odds == "+145"
    assert source_rows[0].timestamp_quality == "open_close_label_only"

    result = adapt_bestfightodds_rows(source_rows, _local_fights())

    assert result.matched_rows == ()
    assert len(result.unmatched_rows) == 2
    assert {row["source_timestamp_quality"] for row in result.unmatched_rows} == {
        "open_close_label_only",
    }
    assert {row["rejection_reason"] for row in result.unmatched_rows} == {
        "missing_opening_observed_timestamp_open_close_label_only",
    }


def test_missing_moneyline_odds_goes_to_unmatched():
    row = parse_snapshot_rows(_snapshot("fighter_history_timestamped.html"))[0]
    missing = row.__class__(
        **{
            **row.__dict__,
            "open_odds": "",
            "close_odds": "",
        }
    )

    result = adapt_bestfightodds_rows([missing], _local_fights())

    assert result.matched_rows == ()
    assert result.unmatched_rows[0]["rejection_reason"] == "missing_moneyline_odds"


def test_unmatched_fighter_pair_is_not_guessed():
    row = parse_snapshot_rows(_snapshot("fighter_history_timestamped.html"))[0]
    unmatched = row.__class__(
        **{
            **row.__dict__,
            "fighter_name": "Unknown Fighter",
        }
    )

    result = adapt_bestfightodds_rows([unmatched], _local_fights())

    assert result.matched_rows == ()
    assert result.unmatched_rows[0]["rejection_reason"] == "unknown_local_fight_pair"


def test_ambiguous_event_match_is_not_guessed():
    row = parse_snapshot_rows(_snapshot("fighter_history_timestamped.html"))[0]
    ambiguous = row.__class__(
        **{
            **row.__dict__,
            "source_event_name": "",
        }
    )

    result = adapt_bestfightodds_rows([ambiguous], _local_fights(duplicate=True))

    assert result.matched_rows == ()
    assert result.unmatched_rows[0]["rejection_reason"] == "ambiguous_local_fight_pair"


def test_duplicate_market_sides_are_reviewed_not_loaded():
    row = parse_snapshot_rows(_snapshot("fighter_history_timestamped.html"))[0]

    result = adapt_bestfightodds_rows([row, row], _local_fights())

    assert len(result.matched_rows) == 2
    assert result.skipped_rows == 1
    assert result.unmatched_rows[0]["rejection_reason"] == "duplicate_stable_odds_key"


def test_non_moneyline_market_is_filtered_to_unmatched():
    row = parse_snapshot_rows(_snapshot("fighter_history_timestamped.html"))[0]
    prop = row.__class__(
        **{
            **row.__dict__,
            "market": "method of victory",
        }
    )

    result = adapt_bestfightodds_rows([prop], _local_fights())

    assert result.matched_rows == ()
    assert result.unmatched_rows[0]["rejection_reason"] == "non_moneyline_market"


def test_file_adapter_writes_source_and_unmatched_outputs(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    html_path = raw_dir / "20260819T120000Z_fighter_fixture.html"
    html_path.write_text(
        (FIXTURES / "fighter_history_timestamped.html").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    html_path.with_suffix(".metadata.json").write_text(
        json.dumps({
            "source_url": "https://www.bestfightodds.com/fighters/fighter-one-1",
            "fetched_at": "2026-08-19T12:00:00Z",
            "content_sha256": "fixture-sha",
        }),
        encoding="utf-8",
    )
    events_csv = tmp_path / "events.csv"
    fights_csv = tmp_path / "fights.csv"
    fighters_csv = tmp_path / "fighters.csv"
    source_output = tmp_path / "sources" / "bestfightodds_fight_odds.csv"
    unmatched_output = tmp_path / "sources" / "bestfightodds_unmatched_odds.csv"
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

    result = adapt_bestfightodds_snapshots(
        raw_dir=raw_dir,
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
        source_output=source_output,
        unmatched_output=unmatched_output,
        imported_at="2026-08-19T13:00:00+00:00",
    )

    assert result.snapshots_read == 1
    assert result.source_rows_read == 2
    assert len(result.matched_rows) == 4
    source_rows = _read_csv(source_output)
    unmatched_rows = _read_csv(unmatched_output)
    assert set(CANONICAL_COLUMNS).issubset(source_rows[0].keys())
    assert BFO_SOURCE_COLUMNS[: len(CANONICAL_COLUMNS)] == CANONICAL_COLUMNS
    assert unmatched_rows == []
    assert _read_header(unmatched_output) == UNMATCHED_COLUMNS


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _read_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as f:
        return next(csv.reader(f))
