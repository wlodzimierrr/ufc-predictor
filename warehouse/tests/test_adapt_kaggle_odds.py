"""Tests for adapting Kaggle UFC odds into the canonical odds CSV."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from warehouse import adapt_kaggle_odds
from warehouse.adapt_kaggle_odds import (
    CANONICAL_COLUMNS,
    QA_COLUMNS,
    UNMATCHED_COLUMNS,
    adapt_kaggle_odds_rows,
    build_identity_map,
    qa_summary_counts,
)


EVENT_ID = "11111111-1111-1111-1111-111111111111"
FIGHT_ID = "22222222-2222-2222-2222-222222222222"
FIGHTER_1_ID = "33333333-3333-3333-3333-333333333333"
FIGHTER_2_ID = "44444444-4444-4444-4444-444444444444"
FIGHT_URL = "http://www.ufcstats.com/fight-details/abc123"
FIGHTER_1_URL = "http://ufcstats.com/fighter-details/f1"
FIGHTER_2_URL = "http://ufcstats.com/fighter-details/f2"


def _identity_map(**overrides):
    events = [{
        "event_id": EVENT_ID,
        "name": "UFC Test Card",
        "date_formatted": "2026-08-01",
    }]
    fights = [{
        "fight_id": FIGHT_ID,
        "event_id": EVENT_ID,
        "url": FIGHT_URL,
        "fighter_1_id": FIGHTER_1_ID,
        "fighter_2_id": FIGHTER_2_ID,
    }]
    fighters = [
        {
            "fighter_id": FIGHTER_1_ID,
            "url": FIGHTER_1_URL,
            "full_name": "Fighter One",
        },
        {
            "fighter_id": FIGHTER_2_ID,
            "url": FIGHTER_2_URL,
            "full_name": "Fighter Two",
        },
    ]
    fights = overrides.get("fights", fights)
    fighters = overrides.get("fighters", fighters)
    events = overrides.get("events", events)
    return build_identity_map(events=events, fights=fights, fighters=fighters)


def _raw_row(**overrides) -> dict[str, str]:
    row = {
        "fight_url": "http://ufcstats.com/fight-details/abc123",
        "fighter_1_url": FIGHTER_1_URL,
        "fighter_2_url": FIGHTER_2_URL,
        "fighter_1": "Fighter One",
        "fighter_2": "Fighter Two",
        "odds_1": "2.50",
        "odds_2": "1.60",
        "f1_ko_odds": "4.00",
        "f2_ko_odds": "",
        "f1_sub_odds": "",
        "f2_sub_odds": "",
        "f1_dec_odds": "",
        "f2_dec_odds": "",
        "event_date": "2026-08-01",
        "adding_date": "2026-07-31 12:00:00+00:00",
        "source": "TestBook",
        "region": "us",
    }
    row.update(overrides)
    return row


def test_kaggle_adapter_maps_exact_urls_to_two_moneyline_rows():
    result = adapt_kaggle_odds_rows([(2, _raw_row())], _identity_map())

    assert result.rows_read == 1
    assert result.unmatched_rows == ()
    assert len(result.matched_rows) == 2
    assert {row["fighter_id"] for row in result.matched_rows} == {
        FIGHTER_1_ID,
        FIGHTER_2_ID,
    }
    assert {row["opponent_fighter_id"] for row in result.matched_rows} == {
        FIGHTER_1_ID,
        FIGHTER_2_ID,
    }
    assert {row["bookmaker"] for row in result.matched_rows} == {"TestBook/us"}
    assert {row["market"] for row in result.matched_rows} == {"moneyline"}
    assert {row["line_type"] for row in result.matched_rows} == {"current"}
    assert {row["source"] for row in result.matched_rows} == {
        "kaggle_ufc_betting_odds_daily",
    }
    assert {row["odds_timestamp"] for row in result.matched_rows} == {
        "2026-07-31T12:00:00+00:00",
    }
    assert [row["decimal_odds"] for row in result.matched_rows] == ["2.50", "1.60"]
    assert all(row["american_odds"] == "" for row in result.matched_rows)
    assert len(result.qa_rows) == 1
    assert result.qa_rows[0]["row_status"] == "matched"
    assert result.qa_rows[0]["match_reason"] == "exact_url_match"
    assert result.qa_rows[0]["candidate_fight_id"] == FIGHT_ID
    assert result.qa_rows[0]["candidate_fighter_1_name"] == "Fighter One"
    assert result.qa_rows[0]["candidate_fighter_2_name"] == "Fighter Two"
    assert result.qa_rows[0]["canonical_rows"] == "2"


def test_kaggle_adapter_rejects_unmatched_fighter_url():
    result = adapt_kaggle_odds_rows(
        [(2, _raw_row(fighter_1_url="http://ufcstats.com/fighter-details/unknown"))],
        _identity_map(),
    )

    assert result.matched_rows == ()
    assert len(result.unmatched_rows) == 1
    assert result.unmatched_rows[0]["rejection_reason"] == "unknown_fighter_1_url"
    assert result.unmatched_rows[0]["source_fighter_name"] == "Fighter One"


def test_kaggle_adapter_rejects_ambiguous_fight_url():
    duplicate_fights = [
        {
            "fight_id": FIGHT_ID,
            "event_id": EVENT_ID,
            "url": FIGHT_URL,
            "fighter_1_id": FIGHTER_1_ID,
            "fighter_2_id": FIGHTER_2_ID,
        },
        {
            "fight_id": "55555555-5555-5555-5555-555555555555",
            "event_id": EVENT_ID,
            "url": FIGHT_URL,
            "fighter_1_id": FIGHTER_1_ID,
            "fighter_2_id": FIGHTER_2_ID,
        },
    ]

    result = adapt_kaggle_odds_rows(
        [(2, _raw_row())],
        _identity_map(fights=duplicate_fights),
    )

    assert result.matched_rows == ()
    assert result.unmatched_rows[0]["rejection_reason"] == "ambiguous_fight_url"
    assert result.qa_rows[0]["row_status"] == "ambiguous"
    assert result.qa_rows[0]["candidate_fight_id"] == "|".join([
        FIGHT_ID,
        "55555555-5555-5555-5555-555555555555",
    ])


def test_kaggle_adapter_filters_to_moneyline_and_rejects_missing_moneyline_side():
    prop_values = adapt_kaggle_odds_rows([(2, _raw_row())], _identity_map())
    missing_moneyline = adapt_kaggle_odds_rows(
        [(2, _raw_row(odds_2=""))],
        _identity_map(),
    )

    assert len(prop_values.matched_rows) == 2
    assert {row["market"] for row in prop_values.matched_rows} == {"moneyline"}
    assert missing_moneyline.matched_rows == ()
    assert missing_moneyline.unmatched_rows[0]["rejection_reason"] == "missing_odds_2"
    assert missing_moneyline.qa_rows[0]["row_status"] == "rejected"


def test_kaggle_adapter_builds_qa_rows_and_summary_counts():
    result = adapt_kaggle_odds_rows(
        [
            (2, _raw_row()),
            (3, _raw_row()),
            (4, _raw_row(fight_url="http://ufcstats.com/fight-details/unknown")),
            (5, _raw_row(odds_1="")),
        ],
        _identity_map(),
    )

    assert qa_summary_counts(result.qa_rows) == {
        "matched": 1,
        "unmatched": 1,
        "duplicate": 1,
        "ambiguous": 0,
        "rejected": 1,
    }
    assert [row["row_status"] for row in result.qa_rows] == [
        "matched",
        "duplicate",
        "unmatched",
        "rejected",
    ]
    assert result.qa_rows[1]["rejection_reason"] == "duplicate_stable_odds_key"
    assert result.qa_rows[2]["rejection_reason"] == "unknown_fight_url"
    assert result.qa_rows[3]["rejection_reason"] == "missing_odds_1"


def test_kaggle_adapter_cli_writes_source_canonical_and_unmatched_outputs(tmp_path):
    raw_csv = tmp_path / "raw.csv"
    events_csv = tmp_path / "events.csv"
    fights_csv = tmp_path / "fights.csv"
    fighters_csv = tmp_path / "fighters.csv"
    source_output = tmp_path / "sources" / "kaggle_fight_odds.csv"
    canonical_output = tmp_path / "fight_odds.csv"
    unmatched_output = tmp_path / "unmatched_odds.csv"
    qa_output = tmp_path / "reports" / "odds_matching_qa.csv"
    _write_csv(raw_csv, list(_raw_row().keys()), [_raw_row(), _raw_row(odds_1="")])
    _write_csv(events_csv, ["event_id", "name", "date_formatted"], [{
        "event_id": EVENT_ID,
        "name": "UFC Test Card",
        "date_formatted": "2026-08-01",
    }])
    _write_csv(fights_csv, ["fight_id", "event_id", "url", "fighter_1_id", "fighter_2_id"], [{
        "fight_id": FIGHT_ID,
        "event_id": EVENT_ID,
        "url": FIGHT_URL,
        "fighter_1_id": FIGHTER_1_ID,
        "fighter_2_id": FIGHTER_2_ID,
    }])
    _write_csv(fighters_csv, ["fighter_id", "url", "full_name"], [
        {
            "fighter_id": FIGHTER_1_ID,
            "url": FIGHTER_1_URL,
            "full_name": "Fighter One",
        },
        {
            "fighter_id": FIGHTER_2_ID,
            "url": FIGHTER_2_URL,
            "full_name": "Fighter Two",
        },
    ])

    exit_code = adapt_kaggle_odds.main([
        "--raw-csv",
        str(raw_csv),
        "--events-csv",
        str(events_csv),
        "--fights-csv",
        str(fights_csv),
        "--fighters-csv",
        str(fighters_csv),
        "--source-output",
        str(source_output),
        "--canonical-output",
        str(canonical_output),
        "--unmatched-output",
        str(unmatched_output),
        "--qa-output",
        str(qa_output),
    ])

    assert exit_code == 0
    with source_output.open(newline="", encoding="utf-8") as f:
        source_rows = list(csv.DictReader(f))
    with canonical_output.open(newline="", encoding="utf-8") as f:
        canonical_rows = list(csv.DictReader(f))
    with unmatched_output.open(newline="", encoding="utf-8") as f:
        unmatched_rows = list(csv.DictReader(f))
    with qa_output.open(newline="", encoding="utf-8") as f:
        qa_rows = list(csv.DictReader(f))

    assert list(source_rows[0].keys()) == CANONICAL_COLUMNS
    assert canonical_rows == source_rows
    assert len(canonical_rows) == 2
    assert list(unmatched_rows[0].keys()) == UNMATCHED_COLUMNS
    assert unmatched_rows[0]["rejection_reason"] == "missing_odds_1"
    assert list(qa_rows[0].keys()) == QA_COLUMNS
    assert len(qa_rows) == 2
    assert qa_rows[0]["matched_count"] == "1"
    assert qa_rows[0]["rejected_count"] == "1"


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
