"""Tests for adapting stored BestFightOdds line-history payloads."""

from __future__ import annotations

import base64
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from warehouse.adapt_bestfightodds_line_history_payloads import (
    DEFAULT_BOOKMAKER,
    PRINTABLE_ASCII,
    SOURCE_COLUMNS,
    UNMATCHED_COLUMNS,
    adapt_bestfightodds_line_history_payloads,
    build_line_history_contexts,
    decode_notin_payload,
    read_line_history_payloads,
)
from warehouse.adapt_bestfightodds_snapshots import BfoSnapshot
from warehouse.adapt_kaggle_odds import CANONICAL_COLUMNS
from warehouse.load_fight_odds import FightContext, OddsValidationContext, validate_odds_rows


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "bestfightodds"
EVENT_ID = "11111111-1111-1111-1111-111111111111"
FIGHT_ID = "22222222-2222-2222-2222-222222222222"
FIGHTER_1_ID = "33333333-3333-3333-3333-333333333333"
FIGHTER_2_ID = "44444444-4444-4444-4444-444444444444"


def test_decode_notin_payload_round_trips_encoded_json():
    original = '[{"name":"Mean","data":[{"x":1781737206000,"y":2.5}]}]'

    assert decode_notin_payload(_encode_bfo_payload(original)) == original


def test_build_line_history_contexts_extracts_native_data_li_context():
    snapshot = _snapshot("fighter_history_native.html")

    contexts = build_line_history_contexts([snapshot])

    context = contexts[("123", "1")][0]
    assert context.source_event_name == "UFC Test Card"
    assert context.source_event_date == "2026-08-01"
    assert context.fighter_name == "Fighter One"
    assert context.opponent_name == "Fighter Two"
    assert context.source_snapshot_sha256 == "fixture-sha"


def test_build_line_history_contexts_extracts_event_page_matchup_context():
    snapshot = _snapshot(
        "event_page_context.html",
        source_url="https://www.bestfightodds.com/events/ufc-test-card-1",
    )

    contexts = build_line_history_contexts([snapshot])

    side_one = contexts[("456", "1")][0]
    side_two = contexts[("456", "2")][0]
    assert side_one.source_event_name == "UFC Test Card"
    assert side_one.source_event_date == "2026-08-01"
    assert side_one.fighter_name == "Fighter One"
    assert side_one.opponent_name == "Fighter Two"
    assert side_two.fighter_name == "Fighter Two"
    assert side_two.opponent_name == "Fighter One"


def test_read_line_history_payloads_decodes_stored_api_payload(tmp_path):
    payload_dir = tmp_path / "payloads"
    payload_dir.mkdir()
    payload_path = payload_dir / "20260819T120000Z_detail_payload_api-ggd_fixture.html"
    payload_path.write_text(_encoded_series(), encoding="utf-8")
    payload_path.with_suffix(".metadata.json").write_text(
        json.dumps({
            "source_url": "https://www.bestfightodds.com/api/ggd?m=123&p=1",
            "content_sha256": "payload-sha",
        }),
        encoding="utf-8",
    )

    payloads = read_line_history_payloads(payload_dir)

    assert len(payloads) == 1
    assert payloads[0].matchup_id == "123"
    assert payloads[0].side == "1"
    assert json.loads(payloads[0].decoded_json)[0]["name"] == "Mean"


def test_file_adapter_maps_timestamped_mean_points_to_loadable_output(tmp_path):
    raw_dir, payload_dir = _write_raw_fixture(tmp_path, matchup_id="123", side="1")
    events_csv, fights_csv, fighters_csv = _write_local_csvs(tmp_path)
    source_output = tmp_path / "sources" / "bestfightodds_line_history_fight_odds.csv"
    unmatched_output = tmp_path / "sources" / "bestfightodds_line_history_unmatched_odds.csv"

    result = adapt_bestfightodds_line_history_payloads(
        raw_dir=raw_dir,
        payload_dir=payload_dir,
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
        source_output=source_output,
        unmatched_output=unmatched_output,
        imported_at="2026-08-19T13:00:00+00:00",
    )

    assert result.payloads_read == 1
    assert result.source_points_read == 2
    assert result.unmatched_rows == ()
    assert len(result.matched_rows) == 2

    rows = _read_csv(source_output)
    assert set(CANONICAL_COLUMNS).issubset(rows[0].keys())
    assert SOURCE_COLUMNS[: len(CANONICAL_COLUMNS)] == CANONICAL_COLUMNS
    assert _read_header(unmatched_output) == UNMATCHED_COLUMNS
    assert rows[0]["bookmaker"] == DEFAULT_BOOKMAKER
    assert rows[0]["line_type"] == "current"
    assert rows[0]["odds_timestamp"] == "2026-06-17T23:00:06+00:00"
    assert rows[0]["decimal_odds"] == "2.5"
    assert rows[0]["american_odds"] == ""
    assert rows[0]["source_timestamp_quality"] == "observed_timestamp"
    assert rows[0]["source_matchup_id"] == "123"
    assert rows[0]["source_side"] == "1"
    assert rows[0]["source_series_name"] == "Mean"

    loader_rows = [
        (index, row)
        for index, row in enumerate(rows, start=2)
    ]
    validation = validate_odds_rows(loader_rows, _validation_context())
    assert validation.rejected == []
    assert validation.skipped == []
    assert len(validation.rows) == 2


def test_missing_data_li_context_goes_to_unmatched(tmp_path):
    raw_dir, payload_dir = _write_raw_fixture(tmp_path, matchup_id="999", side="1")
    events_csv, fights_csv, fighters_csv = _write_local_csvs(tmp_path)
    source_output = tmp_path / "sources" / "bestfightodds_line_history_fight_odds.csv"
    unmatched_output = tmp_path / "sources" / "bestfightodds_line_history_unmatched_odds.csv"

    result = adapt_bestfightodds_line_history_payloads(
        raw_dir=raw_dir,
        payload_dir=payload_dir,
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
        source_output=source_output,
        unmatched_output=unmatched_output,
    )

    assert result.matched_rows == ()
    assert len(result.unmatched_rows) == 2
    assert {row["rejection_reason"] for row in result.unmatched_rows} == {
        "missing_line_history_context",
    }
    assert len(_read_csv(source_output)) == 0
    assert len(_read_csv(unmatched_output)) == 2


def test_unknown_local_fight_pair_goes_to_unmatched(tmp_path):
    raw_dir, payload_dir = _write_raw_fixture(tmp_path, matchup_id="123", side="1")
    events_csv, fights_csv, fighters_csv = _write_local_csvs(tmp_path, fighter_two_name="Different Opponent")
    source_output = tmp_path / "sources" / "bestfightodds_line_history_fight_odds.csv"
    unmatched_output = tmp_path / "sources" / "bestfightodds_line_history_unmatched_odds.csv"

    result = adapt_bestfightodds_line_history_payloads(
        raw_dir=raw_dir,
        payload_dir=payload_dir,
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
        source_output=source_output,
        unmatched_output=unmatched_output,
    )

    assert result.matched_rows == ()
    assert len(result.unmatched_rows) == 2
    assert {row["rejection_reason"] for row in result.unmatched_rows} == {
        "unknown_local_fight_pair",
    }


def test_unique_date_pair_match_allows_bfo_event_alias(tmp_path):
    raw_dir, payload_dir = _write_raw_fixture(tmp_path, matchup_id="123", side="1")
    events_csv, fights_csv, fighters_csv = _write_local_csvs(
        tmp_path,
        event_name="UFC Fight Night: Fighter One vs. Fighter Two",
    )
    source_output = tmp_path / "sources" / "bestfightodds_line_history_fight_odds.csv"
    unmatched_output = tmp_path / "sources" / "bestfightodds_line_history_unmatched_odds.csv"

    result = adapt_bestfightodds_line_history_payloads(
        raw_dir=raw_dir,
        payload_dir=payload_dir,
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
        source_output=source_output,
        unmatched_output=unmatched_output,
    )

    assert len(result.matched_rows) == 2
    assert result.unmatched_rows == ()


def _snapshot(
    filename: str,
    *,
    source_url: str = "https://www.bestfightodds.com/fighters/fighter-one-1",
) -> BfoSnapshot:
    path = FIXTURES / filename
    return BfoSnapshot(
        path=path,
        html_text=path.read_text(encoding="utf-8"),
        source_url=source_url,
        fetched_at="2026-08-19T12:00:00+00:00",
        content_sha256="fixture-sha",
    )


def _write_raw_fixture(tmp_path: Path, *, matchup_id: str, side: str) -> tuple[Path, Path]:
    raw_dir = tmp_path / "raw"
    payload_dir = raw_dir / "payloads"
    raw_dir.mkdir()
    payload_dir.mkdir()

    html_path = raw_dir / "20260819T120000Z_fighter_fixture.html"
    html_path.write_text(
        (FIXTURES / "fighter_history_native.html").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    html_path.with_suffix(".metadata.json").write_text(
        json.dumps({
            "source_url": "https://www.bestfightodds.com/fighters/fighter-one-1",
            "fetched_at": "2026-08-19T12:00:00Z",
            "content_sha256": "snapshot-sha",
        }),
        encoding="utf-8",
    )

    payload_path = payload_dir / "20260819T121000Z_detail_payload_api-ggd_fixture.html"
    payload_path.write_text(_encoded_series(), encoding="utf-8")
    payload_path.with_suffix(".metadata.json").write_text(
        json.dumps({
            "source_url": f"https://www.bestfightodds.com/api/ggd?m={matchup_id}&p={side}",
            "content_sha256": "payload-sha",
        }),
        encoding="utf-8",
    )
    return raw_dir, payload_dir


def _write_local_csvs(
    tmp_path: Path,
    *,
    fighter_two_name: str = "Fighter Two",
    event_name: str = "UFC Test Card",
) -> tuple[Path, Path, Path]:
    events_csv = tmp_path / "events.csv"
    fights_csv = tmp_path / "fights.csv"
    fighters_csv = tmp_path / "fighters.csv"
    _write_csv(events_csv, ["event_id", "name", "date_formatted"], [{
        "event_id": EVENT_ID,
        "name": event_name,
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
        {"fighter_id": FIGHTER_2_ID, "full_name": fighter_two_name},
    ])
    return events_csv, fights_csv, fighters_csv


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


def _encoded_series() -> str:
    return _encode_bfo_payload(json.dumps([{
        "name": "Mean",
        "data": [
            {"x": 1781737206000, "y": 2.5},
            {"x": 1781737325000, "y": 2.45},
        ],
    }], separators=(",", ":")))


def _encode_bfo_payload(value: str) -> str:
    half = len(PRINTABLE_ASCII) // 2
    translated = []
    for character in value:
        index = PRINTABLE_ASCII.find(character)
        if index >= 0:
            translated.append(PRINTABLE_ASCII[(index + half) % len(PRINTABLE_ASCII)])
        else:
            translated.append(character)
    return base64.b64encode("".join(translated).encode("utf-8")).decode("ascii")


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
