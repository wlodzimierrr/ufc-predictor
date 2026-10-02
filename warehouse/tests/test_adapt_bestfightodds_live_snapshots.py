"""Tests for adapting BestFightOdds event-page snapshots into current odds rows."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from warehouse.adapt_bestfightodds_live_snapshots import (
    LIVE_SOURCE_COLUMNS,
    UNMATCHED_COLUMNS,
    BESTFIGHTODDS_LIVE_SOURCE_LABEL,
    LocalFight,
    adapt_bestfightodds_live_snapshots,
    adapt_live_rows,
    parse_live_snapshot_rows,
    read_name_aliases,
    _match_index,
)
from warehouse.adapt_bestfightodds_snapshots import BfoSnapshot
from warehouse.load_fight_odds import FightContext, OddsValidationContext, validate_odds_rows


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "bestfightodds"
EVENT_ID = "11111111-1111-1111-1111-111111111111"
FIGHT_ID = "22222222-2222-2222-2222-222222222222"
FIGHTER_1_ID = "33333333-3333-3333-3333-333333333333"
FIGHTER_2_ID = "44444444-4444-4444-4444-444444444444"
FETCHED_AT = "2026-08-01T12:00:00+00:00"


def _snapshot(filename: str = "event_live_odds.html") -> BfoSnapshot:
    path = FIXTURES / filename
    return BfoSnapshot(
        path=path,
        html_text=path.read_text(encoding="utf-8"),
        source_url="https://www.bestfightodds.com/events/ufc-test-1",
        fetched_at=FETCHED_AT,
        content_sha256="fixture-sha",
    )


def _local_fight(
    *,
    event_name: str = "UFC Test Card",
    event_date: str = "2026-08-01",
    fighter_2_name: str = "Fighter Two",
) -> LocalFight:
    return LocalFight(
        event_id=EVENT_ID,
        event_name=event_name,
        event_date=event_date,
        fight_id=FIGHT_ID,
        fighter_1_id=FIGHTER_1_ID,
        fighter_1_name="Fighter One",
        fighter_2_id=FIGHTER_2_ID,
        fighter_2_name=fighter_2_name,
    )


def _index(*fights: LocalFight, aliases: dict[str, str] | None = None):
    return _match_index({("seed", ("a", "b")): tuple(fights)}, aliases or {})


def test_parses_only_moneyline_sides_with_bookmaker_from_markup():
    rows = parse_live_snapshot_rows(_snapshot())

    # 2 fighter sides x 2 bookmakers with odds; prop/total rows are skipped.
    assert len(rows) == 4
    assert {row.bookmaker for row in rows} == {"FanDuel", "BetRivers"}
    assert {row.fighter_name for row in rows} == {"Fighter One", "Fighter Two Jr."}
    assert {row.matchup_id for row in rows} == {"900"}
    assert {row.source_event_name for row in rows} == {"UFC Test Card"}
    assert {row.source_event_date for row in rows} == {"2026-08-01"}
    assert {row.odds_timestamp for row in rows} == {FETCHED_AT}

    fighter_one = {row.bookmaker: row.odds_text for row in rows if row.fighter_name == "Fighter One"}
    assert fighter_one == {"FanDuel": "-150", "BetRivers": "-145"}
    assert all(row.opponent_name == "Fighter Two Jr." for row in rows if row.fighter_name == "Fighter One")


def test_empty_bookmaker_column_is_not_emitted():
    rows = parse_live_snapshot_rows(_snapshot())

    # DraftKings has a header but no odds cells on this card.
    assert "DraftKings" not in {row.bookmaker for row in rows}


def test_adapts_rows_to_current_line_type_at_capture_time():
    result = adapt_live_rows(
        parse_live_snapshot_rows(_snapshot()),
        _index(_local_fight(fighter_2_name="Fighter Two Jr.")),
        {},
        imported_at="2026-08-01T12:05:00+00:00",
    )

    assert result.unmatched_rows == ()
    assert len(result.matched_rows) == 4
    row = next(r for r in result.matched_rows if r["bookmaker"] == "FanDuel" and r["fighter_id"] == FIGHTER_1_ID)
    assert row["line_type"] == "current"
    assert row["market"] == "moneyline"
    assert row["odds_timestamp"] == FETCHED_AT
    assert row["american_odds"] == "-150"
    assert row["decimal_odds"].startswith("1.666")
    assert row["source"] == BESTFIGHTODDS_LIVE_SOURCE_LABEL
    assert row["event_id"] == EVENT_ID
    assert row["fight_id"] == FIGHT_ID
    assert row["opponent_fighter_id"] == FIGHTER_2_ID
    assert row["source_timestamp_quality"] == "observed_snapshot_capture_time"


def test_generational_suffix_variant_still_matches():
    # BFO writes "Fighter Two Jr."; UFCStats records "Fighter Two".
    result = adapt_live_rows(
        parse_live_snapshot_rows(_snapshot()),
        _index(_local_fight(fighter_2_name="Fighter Two")),
        {},
    )

    assert result.unmatched_rows == ()
    assert len(result.matched_rows) == 4


def test_internal_whitespace_variant_still_matches():
    # BFO writes "Fighter Two Jr."; a local "FighterTwo" spelling must still match.
    result = adapt_live_rows(
        parse_live_snapshot_rows(_snapshot()),
        _index(_local_fight(fighter_2_name="FighterTwo")),
        {},
    )

    assert result.unmatched_rows == ()
    assert len(result.matched_rows) == 4


def test_reviewed_alias_matches_a_different_local_surname():
    aliases = {"fighter two jr": "fighter two ringname"}
    result = adapt_live_rows(
        parse_live_snapshot_rows(_snapshot()),
        _index(_local_fight(fighter_2_name="Fighter Two Ringname"), aliases=aliases),
        aliases,
    )

    assert result.unmatched_rows == ()
    assert len(result.matched_rows) == 4


def test_unmatched_name_goes_to_review_output():
    result = adapt_live_rows(
        parse_live_snapshot_rows(_snapshot()),
        _index(_local_fight(fighter_2_name="Someone Else Entirely")),
        {},
    )

    assert result.matched_rows == ()
    assert {row["rejection_reason"] for row in result.unmatched_rows} == {"unknown_local_fight_pair"}


def test_unique_date_pair_match_allows_bfo_city_event_name():
    # BFO names Fight Nights by host city; UFCStats names them by main event.
    result = adapt_live_rows(
        parse_live_snapshot_rows(_snapshot()),
        _index(_local_fight(
            event_name="UFC Fight Night: Fighter One vs. Fighter Two",
            fighter_2_name="Fighter Two Jr.",
        )),
        {},
    )

    assert result.unmatched_rows == ()
    assert len(result.matched_rows) == 4


def test_ambiguous_date_pair_without_event_name_match_is_rejected():
    other = LocalFight(
        event_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        event_name="Some Other Card",
        event_date="2026-08-01",
        fight_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        fighter_1_id=FIGHTER_1_ID,
        fighter_1_name="Fighter One",
        fighter_2_id=FIGHTER_2_ID,
        fighter_2_name="Fighter Two Jr.",
    )
    result = adapt_live_rows(
        parse_live_snapshot_rows(_snapshot()),
        _index(_local_fight(event_name="Unrelated Card", fighter_2_name="Fighter Two Jr."), other),
        {},
    )

    assert result.matched_rows == ()
    assert {row["rejection_reason"] for row in result.unmatched_rows} == {"unknown_local_event_name"}


def test_event_date_one_day_either_side_still_matches():
    # BFO labels an event with its local calendar date, which can sit a day off.
    result = adapt_live_rows(
        parse_live_snapshot_rows(_snapshot()),
        _index(_local_fight(event_date="2026-07-31", fighter_2_name="Fighter Two Jr.")),
        {},
    )

    assert result.unmatched_rows == ()
    assert len(result.matched_rows) == 4
    # The local event date wins over the BFO label.
    assert {row["event_date"] for row in result.matched_rows} == {"2026-07-31"}


def test_event_date_two_days_away_is_rejected():
    result = adapt_live_rows(
        parse_live_snapshot_rows(_snapshot()),
        _index(_local_fight(event_date="2026-08-03", fighter_2_name="Fighter Two Jr.")),
        {},
    )

    assert result.matched_rows == ()
    assert {row["rejection_reason"] for row in result.unmatched_rows} == {"unknown_local_fight_pair"}


def test_duplicate_stable_key_is_skipped_once():
    rows = parse_live_snapshot_rows(_snapshot())
    result = adapt_live_rows(
        rows + rows,
        _index(_local_fight(fighter_2_name="Fighter Two Jr.")),
        {},
    )

    assert len(result.matched_rows) == 4
    assert result.skipped_rows == 4
    assert {row["rejection_reason"] for row in result.unmatched_rows} == {"duplicate_stable_odds_key"}


def test_matched_rows_pass_the_odds_loader_validator():
    result = adapt_live_rows(
        parse_live_snapshot_rows(_snapshot()),
        _index(_local_fight(fighter_2_name="Fighter Two Jr.")),
        {},
    )
    context = OddsValidationContext(
        event_ids={EVENT_ID},
        fighter_ids={FIGHTER_1_ID, FIGHTER_2_ID},
        fights={FIGHT_ID: FightContext(
            event_id=EVENT_ID,
            fighter_1_id=FIGHTER_1_ID,
            fighter_2_id=FIGHTER_2_ID,
        )},
    )

    loader_rows = [(index, row) for index, row in enumerate(result.matched_rows, start=2)]

    validation = validate_odds_rows(loader_rows, context)

    assert validation.rejected == []
    assert validation.skipped == []
    assert len(validation.rows) == 4


def test_read_name_aliases_normalizes_keys(tmp_path):
    path = tmp_path / "aliases.csv"
    path.write_text(
        "source_name,local_name,notes\nPatricio Freire,Patricio Pitbull,ring name\n",
        encoding="utf-8",
    )

    assert read_name_aliases(path) == {"patricio freire": "patricio pitbull"}


def test_missing_alias_file_is_not_an_error(tmp_path):
    assert read_name_aliases(tmp_path / "absent.csv") == {}


def test_end_to_end_writes_source_and_review_outputs(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    html_path = raw_dir / "20260801T120000Z_event_ufc-test-1_fixture.html"
    html_path.write_text((FIXTURES / "event_live_odds.html").read_text(encoding="utf-8"), encoding="utf-8")
    html_path.with_suffix(".metadata.json").write_text(json.dumps({
        "target_type": "event",
        "source_url": "https://www.bestfightodds.com/events/ufc-test-1",
        "fetched_at": FETCHED_AT,
        "content_sha256": "fixture-sha",
    }), encoding="utf-8")

    events_csv = tmp_path / "events.csv"
    events_csv.write_text(
        "event_id,name,date_formatted\n" f"{EVENT_ID},UFC Test Card,2026-08-01\n",
        encoding="utf-8",
    )
    fights_csv = tmp_path / "fights.csv"
    fights_csv.write_text(
        "fight_id,event_id,fighter_1_id,fighter_2_id\n"
        f"{FIGHT_ID},{EVENT_ID},{FIGHTER_1_ID},{FIGHTER_2_ID}\n",
        encoding="utf-8",
    )
    fighters_csv = tmp_path / "fighters.csv"
    fighters_csv.write_text(
        "fighter_id,full_name\n"
        f"{FIGHTER_1_ID},Fighter One\n"
        f"{FIGHTER_2_ID},Fighter Two\n",
        encoding="utf-8",
    )
    source_output = tmp_path / "sources" / "bestfightodds_live_fight_odds.csv"
    unmatched_output = tmp_path / "sources" / "bestfightodds_live_unmatched_odds.csv"

    result = adapt_bestfightodds_live_snapshots(
        raw_dir=raw_dir,
        events_csv=events_csv,
        fights_csv=fights_csv,
        fighters_csv=fighters_csv,
        aliases_csv=None,
        source_output=source_output,
        unmatched_output=unmatched_output,
    )

    assert result.snapshots_read == 1
    assert len(result.matched_rows) == 4
    assert result.unmatched_rows == ()

    with source_output.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == LIVE_SOURCE_COLUMNS
        assert len(list(reader)) == 4
    with unmatched_output.open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == UNMATCHED_COLUMNS


def test_non_event_snapshots_are_ignored(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    html_path = raw_dir / "20260801T120000Z_fighter_someone_fixture.html"
    html_path.write_text((FIXTURES / "event_live_odds.html").read_text(encoding="utf-8"), encoding="utf-8")
    html_path.with_suffix(".metadata.json").write_text(json.dumps({
        "target_type": "fighter",
        "source_url": "https://www.bestfightodds.com/fighters/someone-1",
        "fetched_at": FETCHED_AT,
        "content_sha256": "fixture-sha",
    }), encoding="utf-8")

    from warehouse.adapt_bestfightodds_live_snapshots import read_event_snapshots

    assert read_event_snapshots(raw_dir) == ()
