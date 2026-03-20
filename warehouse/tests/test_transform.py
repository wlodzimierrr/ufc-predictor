"""Unit tests for warehouse/transform.py."""

import datetime
import pytest

from warehouse.transform import (
    transform_event,
    transform_fight,
    transform_fighter,
    transform_fight_stat,
)

# ---------------------------------------------------------------------------
# Fixtures: minimal valid raw CSV rows
# ---------------------------------------------------------------------------

EVENT_ROW = {
    "scraped_at": "2026-02-19 21:43:16 UTC",
    "event_id": "4f9359b3-e2ff-555f-9224-ec0c87e52af5",
    "url": "http://www.ufcstats.com/event-details/abc",
    "name": "UFC Fight Night: Grasso vs. Shevchenko 2",
    "date": "September 16, 2023",
    "date_formatted": "2023-09-16",
    "city": "Las Vegas",
    "state": "Nevada",
    "country": "USA",
    "fights": "uuid1, uuid2",
}

FIGHT_ROW_WIN = {
    "scraped_at": "2026-02-19 21:59:47 UTC",
    "fight_id": "b20dee5f-75e0-5752-9c49-25f3590add8a",
    "event_id": "4f9359b3-e2ff-555f-9224-ec0c87e52af5",
    "url": "http://www.ufcstats.com/fight-details/abc",
    "fighter_1_id": "aaaa0000-0000-0000-0000-000000000001",
    "fighter_2_id": "bbbb0000-0000-0000-0000-000000000002",
    "fighter_1_outcome": "W",
    "fighter_2_outcome": "L",
    "bout_type": "Women's Strawweight Bout",
    "weight_class": "women's strawweight",
    "num_rounds": "3",
    "primary_finish_method": "decision",
    "secondary_finish_method": "unanimous",
    "finish_round": "3",
    "finish_time_minute": "5",
    "finish_time_second": "0",
    "referee": "Chris Tognoni",
    "judge_1": "Mike Bell",
    "judge_2": "Sal D'amato",
    "judge_3": "Bryan Miner",
}

FIGHT_ROW_KO = {**FIGHT_ROW_WIN, "primary_finish_method": "ko/tko", "secondary_finish_method": "punches", "finish_round": "1", "finish_time_minute": "2", "finish_time_second": "34"}
FIGHT_ROW_SUB = {**FIGHT_ROW_WIN, "primary_finish_method": "submission", "secondary_finish_method": "rear naked choke", "finish_round": "2", "finish_time_minute": "4", "finish_time_second": "12"}

FIGHT_ROW_DRAW = {
    **FIGHT_ROW_WIN,
    "fighter_1_outcome": "D",
    "fighter_2_outcome": "D",
    "primary_finish_method": "decision",
    "secondary_finish_method": "majority",
}

FIGHT_ROW_NC = {
    **FIGHT_ROW_WIN,
    "fighter_1_outcome": "NC",
    "fighter_2_outcome": "NC",
    "primary_finish_method": "overturned",
    "secondary_finish_method": "",
}

FIGHT_ROW_TITLE = {
    **FIGHT_ROW_WIN,
    "bout_type": "Heavyweight Title Bout",
}

FIGHT_ROW_INTERIM = {
    **FIGHT_ROW_WIN,
    "bout_type": "Interim Lightweight Title Bout",
}

FIGHT_ROW_NO_WEIGHT = {
    **FIGHT_ROW_WIN,
    "bout_type": "UFC 3 Tournament Title Bout",
}

FIGHTER_ROW = {
    "scraped_at": "2026-02-19 21:43:16 UTC",
    "fighter_id": "32833121-9382-555b-b4a4-1ecd72824b79",
    "url": "http://www.ufcstats.com/fighter-details/abc",
    "full_name": "Jon Jones",
    "first_name": "Jon",
    "last_names": "Jones",
    "nickname": "Bones",
    "height_cm": "193.04",
    "weight_lbs": "260",
    "reach_cm": "215.9",
    "stance": "Orthodox",
    "dob_formatted": "1987-07-19",
}

FIGHTER_ROW_NULLS = {
    **FIGHTER_ROW,
    "nickname": "",
    "height_cm": "",
    "weight_lbs": "",
    "reach_cm": "",
    "stance": "",
    "dob_formatted": "",
}

FIGHT_STAT_ROW = {
    "scraped_at": "2026-02-21 08:13:03 UTC",
    "fight_stat_id": "d0958472-6af1-532c-a58d-56dded8d1fdd",
    "fight_id": "b20dee5f-75e0-5752-9c49-25f3590add8a",
    "fighter_id": "405205df-b692-5d68-8d29-08c29eb6d8b1",
    "url": "http://www.ufcstats.com/fight-details/abc",
    "total_strikes_landed": "147",
    "total_strikes_attempted": "206",
    "significant_strikes_landed": "81",
    "significant_strikes_attempted": "128",
    "significant_strikes_landed_head": "53",
    "significant_strikes_attempted_head": "97",
    "significant_strikes_landed_body": "20",
    "significant_strikes_attempted_body": "23",
    "significant_strikes_landed_leg": "8",
    "significant_strikes_attempted_leg": "8",
    "significant_strikes_landed_distance": "38",
    "significant_strikes_attempted_distance": "69",
    "significant_strikes_landed_clinch": "8",
    "significant_strikes_attempted_clinch": "11",
    "significant_strikes_landed_ground": "35",
    "significant_strikes_attempted_ground": "48",
    "knockdowns": "0",
    "takedowns_landed": "3",
    "takedowns_attempted": "4",
    "control_time_minutes": "10",
    "control_time_seconds": "49",
    "submissions_attempted": "0",
    "reversals": "0",
}

FIGHT_STAT_BY_ROUND_ROW = {
    **FIGHT_STAT_ROW,
    "fight_stat_by_round_id": "75f9b42c-2deb-58d1-bdd4-67c420216a58",
    "round": "1",
}


# ---------------------------------------------------------------------------
# transform_event
# ---------------------------------------------------------------------------

class TestTransformEvent:
    def test_standard(self):
        r = transform_event(EVENT_ROW)
        assert r["event_id"] == "4f9359b3-e2ff-555f-9224-ec0c87e52af5"
        assert r["event_name"] == "UFC Fight Night: Grasso vs. Shevchenko 2"
        assert r["event_date"] == datetime.date(2023, 9, 16)
        assert r["city"] == "Las Vegas"
        assert r["state"] == "Nevada"
        assert r["country"] == "USA"
        assert r["event_status"] == "completed"
        assert r["scraped_at"] == datetime.datetime(2026, 2, 19, 21, 43, 16, tzinfo=datetime.timezone.utc)

    def test_explicit_event_status(self):
        row = {**EVENT_ROW, "event_status": "upcoming"}
        r = transform_event(row)
        assert r["event_status"] == "upcoming"

    def test_empty_city_is_null(self):
        row = {**EVENT_ROW, "city": "", "state": ""}
        r = transform_event(row)
        assert r["city"] is None
        assert r["state"] is None


# ---------------------------------------------------------------------------
# transform_fight
# ---------------------------------------------------------------------------

class TestTransformFight:
    def test_win_decision(self):
        r = transform_fight(FIGHT_ROW_WIN)
        assert r["result_type"] == "win"
        assert r["winner_fighter_id"] == FIGHT_ROW_WIN["fighter_1_id"]
        assert r["finish_method"] == "decision"
        assert r["finish_detail"] == "unanimous"
        assert r["weight_class"] == "women_strawweight"
        assert r["is_title_fight"] is False
        assert r["is_interim_title"] is False
        assert r["finish_time_seconds"] == 5 * 60 + 0

    def test_win_reversed_outcome(self):
        row = {**FIGHT_ROW_WIN, "fighter_1_outcome": "L", "fighter_2_outcome": "W"}
        r = transform_fight(row)
        assert r["result_type"] == "win"
        assert r["winner_fighter_id"] == FIGHT_ROW_WIN["fighter_2_id"]

    def test_ko_tko(self):
        r = transform_fight(FIGHT_ROW_KO)
        assert r["finish_method"] == "ko_tko"
        assert r["finish_time_seconds"] == 2 * 60 + 34

    def test_submission(self):
        r = transform_fight(FIGHT_ROW_SUB)
        assert r["finish_method"] == "submission"
        assert r["finish_detail"] == "rear naked choke"

    def test_draw(self):
        r = transform_fight(FIGHT_ROW_DRAW)
        assert r["result_type"] == "draw"
        assert r["winner_fighter_id"] is None

    def test_no_contest(self):
        r = transform_fight(FIGHT_ROW_NC)
        assert r["result_type"] == "nc"
        assert r["winner_fighter_id"] is None
        assert r["finish_method"] == "overturned"
        assert r["finish_detail"] is None

    def test_title_fight(self):
        r = transform_fight(FIGHT_ROW_TITLE)
        assert r["is_title_fight"] is True
        assert r["is_interim_title"] is False
        assert r["weight_class"] == "heavyweight"

    def test_interim_title(self):
        r = transform_fight(FIGHT_ROW_INTERIM)
        assert r["is_title_fight"] is True
        assert r["is_interim_title"] is True
        assert r["weight_class"] == "lightweight"

    def test_no_weight_class(self):
        r = transform_fight(FIGHT_ROW_NO_WEIGHT)
        assert r["weight_class"] is None
        assert r["is_title_fight"] is True

    def test_women_weight_class_not_confused_with_men(self):
        row = {**FIGHT_ROW_WIN, "bout_type": "Women's Bantamweight Title Bout"}
        r = transform_fight(row)
        assert r["weight_class"] == "women_bantamweight"

    def test_doctor_stoppage(self):
        row = {**FIGHT_ROW_WIN, "primary_finish_method": "tko - doctor's stoppage"}
        r = transform_fight(row)
        assert r["finish_method"] == "doctor_stoppage"


# ---------------------------------------------------------------------------
# transform_fighter
# ---------------------------------------------------------------------------

class TestTransformFighter:
    def test_standard(self):
        r = transform_fighter(FIGHTER_ROW)
        assert r["fighter_id"] == "32833121-9382-555b-b4a4-1ecd72824b79"
        assert r["full_name"] == "Jon Jones"
        assert r["first_name"] == "Jon"
        assert r["last_name"] == "Jones"
        assert r["nickname"] == "Bones"
        assert r["height_cm"] == pytest.approx(193.04)
        assert r["weight_lbs"] == pytest.approx(260.0)
        assert r["reach_cm"] == pytest.approx(215.9)
        assert r["stance"] == "Orthodox"
        assert r["dob"] == datetime.date(1987, 7, 19)

    def test_empty_physical_attributes_are_null(self):
        r = transform_fighter(FIGHTER_ROW_NULLS)
        assert r["nickname"] is None
        assert r["height_cm"] is None
        assert r["weight_lbs"] is None
        assert r["reach_cm"] is None
        assert r["stance"] is None
        assert r["dob"] is None


# ---------------------------------------------------------------------------
# transform_fight_stat
# ---------------------------------------------------------------------------

class TestTransformFightStat:
    def test_aggregate(self):
        r = transform_fight_stat(FIGHT_STAT_ROW, by_round=False)
        assert r["fight_stat_id"] == "d0958472-6af1-532c-a58d-56dded8d1fdd"
        assert "fight_stat_by_round_id" not in r
        assert "round" not in r
        assert r["total_strikes_landed"] == 147
        assert r["sig_strikes_landed"] == 81
        assert r["control_time_seconds"] == 10 * 60 + 49
        assert r["knockdowns"] == 0

    def test_by_round(self):
        r = transform_fight_stat(FIGHT_STAT_BY_ROUND_ROW, by_round=True)
        assert r["fight_stat_by_round_id"] == "75f9b42c-2deb-58d1-bdd4-67c420216a58"
        assert "fight_stat_id" not in r
        assert r["round"] == 1

    def test_empty_stats_default_to_zero(self):
        row = {
            **FIGHT_STAT_ROW,
            "total_strikes_landed": "",
            "knockdowns": "",
            "control_time_minutes": "",
            "control_time_seconds": "",
        }
        r = transform_fight_stat(row, by_round=False)
        assert r["total_strikes_landed"] == 0
        assert r["knockdowns"] == 0
        assert r["control_time_seconds"] == 0
