"""Unit tests for features.history — fighter fight-history index.

All tests run without a database: WarehouseData is constructed in-memory
using minimal dicts that match the shape produced by load_all_data().
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from features.data_loader import WarehouseData
from features.history import FightHistory, build_fighter_index, get_history


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_fight(
    fight_id: str,
    event_id: str,
    event_date: date,
    f1: str,
    f2: str,
    winner: str | None = None,
    result_type: str = "win",
) -> dict:
    return {
        "fight_id": fight_id,
        "event_id": event_id,
        "event_date": event_date,
        "fighter_1_id": f1,
        "fighter_2_id": f2,
        "winner_fighter_id": winner,
        "result_type": result_type,
        "weight_class": "lightweight",
        "is_title_fight": False,
        "is_interim_title": False,
        "scheduled_rounds": 3,
        "finish_method": "decision",
        "finish_detail": None,
        "finish_round": 3,
        "finish_time_seconds": 300,
        "referee": None,
        "source_url": "http://example.com",
    }


def _make_data(fights: list[dict]) -> WarehouseData:
    data = WarehouseData()
    data.fights = fights
    data.fight_by_id = {f["fight_id"]: f for f in fights}
    # stats_by_fight_fighter left empty — OK for history tests
    return data


# ── Fixtures ─────────────────────────────────────────────────────────────────

FIGHTER_A = "aaaa-aaaa"
FIGHTER_B = "bbbb-bbbb"
FIGHTER_C = "cccc-cccc"

DATES = [
    date(2020, 1, 1),
    date(2020, 6, 1),
    date(2021, 1, 1),
    date(2021, 6, 1),
    date(2022, 1, 1),
]


@pytest.fixture()
def five_fight_data() -> WarehouseData:
    """Fighter A has 5 fights (fight 5 is the 'target')."""
    fights = [
        _make_fight(f"f{i}", f"e{i}", DATES[i], FIGHTER_A, FIGHTER_B, winner=FIGHTER_A)
        for i in range(5)
    ]
    return _make_data(fights)


@pytest.fixture()
def five_fight_index(five_fight_data) -> dict:
    return build_fighter_index(five_fight_data)


# ── Tests: build_fighter_index ────────────────────────────────────────────────

class TestBuildFighterIndex:
    def test_both_fighters_indexed(self, five_fight_data):
        index = build_fighter_index(five_fight_data)
        assert FIGHTER_A in index
        assert FIGHTER_B in index

    def test_sorted_chronologically(self, five_fight_index):
        history = five_fight_index[FIGHTER_A]
        dates = [h.event_date for h in history]
        assert dates == sorted(dates)

    def test_won_flag_correct(self, five_fight_index):
        # FIGHTER_A won all fights
        for h in five_fight_index[FIGHTER_A]:
            assert h.won is True
        # FIGHTER_B lost all fights
        for h in five_fight_index[FIGHTER_B]:
            assert h.won is False

    def test_opponent_id_correct(self, five_fight_index):
        for h in five_fight_index[FIGHTER_A]:
            assert h.opponent_id == FIGHTER_B
        for h in five_fight_index[FIGHTER_B]:
            assert h.opponent_id == FIGHTER_A

    def test_fight_without_event_date_skipped(self):
        fight = _make_fight("f0", "e0", date(2020, 1, 1), FIGHTER_A, FIGHTER_B)
        fight["event_date"] = None  # simulate broken FK
        data = _make_data([fight])
        index = build_fighter_index(data)
        assert index.get(FIGHTER_A, []) == []

    def test_unknown_fighter_not_in_index(self, five_fight_index):
        assert "zzzz-zzzz" not in five_fight_index


# ── Tests: get_history ────────────────────────────────────────────────────────

class TestGetHistory:
    def test_debut_returns_empty(self, five_fight_index):
        assert get_history(five_fight_index, FIGHTER_A, DATES[0]) == []

    def test_one_prior_fight(self, five_fight_index):
        result = get_history(five_fight_index, FIGHTER_A, DATES[1])
        assert len(result) == 1
        assert result[0].event_date == DATES[0]

    def test_two_prior_fights(self, five_fight_index):
        result = get_history(five_fight_index, FIGHTER_A, DATES[2])
        assert len(result) == 2

    def test_three_prior_fights(self, five_fight_index):
        result = get_history(five_fight_index, FIGHTER_A, DATES[3])
        assert len(result) == 3

    def test_four_prior_fights(self, five_fight_index):
        result = get_history(five_fight_index, FIGHTER_A, DATES[4])
        assert len(result) == 4

    def test_target_fight_excluded(self, five_fight_index):
        """get_history with cutoff = DATES[4] must NOT return fight f4."""
        history = get_history(five_fight_index, FIGHTER_A, DATES[4])
        fight_ids = {h.fight_id for h in history}
        assert "f4" not in fight_ids

    def test_same_date_cutoff_excluded(self):
        """Fights on the same date as the cutoff must be excluded."""
        same_date = date(2021, 3, 15)
        fights = [
            _make_fight("f0", "e0", date(2021, 1, 1), FIGHTER_A, FIGHTER_B),
            _make_fight("f1", "e1", same_date, FIGHTER_A, FIGHTER_B),   # same as cutoff
        ]
        data = _make_data(fights)
        index = build_fighter_index(data)
        result = get_history(index, FIGHTER_A, same_date)
        assert len(result) == 1
        assert result[0].fight_id == "f0"

    def test_unknown_fighter_returns_empty(self, five_fight_index):
        result = get_history(five_fight_index, "zzzz-zzzz", DATES[2])
        assert result == []

    def test_draw_result_type(self):
        fight = _make_fight(
            "f0", "e0", date(2020, 1, 1), FIGHTER_A, FIGHTER_B,
            winner=None, result_type="draw",
        )
        data = _make_data([fight])
        index = build_fighter_index(data)
        hist_a = index[FIGHTER_A]
        assert hist_a[0].result_type == "draw"
        assert hist_a[0].won is False
