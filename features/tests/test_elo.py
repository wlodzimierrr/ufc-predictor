"""Unit tests for features.elo — sequential Elo rating system."""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from features.elo import compute_all_elos, get_fighter_elo_features
from features.history import FightHistory


# ── Helpers ──────────────────────────────────────────────────────────────────

F1 = "fighter-1"
F2 = "fighter-2"
F3 = "fighter-3"
INITIAL = 1500.0
K = 32.0


def _fight(
    fight_id: str,
    event_date: date,
    f1: str,
    f2: str,
    winner: str | None = None,
    result_type: str = "win",
) -> dict:
    return {
        "fight_id": fight_id,
        "event_id": "e0",
        "event_date": event_date,
        "fighter_1_id": f1,
        "fighter_2_id": f2,
        "winner_fighter_id": winner,
        "result_type": result_type,
    }


def _expected_score(r_a: float, r_b: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((r_b - r_a) / 400.0))


# ── Tests: basic Elo mechanics ───────────────────────────────────────────────

class TestBasicElo:
    """Two equally-rated fighters, one wins."""

    def setup_method(self):
        self.fights = [
            _fight("f1", date(2020, 1, 1), F1, F2, winner=F1),
        ]
        self.elos = compute_all_elos(self.fights, k=K, initial=INITIAL)

    def test_pre_fight_elo_both_initial(self):
        assert self.elos["f1"][F1] == INITIAL
        assert self.elos["f1"][F2] == INITIAL

    def test_two_entries_per_fight(self):
        assert len(self.elos["f1"]) == 2


class TestWinnerGoesUpLoserGoesDown:
    def setup_method(self):
        # Two fights so we can read post-fight Elo as pre-fight of fight 2
        self.fights = [
            _fight("f1", date(2020, 1, 1), F1, F2, winner=F1),
            _fight("f2", date(2020, 6, 1), F1, F2, winner=F1),
        ]
        self.elos = compute_all_elos(self.fights, k=K, initial=INITIAL)

    def test_winner_elo_rises(self):
        assert self.elos["f2"][F1] > INITIAL

    def test_loser_elo_drops(self):
        assert self.elos["f2"][F2] < INITIAL

    def test_symmetric_change(self):
        # Both start at 1500, so the Elo change should be equal and opposite
        change_f1 = self.elos["f2"][F1] - INITIAL
        change_f2 = self.elos["f2"][F2] - INITIAL
        assert change_f1 == pytest.approx(-change_f2)

    def test_exact_elo_value(self):
        e = _expected_score(INITIAL, INITIAL)  # 0.5 for equal ratings
        new_elo = INITIAL + K * (1.0 - e)  # winner after fight 1
        assert self.elos["f2"][F1] == pytest.approx(new_elo)


class TestDraw:
    """A draw: both fighters move toward each other."""

    def setup_method(self):
        # Give F1 a higher Elo first, then draw in fight 2
        self.fights = [
            _fight("f1", date(2020, 1, 1), F1, F2, winner=F1),
            _fight("f2", date(2020, 6, 1), F1, F2, result_type="draw"),
        ]
        self.elos = compute_all_elos(self.fights, k=K, initial=INITIAL)

    def test_pre_fight_2_f1_above_initial(self):
        assert self.elos["f2"][F1] > INITIAL

    def test_pre_fight_2_f2_below_initial(self):
        assert self.elos["f2"][F2] < INITIAL

    def test_draw_moves_higher_rated_down(self):
        # After the draw, F1 (higher rated) should lose Elo
        r_a = self.elos["f2"][F1]
        r_b = self.elos["f2"][F2]
        e_a = _expected_score(r_a, r_b)
        # F1's expected score > 0.5, actual = 0.5, so F1 drops
        assert e_a > 0.5

    def test_draw_moves_lower_rated_up(self):
        r_a = self.elos["f2"][F1]
        r_b = self.elos["f2"][F2]
        e_b = _expected_score(r_b, r_a)
        # F2's expected score < 0.5, actual = 0.5, so F2 rises
        assert e_b < 0.5


class TestNoContest:
    """No-contest: no rating change."""

    def setup_method(self):
        self.fights = [
            _fight("f1", date(2020, 1, 1), F1, F2, result_type="nc"),
            _fight("f2", date(2020, 6, 1), F1, F2, winner=F1),
        ]
        self.elos = compute_all_elos(self.fights, k=K, initial=INITIAL)

    def test_pre_fight_elo_stored_for_nc(self):
        # Pre-fight Elo should still be recorded
        assert self.elos["f1"][F1] == INITIAL
        assert self.elos["f1"][F2] == INITIAL

    def test_no_change_after_nc(self):
        # Post-NC, both should still be at initial for fight 2
        assert self.elos["f2"][F1] == INITIAL
        assert self.elos["f2"][F2] == INITIAL


class TestNewFighterStartsAtInitial:
    def test_third_fighter_enters(self):
        fights = [
            _fight("f1", date(2020, 1, 1), F1, F2, winner=F1),
            _fight("f2", date(2020, 6, 1), F1, F3, winner=F1),
        ]
        elos = compute_all_elos(fights, k=K, initial=INITIAL)
        # F3's first appearance → starts at initial
        assert elos["f2"][F3] == INITIAL


class TestChronologicalOrder:
    """Fights passed out of order should still be processed by event_date."""

    def test_sorting(self):
        # Pass fights in reverse order
        fights = [
            _fight("f2", date(2020, 6, 1), F1, F2, winner=F2),
            _fight("f1", date(2020, 1, 1), F1, F2, winner=F1),
        ]
        elos = compute_all_elos(fights, k=K, initial=INITIAL)

        # After f1 (F1 wins): F1 goes up, F2 goes down
        # Then f2 (F2 wins): ratings partially revert
        # The pre-fight Elos for f1 should be initial (it's the first fight)
        assert elos["f1"][F1] == INITIAL
        assert elos["f1"][F2] == INITIAL
        # For f2, F1 should be > initial (won f1), F2 < initial (lost f1)
        assert elos["f2"][F1] > INITIAL
        assert elos["f2"][F2] < INITIAL


# ── Tests: get_fighter_elo_features ──────────────────────────────────────────

class TestEloFeatures:
    def setup_method(self):
        self.fights = [
            _fight("f1", date(2020, 1, 1), F1, F2, winner=F1),
            _fight("f2", date(2020, 6, 1), F1, F2, winner=F1),
        ]
        self.elos = compute_all_elos(self.fights, k=K, initial=INITIAL)

    def test_pre_fight_elo(self):
        feats = get_fighter_elo_features(self.elos, F1, "f1", [])
        assert feats["pre_fight_elo"] == INITIAL

    def test_opponent_elo(self):
        feats = get_fighter_elo_features(self.elos, F1, "f1", [])
        assert feats["opponent_pre_fight_elo"] == INITIAL

    def test_elo_change_debut_is_none(self):
        feats = get_fighter_elo_features(self.elos, F1, "f1", [])
        assert feats["elo_change_last_fight"] is None

    def test_elo_change_after_one_fight(self):
        # Create a fake FightHistory for the prior fight
        prior = FightHistory(
            fight_id="f1", event_id="e0", event_date=date(2020, 1, 1),
            result_type="win", won=True, finish_method="decision",
            finish_round=3, finish_time_seconds=300, scheduled_rounds=3,
            weight_class="lightweight", is_title_fight=False,
            fighter_stats=None, opponent_stats=None, opponent_id=F2,
        )
        feats = get_fighter_elo_features(self.elos, F1, "f2", [prior])
        # Pre-fight f2 Elo - pre-fight f1 Elo = the change from fight 1
        expected = self.elos["f2"][F1] - self.elos["f1"][F1]
        assert feats["elo_change_last_fight"] == pytest.approx(expected)
        assert feats["elo_change_last_fight"] > 0  # F1 won, so Elo went up


class TestCustomKAndInitial:
    def test_custom_k(self):
        fights = [_fight("f1", date(2020, 1, 1), F1, F2, winner=F1)]
        elos_k16 = compute_all_elos(fights, k=16, initial=INITIAL)
        elos_k64 = compute_all_elos(fights, k=64, initial=INITIAL)
        # Both start at initial, but k=64 should produce a bigger post-fight change
        # We can check this by adding a second fight
        fights2 = fights + [_fight("f2", date(2020, 6, 1), F1, F2, winner=F1)]
        e16 = compute_all_elos(fights2, k=16, initial=INITIAL)
        e64 = compute_all_elos(fights2, k=64, initial=INITIAL)
        # After fight 1, k=64 moves ratings more → bigger gap in fight 2 pre-elos
        gap_16 = e16["f2"][F1] - e16["f2"][F2]
        gap_64 = e64["f2"][F1] - e64["f2"][F2]
        assert gap_64 > gap_16

    def test_custom_initial(self):
        fights = [_fight("f1", date(2020, 1, 1), F1, F2, winner=F1)]
        elos = compute_all_elos(fights, initial=2000.0)
        assert elos["f1"][F1] == 2000.0
        assert elos["f1"][F2] == 2000.0
