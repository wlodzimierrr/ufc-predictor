"""Unit tests for features.rolling — rolling window features.

All tests use FightHistory objects constructed in-memory (no DB required).
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from features.history import FightHistory
from features.rolling import compute_rolling_features


# ── Helpers ──────────────────────────────────────────────────────────────────

def _stats(
    sig_landed=0, sig_attempted=0,
    td_landed=0, td_attempted=0,
    knockdowns=0, control_time_seconds=0,
) -> dict:
    return {
        "sig_strikes_landed": sig_landed,
        "sig_strikes_attempted": sig_attempted,
        "takedowns_landed": td_landed,
        "takedowns_attempted": td_attempted,
        "submissions_attempted": 0,
        "knockdowns": knockdowns,
        "control_time_seconds": control_time_seconds,
    }


def _h(
    won: bool = True,
    result_type: str = "win",
    finish_method: str | None = "decision",
    finish_round: int | None = 3,
    finish_time_seconds: int | None = 300,
    scheduled_rounds: int = 3,
    fighter_stats: dict | None = None,
    opponent_stats: dict | None = None,
) -> FightHistory:
    return FightHistory(
        fight_id="f0",
        event_id="e0",
        event_date=date(2020, 1, 1),
        result_type=result_type,
        won=won,
        finish_method=finish_method,
        finish_round=finish_round,
        finish_time_seconds=finish_time_seconds,
        scheduled_rounds=scheduled_rounds,
        weight_class="lightweight",
        is_title_fight=False,
        fighter_stats=fighter_stats,
        opponent_stats=opponent_stats,
        opponent_id="opp",
    )


def _decision_win(sig_landed=20, sig_attempted=40, td_landed=1, td_attempted=2):
    return _h(
        won=True, finish_method="decision",
        finish_round=3, finish_time_seconds=300, scheduled_rounds=3,
        fighter_stats=_stats(sig_landed=sig_landed, sig_attempted=sig_attempted,
                              td_landed=td_landed, td_attempted=td_attempted),
        opponent_stats=_stats(sig_landed=10, sig_attempted=25),
    )


def _ko_win():
    """KO win in R2 at 1:00 → (2-1)*300 + 60 = 360 s."""
    return _h(
        won=True, finish_method="ko_tko",
        finish_round=2, finish_time_seconds=60, scheduled_rounds=3,
        fighter_stats=_stats(sig_landed=15, sig_attempted=20, knockdowns=1),
        opponent_stats=_stats(sig_landed=5, sig_attempted=10),
    )


def _loss():
    return _h(
        won=False, result_type="win", finish_method="decision",
        finish_round=3, finish_time_seconds=300, scheduled_rounds=3,
        fighter_stats=_stats(sig_landed=10, sig_attempted=30),
        opponent_stats=_stats(sig_landed=20, sig_attempted=35),
    )


# ── Tests: debut (empty history) ─────────────────────────────────────────────

class TestDebut:
    def setup_method(self):
        self.f = compute_rolling_features([])

    def test_has_flags_all_false(self):
        assert self.f["has_1_fights"] is False
        assert self.f["has_3_fights"] is False
        assert self.f["has_5_fights"] is False

    def test_wins_are_zero(self):
        assert self.f["last1_wins"] == 0
        assert self.f["last3_wins"] == 0
        assert self.f["last5_wins"] == 0

    def test_rates_are_none(self):
        for n in (1, 3, 5):
            assert self.f[f"last{n}_sig_strikes_landed_per_min"] is None
            assert self.f[f"last{n}_finish_rate"] is None


# ── Tests: exactly 1 fight ────────────────────────────────────────────────────

class TestOneFight:
    """Fighter has exactly 1 prior fight: a decision win (3*300 = 900 s)."""

    def setup_method(self):
        self.history = [_decision_win(sig_landed=30, sig_attempted=60,
                                       td_landed=2, td_attempted=4)]
        self.f = compute_rolling_features(self.history)

    def test_has_flags(self):
        assert self.f["has_1_fights"] is True
        assert self.f["has_3_fights"] is False
        assert self.f["has_5_fights"] is False

    def test_last1_wins(self):
        assert self.f["last1_wins"] == 1

    def test_last3_wins_uses_all_available(self):
        # Only 1 fight available, window falls back to all fights
        assert self.f["last3_wins"] == 1

    def test_last1_sig_accuracy(self):
        assert self.f["last1_sig_strike_accuracy"] == pytest.approx(30 / 60)

    def test_last1_takedown_accuracy(self):
        assert self.f["last1_takedown_accuracy"] == pytest.approx(2 / 4)

    def test_last1_finish_rate(self):
        # Decision win → finish_rate = 0
        assert self.f["last1_finish_rate"] == pytest.approx(0.0)

    def test_last1_sig_landed_per_min(self):
        cage_min = 900 / 60
        assert self.f["last1_sig_strikes_landed_per_min"] == pytest.approx(30 / cage_min)


# ── Tests: exactly 3 fights ───────────────────────────────────────────────────

class TestThreeFights:
    """
    3 fights:
      f0: decision win  (900 s)  sig 30/60, td 2/4
      f1: ko win        (360 s)  sig 15/20, td 0/0, kd 1
      f2: decision loss (900 s)  sig 10/30, td 0/0
    """

    def setup_method(self):
        self.history = [
            _decision_win(sig_landed=30, sig_attempted=60, td_landed=2, td_attempted=4),
            _ko_win(),
            _loss(),
        ]
        self.f = compute_rolling_features(self.history)

    def test_has_flags(self):
        assert self.f["has_3_fights"] is True
        assert self.f["has_5_fights"] is False

    def test_last1_uses_most_recent(self):
        # Most recent is the loss
        assert self.f["last1_wins"] == 0

    def test_last3_wins(self):
        assert self.f["last3_wins"] == 2

    def test_last3_finish_rate(self):
        # 1 finish (ko) out of 3 fights
        assert self.f["last3_finish_rate"] == pytest.approx(1 / 3)

    def test_last3_sig_accuracy(self):
        # pooled: (30+15+10) / (60+20+30) = 55/110
        assert self.f["last3_sig_strike_accuracy"] == pytest.approx(55 / 110)

    def test_last3_knockdowns_per_fight(self):
        # only fight2 has kd=1
        assert self.f["last3_knockdowns_per_fight"] == pytest.approx(1 / 3)

    def test_last5_same_as_last3_when_fewer_fights(self):
        # only 3 fights available, so last5 uses the same data
        assert self.f["last5_wins"] == self.f["last3_wins"]
        assert self.f["last5_sig_strike_accuracy"] == self.f["last3_sig_strike_accuracy"]


# ── Tests: 10 fights (window slicing) ────────────────────────────────────────

class TestTenFights:
    """
    10 fights: first 5 are losses, last 5 are wins (all decision, 900 s each).
    This verifies that rolling windows pick the most recent N, not oldest N.
    """

    def setup_method(self):
        self.history = [_loss() for _ in range(5)] + [_decision_win() for _ in range(5)]
        self.f = compute_rolling_features(self.history)

    def test_has_flags_all_true(self):
        assert self.f["has_1_fights"] is True
        assert self.f["has_3_fights"] is True
        assert self.f["has_5_fights"] is True

    def test_last1_win(self):
        assert self.f["last1_wins"] == 1

    def test_last3_all_wins(self):
        assert self.f["last3_wins"] == 3

    def test_last5_all_wins(self):
        assert self.f["last5_wins"] == 5

    def test_last5_does_not_include_older_losses(self):
        # last5 should not include any of the first-5 losses
        # If it did, last5_wins would be < 5
        assert self.f["last5_wins"] == 5

    def test_last1_finish_rate_zero(self):
        # Most recent fight is a decision win
        assert self.f["last1_finish_rate"] == pytest.approx(0.0)


# ── Tests: finish rate with KO/sub mix ───────────────────────────────────────

class TestFinishRate:
    def test_all_finishes(self):
        history = [_ko_win(), _ko_win(), _ko_win()]
        f = compute_rolling_features(history, windows=(3,))
        assert f["last3_finish_rate"] == pytest.approx(1.0)

    def test_submission_counts_as_finish(self):
        sub_win = _h(
            won=True, finish_method="submission",
            finish_round=2, finish_time_seconds=120, scheduled_rounds=3,
            fighter_stats=_stats(), opponent_stats=_stats(),
        )
        f = compute_rolling_features([sub_win], windows=(1,))
        assert f["last1_finish_rate"] == pytest.approx(1.0)

    def test_doctor_stoppage_counts_as_finish(self):
        ds_win = _h(
            won=True, finish_method="doctor_stoppage",
            finish_round=2, finish_time_seconds=60, scheduled_rounds=3,
            fighter_stats=_stats(), opponent_stats=_stats(),
        )
        f = compute_rolling_features([ds_win], windows=(1,))
        assert f["last1_finish_rate"] == pytest.approx(1.0)


# ── Tests: custom windows ─────────────────────────────────────────────────────

class TestCustomWindows:
    def test_single_custom_window(self):
        history = [_decision_win() for _ in range(3)]
        f = compute_rolling_features(history, windows=(2,))
        assert "last2_wins" in f
        assert "last2_has_fights" not in f  # key is has_2_fights
        assert "has_2_fights" in f
        assert f["has_2_fights"] is True
        assert f["last2_wins"] == 2

    def test_no_last1_key_when_not_in_windows(self):
        history = [_decision_win()]
        f = compute_rolling_features(history, windows=(3,))
        assert "last1_wins" not in f
