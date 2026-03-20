"""Unit tests for features.career — career aggregate features.

All tests use FightHistory objects constructed in-memory (no DB required).
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from features.history import FightHistory
from features.career import compute_career_features


# ── Helpers ──────────────────────────────────────────────────────────────────

def _stats(
    sig_landed=0, sig_attempted=0,
    td_landed=0, td_attempted=0,
    sub_attempts=0, knockdowns=0,
    control_time_seconds=0,
) -> dict:
    return {
        "sig_strikes_landed": sig_landed,
        "sig_strikes_attempted": sig_attempted,
        "takedowns_landed": td_landed,
        "takedowns_attempted": td_attempted,
        "submissions_attempted": sub_attempts,
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
    is_title_fight: bool = False,
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
        is_title_fight=is_title_fight,
        fighter_stats=fighter_stats,
        opponent_stats=opponent_stats,
        opponent_id="opp",
    )


# ── Tests: debut (empty history) ─────────────────────────────────────────────

class TestDebut:
    def setup_method(self):
        self.f = compute_career_features([])

    def test_total_fights_zero(self):
        assert self.f["total_fights"] == 0

    def test_wins_zero(self):
        assert self.f["wins"] == 0

    def test_win_rate_none(self):
        assert self.f["win_rate"] is None

    def test_finish_rate_none(self):
        assert self.f["finish_rate"] is None

    def test_sig_strike_accuracy_none(self):
        assert self.f["career_sig_strike_accuracy"] is None

    def test_sig_strike_defense_none(self):
        assert self.f["career_sig_strike_defense"] is None

    def test_takedown_accuracy_none(self):
        assert self.f["career_takedown_accuracy"] is None

    def test_takedown_defense_none(self):
        assert self.f["career_takedown_defense"] is None

    def test_per_min_stats_none(self):
        assert self.f["career_sig_strikes_landed_per_min"] is None
        assert self.f["career_sig_strikes_absorbed_per_min"] is None

    def test_streaks_zero(self):
        assert self.f["current_win_streak"] == 0
        assert self.f["current_lose_streak"] == 0

    def test_cage_time_zero(self):
        assert self.f["total_cage_time_seconds"] == 0


# ── Tests: single fight ───────────────────────────────────────────────────────

class TestOneFight:
    """One decision win, 3 rounds (3 * 300 = 900 s cage time)."""

    def setup_method(self):
        self.h = _h(
            won=True,
            result_type="win",
            finish_method="decision",
            finish_round=3,
            finish_time_seconds=300,
            scheduled_rounds=3,
            fighter_stats=_stats(sig_landed=30, sig_attempted=60, td_landed=2, td_attempted=4),
            opponent_stats=_stats(sig_landed=15, sig_attempted=40, td_landed=0, td_attempted=2),
        )
        self.f = compute_career_features([self.h])

    def test_counts(self):
        assert self.f["total_fights"] == 1
        assert self.f["wins"] == 1
        assert self.f["losses"] == 0
        assert self.f["dec_wins"] == 1
        assert self.f["ko_tko_wins"] == 0

    def test_win_rate(self):
        assert self.f["win_rate"] == pytest.approx(1.0)

    def test_cage_time(self):
        # decision 3 rounds → 3 * 300 = 900 s
        assert self.f["total_cage_time_seconds"] == 900
        assert self.f["avg_fight_time_seconds"] == pytest.approx(900.0)

    def test_sig_strike_accuracy(self):
        assert self.f["career_sig_strike_accuracy"] == pytest.approx(30 / 60)

    def test_sig_strike_defense(self):
        # 1 - 15/40
        assert self.f["career_sig_strike_defense"] == pytest.approx(1 - 15 / 40)

    def test_takedown_accuracy(self):
        assert self.f["career_takedown_accuracy"] == pytest.approx(2 / 4)

    def test_takedown_defense(self):
        # 1 - 0/2
        assert self.f["career_takedown_defense"] == pytest.approx(1.0)

    def test_sig_landed_per_min(self):
        cage_minutes = 900 / 60
        assert self.f["career_sig_strikes_landed_per_min"] == pytest.approx(30 / cage_minutes)

    def test_win_streak(self):
        assert self.f["current_win_streak"] == 1
        assert self.f["current_lose_streak"] == 0


# ── Tests: multi-fight mixed history ─────────────────────────────────────────

class TestMultiFight:
    """
    4-fight history:
      f0: KO/TKO win   R2 @ 2:30 → (1)*300 + 150 = 450 s
      f1: decision loss  3 rounds → 3*300 = 900 s
      f2: draw           3 rounds → 3*300 = 900 s
      f3: submission win R1 @ 4:00 → (0)*300 + 240 = 240 s
    Total cage time = 450 + 900 + 900 + 240 = 2490 s
    """

    def setup_method(self):
        self.history = [
            _h(won=True, result_type="win", finish_method="ko_tko",
               finish_round=2, finish_time_seconds=150, scheduled_rounds=3,
               fighter_stats=_stats(sig_landed=20, sig_attempted=30, knockdowns=1),
               opponent_stats=_stats(sig_landed=5, sig_attempted=10)),
            _h(won=False, result_type="win", finish_method="decision",
               finish_round=3, finish_time_seconds=300, scheduled_rounds=3,
               fighter_stats=_stats(sig_landed=15, sig_attempted=40),
               opponent_stats=_stats(sig_landed=25, sig_attempted=45)),
            _h(won=False, result_type="draw", finish_method="decision",
               finish_round=3, finish_time_seconds=300, scheduled_rounds=3,
               fighter_stats=_stats(sig_landed=10, sig_attempted=20),
               opponent_stats=_stats(sig_landed=10, sig_attempted=20)),
            _h(won=True, result_type="win", finish_method="submission",
               finish_round=1, finish_time_seconds=240, scheduled_rounds=3,
               is_title_fight=True,
               fighter_stats=_stats(sig_landed=5, sig_attempted=8,
                                     td_landed=1, td_attempted=2, sub_attempts=3),
               opponent_stats=_stats(sig_landed=3, sig_attempted=5,
                                     td_landed=0, td_attempted=1)),
        ]
        self.f = compute_career_features(self.history)

    def test_record(self):
        assert self.f["total_fights"] == 4
        assert self.f["wins"] == 2
        assert self.f["losses"] == 1
        assert self.f["draws"] == 1
        assert self.f["no_contests"] == 0

    def test_win_rate(self):
        assert self.f["win_rate"] == pytest.approx(2 / 4)

    def test_finish_profile_wins(self):
        assert self.f["ko_tko_wins"] == 1
        assert self.f["sub_wins"] == 1
        assert self.f["dec_wins"] == 0
        assert self.f["finish_rate"] == pytest.approx(2 / 2)  # both wins are finishes

    def test_finish_profile_losses(self):
        assert self.f["ko_tko_losses"] == 0
        assert self.f["sub_losses"] == 0
        assert self.f["dec_losses"] == 1

    def test_title(self):
        assert self.f["title_fights"] == 1
        assert self.f["title_wins"] == 1

    def test_cage_time(self):
        assert self.f["total_cage_time_seconds"] == 2490
        assert self.f["avg_fight_time_seconds"] == pytest.approx(2490 / 4)

    def test_win_streak(self):
        # Last fight was a submission win → streak = 1
        assert self.f["current_win_streak"] == 1
        assert self.f["current_lose_streak"] == 0

    def test_sig_strike_accuracy(self):
        # pooled: (20+15+10+5) / (30+40+20+8) = 50 / 98
        assert self.f["career_sig_strike_accuracy"] == pytest.approx(50 / 98)

    def test_sig_strike_defense(self):
        # 1 - (5+25+10+3) / (10+45+20+5) = 1 - 43/80
        assert self.f["career_sig_strike_defense"] == pytest.approx(1 - 43 / 80)

    def test_takedown_accuracy(self):
        # only fight4 has td data: 1/2
        assert self.f["career_takedown_accuracy"] == pytest.approx(1 / 2)

    def test_takedown_defense(self):
        # opp td: 0/1+0/1 = 0 / (1+1) = 0/2 → defense = 1
        assert self.f["career_takedown_defense"] == pytest.approx(1.0)

    def test_knockdowns_per_fight(self):
        assert self.f["career_knockdowns_per_fight"] == pytest.approx(1 / 4)

    def test_control_time_per_fight(self):
        # no control time in any fight → 0
        assert self.f["career_control_time_per_fight"] == pytest.approx(0.0)


# ── Tests: no-contest ─────────────────────────────────────────────────────────

class TestNoContest:
    def test_nc_counted(self):
        h = _h(won=False, result_type="nc", finish_method=None,
                finish_round=None, finish_time_seconds=None)
        f = compute_career_features([h])
        assert f["no_contests"] == 1
        assert f["wins"] == 0
        assert f["losses"] == 0


# ── Tests: doctor stoppage counts as KO/TKO ──────────────────────────────────

class TestDoctorStoppage:
    def test_doctor_stoppage_win_is_ko_tko(self):
        h = _h(won=True, result_type="win", finish_method="doctor_stoppage",
                finish_round=2, finish_time_seconds=60, scheduled_rounds=3)
        f = compute_career_features([h])
        assert f["ko_tko_wins"] == 1

    def test_doctor_stoppage_loss_is_ko_tko(self):
        h = _h(won=False, result_type="win", finish_method="doctor_stoppage",
                finish_round=2, finish_time_seconds=60, scheduled_rounds=3)
        f = compute_career_features([h])
        assert f["ko_tko_losses"] == 1


# ── Tests: fight duration calculation ────────────────────────────────────────

class TestFightDuration:
    def test_stoppage_r1_at_2m30s(self):
        # R1 at 2:30 → (1-1)*300 + 150 = 150 s
        h = _h(finish_method="ko_tko", finish_round=1, finish_time_seconds=150,
                scheduled_rounds=3)
        f = compute_career_features([h])
        assert f["total_cage_time_seconds"] == 150

    def test_stoppage_r3_at_5m(self):
        # R3 at 5:00 → (3-1)*300 + 300 = 900 s
        h = _h(finish_method="ko_tko", finish_round=3, finish_time_seconds=300,
                scheduled_rounds=3)
        f = compute_career_features([h])
        assert f["total_cage_time_seconds"] == 900

    def test_decision_3_rounds(self):
        # 3 * 300 = 900 s
        h = _h(finish_method="decision", scheduled_rounds=3,
                finish_round=3, finish_time_seconds=300)
        f = compute_career_features([h])
        assert f["total_cage_time_seconds"] == 900

    def test_decision_5_rounds(self):
        # 5 * 300 = 1500 s
        h = _h(finish_method="decision", scheduled_rounds=5,
                finish_round=5, finish_time_seconds=300)
        f = compute_career_features([h])
        assert f["total_cage_time_seconds"] == 1500
