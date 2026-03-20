"""Unit tests for features.decay — exponentially decayed features.

All tests use FightHistory objects constructed in-memory (no DB required).
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from features.history import FightHistory
from features.decay import compute_decayed_features


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
    event_date: date,
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
        event_date=event_date,
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


CUTOFF = date(2023, 1, 1)
HALF_LIFE = 365


# ── Tests: debut (empty history) ─────────────────────────────────────────────

class TestDebut:
    def setup_method(self):
        self.f = compute_decayed_features([], CUTOFF, HALF_LIFE)

    def test_all_none(self):
        for key in self.f:
            assert self.f[key] is None, f"{key} should be None"


# ── Tests: single fight ───────────────────────────────────────────────────────

class TestOneFight:
    """One fight exactly 0 days before cutoff → weight = 2^0 = 1.0.
    Weighted avg of one fight = that fight's value.
    """

    def setup_method(self):
        h = _h(
            event_date=CUTOFF,  # 0 days ago → weight 1.0
            won=True,
            finish_method="decision",
            finish_round=3,
            finish_time_seconds=300,
            scheduled_rounds=3,
            fighter_stats=_stats(sig_landed=30, sig_attempted=60,
                                  td_landed=2, td_attempted=4,
                                  knockdowns=1, control_time_seconds=90),
        )
        self.f = compute_decayed_features([h], CUTOFF, HALF_LIFE)

    def test_win_rate(self):
        assert self.f["decay_win_rate"] == pytest.approx(1.0)

    def test_finish_rate_zero_for_decision_win(self):
        assert self.f["decay_finish_rate"] == pytest.approx(0.0)

    def test_sig_strike_accuracy(self):
        assert self.f["decay_sig_strike_accuracy"] == pytest.approx(30 / 60)

    def test_knockdowns_per_fight(self):
        assert self.f["decay_knockdowns_per_fight"] == pytest.approx(1.0)

    def test_control_time_per_fight(self):
        assert self.f["decay_control_time_per_fight"] == pytest.approx(90.0)

    def test_sig_strike_rate(self):
        cage_min = 900 / 60
        assert self.f["decay_sig_strike_rate"] == pytest.approx(30 / cage_min)

    def test_takedown_accuracy(self):
        assert self.f["decay_takedown_accuracy"] == pytest.approx(2 / 4)


# ── Tests: half-life boundary ─────────────────────────────────────────────────

class TestHalfLife:
    """A fight exactly half_life_days ago has weight 0.5; today has weight 1.0.
    With two fights (old=0.5, recent=1.0) on the same binary metric:
      old win_rate value = 0.0 (loss), recent win_rate value = 1.0 (win)
      expected = (0.5*0.0 + 1.0*1.0) / (0.5 + 1.0) = 1.0 / 1.5
    """

    def setup_method(self):
        from datetime import timedelta
        old_date = date(CUTOFF.year - 1, CUTOFF.month, CUTOFF.day)  # ~365 days ago
        self.history = [
            _h(event_date=old_date, won=False, result_type="win",
               finish_method="decision", scheduled_rounds=3,
               finish_round=3, finish_time_seconds=300),
            _h(event_date=CUTOFF, won=True,
               finish_method="decision", scheduled_rounds=3,
               finish_round=3, finish_time_seconds=300),
        ]
        self.f = compute_decayed_features(self.history, CUTOFF, HALF_LIFE)
        self.w_old = 2.0 ** (-365 / HALF_LIFE)  # ≈ 0.5
        self.w_new = 1.0

    def test_old_fight_weight_is_half(self):
        assert self.w_old == pytest.approx(0.5, abs=1e-6)

    def test_win_rate_recency_weighted(self):
        expected = (self.w_old * 0.0 + self.w_new * 1.0) / (self.w_old + self.w_new)
        assert self.f["decay_win_rate"] == pytest.approx(expected, rel=1e-5)


# ── Tests: recency weighting (recent fights matter more) ─────────────────────

class TestRecencyWeighting:
    """Two fights with the same sig accuracy but different recency.
    The weighted average should be closer to the more recent fight's value.
    """

    def setup_method(self):
        # Old fight (2 years ago, weight ≈ 0.25): accuracy = 0.25
        # Recent fight (today, weight = 1.0):     accuracy = 0.75
        old_date = date(CUTOFF.year - 2, CUTOFF.month, CUTOFF.day)
        self.history = [
            _h(event_date=old_date, finish_method="decision",
               scheduled_rounds=3, finish_round=3, finish_time_seconds=300,
               fighter_stats=_stats(sig_landed=10, sig_attempted=40)),
            _h(event_date=CUTOFF, finish_method="decision",
               scheduled_rounds=3, finish_round=3, finish_time_seconds=300,
               fighter_stats=_stats(sig_landed=30, sig_attempted=40)),
        ]
        self.f = compute_decayed_features(self.history, CUTOFF, HALF_LIFE)
        self.w_old = 2.0 ** (-730 / HALF_LIFE)  # ~0.25
        self.w_new = 1.0

    def test_recent_fight_dominates(self):
        # Simple average would be 0.5; recency-weighted should be > 0.5
        assert self.f["decay_sig_strike_accuracy"] > 0.5

    def test_exact_weighted_value(self):
        expected = (
            (self.w_old * (10 / 40) + self.w_new * (30 / 40))
            / (self.w_old + self.w_new)
        )
        assert self.f["decay_sig_strike_accuracy"] == pytest.approx(expected, rel=1e-5)


# ── Tests: finish rate with ko/submission ─────────────────────────────────────

class TestFinishRate:
    def test_ko_win_finish_rate_one(self):
        h = _h(event_date=CUTOFF, won=True, finish_method="ko_tko",
                finish_round=2, finish_time_seconds=60, scheduled_rounds=3)
        f = compute_decayed_features([h], CUTOFF, HALF_LIFE)
        assert f["decay_finish_rate"] == pytest.approx(1.0)

    def test_submission_win_finish_rate_one(self):
        h = _h(event_date=CUTOFF, won=True, finish_method="submission",
                finish_round=1, finish_time_seconds=180, scheduled_rounds=3)
        f = compute_decayed_features([h], CUTOFF, HALF_LIFE)
        assert f["decay_finish_rate"] == pytest.approx(1.0)

    def test_finish_loss_does_not_count(self):
        # Losing by KO/TKO should NOT count as a finish for the loser's finish_rate
        h = _h(event_date=CUTOFF, won=False, result_type="win",
                finish_method="ko_tko", finish_round=2, finish_time_seconds=60,
                scheduled_rounds=3)
        f = compute_decayed_features([h], CUTOFF, HALF_LIFE)
        assert f["decay_finish_rate"] == pytest.approx(0.0)


# ── Tests: missing stats ──────────────────────────────────────────────────────

class TestMissingStats:
    def test_stat_features_none_when_no_stats(self):
        h = _h(event_date=CUTOFF, fighter_stats=None)
        f = compute_decayed_features([h], CUTOFF, HALF_LIFE)
        # Outcome features still computable
        assert f["decay_win_rate"] is not None
        # Stats-dependent features are None
        assert f["decay_sig_strike_accuracy"] is None
        assert f["decay_takedown_accuracy"] is None

    def test_win_rate_computed_even_without_stats(self):
        h = _h(event_date=CUTOFF, won=True, fighter_stats=None)
        f = compute_decayed_features([h], CUTOFF, HALF_LIFE)
        assert f["decay_win_rate"] == pytest.approx(1.0)

    def test_partial_stats_available(self):
        """One fight has stats, one does not — stats metrics use only the fight with stats."""
        h_with = _h(event_date=CUTOFF, won=True,
                     finish_method="decision", scheduled_rounds=3,
                     finish_round=3, finish_time_seconds=300,
                     fighter_stats=_stats(sig_landed=20, sig_attempted=40))
        h_without = _h(event_date=date(2022, 1, 1), won=True,
                        fighter_stats=None)
        f = compute_decayed_features([h_without, h_with], CUTOFF, HALF_LIFE)
        # sig accuracy uses only h_with → 20/40 = 0.5
        assert f["decay_sig_strike_accuracy"] == pytest.approx(0.5)
        # win_rate uses both fights
        assert f["decay_win_rate"] is not None
