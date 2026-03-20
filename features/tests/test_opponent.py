"""Unit tests for features.opponent — opponent-adjusted metrics.

Tests build a mini fight universe with known stats and Elo, then verify
that the adjustment ratios and averages are correct.
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from features.data_loader import WarehouseData
from features.history import FightHistory, build_fighter_index, get_history
from features.elo import compute_all_elos
from features.opponent import compute_opponent_adjusted


# ── Helpers ──────────────────────────────────────────────────────────────────

FA = "fighter-a"
FB = "fighter-b"
FC = "fighter-c"


def _stats(sig_landed=0, sig_attempted=0, td_landed=0, td_attempted=0,
           knockdowns=0, control_time_seconds=0):
    return {
        "sig_strikes_landed": sig_landed,
        "sig_strikes_attempted": sig_attempted,
        "takedowns_landed": td_landed,
        "takedowns_attempted": td_attempted,
        "submissions_attempted": 0,
        "knockdowns": knockdowns,
        "control_time_seconds": control_time_seconds,
    }


def _make_fight(fight_id, event_id, event_date, f1, f2, winner=None,
                result_type="win", f1_stats=None, f2_stats=None):
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
        "_f1_stats": f1_stats,
        "_f2_stats": f2_stats,
    }


def _build_data(fights):
    """Build WarehouseData with stats indexes from augmented fight dicts."""
    data = WarehouseData()
    clean_fights = []
    for f in fights:
        f1_stats = f.pop("_f1_stats", None)
        f2_stats = f.pop("_f2_stats", None)
        clean_fights.append(f)

        if f1_stats is not None:
            key = (f["fight_id"], f["fighter_1_id"])
            data.stats_by_fight_fighter[key] = f1_stats
            data.stats_by_fight.setdefault(f["fight_id"], []).append(f1_stats)
        if f2_stats is not None:
            key = (f["fight_id"], f["fighter_2_id"])
            data.stats_by_fight_fighter[key] = f2_stats
            data.stats_by_fight.setdefault(f["fight_id"], []).append(f2_stats)

    data.fights = clean_fights
    data.fight_by_id = {f["fight_id"]: f for f in clean_fights}
    return data


# ── Tests: debut ──────────────────────────────────────────────────────────────

class TestDebut:
    def test_all_none(self):
        f = compute_opponent_adjusted([], {})
        assert f["opp_adjusted_sig_strike_rate"] is None
        assert f["opp_adjusted_takedown_rate"] is None
        assert f["opp_adjusted_control_rate"] is None
        assert f["avg_opponent_elo"] is None
        assert f["avg_opponent_win_rate"] is None


# ── Tests: opponent with known career stats ──────────────────────────────────

class TestSingleOpponent:
    """
    Fight universe:
      f1: FC beats FB on 2020-01-01 — FB absorbs 10 sig / 15 min = 0.667 sig/min
      f2: FA beats FB on 2020-06-01 — FA lands 20 sig / 15 min = 1.333 sig/min
          FB's career sig_absorbed_pm before f2 = 10/15 = 0.667 sig/min
          Adjusted ratio = 1.333 / 0.667 = 2.0

    All fights are 3-round decisions (900 s = 15 min).
    """

    def setup_method(self):
        self.fights = [
            _make_fight("f1", "e1", date(2020, 1, 1), FC, FB, winner=FC,
                        f1_stats=_stats(sig_landed=10, sig_attempted=20,
                                         td_landed=2, td_attempted=4,
                                         control_time_seconds=120),
                        f2_stats=_stats(sig_landed=5, sig_attempted=15,
                                         td_landed=0, td_attempted=2,
                                         control_time_seconds=60)),
            _make_fight("f2", "e2", date(2020, 6, 1), FA, FB, winner=FA,
                        f1_stats=_stats(sig_landed=20, sig_attempted=30,
                                         td_landed=3, td_attempted=5,
                                         control_time_seconds=180),
                        f2_stats=_stats(sig_landed=8, sig_attempted=20,
                                         td_landed=1, td_attempted=3,
                                         control_time_seconds=30)),
        ]
        data = _build_data(self.fights)
        self.index = build_fighter_index(data)
        self.elos = compute_all_elos(data.fights)
        # FA's history before some future fight
        self.fa_history = get_history(self.index, FA, date(2021, 1, 1))

    def test_adjusted_sig_strike_rate(self):
        f = compute_opponent_adjusted(self.fa_history, self.index, self.elos)
        # FA landed 20 sig in 15 min = 1.333/min
        # FB's career absorbed before f2: FC landed 10 sig on FB in 15 min = 0.667/min
        # Ratio = 1.333 / 0.667 = 2.0
        assert f["opp_adjusted_sig_strike_rate"] == pytest.approx(2.0, rel=1e-4)

    def test_adjusted_takedown_rate(self):
        f = compute_opponent_adjusted(self.fa_history, self.index, self.elos)
        # FA: 3 td in 15 min = 0.2/min
        # FB absorbed from FC: 2 td in 15 min = 0.1333/min
        # Ratio = 0.2 / 0.1333 = 1.5
        assert f["opp_adjusted_takedown_rate"] == pytest.approx(1.5, rel=1e-4)

    def test_adjusted_control_rate(self):
        f = compute_opponent_adjusted(self.fa_history, self.index, self.elos)
        # FA: 180 s control
        # FB absorbed from FC: 120 s / 1 fight = 120 per fight
        # Ratio = 180 / 120 = 1.5
        assert f["opp_adjusted_control_rate"] == pytest.approx(1.5, rel=1e-4)

    def test_avg_opponent_elo(self):
        f = compute_opponent_adjusted(self.fa_history, self.index, self.elos)
        # FB's pre-fight Elo for f2: started at 1500, lost f1 → dropped
        fb_elo_f2 = self.elos["f2"][FB]
        assert f["avg_opponent_elo"] == pytest.approx(fb_elo_f2)

    def test_avg_opponent_win_rate(self):
        f = compute_opponent_adjusted(self.fa_history, self.index, self.elos)
        # FB's history before f2: lost f1 → win rate = 0/1 = 0.0
        assert f["avg_opponent_win_rate"] == pytest.approx(0.0)


# ── Tests: multiple opponents of varying quality ──────────────────────────────

class TestMultipleOpponents:
    """
    FA fights FB on 2020-01-01 and FC on 2020-06-01.
    Both opponents have prior history so we can test averaging.
    """

    def setup_method(self):
        # Give FB and FC some prior fights against each other
        fights = [
            # FB beats FC — establishes both have history
            _make_fight("f0", "e0", date(2019, 1, 1), FB, FC, winner=FB,
                        f1_stats=_stats(sig_landed=15, sig_attempted=30),
                        f2_stats=_stats(sig_landed=10, sig_attempted=25)),
            # FA beats FB
            _make_fight("f1", "e1", date(2020, 1, 1), FA, FB, winner=FA,
                        f1_stats=_stats(sig_landed=12, sig_attempted=20),
                        f2_stats=_stats(sig_landed=8, sig_attempted=15)),
            # FA beats FC
            _make_fight("f2", "e2", date(2020, 6, 1), FA, FC, winner=FA,
                        f1_stats=_stats(sig_landed=18, sig_attempted=25),
                        f2_stats=_stats(sig_landed=6, sig_attempted=18)),
        ]
        data = _build_data(fights)
        self.index = build_fighter_index(data)
        self.elos = compute_all_elos(data.fights)
        self.fa_history = get_history(self.index, FA, date(2021, 1, 1))

    def test_has_two_fights(self):
        assert len(self.fa_history) == 2

    def test_avg_opponent_elo_is_mean(self):
        f = compute_opponent_adjusted(self.fa_history, self.index, self.elos)
        fb_elo = self.elos["f1"][FB]  # FB's pre-fight Elo for f1
        fc_elo = self.elos["f2"][FC]  # FC's pre-fight Elo for f2
        expected = (fb_elo + fc_elo) / 2.0
        assert f["avg_opponent_elo"] == pytest.approx(expected)

    def test_avg_opponent_win_rate_is_mean(self):
        f = compute_opponent_adjusted(self.fa_history, self.index, self.elos)
        # FB's history before f1: f0 where FB won → win rate = 1.0
        # FC's history before f2: f0 where FC lost → win rate = 0.0
        expected = (1.0 + 0.0) / 2.0
        assert f["avg_opponent_win_rate"] == pytest.approx(expected)

    def test_adjusted_sig_rate_is_mean_of_ratios(self):
        f = compute_opponent_adjusted(self.fa_history, self.index, self.elos)
        # Fight f1: FA lands 12 sig / 15 min = 0.8/min
        #   FB's career sig absorbed before f1: in f0, FC landed 10 on FB / 15 min = 0.667/min
        #   Ratio = 0.8 / 0.667 = 1.2
        # Fight f2: FA lands 18 sig / 15 min = 1.2/min
        #   FC's career sig absorbed before f2: in f0, FB landed 15 on FC / 15 min = 1.0/min
        #   But also in f1, FA landed 12 on... wait, FC wasn't in f1. FC only has f0.
        #   FC absorbed from FB in f0: 15 sig / 15 min = 1.0/min
        #   Ratio = 1.2 / 1.0 = 1.2
        # Mean = (1.2 + 1.2) / 2 = 1.2
        assert f["opp_adjusted_sig_strike_rate"] == pytest.approx(1.2, rel=1e-3)


# ── Tests: opponent with no prior history ─────────────────────────────────────

class TestOpponentDebut:
    """If the opponent has no prior fights, adjusted rates can't be computed."""

    def setup_method(self):
        fights = [
            _make_fight("f1", "e1", date(2020, 1, 1), FA, FB, winner=FA,
                        f1_stats=_stats(sig_landed=20, sig_attempted=30),
                        f2_stats=_stats(sig_landed=10, sig_attempted=20)),
        ]
        data = _build_data(fights)
        self.index = build_fighter_index(data)
        self.elos = compute_all_elos(data.fights)
        self.fa_history = get_history(self.index, FA, date(2021, 1, 1))

    def test_adjusted_rates_none_when_opp_has_no_history(self):
        f = compute_opponent_adjusted(self.fa_history, self.index, self.elos)
        # FB had no fights before f1, so opponent baselines are unavailable
        assert f["opp_adjusted_sig_strike_rate"] is None
        assert f["opp_adjusted_takedown_rate"] is None
        assert f["opp_adjusted_control_rate"] is None

    def test_opp_win_rate_none_when_no_history(self):
        f = compute_opponent_adjusted(self.fa_history, self.index, self.elos)
        assert f["avg_opponent_win_rate"] is None

    def test_opp_elo_still_available(self):
        f = compute_opponent_adjusted(self.fa_history, self.index, self.elos)
        # FB starts at 1500, even with no prior fights
        assert f["avg_opponent_elo"] == pytest.approx(1500.0)


# ── Tests: no elos passed ────────────────────────────────────────────────────

class TestWithoutElos:
    def test_avg_opponent_elo_none(self):
        fights = [
            _make_fight("f0", "e0", date(2019, 1, 1), FB, FC, winner=FB,
                        f1_stats=_stats(sig_landed=10), f2_stats=_stats(sig_landed=5)),
            _make_fight("f1", "e1", date(2020, 1, 1), FA, FB, winner=FA,
                        f1_stats=_stats(sig_landed=20), f2_stats=_stats(sig_landed=8)),
        ]
        data = _build_data(fights)
        index = build_fighter_index(data)
        history = get_history(index, FA, date(2021, 1, 1))
        f = compute_opponent_adjusted(history, index, elos=None)
        assert f["avg_opponent_elo"] is None
        # Other features should still work
        assert f["avg_opponent_win_rate"] is not None
