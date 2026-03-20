"""Unit tests for features.snapshot and features.bout."""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from features.data_loader import WarehouseData
from features.history import FightHistory, build_fighter_index, get_history
from features.elo import compute_all_elos
from features.snapshot import build_fighter_snapshot
from features.bout import build_bout_features


# ── Helpers ──────────────────────────────────────────────────────────────────

FA = "fighter-a"
FB = "fighter-b"


def _stats(sig_landed=10, sig_attempted=20, td_landed=1, td_attempted=2,
           knockdowns=0, control_time_seconds=60):
    return {
        "sig_strikes_landed": sig_landed,
        "sig_strikes_attempted": sig_attempted,
        "takedowns_landed": td_landed,
        "takedowns_attempted": td_attempted,
        "submissions_attempted": 0,
        "knockdowns": knockdowns,
        "control_time_seconds": control_time_seconds,
    }


def _make_fight(fight_id, event_date, f1, f2, winner=None, result_type="win",
                f1_stats=None, f2_stats=None, scheduled_rounds=3):
    return {
        "fight_id": fight_id,
        "event_id": "e0",
        "event_date": event_date,
        "fighter_1_id": f1,
        "fighter_2_id": f2,
        "winner_fighter_id": winner,
        "result_type": result_type,
        "weight_class": "lightweight",
        "is_title_fight": False,
        "is_interim_title": False,
        "scheduled_rounds": scheduled_rounds,
        "finish_method": "decision",
        "finish_detail": None,
        "finish_round": scheduled_rounds,
        "finish_time_seconds": 300,
        "referee": None,
        "source_url": "http://example.com",
        "_f1_stats": f1_stats,
        "_f2_stats": f2_stats,
    }


def _fighter_row(fighter_id, height=180.0, reach=185.0, weight=155.0,
                 stance="Orthodox", dob=date(1990, 1, 1)):
    return {
        "fighter_id": fighter_id,
        "full_name": "Test",
        "height_cm": height,
        "reach_cm": reach,
        "weight_lbs": weight,
        "stance": stance,
        "dob": dob,
        "source_url": "http://example.com",
    }


def _build_universe(fights):
    """Build WarehouseData + indexes from augmented fight dicts."""
    data = WarehouseData()
    clean = []
    for f in fights:
        f1s = f.pop("_f1_stats", None)
        f2s = f.pop("_f2_stats", None)
        clean.append(f)
        if f1s:
            data.stats_by_fight_fighter[(f["fight_id"], f["fighter_1_id"])] = f1s
            data.stats_by_fight.setdefault(f["fight_id"], []).append(f1s)
        if f2s:
            data.stats_by_fight_fighter[(f["fight_id"], f["fighter_2_id"])] = f2s
            data.stats_by_fight.setdefault(f["fight_id"], []).append(f2s)
    data.fights = clean
    data.fight_by_id = {f["fight_id"]: f for f in clean}
    index = build_fighter_index(data)
    elos = compute_all_elos(clean)
    return data, index, elos


# ── Tests: build_fighter_snapshot ─────────────────────────────────────────────

class TestSnapshot:
    """FA has 2 prior fights, then we build a snapshot for a 3rd fight."""

    def setup_method(self):
        fights = [
            _make_fight("f1", date(2020, 1, 1), FA, FB, winner=FA,
                        f1_stats=_stats(sig_landed=20, sig_attempted=40,
                                         td_landed=2, td_attempted=4,
                                         knockdowns=1, control_time_seconds=120),
                        f2_stats=_stats(sig_landed=10, sig_attempted=30,
                                         td_landed=0, td_attempted=2,
                                         control_time_seconds=30)),
            _make_fight("f2", date(2020, 6, 1), FA, FB, winner=FA,
                        f1_stats=_stats(sig_landed=15, sig_attempted=25,
                                         td_landed=1, td_attempted=3,
                                         knockdowns=0, control_time_seconds=90),
                        f2_stats=_stats(sig_landed=12, sig_attempted=28,
                                         td_landed=1, td_attempted=2,
                                         control_time_seconds=45)),
        ]
        self.data, self.index, self.elos = _build_universe(fights)
        self.fighter_row = _fighter_row(FA, height=180.0, reach=185.0,
                                        dob=date(1990, 1, 1))
        self.cutoff = date(2021, 1, 1)
        self.history = get_history(self.index, FA, self.cutoff)
        self.snap = build_fighter_snapshot(
            self.fighter_row, self.history, self.cutoff,
            self.elos, self.index,
            fighter_id=FA, fight_id="f_target",
        )

    def test_has_career_features(self):
        assert self.snap["total_fights"] == 2
        assert self.snap["wins"] == 2

    def test_has_rolling_features(self):
        assert "last1_wins" in self.snap
        assert "last3_wins" in self.snap
        assert self.snap["last1_wins"] == 1  # last 1 fight: won

    def test_has_decay_features(self):
        assert "decay_win_rate" in self.snap
        assert self.snap["decay_win_rate"] is not None

    def test_has_physical_features(self):
        assert self.snap["height_cm"] == pytest.approx(180.0)
        assert self.snap["age_at_fight"] is not None

    def test_has_elo_features(self):
        assert "pre_fight_elo" in self.snap

    def test_has_opponent_features(self):
        assert "avg_opponent_elo" in self.snap

    def test_metadata(self):
        assert self.snap["fighter_id"] == FA
        assert self.snap["fight_id"] == "f_target"
        assert self.snap["as_of_date"] == self.cutoff
        assert self.snap["feature_version"] == 1

    def test_is_flat_dict(self):
        # No nested dicts — all values are scalars or None
        for k, v in self.snap.items():
            assert not isinstance(v, dict), f"{k} is a dict, expected flat"


class TestSnapshotDebut:
    """Debut fighter: no prior fights."""

    def setup_method(self):
        self.data, self.index, self.elos = _build_universe([])
        self.fighter_row = _fighter_row(FA)
        self.snap = build_fighter_snapshot(
            self.fighter_row, [], date(2021, 1, 1),
            {}, self.index,
            fighter_id=FA, fight_id="f1",
        )

    def test_debut_career_zero(self):
        assert self.snap["total_fights"] == 0
        assert self.snap["wins"] == 0

    def test_debut_elo_default(self):
        assert self.snap["pre_fight_elo"] == 1500.0

    def test_debut_is_debut(self):
        assert self.snap["is_debut"] is True


# ── Tests: build_bout_features ────────────────────────────────────────────────

class TestBoutFeatures:
    """Two snapshots merged into a bout row."""

    def setup_method(self):
        self.snap_a = {
            "age_at_fight": 30.0,
            "height_cm": 180.0,
            "reach_cm": 185.0,
            "pre_fight_elo": 1550.0,
            "win_rate": 0.8,
            "career_sig_strikes_landed_per_min": 5.0,
            "career_takedown_accuracy": 0.4,
            "career_control_time_per_fight": 120.0,
            "ufc_fight_count": 10,
            "career_sig_strike_accuracy": 0.5,
            "stance": "orthodox",
        }
        self.snap_b = {
            "age_at_fight": 25.0,
            "height_cm": 175.0,
            "reach_cm": 180.0,
            "pre_fight_elo": 1450.0,
            "win_rate": 0.6,
            "career_sig_strikes_landed_per_min": 4.0,
            "career_takedown_accuracy": 0.3,
            "career_control_time_per_fight": 80.0,
            "ufc_fight_count": 5,
            "career_sig_strike_accuracy": 0.45,
            "stance": "southpaw",
        }
        self.fight = {
            "fight_id": "bout1",
            "event_date": date(2023, 1, 1),
            "fighter_1_id": FA,
            "fighter_2_id": FB,
            "winner_fighter_id": FA,
            "result_type": "win",
            "weight_class": "lightweight",
            "is_title_fight": True,
            "scheduled_rounds": 5,
        }
        self.row = build_bout_features(self.fight, self.snap_a, self.snap_b)

    # ── Metadata ──────────────────────────────────────────────────────────
    def test_metadata(self):
        assert self.row["fight_id"] == "bout1"
        assert self.row["event_date"] == date(2023, 1, 1)
        assert self.row["weight_class"] == "lightweight"
        assert self.row["is_title_fight"] is True
        assert self.row["scheduled_rounds"] == 5
        assert self.row["fighter_1_id"] == FA
        assert self.row["fighter_2_id"] == FB

    # ── Difference features (A − B) ──────────────────────────────────────
    def test_age_diff(self):
        assert self.row["age_diff"] == pytest.approx(5.0)  # 30 - 25

    def test_height_diff(self):
        assert self.row["height_diff"] == pytest.approx(5.0)  # 180 - 175

    def test_reach_diff(self):
        assert self.row["reach_diff"] == pytest.approx(5.0)  # 185 - 180

    def test_elo_diff(self):
        assert self.row["elo_diff"] == pytest.approx(100.0)  # 1550 - 1450

    def test_win_rate_diff(self):
        assert self.row["win_rate_diff"] == pytest.approx(0.2)  # 0.8 - 0.6

    def test_experience_diff(self):
        assert self.row["experience_diff"] == 5  # 10 - 5

    def test_sig_strike_rate_diff(self):
        assert self.row["career_sig_strike_rate_diff"] == pytest.approx(1.0)

    def test_control_time_diff(self):
        assert self.row["career_control_time_diff"] == pytest.approx(40.0)

    # ── Ratio features ────────────────────────────────────────────────────
    def test_experience_ratio(self):
        assert self.row["experience_ratio"] == pytest.approx(10 / 15)

    def test_win_rate_ratio(self):
        assert self.row["win_rate_ratio"] == pytest.approx(0.8 / 1.4)

    def test_sig_accuracy_ratio(self):
        assert self.row["sig_strike_accuracy_ratio"] == pytest.approx(0.5 / 0.95)

    def test_takedown_accuracy_ratio(self):
        assert self.row["takedown_accuracy_ratio"] == pytest.approx(0.4 / 0.7)

    # ── Matchup flags ─────────────────────────────────────────────────────
    def test_stance_matchup(self):
        assert self.row["stance_matchup"] == "orthodox_vs_southpaw"

    def test_reach_advantage(self):
        assert self.row["is_reach_advantage_a"] is True

    def test_experience_advantage(self):
        assert self.row["is_experience_advantage_a"] is True

    # ── Label ─────────────────────────────────────────────────────────────
    def test_label_fighter_1_wins(self):
        assert self.row["label"] == 1

    def test_feature_version(self):
        assert self.row["feature_version"] == 1


class TestBoutLabels:
    FIGHT_BASE = {
        "fight_id": "b1",
        "event_date": date(2023, 1, 1),
        "fighter_1_id": FA,
        "fighter_2_id": FB,
        "weight_class": "lightweight",
        "is_title_fight": False,
        "scheduled_rounds": 3,
    }

    def _row(self, winner, result_type):
        fight = {**self.FIGHT_BASE, "winner_fighter_id": winner,
                 "result_type": result_type}
        return build_bout_features(fight, {}, {})

    def test_label_fighter_2_wins(self):
        row = self._row(FB, "win")
        assert row["label"] == 0

    def test_label_draw_is_none(self):
        row = self._row(None, "draw")
        assert row["label"] is None

    def test_label_nc_is_none(self):
        row = self._row(None, "nc")
        assert row["label"] is None


class TestBoutNullHandling:
    """Difference/ratio features with None inputs."""

    def test_diff_with_none(self):
        snap_a = {"age_at_fight": 30.0, "ufc_fight_count": 5}
        snap_b = {"age_at_fight": None, "ufc_fight_count": 3}
        fight = {
            "fight_id": "b1", "event_date": date(2023, 1, 1),
            "fighter_1_id": FA, "fighter_2_id": FB,
            "winner_fighter_id": FA, "result_type": "win",
            "weight_class": "lw", "is_title_fight": False,
            "scheduled_rounds": 3,
        }
        row = build_bout_features(fight, snap_a, snap_b)
        assert row["age_diff"] is None
        assert row["experience_diff"] == 2

    def test_ratio_both_zero(self):
        snap_a = {"ufc_fight_count": 0}
        snap_b = {"ufc_fight_count": 0}
        fight = {
            "fight_id": "b1", "event_date": date(2023, 1, 1),
            "fighter_1_id": FA, "fighter_2_id": FB,
            "winner_fighter_id": FA, "result_type": "win",
            "weight_class": "lw", "is_title_fight": False,
            "scheduled_rounds": 3,
        }
        row = build_bout_features(fight, snap_a, snap_b)
        assert row["experience_ratio"] is None
