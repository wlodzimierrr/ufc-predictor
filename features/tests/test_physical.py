"""Unit tests for features.physical — physical, demographic, and activity features."""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from features.history import FightHistory
from features.physical import compute_physical_features


# ── Helpers ──────────────────────────────────────────────────────────────────

def _fighter(
    height_cm=180.0,
    reach_cm=185.0,
    weight_lbs=155.0,
    stance="Orthodox",
    dob=date(1990, 1, 1),
) -> dict:
    return {
        "fighter_id": "aaa",
        "full_name": "Test Fighter",
        "height_cm": height_cm,
        "reach_cm": reach_cm,
        "weight_lbs": weight_lbs,
        "stance": stance,
        "dob": dob,
        "source_url": "http://example.com",
    }


def _h(
    event_date: date,
    scheduled_rounds: int = 3,
    is_title_fight: bool = False,
) -> FightHistory:
    return FightHistory(
        fight_id="f0",
        event_id="e0",
        event_date=event_date,
        result_type="win",
        won=True,
        finish_method="decision",
        finish_round=scheduled_rounds,
        finish_time_seconds=300,
        scheduled_rounds=scheduled_rounds,
        weight_class="lightweight",
        is_title_fight=is_title_fight,
        fighter_stats=None,
        opponent_stats=None,
        opponent_id="opp",
    )


CUTOFF = date(2023, 6, 1)


# ── Tests: full profile, multi-fight history ──────────────────────────────────

class TestFullProfile:
    def setup_method(self):
        self.fighter = _fighter(
            height_cm=180.0, reach_cm=185.0, weight_lbs=155.0,
            stance="Orthodox", dob=date(1990, 6, 1),
        )
        self.history = [
            _h(date(2018, 1, 1)),
            _h(date(2019, 6, 1), scheduled_rounds=5, is_title_fight=True),
            _h(date(2021, 1, 1)),
            _h(date(2022, 6, 1)),
        ]
        self.f = compute_physical_features(self.fighter, self.history, CUTOFF)

    def test_physical_passthrough(self):
        assert self.f["height_cm"] == pytest.approx(180.0)
        assert self.f["reach_cm"] == pytest.approx(185.0)
        assert self.f["weight_lbs"] == pytest.approx(155.0)

    def test_reach_to_height(self):
        assert self.f["reach_to_height_ratio"] == pytest.approx(185.0 / 180.0)

    def test_age_at_fight(self):
        # DOB = 1990-06-01, cutoff = 2023-06-01 → exactly 33 years
        assert self.f["age_at_fight"] == pytest.approx(33.0, abs=0.01)

    def test_age_squared(self):
        assert self.f["age_squared"] == pytest.approx(self.f["age_at_fight"] ** 2)

    def test_stance(self):
        assert self.f["stance"] == "orthodox"

    def test_days_since_last_fight(self):
        # last fight = 2022-06-01, cutoff = 2023-06-01 → 365 days
        assert self.f["days_since_last_fight"] == 365

    def test_is_long_layoff_false(self):
        # 365 days is not > 365
        assert self.f["is_long_layoff"] is False

    def test_fights_per_year(self):
        # 4 fights, span = 2018-01-01 to 2023-06-01
        span_years = (CUTOFF - date(2018, 1, 1)).days / 365.25
        assert self.f["fights_per_year"] == pytest.approx(4 / span_years, rel=1e-4)

    def test_is_debut_false(self):
        assert self.f["is_debut"] is False

    def test_ufc_fight_count(self):
        assert self.f["ufc_fight_count"] == 4

    def test_five_round_experience(self):
        assert self.f["five_round_experience"] == 1

    def test_title_fight_experience(self):
        assert self.f["title_fight_experience"] == 1

    def test_no_missing_flags(self):
        assert self.f["height_missing"] is False
        assert self.f["reach_missing"] is False
        assert self.f["dob_missing"] is False


# ── Tests: all-null profile ───────────────────────────────────────────────────

class TestNullProfile:
    def setup_method(self):
        self.fighter = _fighter(
            height_cm=None, reach_cm=None, weight_lbs=None,
            stance=None, dob=None,
        )
        self.history = [_h(date(2020, 1, 1)), _h(date(2021, 1, 1))]
        self.f = compute_physical_features(self.fighter, self.history, CUTOFF)

    def test_physical_are_none(self):
        assert self.f["height_cm"] is None
        assert self.f["reach_cm"] is None
        assert self.f["weight_lbs"] is None
        assert self.f["reach_to_height_ratio"] is None

    def test_demographic_are_none(self):
        assert self.f["age_at_fight"] is None
        assert self.f["age_squared"] is None

    def test_stance_is_none(self):
        assert self.f["stance"] is None

    def test_missing_flags_all_true(self):
        assert self.f["height_missing"] is True
        assert self.f["reach_missing"] is True
        assert self.f["dob_missing"] is True

    def test_activity_still_computed(self):
        # Activity uses history, not the profile
        assert self.f["days_since_last_fight"] is not None
        assert self.f["ufc_fight_count"] == 2


# ── Tests: debut fighter ──────────────────────────────────────────────────────

class TestDebut:
    def setup_method(self):
        self.fighter = _fighter(dob=date(1995, 3, 15))
        self.f = compute_physical_features(self.fighter, [], CUTOFF)

    def test_is_debut(self):
        assert self.f["is_debut"] is True

    def test_ufc_fight_count_zero(self):
        assert self.f["ufc_fight_count"] == 0

    def test_days_since_last_fight_none(self):
        assert self.f["days_since_last_fight"] is None

    def test_is_long_layoff_false(self):
        assert self.f["is_long_layoff"] is False

    def test_fights_per_year_none(self):
        assert self.f["fights_per_year"] is None

    def test_experience_counts_zero(self):
        assert self.f["five_round_experience"] == 0
        assert self.f["title_fight_experience"] == 0

    def test_age_computed_from_profile(self):
        # DOB = 1995-03-15, cutoff = 2023-06-01 → ~28.2 years
        assert self.f["age_at_fight"] == pytest.approx(28.2, abs=0.1)


# ── Tests: long layoff ────────────────────────────────────────────────────────

class TestLongLayoff:
    def test_exactly_366_days_is_long(self):
        from datetime import timedelta
        last = CUTOFF - timedelta(days=366)
        f = compute_physical_features(_fighter(), [_h(last)], CUTOFF)
        assert f["is_long_layoff"] is True

    def test_exactly_365_days_not_long(self):
        from datetime import timedelta
        last = CUTOFF - timedelta(days=365)
        f = compute_physical_features(_fighter(), [_h(last)], CUTOFF)
        assert f["is_long_layoff"] is False


# ── Tests: stance normalisation ───────────────────────────────────────────────

class TestStance:
    @pytest.mark.parametrize("raw,expected", [
        ("Orthodox", "orthodox"),
        ("Southpaw", "southpaw"),
        ("Switch", "switch"),
        ("  Orthodox  ", "orthodox"),
    ])
    def test_stance_lowercased(self, raw, expected):
        f = compute_physical_features(_fighter(stance=raw), [], CUTOFF)
        assert f["stance"] == expected
