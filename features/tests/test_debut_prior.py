"""Unit tests for debut_prior module.

Tests both the prior computation and the feature application logic.

Usage:
    python -m pytest features/tests/test_debut_prior.py -v
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from features.debut_prior import compute_debut_priors, apply_debut_features


@pytest.fixture
def sample_train_df():
    """Minimal training DataFrame for testing."""
    rows = []
    # 30 both-debuting bouts in lightweight
    for i in range(30):
        rows.append({
            "fight_id": f"debut-lw-{i}",
            "fighter_1_id": f"f1-{i}",
            "fighter_2_id": f"f2-{i}",
            "event_date": pd.Timestamp("2020-01-01"),
            "weight_class": "lightweight",
            "label": 1 if i < 21 else 0,
            "both_debuting": 1,
            "diff_height_cm": float(i - 15),
            "diff_reach_cm": float(i - 15) * 1.5,
            "diff_elo": 0.0,
        })
    # 5 both-debuting in flyweight (too few for bucket)
    for i in range(5):
        rows.append({
            "fight_id": f"debut-fly-{i}",
            "fighter_1_id": f"f1-fly-{i}",
            "fighter_2_id": f"f2-fly-{i}",
            "event_date": pd.Timestamp("2020-01-01"),
            "weight_class": "flyweight",
            "label": 1 if i < 3 else 0,
            "both_debuting": 1,
            "diff_height_cm": float(i),
            "diff_reach_cm": float(i),
            "diff_elo": 0.0,
        })
    # 100 experienced bouts in lightweight (for height/reach stats)
    rng = np.random.RandomState(42)
    for i in range(100):
        rows.append({
            "fight_id": f"exp-lw-{i}",
            "fighter_1_id": f"f1-exp-{i}",
            "fighter_2_id": f"f2-exp-{i}",
            "event_date": pd.Timestamp("2020-06-01"),
            "weight_class": "lightweight",
            "label": int(rng.random() > 0.4),
            "both_debuting": 0,
            "diff_height_cm": rng.normal(0, 5),
            "diff_reach_cm": rng.normal(0, 7),
            "diff_elo": rng.normal(0, 100),
        })

    return pd.DataFrame(rows)


class TestComputeDebutPriors:

    def test_base_prior_is_half(self, sample_train_df):
        priors = compute_debut_priors(sample_train_df)
        assert priors["base_prior"] == 0.5

    def test_training_win_rate_recorded(self, sample_train_df):
        priors = compute_debut_priors(sample_train_df)
        # 24 wins out of 35 both-debuting
        expected = 24 / 35
        assert abs(priors["training_debut_win_rate"] - expected) < 1e-6

    def test_height_stats_populated(self, sample_train_df):
        priors = compute_debut_priors(sample_train_df)
        assert "lightweight" in priors["height_stats"]
        assert priors["height_stats"]["lightweight"]["std"] > 0

    def test_global_fallback_std(self, sample_train_df):
        priors = compute_debut_priors(sample_train_df)
        assert priors["global_height_std"] > 0
        assert priors["global_reach_std"] > 0


class TestApplyDebutFeatures:

    def test_debut_bouts_get_prior(self, sample_train_df):
        priors = compute_debut_priors(sample_train_df)
        df = apply_debut_features(sample_train_df.copy(), priors)
        debut_mask = df["both_debuting"] == 1
        assert df.loc[debut_mask, "debut_prior_win_prob_f1"].notna().all()

    def test_non_debut_bouts_are_nan(self, sample_train_df):
        priors = compute_debut_priors(sample_train_df)
        df = apply_debut_features(sample_train_df.copy(), priors)
        non_debut = df["both_debuting"] == 0
        assert df.loc[non_debut, "debut_prior_win_prob_f1"].isna().all()

    def test_prior_is_half(self, sample_train_df):
        priors = compute_debut_priors(sample_train_df)
        df = apply_debut_features(sample_train_df.copy(), priors)
        debut_probs = df["debut_prior_win_prob_f1"].dropna()
        assert (debut_probs == 0.5).all()

    def test_prior_within_bounds(self, sample_train_df):
        priors = compute_debut_priors(sample_train_df)
        df = apply_debut_features(sample_train_df.copy(), priors)
        debut_probs = df["debut_prior_win_prob_f1"].dropna()
        assert (debut_probs >= 0.3).all()
        assert (debut_probs <= 0.7).all()

    def test_height_adv_populated_for_debut(self, sample_train_df):
        priors = compute_debut_priors(sample_train_df)
        df = apply_debut_features(sample_train_df.copy(), priors)
        debut_mask = df["both_debuting"] == 1
        has_height = df["diff_height_cm"].notna()
        assert df.loc[debut_mask & has_height, "debut_height_adv"].notna().all()

    def test_reach_adv_populated_for_debut(self, sample_train_df):
        priors = compute_debut_priors(sample_train_df)
        df = apply_debut_features(sample_train_df.copy(), priors)
        debut_mask = df["both_debuting"] == 1
        has_reach = df["diff_reach_cm"].notna()
        assert df.loc[debut_mask & has_reach, "debut_reach_adv"].notna().all()

    def test_three_columns_added(self, sample_train_df):
        priors = compute_debut_priors(sample_train_df)
        df = apply_debut_features(sample_train_df.copy(), priors)
        assert "debut_prior_win_prob_f1" in df.columns
        assert "debut_reach_adv" in df.columns
        assert "debut_height_adv" in df.columns

    def test_height_adv_is_standardized(self, sample_train_df):
        """Height advantage should be z-scored: mean near 0, std near 1 for
        a weight class with enough data."""
        priors = compute_debut_priors(sample_train_df)
        df = apply_debut_features(sample_train_df.copy(), priors)
        lw_debut = (df["both_debuting"] == 1) & (df["weight_class"] == "lightweight")
        h_adv = df.loc[lw_debut, "debut_height_adv"].dropna()
        # Should be normalized by weight-class std (~5.0 for the LW experienced bouts)
        assert h_adv.abs().max() < 10, "Height advantage seems unnormalized"
