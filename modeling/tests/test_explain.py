"""Tests for modeling/explain.py — feature importance and SHAP analysis."""

from __future__ import annotations

import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from modeling.explain import feature_importance_plot, shap_summary, shap_single_fight


# ── Fixtures ─────────────────────────────────────────────────────────────────

FEATURE_NAMES = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e"]


@pytest.fixture(scope="module")
def trained_model():
    """Train a small LightGBM model for testing."""
    rng = np.random.default_rng(42)
    n = 500
    X = rng.normal(0, 1, (n, len(FEATURE_NAMES)))
    # Make feat_a predictive
    y = (X[:, 0] + rng.normal(0, 0.5, n) > 0).astype(int)

    model = lgb.LGBMClassifier(
        n_estimators=50, num_leaves=8, learning_rate=0.1,
        verbosity=-1, random_state=42,
    )
    model.fit(X, y)
    return model


@pytest.fixture(scope="module")
def test_data():
    rng = np.random.default_rng(99)
    X = rng.normal(0, 1, (100, len(FEATURE_NAMES)))
    return X


# ── feature_importance_plot ──────────────────────────────────────────────────

class TestFeatureImportancePlot:
    def test_returns_dataframe(self, trained_model):
        df = feature_importance_plot(trained_model, FEATURE_NAMES)
        assert isinstance(df, pd.DataFrame)

    def test_has_correct_columns(self, trained_model):
        df = feature_importance_plot(trained_model, FEATURE_NAMES)
        assert set(df.columns) == {"feature", "importance"}

    def test_all_features_present(self, trained_model):
        df = feature_importance_plot(trained_model, FEATURE_NAMES)
        assert set(df["feature"]) == set(FEATURE_NAMES)

    def test_sorted_descending(self, trained_model):
        df = feature_importance_plot(trained_model, FEATURE_NAMES)
        imps = df["importance"].values
        assert all(imps[i] >= imps[i + 1] for i in range(len(imps) - 1))

    def test_saves_plot(self, trained_model, tmp_path):
        save_path = tmp_path / "importance.png"
        feature_importance_plot(trained_model, FEATURE_NAMES, save_path=str(save_path))
        assert save_path.exists()
        assert save_path.stat().st_size > 0


# ── shap_summary ─────────────────────────────────────────────────────────────

class TestShapSummary:
    def test_returns_array(self, trained_model, test_data):
        sv = shap_summary(trained_model, test_data, FEATURE_NAMES)
        assert isinstance(sv, np.ndarray)

    def test_correct_shape(self, trained_model, test_data):
        sv = shap_summary(trained_model, test_data, FEATURE_NAMES)
        assert sv.shape == (100, len(FEATURE_NAMES))

    def test_saves_plot(self, trained_model, test_data, tmp_path):
        save_path = tmp_path / "shap_summary.png"
        shap_summary(trained_model, test_data, FEATURE_NAMES, save_path=str(save_path))
        assert save_path.exists()
        assert save_path.stat().st_size > 0

    def test_feat_a_most_important(self, trained_model, test_data):
        """feat_a is the only predictive feature — should have highest mean |SHAP|."""
        sv = shap_summary(trained_model, test_data, FEATURE_NAMES)
        mean_abs = np.abs(sv).mean(axis=0)
        assert np.argmax(mean_abs) == 0  # feat_a is index 0


# ── shap_single_fight ────────────────────────────────────────────────────────

class TestShapSingleFight:
    def test_returns_dict(self, trained_model, test_data):
        result = shap_single_fight(trained_model, test_data[0], FEATURE_NAMES)
        assert isinstance(result, dict)

    def test_has_required_keys(self, trained_model, test_data):
        result = shap_single_fight(trained_model, test_data[0], FEATURE_NAMES)
        assert "shap_values" in result
        assert "base_value" in result
        assert "prediction" in result

    def test_shap_values_all_features(self, trained_model, test_data):
        result = shap_single_fight(trained_model, test_data[0], FEATURE_NAMES)
        assert set(result["shap_values"].keys()) == set(FEATURE_NAMES)

    def test_prediction_in_bounds(self, trained_model, test_data):
        result = shap_single_fight(trained_model, test_data[0], FEATURE_NAMES)
        assert 0.0 <= result["prediction"] <= 1.0

    def test_additivity(self, trained_model, test_data):
        """SHAP values + base_value should approximately equal the log-odds of the prediction."""
        result = shap_single_fight(trained_model, test_data[0], FEATURE_NAMES)
        shap_sum = sum(result["shap_values"].values()) + result["base_value"]
        # Convert prediction to log-odds
        pred = result["prediction"]
        pred_clipped = np.clip(pred, 1e-8, 1 - 1e-8)
        log_odds_pred = np.log(pred_clipped / (1 - pred_clipped))
        # SHAP values in log-odds space should sum to model output
        assert shap_sum == pytest.approx(log_odds_pred, abs=0.1)

    def test_accepts_dataframe_row(self, trained_model, test_data):
        df = pd.DataFrame(test_data, columns=FEATURE_NAMES)
        result = shap_single_fight(trained_model, df.iloc[[0]], FEATURE_NAMES)
        assert 0.0 <= result["prediction"] <= 1.0
