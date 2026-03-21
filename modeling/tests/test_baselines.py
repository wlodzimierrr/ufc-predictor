"""Tests for modeling/baselines.py — naive baseline predictions."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from modeling.baselines import coin_flip_baseline, favorite_baseline, elo_baseline


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_df(n: int = 100, seed: int = 42) -> pd.DataFrame:
    """Minimal DataFrame that looks like bout_features output."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "diff_career_win_rate": rng.uniform(-0.5, 0.5, n),
        "diff_elo": rng.normal(0, 150, n),
        "label": rng.integers(0, 2, n),
    })


def _make_df_with_nans(n: int = 100) -> pd.DataFrame:
    df = _make_df(n)
    df.loc[0:9, "diff_career_win_rate"] = np.nan
    df.loc[0:9, "diff_elo"] = np.nan
    return df


# ── coin_flip_baseline ───────────────────────────────────────────────────────

class TestCoinFlip:
    def test_returns_array(self):
        result = coin_flip_baseline(_make_df())
        assert isinstance(result, np.ndarray)

    def test_correct_length(self):
        df = _make_df(50)
        assert len(coin_flip_baseline(df)) == 50

    def test_all_values_are_half(self):
        result = coin_flip_baseline(_make_df())
        assert np.all(result == 0.5)

    def test_empty_dataframe(self):
        df = _make_df(0)
        result = coin_flip_baseline(df)
        assert len(result) == 0


# ── favorite_baseline ────────────────────────────────────────────────────────

class TestFavoriteHard:
    def test_returns_array(self):
        result = favorite_baseline(_make_df())
        assert isinstance(result, np.ndarray)

    def test_correct_length(self):
        df = _make_df(50)
        assert len(favorite_baseline(df)) == 50

    def test_positive_diff_predicts_one(self):
        df = pd.DataFrame({"diff_career_win_rate": [0.2, 0.5, 0.01]})
        result = favorite_baseline(df)
        np.testing.assert_array_equal(result, [1.0, 1.0, 1.0])

    def test_negative_diff_predicts_zero(self):
        df = pd.DataFrame({"diff_career_win_rate": [-0.1, -0.5, -0.01]})
        result = favorite_baseline(df)
        np.testing.assert_array_equal(result, [0.0, 0.0, 0.0])

    def test_zero_diff_predicts_half(self):
        df = pd.DataFrame({"diff_career_win_rate": [0.0]})
        result = favorite_baseline(df)
        assert result[0] == 0.5

    def test_values_in_bounds(self):
        result = favorite_baseline(_make_df())
        assert np.all((result >= 0.0) & (result <= 1.0))


class TestFavoriteSoft:
    def test_returns_array(self):
        result = favorite_baseline(_make_df(), soft=True)
        assert isinstance(result, np.ndarray)

    def test_values_in_range(self):
        result = favorite_baseline(_make_df(), soft=True)
        assert np.all((result >= 0.3) & (result <= 0.7))

    def test_zero_diff_maps_to_half(self):
        df = pd.DataFrame({"diff_career_win_rate": [0.0]})
        result = favorite_baseline(df, soft=True)
        assert result[0] == pytest.approx(0.5)

    def test_positive_diff_above_half(self):
        df = pd.DataFrame({"diff_career_win_rate": [0.3]})
        result = favorite_baseline(df, soft=True)
        assert result[0] > 0.5

    def test_negative_diff_below_half(self):
        df = pd.DataFrame({"diff_career_win_rate": [-0.3]})
        result = favorite_baseline(df, soft=True)
        assert result[0] < 0.5

    def test_nan_maps_to_half(self):
        df = pd.DataFrame({"diff_career_win_rate": [np.nan]})
        result = favorite_baseline(df, soft=True)
        assert result[0] == pytest.approx(0.5)

    def test_extreme_values_clamped(self):
        df = pd.DataFrame({"diff_career_win_rate": [5.0, -5.0]})
        result = favorite_baseline(df, soft=True)
        assert result[0] == pytest.approx(0.7)
        assert result[1] == pytest.approx(0.3)


# ── elo_baseline ─────────────────────────────────────────────────────────────

class TestEloBaseline:
    def test_returns_array(self):
        result = elo_baseline(_make_df())
        assert isinstance(result, np.ndarray)

    def test_correct_length(self):
        df = _make_df(50)
        assert len(elo_baseline(df)) == 50

    def test_zero_diff_gives_half(self):
        df = pd.DataFrame({"diff_elo": [0.0]})
        result = elo_baseline(df)
        assert result[0] == pytest.approx(0.5)

    def test_positive_diff_above_half(self):
        df = pd.DataFrame({"diff_elo": [200.0]})
        result = elo_baseline(df)
        assert result[0] > 0.5

    def test_negative_diff_below_half(self):
        df = pd.DataFrame({"diff_elo": [-200.0]})
        result = elo_baseline(df)
        assert result[0] < 0.5

    def test_symmetric(self):
        """P(+d) + P(-d) = 1.0 by the logistic formula."""
        df_pos = pd.DataFrame({"diff_elo": [150.0]})
        df_neg = pd.DataFrame({"diff_elo": [-150.0]})
        p_pos = elo_baseline(df_pos)[0]
        p_neg = elo_baseline(df_neg)[0]
        assert p_pos + p_neg == pytest.approx(1.0)

    def test_400_diff_gives_expected_value(self):
        """diff_elo=400 → P = 1/(1+10^-1) ≈ 0.909."""
        df = pd.DataFrame({"diff_elo": [400.0]})
        result = elo_baseline(df)
        assert result[0] == pytest.approx(10.0 / 11.0, abs=0.001)

    def test_nan_gives_half(self):
        df = pd.DataFrame({"diff_elo": [np.nan]})
        result = elo_baseline(df)
        assert result[0] == pytest.approx(0.5)

    def test_values_in_bounds(self):
        result = elo_baseline(_make_df())
        assert np.all((result >= 0.0) & (result <= 1.0))

    def test_large_diff_near_one(self):
        df = pd.DataFrame({"diff_elo": [1000.0]})
        result = elo_baseline(df)
        assert result[0] > 0.99
