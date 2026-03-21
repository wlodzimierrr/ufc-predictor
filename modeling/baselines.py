"""Naive baselines for UFC fight prediction.

These require no training and set the floor for model performance.
All functions take a DataFrame (with bout_features columns) and return
a numpy array of P(fighter_1 wins).

Usage:
    from modeling.baselines import coin_flip_baseline, favorite_baseline, elo_baseline
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def coin_flip_baseline(df: pd.DataFrame) -> np.ndarray:
    """Return 0.5 for every fight — the calibration reference point."""
    return np.full(len(df), 0.5)


def favorite_baseline(
    df: pd.DataFrame, *, soft: bool = False
) -> np.ndarray:
    """Predict the fighter with the higher career win rate wins.

    Args:
        df: DataFrame with ``diff_career_win_rate`` column.
        soft: If False (default), returns hard 1.0/0.0 predictions.
              If True, maps the raw win-rate difference to [0.3, 0.7]
              via a linear rescale (ties → 0.5).

    Returns:
        Array of P(fighter_1 wins).
    """
    diff = df["diff_career_win_rate"].values.astype(float)

    if not soft:
        # diff > 0 → fighter_1 favoured → 1.0; diff < 0 → 0.0; tie → 0.5
        probs = np.where(diff > 0, 1.0, np.where(diff < 0, 0.0, 0.5))
    else:
        # Linear map: clamp diff to [-1, 1], rescale to [0.3, 0.7]
        clamped = np.clip(diff, -1.0, 1.0)
        # NaN handling: where diff is NaN, predict 0.5
        probs = np.where(np.isnan(clamped), 0.5, 0.3 + 0.4 * (clamped + 1) / 2)

    return probs


def elo_baseline(df: pd.DataFrame) -> np.ndarray:
    """Convert diff_elo to win probability using the standard logistic formula.

    P(fighter_1 wins) = 1 / (1 + 10^(-diff_elo / 400))

    Where diff_elo is NaN (both debuting), returns 0.5.
    """
    diff_elo = df["diff_elo"].values.astype(float)
    probs = 1.0 / (1.0 + np.power(10.0, -diff_elo / 400.0))
    # NaN → 0.5 (no Elo information)
    probs = np.where(np.isnan(probs), 0.5, probs)
    return probs
