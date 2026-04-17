"""Debut fighter prior module.

Computes informative priors for bouts involving debuting fighters, where
diff features are NULL. The prior is fit on training data only and provides
three features:
  - debut_prior_win_prob_f1: base-rate probability fighter_1 wins
  - debut_reach_adv: reach difference standardized by weight-class std
  - debut_height_adv: height difference standardized by weight-class std

Usage:
    priors = compute_debut_priors(train_df)
    df = apply_debut_features(df, priors)
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


_MIN_BUCKET_SIZE = 20  # fall back to global prior if bucket has fewer


def compute_debut_priors(train_df: pd.DataFrame) -> dict:
    """Compute debut priors from training data.

    The base win probability is fixed at 0.5 (no positional bias). Historical
    data shows fighter_1 positional bias (~76% win rate for both-debuting bouts
    pre-2022) does NOT hold in recent data (~36% in 2024+), making era-dependent
    win-rate priors counterproductive. Instead, this module provides:
      - A neutral base rate (0.5)
      - Physical standardization stats so height/reach differences can be
        expressed as z-scores relative to weight-class norms

    Args:
        train_df: Training DataFrame from load_bout_data(). Must contain
            columns: label, both_debuting, weight_class, diff_height_cm,
            diff_reach_cm.

    Returns:
        dict with keys:
            base_prior: float — 0.5 (no positional bias)
            height_stats: dict[str, dict] — per-weight-class {mean, std} for diff_height_cm
            reach_stats: dict[str, dict] — per-weight-class {mean, std} for diff_reach_cm
            global_height_std: float — fallback std for height
            global_reach_std: float — fallback std for reach
            training_debut_win_rate: float — informational only, NOT used as prior
    """
    both = train_df[train_df["both_debuting"] == 1].copy()

    # Informational only: document the training distribution shift risk
    training_debut_win_rate = float(both["label"].mean()) if len(both) > 0 else 0.5

    # Physical stats per weight class (from ALL training data, not just debuts,
    # so we have robust denominators for standardization)
    height_stats: dict[str, dict] = {}
    reach_stats: dict[str, dict] = {}

    for wc, grp in train_df.groupby("weight_class"):
        h = grp["diff_height_cm"].dropna()
        r = grp["diff_reach_cm"].dropna()
        if len(h) >= _MIN_BUCKET_SIZE:
            height_stats[wc] = {"mean": float(h.mean()), "std": float(h.std())}
        if len(r) >= _MIN_BUCKET_SIZE:
            reach_stats[wc] = {"mean": float(r.mean()), "std": float(r.std())}

    # Global fallback stats
    all_h = train_df["diff_height_cm"].dropna()
    all_r = train_df["diff_reach_cm"].dropna()
    global_height_std = float(all_h.std()) if len(all_h) > 1 else 1.0
    global_reach_std = float(all_r.std()) if len(all_r) > 1 else 1.0

    return {
        "base_prior": 0.5,
        "height_stats": height_stats,
        "reach_stats": reach_stats,
        "global_height_std": global_height_std,
        "global_reach_std": global_reach_std,
        "training_debut_win_rate": training_debut_win_rate,
    }


def apply_debut_features(bout_df: pd.DataFrame, priors: dict) -> pd.DataFrame:
    """Add debut prior features to the bout DataFrame.

    Adds three columns:
      - debut_prior_win_prob_f1: prior probability fighter_1 wins, clipped to [0.3, 0.7].
        For non-debut bouts, this is NaN (the model should ignore it).
      - debut_reach_adv: diff_reach_cm standardized by weight-class std.
        Non-NaN for debut bouts with reach data, NaN otherwise.
      - debut_height_adv: diff_height_cm standardized by weight-class std.
        Non-NaN for debut bouts with height data, NaN otherwise.

    Args:
        bout_df: DataFrame from load_bout_data(). Modified in-place and returned.
        priors:  Output of compute_debut_priors().

    Returns:
        The same DataFrame with three new columns added.
    """
    df = bout_df
    n = len(df)

    # Initialize with NaN (non-debut bouts stay NaN)
    df["debut_prior_win_prob_f1"] = np.nan
    df["debut_reach_adv"] = np.nan
    df["debut_height_adv"] = np.nan

    # Identify debut bouts: both_debuting == 1
    is_debut = df["both_debuting"] == 1
    if not is_debut.any():
        return df

    base_prior = priors["base_prior"]
    height_stats = priors["height_stats"]
    reach_stats = priors["reach_stats"]
    global_h_std = priors["global_height_std"]
    global_r_std = priors["global_reach_std"]

    # Compute debut_prior_win_prob_f1 (fixed at 0.5 — no era-dependent bias)
    debut_idx = df.index[is_debut]
    df.loc[debut_idx, "debut_prior_win_prob_f1"] = base_prior

    # Compute debut_height_adv (standardized height diff)
    for idx in debut_idx:
        h_diff = df.at[idx, "diff_height_cm"]
        if pd.isna(h_diff):
            continue
        wc = df.at[idx, "weight_class"]
        stats = height_stats.get(wc)
        std = stats["std"] if stats and stats["std"] > 0 else global_h_std
        if std > 0:
            df.at[idx, "debut_height_adv"] = h_diff / std

    # Compute debut_reach_adv (standardized reach diff)
    for idx in debut_idx:
        r_diff = df.at[idx, "diff_reach_cm"]
        if pd.isna(r_diff):
            continue
        wc = df.at[idx, "weight_class"]
        stats = reach_stats.get(wc)
        std = stats["std"] if stats and stats["std"] > 0 else global_r_std
        if std > 0:
            df.at[idx, "debut_reach_adv"] = r_diff / std

    return df


def save_priors(priors: dict, path: str | Path) -> None:
    """Serialize priors to JSON for inference-time use."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(priors, f, indent=2)


def load_priors(path: str | Path) -> dict:
    """Load priors from JSON."""
    with open(path) as f:
        return json.load(f)
