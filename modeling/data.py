"""Data loading and temporal splitting for the modeling pipeline.

Loads bout_features from the warehouse, selects model-ready columns, and
produces temporal train/validation/test splits with no data leakage.

Usage:
    from modeling.data import load_bout_data, temporal_split, rolling_cv, FEATURE_COLS
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PGConnection


# ── Feature column registry ───────────────────────────────────────────────────
# Canonical list of columns used as model inputs. Order is stable.
# Excludes: fight_id, fighter_*_id, event_date, weight_class, label,
#           feature_version, computed_at (metadata / identifiers / target).

FEATURE_COLS: list[str] = [
    # Differences (fighter_1 - fighter_2)
    "diff_elo",
    "diff_career_wins",
    "diff_career_fights",
    "diff_career_win_rate",
    "diff_career_finish_rate",
    "diff_career_sig_strikes_landed_pm",
    "diff_career_sig_strike_accuracy",
    "diff_career_takedown_accuracy",
    "diff_career_control_rate",
    "diff_age",
    "diff_height_cm",
    "diff_reach_cm",
    "diff_days_since_last_fight",
    "diff_win_rate_last3",
    "diff_sig_strikes_landed_pm_last3",
    "diff_takedown_accuracy_last3",
    "diff_control_rate_last3",
    "diff_sig_strikes_landed_pm_decay",
    "diff_win_rate_decay",
    "diff_opp_avg_elo",
    # Ratios (fighter_1 / (fighter_1 + fighter_2))
    "ratio_career_wins",
    "ratio_career_fights",
    "ratio_career_sig_strikes_landed_pm",
    "ratio_career_control_rate",
    "ratio_elo",
    # Matchup / metadata
    "is_title_fight",
    "scheduled_rounds",
    "is_orthodox_vs_southpaw",
    "both_debuting",
]


# ── Data loading ──────────────────────────────────────────────────────────────

def load_bout_data(conn: "PGConnection") -> pd.DataFrame:
    """Load bout_features from the warehouse into a model-ready DataFrame.

    - Excludes draws and no-contests (NULL label).
    - Sorts by event_date ascending.
    - Casts boolean columns to int (0/1) for model compatibility.
    - All FEATURE_COLS are present; NaNs are preserved (not dropped).

    Returns a DataFrame with columns: fight_id, fighter_1_id, fighter_2_id,
    event_date, weight_class, label, both_debuting, + all FEATURE_COLS.
    """
    cols = (
        "fight_id, fighter_1_id, fighter_2_id, event_date, weight_class, "
        "label, " + ", ".join(FEATURE_COLS)
    )
    query = f"""
        SELECT {cols}
        FROM bout_features
        WHERE label IS NOT NULL
        ORDER BY event_date ASC
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=col_names)

    # Cast boolean columns to int so all features are numeric
    bool_cols = ["is_title_fight", "is_orthodox_vs_southpaw", "both_debuting"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype(int)

    df["label"] = df["label"].astype(int)
    df["event_date"] = pd.to_datetime(df["event_date"])

    return df


def label_distribution(df: pd.DataFrame, name: str = "") -> None:
    """Print label distribution for a DataFrame split."""
    n = len(df)
    n1 = (df["label"] == 1).sum()
    n0 = (df["label"] == 0).sum()
    pct1 = 100.0 * n1 / n if n else 0
    prefix = f"[{name}] " if name else ""
    print(f"  {prefix}n={n:,}  label=1: {n1:,} ({pct1:.1f}%)  label=0: {n0:,} ({100-pct1:.1f}%)")


# ── Temporal splitting ────────────────────────────────────────────────────────

def temporal_split(
    df: pd.DataFrame,
    val_date: date | str | None = None,
    test_date: date | str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split df into train, validation, and test sets by event_date.

    Boundaries:
        train: event_date < val_date
        val:   val_date <= event_date < test_date
        test:  event_date >= test_date

    Default split dates (configurable):
        test_date = latest event_date - 2 years
        val_date  = test_date - 2 years

    Args:
        df: DataFrame with event_date column (datetime).
        val_date: Start of validation window (inclusive).
        test_date: Start of test holdout (inclusive).

    Returns:
        (train, val, test) DataFrames.
    """
    max_date = df["event_date"].max()

    if test_date is None:
        test_date = max_date - pd.DateOffset(years=2)
    if val_date is None:
        val_date = pd.Timestamp(test_date) - pd.DateOffset(years=2)

    test_date = pd.Timestamp(test_date)
    val_date = pd.Timestamp(val_date)

    train = df[df["event_date"] < val_date].copy()
    val = df[(df["event_date"] >= val_date) & (df["event_date"] < test_date)].copy()
    test = df[df["event_date"] >= test_date].copy()

    return train, val, test


def rolling_cv(
    df: pd.DataFrame,
    n_folds: int = 5,
    min_train_years: int = 3,
    val_months: int = 12,
) -> list[tuple[pd.DataFrame, pd.DataFrame]]:
    """Generate time-ordered rolling CV folds with expanding training windows.

    Each fold's validation window is `val_months` long and comes after all
    prior folds' validation windows. The training set expands to include all
    data before the fold's validation start.

    Args:
        df: DataFrame with event_date column (datetime), sorted ascending.
        n_folds: Number of folds.
        min_train_years: Minimum years of training data for the first fold.
        val_months: Length of each validation window in months.

    Returns:
        List of (train_df, val_df) tuples, ordered chronologically.
        Folds with fewer than 50 rows in train or val are silently skipped.
    """
    min_date = df["event_date"].min()
    max_date = df["event_date"].max()

    # The last validation window ends at max_date.
    # Work backwards to place n_folds non-overlapping windows.
    val_end = max_date
    fold_boundaries = []  # list of (val_start, val_end)
    for _ in range(n_folds):
        val_start = val_end - pd.DateOffset(months=val_months)
        fold_boundaries.append((val_start, val_end))
        val_end = val_start

    # Reverse so folds are in chronological order
    fold_boundaries.reverse()

    folds = []
    for val_start, val_end in fold_boundaries:
        train_cutoff = val_start
        train_df = df[df["event_date"] < train_cutoff].copy()
        val_df = df[
            (df["event_date"] >= val_start) & (df["event_date"] < val_end)
        ].copy()

        # Skip folds that don't have enough training history
        if train_df["event_date"].min() > (min_date + pd.DateOffset(years=min_train_years)):
            continue
        if len(train_df) < 50 or len(val_df) < 10:
            continue

        folds.append((train_df, val_df))

    return folds


# ── Convenience ───────────────────────────────────────────────────────────────

def describe_splits(
    train: pd.DataFrame,
    val: pd.DataFrame,
    test: pd.DataFrame,
) -> None:
    """Print a summary of train/val/test split sizes and date ranges."""
    print("Split summary:")
    for name, df in [("train", train), ("val", val), ("test", test)]:
        if len(df) == 0:
            print(f"  {name}: 0 rows")
            continue
        d_min = df["event_date"].min().date()
        d_max = df["event_date"].max().date()
        label_distribution(df, f"{name} {d_min}→{d_max}")
