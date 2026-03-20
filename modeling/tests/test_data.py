"""Tests for modeling/data.py — temporal splits and data loading utilities."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from modeling.data import (
    FEATURE_COLS,
    temporal_split,
    rolling_cv,
    label_distribution,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_df(n: int = 200, start_year: int = 2010) -> pd.DataFrame:
    """Build a synthetic DataFrame resembling bout_features output."""
    import numpy as np

    rng = np.random.default_rng(42)
    dates = pd.date_range(start=f"{start_year}-01-01", periods=n, freq="W")

    df = pd.DataFrame({
        "fight_id": [f"fight_{i}" for i in range(n)],
        "event_date": dates,
        "label": rng.integers(0, 2, size=n),
        "diff_elo": rng.normal(0, 30, n),
        "diff_career_win_rate": rng.uniform(-1, 1, n),
        "both_debuting": rng.integers(0, 2, size=n),
    })

    # Add remaining FEATURE_COLS as NaN (simulates sparse features)
    for col in FEATURE_COLS:
        if col not in df.columns:
            df[col] = float("nan")

    return df


# ── FEATURE_COLS ──────────────────────────────────────────────────────────────

class TestFeatureCols:
    def test_no_duplicates(self):
        assert len(FEATURE_COLS) == len(set(FEATURE_COLS))

    def test_no_id_or_target_cols(self):
        forbidden = {"fight_id", "fighter_1_id", "fighter_2_id", "label",
                     "event_date", "feature_version", "computed_at"}
        overlap = set(FEATURE_COLS) & forbidden
        assert overlap == set(), f"FEATURE_COLS contains forbidden columns: {overlap}"

    def test_minimum_count(self):
        # At least the 29 columns defined in bout_features DDL
        assert len(FEATURE_COLS) >= 29

    def test_core_diffs_present(self):
        core = [
            "diff_elo", "diff_career_win_rate", "diff_career_fights",
            "diff_win_rate_last3", "diff_win_rate_decay",
        ]
        for col in core:
            assert col in FEATURE_COLS, f"{col} missing from FEATURE_COLS"

    def test_ratios_present(self):
        assert "ratio_elo" in FEATURE_COLS
        assert "ratio_career_wins" in FEATURE_COLS

    def test_matchup_flags_present(self):
        assert "is_title_fight" in FEATURE_COLS
        assert "both_debuting" in FEATURE_COLS
        assert "is_orthodox_vs_southpaw" in FEATURE_COLS


# ── temporal_split ────────────────────────────────────────────────────────────

class TestTemporalSplit:
    def test_no_leakage(self):
        """Training data must be strictly before validation start."""
        df = _make_df(300, start_year=2010)
        train, val, test = temporal_split(df, val_date="2020-01-01", test_date="2022-01-01")

        assert train["event_date"].max() < pd.Timestamp("2020-01-01")

    def test_val_window(self):
        """Val must be [val_date, test_date)."""
        df = _make_df(300, start_year=2010)
        train, val, test = temporal_split(df, val_date="2020-01-01", test_date="2022-01-01")

        if len(val) > 0:
            assert val["event_date"].min() >= pd.Timestamp("2020-01-01")
            assert val["event_date"].max() < pd.Timestamp("2022-01-01")

    def test_test_window(self):
        """Test must be >= test_date."""
        df = _make_df(300, start_year=2010)
        train, val, test = temporal_split(df, val_date="2020-01-01", test_date="2022-01-01")

        if len(test) > 0:
            assert test["event_date"].min() >= pd.Timestamp("2022-01-01")

    def test_no_overlap(self):
        """Splits must be mutually exclusive."""
        df = _make_df(300, start_year=2010)
        train, val, test = temporal_split(df, val_date="2020-01-01", test_date="2022-01-01")

        train_ids = set(train["fight_id"])
        val_ids = set(val["fight_id"])
        test_ids = set(test["fight_id"])

        assert train_ids.isdisjoint(val_ids)
        assert train_ids.isdisjoint(test_ids)
        assert val_ids.isdisjoint(test_ids)

    def test_exhaustive_coverage(self):
        """Every row must appear in exactly one split."""
        df = _make_df(300, start_year=2010)
        train, val, test = temporal_split(df, val_date="2020-01-01", test_date="2022-01-01")

        total = len(train) + len(val) + len(test)
        assert total == len(df)

    def test_default_split_dates(self):
        """Default splits produce non-empty train and test sets."""
        df = _make_df(300, start_year=2010)
        train, val, test = temporal_split(df)

        assert len(train) > 0
        assert len(test) > 0

    def test_accepts_string_dates(self):
        df = _make_df(200, start_year=2015)
        train, val, test = temporal_split(df, val_date="2022-01-01", test_date="2024-01-01")
        assert len(train) + len(val) + len(test) == len(df)

    def test_accepts_date_objects(self):
        df = _make_df(200, start_year=2015)
        train, val, test = temporal_split(df, val_date=date(2022, 1, 1), test_date=date(2024, 1, 1))
        assert len(train) + len(val) + len(test) == len(df)

    def test_null_features_preserved(self):
        """NaN values in feature columns must not be silently dropped."""
        df = _make_df(100, start_year=2015)
        train, val, test = temporal_split(df, val_date="2020-01-01", test_date="2022-01-01")

        for split in (train, val, test):
            for col in FEATURE_COLS:
                assert col in split.columns, f"Column {col} missing from split"

    def test_train_sorted_ascending(self):
        df = _make_df(200, start_year=2010)
        train, _, _ = temporal_split(df, val_date="2020-01-01", test_date="2022-01-01")
        assert (train["event_date"].diff().dropna() >= pd.Timedelta(0)).all()


# ── rolling_cv ────────────────────────────────────────────────────────────────

class TestRollingCV:
    def test_returns_list_of_tuples(self):
        df = _make_df(500, start_year=2005)
        folds = rolling_cv(df, n_folds=4, min_train_years=2, val_months=12)

        assert isinstance(folds, list)
        for fold in folds:
            assert len(fold) == 2

    def test_chronological_order(self):
        """Each fold's validation start must come after the previous fold's validation end."""
        df = _make_df(500, start_year=2005)
        folds = rolling_cv(df, n_folds=5, min_train_years=2, val_months=12)

        for i in range(1, len(folds)):
            prev_val_max = folds[i - 1][1]["event_date"].max()
            curr_val_min = folds[i][1]["event_date"].min()
            assert curr_val_min >= prev_val_max, (
                f"Fold {i} validation starts before fold {i-1} validation ends"
            )

    def test_no_leakage_within_fold(self):
        """In each fold, training data must be strictly before validation data."""
        df = _make_df(500, start_year=2005)
        folds = rolling_cv(df, n_folds=4, min_train_years=2, val_months=12)

        for i, (train, val) in enumerate(folds):
            assert train["event_date"].max() < val["event_date"].min(), (
                f"Fold {i}: training data overlaps with validation data"
            )

    def test_expanding_training_windows(self):
        """Later folds must have more training data than earlier folds."""
        df = _make_df(500, start_year=2005)
        folds = rolling_cv(df, n_folds=4, min_train_years=2, val_months=12)

        if len(folds) >= 2:
            for i in range(1, len(folds)):
                assert len(folds[i][0]) >= len(folds[i - 1][0]), (
                    f"Fold {i} has fewer training rows than fold {i-1}"
                )

    def test_no_empty_folds(self):
        """Each fold must have at least 10 val rows and 50 train rows."""
        df = _make_df(500, start_year=2005)
        folds = rolling_cv(df, n_folds=4, min_train_years=2, val_months=12)

        for i, (train, val) in enumerate(folds):
            assert len(train) >= 50, f"Fold {i} has < 50 train rows"
            assert len(val) >= 10, f"Fold {i} has < 10 val rows"

    def test_returns_at_least_one_fold(self):
        df = _make_df(500, start_year=2000)
        folds = rolling_cv(df, n_folds=3, min_train_years=2, val_months=12)
        assert len(folds) >= 1

    def test_expanding_train_includes_prior_val(self):
        """In expanding-window CV, fold N+1's training set must include fold N's val rows.
        This is the correct behaviour — training windows expand to absorb previous folds.
        """
        df = _make_df(500, start_year=2005)
        folds = rolling_cv(df, n_folds=4, min_train_years=2, val_months=12)

        for i in range(1, len(folds)):
            prev_val_ids = set(folds[i - 1][1]["fight_id"])
            curr_train_ids = set(folds[i][0]["fight_id"])
            # Every prior-val row must be in the next fold's train (expanding window)
            assert prev_val_ids.issubset(curr_train_ids), (
                f"Fold {i} train is missing {len(prev_val_ids - curr_train_ids)} "
                f"rows from fold {i-1} val — expanding window broken"
            )


# ── label_distribution ────────────────────────────────────────────────────────

class TestLabelDistribution:
    def test_runs_without_error(self, capsys):
        df = _make_df(100)
        label_distribution(df, "test")
        captured = capsys.readouterr()
        assert "n=100" in captured.out
        assert "label=1" in captured.out

    def test_empty_df(self, capsys):
        df = _make_df(0)
        label_distribution(df, "empty")
        captured = capsys.readouterr()
        assert "n=0" in captured.out
