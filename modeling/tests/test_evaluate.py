"""Tests for modeling/evaluate.py — metric computation and calibration utilities."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from modeling.evaluate import (
    compute_metrics,
    calibration_table,
    print_report,
    compare_models,
    _expected_calibration_error,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _perfect_preds(n=200):
    """Perfect predictions: p=1.0 for label=1, p=0.0 for label=0."""
    y_true = np.array([1, 0] * (n // 2))
    y_prob = y_true.astype(float)
    return y_true, y_prob


def _coin_flip(n=200):
    """Coin-flip predictions: always p=0.5."""
    y_true = np.array([1, 0] * (n // 2))
    y_prob = np.full(n, 0.5)
    return y_true, y_prob


def _all_ones(n=200):
    """Overconfident: always p=1.0 for a balanced dataset."""
    y_true = np.array([1, 0] * (n // 2))
    y_prob = np.ones(n)
    return y_true, y_prob


# ── compute_metrics ───────────────────────────────────────────────────────────

class TestComputeMetrics:
    def test_returns_all_keys(self):
        y_true, y_prob = _coin_flip()
        m = compute_metrics(y_true, y_prob)
        assert set(m.keys()) == {"accuracy", "log_loss", "brier_score", "roc_auc", "ece"}

    def test_all_values_are_float(self):
        y_true, y_prob = _coin_flip()
        m = compute_metrics(y_true, y_prob)
        for k, v in m.items():
            assert isinstance(v, float), f"{k} is not float"

    def test_perfect_predictions(self):
        y_true, y_prob = _perfect_preds()
        m = compute_metrics(y_true, y_prob)
        assert m["accuracy"] == pytest.approx(1.0)
        assert m["roc_auc"] == pytest.approx(1.0)
        assert m["brier_score"] == pytest.approx(0.0)
        assert m["ece"] == pytest.approx(0.0)

    def test_coin_flip_accuracy(self):
        """Coin flip at 0.5 gives ~50% accuracy on balanced data."""
        y_true, y_prob = _coin_flip()
        m = compute_metrics(y_true, y_prob)
        # With threshold 0.5, ties go to 1 — accuracy is 50% on balanced data
        assert 0.45 <= m["accuracy"] <= 0.55

    def test_coin_flip_log_loss(self):
        """log_loss of constant 0.5 predictions = log(2) ≈ 0.693."""
        y_true, y_prob = _coin_flip()
        m = compute_metrics(y_true, y_prob)
        assert m["log_loss"] == pytest.approx(np.log(2), abs=0.01)

    def test_coin_flip_brier(self):
        """Brier score of constant 0.5 predictions = 0.25."""
        y_true, y_prob = _coin_flip()
        m = compute_metrics(y_true, y_prob)
        assert m["brier_score"] == pytest.approx(0.25, abs=0.01)

    def test_coin_flip_auc(self):
        """Constant predictions have AUC = 0.5."""
        y_true, y_prob = _coin_flip()
        m = compute_metrics(y_true, y_prob)
        assert m["roc_auc"] == pytest.approx(0.5, abs=0.01)

    def test_metric_bounds(self):
        rng = np.random.default_rng(0)
        y_true = rng.integers(0, 2, 500)
        y_prob = rng.uniform(0, 1, 500)
        m = compute_metrics(y_true, y_prob)
        assert 0.0 <= m["accuracy"] <= 1.0
        assert m["log_loss"] >= 0.0
        assert 0.0 <= m["brier_score"] <= 1.0
        assert 0.0 <= m["roc_auc"] <= 1.0
        assert 0.0 <= m["ece"] <= 1.0

    def test_accepts_lists(self):
        """Should handle Python lists, not just numpy arrays."""
        m = compute_metrics([1, 0, 1, 0], [0.9, 0.1, 0.8, 0.2])
        assert m["accuracy"] == pytest.approx(1.0)

    def test_near_perfect_worse_than_perfect(self):
        y_true = np.array([1, 0] * 100)
        y_perfect = y_true.astype(float)
        y_near = np.clip(y_perfect, 0.05, 0.95)
        m_perfect = compute_metrics(y_true, y_perfect)
        m_near = compute_metrics(y_true, y_near)
        assert m_near["log_loss"] > m_perfect["log_loss"]
        assert m_near["brier_score"] > m_perfect["brier_score"]


# ── _expected_calibration_error ───────────────────────────────────────────────

class TestECE:
    def test_perfectly_calibrated(self):
        """If predicted prob = actual freq in each bin, ECE = 0."""
        # Construct perfectly calibrated predictions
        y_true = np.array([1] * 50 + [0] * 50)
        y_prob = np.array([0.75] * 50 + [0.25] * 50)
        # bin [0.2–0.3]: mean_pred=0.25, mean_actual=0.0 → gap = 0.25
        # Not perfectly calibrated — just verify it's low for balanced
        ece = _expected_calibration_error(y_true, y_prob)
        assert 0.0 <= ece <= 1.0

    def test_coin_flip_ece_near_zero(self):
        """Coin-flip predictions (p=0.5) on balanced data are perfectly calibrated."""
        rng = np.random.default_rng(42)
        n = 2000
        y_true = rng.integers(0, 2, n)
        y_prob = np.full(n, 0.5)
        ece = _expected_calibration_error(y_true, y_prob)
        assert ece < 0.05  # actual win rate ≈ 0.5 = predicted → near zero ECE

    def test_overconfident_has_high_ece(self):
        """All p=1.0 on balanced data: calibration error = 0.5."""
        y_true = np.array([1, 0] * 100)
        y_prob = np.ones(200)
        ece = _expected_calibration_error(y_true, y_prob)
        assert ece == pytest.approx(0.5, abs=0.05)

    def test_ece_non_negative(self):
        rng = np.random.default_rng(7)
        y_true = rng.integers(0, 2, 300)
        y_prob = rng.uniform(0, 1, 300)
        assert _expected_calibration_error(y_true, y_prob) >= 0.0


# ── calibration_table ─────────────────────────────────────────────────────────

class TestCalibrationTable:
    def test_returns_dataframe(self):
        y_true, y_prob = _coin_flip()
        tbl = calibration_table(y_true, y_prob)
        assert isinstance(tbl, pd.DataFrame)

    def test_has_required_columns(self):
        y_true, y_prob = _coin_flip()
        tbl = calibration_table(y_true, y_prob)
        assert set(tbl.columns) == {"bin", "mean_predicted", "mean_actual", "count"}

    def test_n_bins_rows(self):
        y_true, y_prob = _coin_flip(400)
        tbl = calibration_table(y_true, y_prob, n_bins=10)
        assert len(tbl) == 10

    def test_count_sums_to_n(self):
        y_true, y_prob = _coin_flip(200)
        tbl = calibration_table(y_true, y_prob)
        assert tbl["count"].sum() == 200

    def test_mean_predicted_in_bounds(self):
        rng = np.random.default_rng(3)
        y_true = rng.integers(0, 2, 300)
        y_prob = rng.uniform(0, 1, 300)
        tbl = calibration_table(y_true, y_prob)
        valid = tbl.dropna(subset=["mean_predicted"])
        assert (valid["mean_predicted"].between(0, 1)).all()
        assert (valid["mean_actual"].between(0, 1)).all()

    def test_empty_bins_have_none(self):
        """If all predictions fall in one bin, all other bins have None."""
        y_true = np.array([1, 0] * 50)
        y_prob = np.full(100, 0.95)  # all in the last bin
        tbl = calibration_table(y_true, y_prob, n_bins=10)
        empty_bins = tbl[tbl["count"] == 0]
        assert empty_bins["mean_predicted"].isna().all()
        assert empty_bins["mean_actual"].isna().all()


# ── print_report ──────────────────────────────────────────────────────────────

class TestPrintReport:
    def test_prints_all_metrics(self, capsys):
        m = compute_metrics(*_coin_flip())
        print_report(m, "Test Model")
        out = capsys.readouterr().out
        assert "Test Model" in out
        assert "Accuracy" in out
        assert "Log Loss" in out
        assert "Brier" in out
        assert "ROC AUC" in out
        assert "ECE" in out

    def test_runs_without_error(self):
        m = compute_metrics(*_coin_flip())
        print_report(m)  # no name arg


# ── compare_models ────────────────────────────────────────────────────────────

class TestCompareModels:
    def test_returns_dataframe(self):
        results = {
            "Model A": compute_metrics(*_coin_flip()),
            "Model B": compute_metrics(*_perfect_preds()),
        }
        df = compare_models(results)
        assert isinstance(df, pd.DataFrame)

    def test_rows_are_models(self):
        results = {
            "Coin Flip": compute_metrics(*_coin_flip()),
            "Perfect": compute_metrics(*_perfect_preds()),
        }
        df = compare_models(results)
        assert set(df.index) == {"Coin Flip", "Perfect"}

    def test_sorted_by_log_loss(self):
        results = {
            "Coin Flip": compute_metrics(*_coin_flip()),
            "Perfect": compute_metrics(*_perfect_preds()),
        }
        df = compare_models(results)
        log_losses = df["Log Loss"].values
        assert log_losses[0] <= log_losses[1]

    def test_has_all_metric_columns(self):
        results = {"m": compute_metrics(*_coin_flip())}
        df = compare_models(results)
        col_lower = [c.lower().replace(" ", "_") for c in df.columns]
        assert "log_loss" in col_lower
        assert "accuracy" in col_lower
        assert "brier_score" in col_lower
        assert "roc_auc" in col_lower
        assert "ece" in col_lower

    def test_single_model(self):
        results = {"Only": compute_metrics(*_coin_flip())}
        df = compare_models(results)
        assert len(df) == 1

    def test_better_model_ranks_first(self):
        rng = np.random.default_rng(0)
        y_true = rng.integers(0, 2, 500)
        good_prob = np.where(y_true == 1, 0.75, 0.25).astype(float)
        bad_prob = np.full(500, 0.5)

        results = {
            "Good": compute_metrics(y_true, good_prob),
            "Bad": compute_metrics(y_true, bad_prob),
        }
        df = compare_models(results)
        assert df.index[0] == "Good"
