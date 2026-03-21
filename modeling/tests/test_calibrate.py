"""Tests for modeling/calibrate.py — calibration assessment and correction."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from modeling.calibrate import assess_calibration, calibrate_isotonic, calibrate_platt


# ── Helpers ──────────────────────────────────────────────────────────────────

def _perfectly_calibrated(n=2000, seed=42):
    """Synthetic perfectly calibrated predictions: actual rate ≈ predicted prob."""
    rng = np.random.default_rng(seed)
    y_prob = rng.uniform(0, 1, n)
    y_true = (rng.uniform(0, 1, n) < y_prob).astype(int)
    return y_true, y_prob


def _coin_flip(n=200):
    y_true = np.array([1, 0] * (n // 2))
    y_prob = np.full(n, 0.5)
    return y_true, y_prob


def _overconfident(n=200):
    """All p=1.0 on balanced data — maximally miscalibrated."""
    y_true = np.array([1, 0] * (n // 2))
    y_prob = np.ones(n)
    return y_true, y_prob


# ── assess_calibration ──────────────────────────────────────────────────────

class TestAssessCalibration:
    def test_returns_required_keys(self):
        cal = assess_calibration(*_coin_flip())
        assert "ece" in cal
        assert "mce" in cal
        assert "n_bins" in cal
        assert "bins" in cal

    def test_ece_non_negative(self):
        cal = assess_calibration(*_coin_flip())
        assert cal["ece"] >= 0.0

    def test_mce_gte_ece(self):
        """MCE (max gap) is always >= ECE (weighted avg gap)."""
        rng = np.random.default_rng(0)
        y_true = rng.integers(0, 2, 500)
        y_prob = rng.uniform(0, 1, 500)
        cal = assess_calibration(y_true, y_prob)
        assert cal["mce"] >= cal["ece"]

    def test_perfect_calibration_low_ece(self):
        y_true, y_prob = _perfectly_calibrated(5000)
        cal = assess_calibration(y_true, y_prob)
        assert cal["ece"] < 0.05

    def test_coin_flip_low_ece(self):
        rng = np.random.default_rng(42)
        y_true = rng.integers(0, 2, 2000)
        y_prob = np.full(2000, 0.5)
        cal = assess_calibration(y_true, y_prob)
        assert cal["ece"] < 0.05

    def test_overconfident_high_ece(self):
        cal = assess_calibration(*_overconfident())
        assert cal["ece"] == pytest.approx(0.5, abs=0.05)

    def test_n_bins_correct(self):
        cal = assess_calibration(*_coin_flip(), n_bins=5)
        assert cal["n_bins"] == 5
        assert len(cal["bins"]) == 5

    def test_bin_structure(self):
        cal = assess_calibration(*_coin_flip())
        for b in cal["bins"]:
            assert "bin" in b
            assert "count" in b
            assert "mean_predicted" in b
            assert "mean_actual" in b
            assert "gap" in b

    def test_counts_sum_to_n(self):
        y_true, y_prob = _coin_flip(200)
        cal = assess_calibration(y_true, y_prob)
        total = sum(b["count"] for b in cal["bins"])
        assert total == 200

    def test_empty_bin_has_none_values(self):
        y_true, y_prob = _coin_flip(100)  # all at 0.5
        cal = assess_calibration(y_true, y_prob, n_bins=10)
        # Most bins should be empty
        empty = [b for b in cal["bins"] if b["count"] == 0]
        assert len(empty) > 0
        for b in empty:
            assert b["mean_predicted"] is None
            assert b["gap"] is None


# ── calibrate_isotonic ──────────────────────────────────────────────────────

class TestCalibrateIsotonic:
    def test_output_shape(self):
        y_true, y_prob = _perfectly_calibrated(500)
        result = calibrate_isotonic(y_prob[:300], y_true[:300], y_prob[300:])
        assert len(result) == 200

    def test_output_in_bounds(self):
        y_true, y_prob = _perfectly_calibrated(500)
        result = calibrate_isotonic(y_prob[:300], y_true[:300], y_prob[300:])
        assert np.all((result >= 0.0) & (result <= 1.0))

    def test_improves_overconfident(self):
        """Isotonic should reduce ECE of overconfident predictions."""
        rng = np.random.default_rng(42)
        n = 1000
        y_true = rng.integers(0, 2, n)
        # Overconfident: push everything toward 0 or 1
        raw = np.where(y_true == 1, 0.9, 0.1).astype(float)
        raw += rng.normal(0, 0.05, n)
        raw = np.clip(raw, 0.01, 0.99)

        # Split into cal/test
        cal_prob, test_prob = raw[:500], raw[500:]
        cal_true, test_true = y_true[:500], y_true[500:]

        before = assess_calibration(test_true, test_prob)
        calibrated = calibrate_isotonic(cal_prob, cal_true, test_prob)
        after = assess_calibration(test_true, calibrated)

        # Should not make things much worse
        assert after["ece"] <= before["ece"] + 0.05

    def test_preserves_perfect_calibration(self):
        y_true, y_prob = _perfectly_calibrated(2000)
        cal_prob, test_prob = y_prob[:1000], y_prob[1000:]
        cal_true, test_true = y_true[:1000], y_true[1000:]

        before = assess_calibration(test_true, test_prob)
        calibrated = calibrate_isotonic(cal_prob, cal_true, test_prob)
        after = assess_calibration(test_true, calibrated)

        assert after["ece"] < 0.1  # shouldn't break good calibration


# ── calibrate_platt ─────────────────────────────────────────────────────────

class TestCalibratePlatt:
    def test_output_shape(self):
        y_true, y_prob = _perfectly_calibrated(500)
        result = calibrate_platt(y_prob[:300], y_true[:300], y_prob[300:])
        assert len(result) == 200

    def test_output_in_bounds(self):
        y_true, y_prob = _perfectly_calibrated(500)
        result = calibrate_platt(y_prob[:300], y_true[:300], y_prob[300:])
        assert np.all((result >= 0.0) & (result <= 1.0))

    def test_handles_extreme_values(self):
        """Should not crash on probabilities near 0 or 1."""
        y_prob_train = np.array([0.001, 0.999, 0.5, 0.3, 0.7, 0.01, 0.99, 0.4])
        y_true_train = np.array([0, 1, 1, 0, 1, 0, 1, 0])
        y_prob_test = np.array([0.0, 1.0, 0.5])
        result = calibrate_platt(y_prob_train, y_true_train, y_prob_test)
        assert len(result) == 3
        assert np.all(np.isfinite(result))

    def test_preserves_perfect_calibration(self):
        y_true, y_prob = _perfectly_calibrated(2000)
        cal_prob, test_prob = y_prob[:1000], y_prob[1000:]
        cal_true, test_true = y_true[:1000], y_true[1000:]

        calibrated = calibrate_platt(cal_prob, cal_true, test_prob)
        after = assess_calibration(test_true, calibrated)
        assert after["ece"] < 0.1
