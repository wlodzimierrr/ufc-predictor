"""Tests for modeling/artifacts.py — round-trip save/load and discovery."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from modeling.artifacts import save_model, load_model, latest_artifact, list_artifacts


# ── Fixtures ──────────────────────────────────────────────────────────────────

class _DummyModel:
    """Minimal sklearn-like model for testing serialisation."""
    def __init__(self, value: float = 0.5):
        self.value = value

    def predict_proba(self, X):
        import numpy as np
        return np.full((len(X), 2), [1 - self.value, self.value])


_METADATA = {
    "model_name": "test_model",
    "feature_cols": ["diff_elo", "diff_age"],
    "feature_version": 1,
    "train_rows": 6000,
    "val_rows": 1000,
    "val_date": "2022-01-01",
    "test_date": "2024-01-01",
    "hyperparameters": {"C": 0.1},
}

_METRICS = {
    "val": {"accuracy": 0.58, "log_loss": 0.67, "brier_score": 0.24, "roc_auc": 0.62, "ece": 0.03},
    "test": {"accuracy": 0.60, "log_loss": 0.65, "brier_score": 0.23, "roc_auc": 0.63, "ece": 0.02},
}


# ── save_model / load_model ───────────────────────────────────────────────────

class TestRoundTrip:
    def test_save_creates_directory(self, tmp_path):
        artifact_dir = save_model(_DummyModel(), _METADATA, tmp_path)
        assert artifact_dir.exists()
        assert artifact_dir.is_dir()

    def test_directory_structure(self, tmp_path):
        artifact_dir = save_model(_DummyModel(), _METADATA, tmp_path)
        assert (artifact_dir / "model.joblib").exists()
        assert (artifact_dir / "metadata.json").exists()

    def test_metrics_file_created_when_provided(self, tmp_path):
        artifact_dir = save_model(_DummyModel(), _METADATA, tmp_path, metrics=_METRICS)
        assert (artifact_dir / "metrics.json").exists()

    def test_no_metrics_file_when_not_provided(self, tmp_path):
        artifact_dir = save_model(_DummyModel(), _METADATA, tmp_path)
        assert not (artifact_dir / "metrics.json").exists()

    def test_directory_path_contains_model_name(self, tmp_path):
        artifact_dir = save_model(_DummyModel(), _METADATA, tmp_path)
        assert "test_model" in str(artifact_dir)

    def test_load_returns_model_and_metadata(self, tmp_path):
        artifact_dir = save_model(_DummyModel(), _METADATA, tmp_path)
        model, meta = load_model(artifact_dir)
        assert model is not None
        assert isinstance(meta, dict)

    def test_loaded_model_is_correct_type(self, tmp_path):
        artifact_dir = save_model(_DummyModel(value=0.7), _METADATA, tmp_path)
        model, _ = load_model(artifact_dir)
        assert isinstance(model, _DummyModel)
        assert model.value == pytest.approx(0.7)

    def test_metadata_round_trip(self, tmp_path):
        artifact_dir = save_model(_DummyModel(), _METADATA, tmp_path)
        _, meta = load_model(artifact_dir)
        assert meta["model_name"] == "test_model"
        assert meta["feature_cols"] == ["diff_elo", "diff_age"]
        assert meta["feature_version"] == 1
        assert meta["train_rows"] == 6000
        assert meta["hyperparameters"] == {"C": 0.1}

    def test_metadata_includes_saved_at(self, tmp_path):
        artifact_dir = save_model(_DummyModel(), _METADATA, tmp_path)
        _, meta = load_model(artifact_dir)
        assert "saved_at" in meta

    def test_metrics_round_trip(self, tmp_path):
        artifact_dir = save_model(_DummyModel(), _METADATA, tmp_path, metrics=_METRICS)
        _, meta = load_model(artifact_dir)
        assert "metrics" in meta
        assert meta["metrics"]["val"]["accuracy"] == pytest.approx(0.58)
        assert meta["metrics"]["test"]["log_loss"] == pytest.approx(0.65)

    def test_no_metrics_key_when_not_saved(self, tmp_path):
        artifact_dir = save_model(_DummyModel(), _METADATA, tmp_path)
        _, meta = load_model(artifact_dir)
        assert "metrics" not in meta

    def test_metadata_json_is_valid_json(self, tmp_path):
        artifact_dir = save_model(_DummyModel(), _METADATA, tmp_path)
        raw = (artifact_dir / "metadata.json").read_text()
        parsed = json.loads(raw)
        assert isinstance(parsed, dict)

    def test_multiple_saves_create_separate_directories(self, tmp_path):
        dir1 = save_model(_DummyModel(), _METADATA, tmp_path)
        import time; time.sleep(1.1)  # ensure different timestamp
        dir2 = save_model(_DummyModel(), _METADATA, tmp_path)
        assert dir1 != dir2


# ── latest_artifact ───────────────────────────────────────────────────────────

class TestLatestArtifact:
    def test_returns_none_when_no_models_dir(self, tmp_path):
        result = latest_artifact(tmp_path / "nonexistent", "test_model")
        assert result is None

    def test_returns_none_when_model_not_found(self, tmp_path):
        save_model(_DummyModel(), _METADATA, tmp_path)
        result = latest_artifact(tmp_path, "other_model")
        assert result is None

    def test_returns_path_when_exists(self, tmp_path):
        artifact_dir = save_model(_DummyModel(), _METADATA, tmp_path)
        result = latest_artifact(tmp_path, "test_model")
        assert result == artifact_dir

    def test_returns_latest_when_multiple_runs(self, tmp_path):
        save_model(_DummyModel(), _METADATA, tmp_path)
        import time; time.sleep(1.1)
        second = save_model(_DummyModel(), _METADATA, tmp_path)
        result = latest_artifact(tmp_path, "test_model")
        assert result == second


# ── list_artifacts ────────────────────────────────────────────────────────────

class TestListArtifacts:
    def test_empty_when_no_dir(self, tmp_path):
        result = list_artifacts(tmp_path / "nonexistent")
        assert result == []

    def test_finds_saved_artifacts(self, tmp_path):
        save_model(_DummyModel(), _METADATA, tmp_path, metrics=_METRICS)
        artifacts = list_artifacts(tmp_path)
        assert len(artifacts) == 1
        assert artifacts[0]["model_name"] == "test_model"

    def test_artifact_entry_structure(self, tmp_path):
        save_model(_DummyModel(), _METADATA, tmp_path, metrics=_METRICS)
        a = list_artifacts(tmp_path)[0]
        assert "model_name" in a
        assert "timestamp" in a
        assert "artifact_dir" in a
        assert "metadata" in a
        assert "metrics" in a

    def test_lists_multiple_models(self, tmp_path):
        meta_a = {**_METADATA, "model_name": "model_a"}
        meta_b = {**_METADATA, "model_name": "model_b"}
        save_model(_DummyModel(), meta_a, tmp_path)
        save_model(_DummyModel(), meta_b, tmp_path)
        artifacts = list_artifacts(tmp_path)
        names = {a["model_name"] for a in artifacts}
        assert names == {"model_a", "model_b"}
