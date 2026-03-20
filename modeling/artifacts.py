"""Model persistence and artifact management.

Saves and loads trained models with metadata and metrics to/from disk.
Artifact layout: models/<model_name>/<timestamp>/
    model.joblib    — serialised model object
    metadata.json   — feature list, hyperparameters, split dates, row counts
    metrics.json    — evaluation results on val and test sets (optional)

Usage:
    from modeling.artifacts import save_model, load_model
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib


# ── Save ──────────────────────────────────────────────────────────────────────

def save_model(
    model: Any,
    metadata: dict,
    path: str | Path,
    metrics: dict | None = None,
) -> Path:
    """Persist a model, its metadata, and optionally its evaluation metrics.

    Args:
        model: Any sklearn-compatible model object (must be joblib-serialisable).
        metadata: Dict describing the training run. Recommended keys:
            - model_name (str)
            - feature_cols (list[str])
            - feature_version (int)
            - train_rows (int)
            - val_rows (int)
            - val_date (str ISO date)
            - test_date (str ISO date)
            - hyperparameters (dict)
        path: Directory under which `<model_name>/<timestamp>/` is created.
              If the path already ends with a timestamp directory, it is used as-is.
        metrics: Optional dict of {split_name: metrics_dict} e.g.
            {"val": {"accuracy": 0.6, ...}, "test": {"accuracy": 0.62, ...}}

    Returns:
        Path to the artifact directory that was created.
    """
    path = Path(path)
    model_name = metadata.get("model_name", "model")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    artifact_dir = path / model_name / timestamp
    artifact_dir.mkdir(parents=True, exist_ok=True)

    # Model binary
    joblib.dump(model, artifact_dir / "model.joblib")

    # Metadata
    meta = {**metadata, "saved_at": timestamp}
    (artifact_dir / "metadata.json").write_text(
        json.dumps(meta, indent=2, default=str), encoding="utf-8"
    )

    # Metrics (optional)
    if metrics is not None:
        (artifact_dir / "metrics.json").write_text(
            json.dumps(metrics, indent=2, default=str), encoding="utf-8"
        )

    print(f"Saved artifact: {artifact_dir}")
    return artifact_dir


# ── Load ──────────────────────────────────────────────────────────────────────

def load_model(path: str | Path) -> tuple[Any, dict]:
    """Load a saved model and its metadata.

    Args:
        path: Path to the artifact directory (contains model.joblib + metadata.json).

    Returns:
        (model, metadata) tuple. metadata includes metrics if metrics.json exists.
    """
    path = Path(path)

    model = joblib.load(path / "model.joblib")

    metadata = json.loads((path / "metadata.json").read_text(encoding="utf-8"))

    metrics_path = path / "metrics.json"
    if metrics_path.exists():
        metadata["metrics"] = json.loads(metrics_path.read_text(encoding="utf-8"))

    return model, metadata


# ── Discovery ─────────────────────────────────────────────────────────────────

def latest_artifact(models_dir: str | Path, model_name: str) -> Path | None:
    """Return the path to the most recent artifact for a given model name.

    Args:
        models_dir: Root models directory (e.g. Path("models")).
        model_name: Model subdirectory name (e.g. "logreg").

    Returns:
        Path to latest artifact directory, or None if none exist.
    """
    model_dir = Path(models_dir) / model_name
    if not model_dir.exists():
        return None
    runs = sorted(model_dir.iterdir())
    return runs[-1] if runs else None


def list_artifacts(models_dir: str | Path) -> list[dict]:
    """List all saved artifacts with their model name, timestamp, and metrics summary.

    Returns:
        List of dicts with keys: model_name, timestamp, artifact_dir, metrics.
    """
    models_dir = Path(models_dir)
    if not models_dir.exists():
        return []

    artifacts = []
    for model_dir in sorted(models_dir.iterdir()):
        if not model_dir.is_dir():
            continue
        for run_dir in sorted(model_dir.iterdir()):
            if not (run_dir / "model.joblib").exists():
                continue
            meta_path = run_dir / "metadata.json"
            metrics_path = run_dir / "metrics.json"
            meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
            metrics = json.loads(metrics_path.read_text()) if metrics_path.exists() else {}
            artifacts.append({
                "model_name": model_dir.name,
                "timestamp": run_dir.name,
                "artifact_dir": run_dir,
                "metadata": meta,
                "metrics": metrics,
            })

    return artifacts
