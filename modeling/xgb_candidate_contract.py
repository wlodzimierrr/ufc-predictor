"""Fixed, offline Phase 3B contracts; no legacy trainer/scorer entrypoints."""

from __future__ import annotations

import importlib.metadata
import math
import platform
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss

from features.debut_prior import apply_debut_features, compute_debut_priors
from modeling.holdout import assert_no_holdout_fights
from modeling.refit_preflight import (
    ALGORITHM_VERSION, DEBUT_COLS, FEATURE_ORDER, FEATURE_VERSION, ROOT,
    SOURCE_MODE, PreflightError, json_bytes, sha256, validate_training_frame,
)

SNAPSHOT = ROOT / "data/experiments/phase3a_pre_april_2026_git_1f477d3_metadata_safe"
MANIFEST_SHA256 = "42e8e1661409b2ca758a4e18775f9816865439448fbce0d88af68dda22325671"
TRAINING_SHA256 = "e53cd2f253985290ca2e03185fd4b67ee67c209473f595d5fda3cecbb34e764c"
FOLDS_SHA256 = "c425c98ff96f7e4dba4505e1215ace017587fef3d6a41f8996ad5ce726b615d1"
CONFIG_SHA256 = "b7e518293c07c0201f9a003087faf86a6a110546c861e48cbf587612f8ab4fdc"
SOURCE_COMMIT = "1f477d3ddc87b123d0099025b669e728e7881a34"
EVENT_CUTOFF = "2026-03-31"
OUTPUT_ROOT = ROOT / "data/experiments/phase3b_xgb_pre_april_2026"
LABEL_MAPPING = {"0": "fighter_2 wins", "1": "fighter_1 wins"}
ORIENTATION = "p_and_label_1_mean_fighter_1_wins"
CALIBRATION_CONTRACT = {
    "method": "platt_log_odds", "clip_epsilon": 1e-8,
    "input": "log(p / (1-p)); raw probability clipped before transformation",
    "C": 1e10, "solver": "lbfgs", "max_iter": 1000,
    "classes": [0, 1], "positive_class": 1, "orientation": ORIENTATION,
}
REQUIRED_COMPONENTS = {
    "base_learner.json", "calibrator.joblib", "final_debut_priors.json",
    "metadata.json", "training_manifest.json", "source_manifest.json",
    "selection.json", "folds.json", "oof.csv", "oof_manifest.json",
    "package_versions.json", "checksums.json", "run_receipt.json",
    "effective_configuration.toml", "preflight.json", "development_results.json",
    "verification.json", "preservation_baseline.json", "preservation_check.json",
}


def digest(value) -> str:
    return sha256(json_bytes(value))


def membership(frame: pd.DataFrame) -> str:
    return digest(sorted(frame.fight_id))


def package_versions() -> dict:
    return {"python": platform.python_version(), **{
        name: importlib.metadata.version(name) for name in
        ("numpy", "pandas", "scikit-learn", "xgboost", "psycopg2-binary", "joblib", "pytest")}}


def guard_partition(whole: pd.DataFrame, partition: pd.DataFrame, *,
                    expected_ids: list[str], start: str | None = None,
                    end: str = EVENT_CUTOFF) -> None:
    """Check exact original membership, labels, orientation and base features.

    Only the three deferred prior slots may differ after preprocessing. This
    guard is repeated immediately before preprocessing and learned fitting.
    """
    assert_no_holdout_fights(partition)
    validate_training_frame(partition, event_cutoff=EVENT_CUTOFF)
    if len(expected_ids) != len(set(expected_ids)) or sorted(partition.fight_id) != sorted(expected_ids):
        raise PreflightError("Operation membership differs from its exact approved partition")
    original = whole.set_index("fight_id").loc[partition.fight_id].reset_index()
    observed = partition.reset_index(drop=True)
    cols = [c for c in whole.columns if c not in DEBUT_COLS and c != "fight_id"]
    if not original[cols].equals(observed[cols]):
        raise PreflightError("Operation labels/orientation/features differ from snapshot")
    dates = partition.event_date
    if (dates >= pd.Timestamp(end)).any() or (start and (dates < pd.Timestamp(start)).any()):
        raise PreflightError("Operation violates chronological boundaries")
    # Membership must also be all eligible rows in the declared interval.
    mask = whole.event_date < pd.Timestamp(end)
    if start:
        mask &= whole.event_date >= pd.Timestamp(start)
    if sorted(whole.loc[mask, "fight_id"]) != sorted(expected_ids):
        raise PreflightError("Operation omits eligible rows or splits an event/date")


def check_priors(priors: dict) -> None:
    keys = {"base_prior", "height_stats", "reach_stats", "global_height_std",
            "global_reach_std", "training_debut_win_rate"}
    if not isinstance(priors, dict) or set(priors) != keys or priors["base_prior"] != 0.5:
        raise PreflightError("Invalid saved debut-prior contract")
    for key in ("global_height_std", "global_reach_std"):
        if not np.isfinite(priors[key]) or priors[key] <= 0:
            raise PreflightError("Invalid debut normalization denominator")
    if not 0 <= priors["training_debut_win_rate"] <= 1:
        raise PreflightError("Invalid debut training diagnostic")
    for key in ("height_stats", "reach_stats"):
        if not isinstance(priors[key], dict):
            raise PreflightError("Invalid debut buckets")
        for stats in priors[key].values():
            if (set(stats) != {"mean", "std"} or not np.isfinite(list(stats.values())).all()
                    or stats["std"] < 0):
                raise PreflightError("Invalid saved debut bucket")


def fit_partition_priors(whole, train, *, expected_ids, end, name) -> dict:
    guard_partition(whole, train, expected_ids=expected_ids, end=end)
    priors = compute_debut_priors(train)
    check_priors(priors)
    return {"schema_version": 1, "name": name, "fit_partition": "train_only",
            "training_rows": len(train), "training_ids_sha256": membership(train),
            "training_end_exclusive": end,
            "actual_training_endpoint": train.event_date.max().date().isoformat(),
            "feature_algorithm_version": ALGORITHM_VERSION, "priors": priors}


def preprocess_partition(whole, frame, *, expected_ids, prior_record, start=None, end=EVENT_CUTOFF):
    guard_partition(whole, frame, expected_ids=expected_ids, start=start, end=end)
    check_priors(prior_record["priors"])
    result = apply_debut_features(frame.copy(), prior_record["priors"])
    # Non-debuts and missing physical measurements legitimately remain NaN.
    debuts = result.both_debuting.eq(1)
    if not result.loc[debuts, DEBUT_COLS[0]].eq(0.5).all():
        raise PreflightError("Deferred debut probabilities were not computed")
    for col, physical in (("debut_height_adv", "diff_height_cm"), ("debut_reach_adv", "diff_reach_cm")):
        if result.loc[debuts & result[physical].notna(), col].isna().any():
            raise PreflightError("Deferred debut normalization was not computed")
    return result


def probabilities(values) -> np.ndarray:
    p = np.asarray(values, dtype=np.float64)
    if p.ndim != 1 or not np.isfinite(p).all() or ((p < 0) | (p > 1)).any():
        raise PreflightError("Invalid probability vector")
    return p


def log_odds(values) -> np.ndarray:
    p = np.clip(probabilities(values), 1e-8, 1 - 1e-8)
    return np.log(p / (1 - p)).reshape(-1, 1)


def calibrate(estimator, raw) -> np.ndarray:
    if list(estimator.classes_) != [0, 1]:
        raise PreflightError("Calibrator class orientation must be [0, 1]")
    return probabilities(estimator.predict_proba(log_odds(raw))[:, 1])


def metrics(labels, predictions) -> dict:
    p = probabilities(predictions)
    return {"log_loss": float(log_loss(labels, p, labels=[0, 1])),
            "brier_score": float(brier_score_loss(labels, p))}


def predict_best_iteration(model, frame) -> np.ndarray:
    """Explicitly use the validation optimum with installed XGBoost >= 2 API."""
    return probabilities(model.predict_proba(
        frame[FEATURE_ORDER], iteration_range=(0, int(model.best_iteration) + 1))[:, 1])


def select_development(results: list[dict]) -> dict:
    groups = {}
    for r in results:
        key = tuple(r["grid_parameters"][k] for k in ("max_depth", "min_child_weight", "reg_lambda"))
        groups.setdefault(key, []).append(r)
    summaries = []
    for key, rows in sorted(groups.items()):
        count = sum(r["prediction_rows"] for r in rows)
        weighted = sum(r["metrics"]["log_loss"] * r["prediction_rows"] for r in rows) / count
        summaries.append({"grid_parameters": dict(zip(
            ("max_depth", "min_child_weight", "reg_lambda"), key)),
            "row_weighted_log_loss": weighted, "validation_rows": count,
            "best_rounds_by_fold": [r["best_iteration"] + 1 for r in rows]})
    if not summaries:
        raise PreflightError("No development results")
    winner = min(summaries, key=lambda s: (s["row_weighted_log_loss"], *s["grid_parameters"].values()))
    rounds = math.floor(float(np.median(winner["best_rounds_by_fold"])) + 0.5)
    if rounds < 1:
        raise PreflightError("Selected round count must be positive")
    return {"schema_version": 1, "selection_metric": "row_weighted_development_log_loss",
            "tie_break": "lexicographic (max_depth, min_child_weight, reg_lambda)",
            "rounds_rule": "floor(median(best_iteration + 1) + 0.5)",
            "selected_parameters": winner["grid_parameters"], "selected_rounds": rounds,
            "winning_weighted_log_loss": winner["row_weighted_log_loss"],
            "configurations": summaries}


def feature_provenance() -> dict:
    """Explicit input contract; column names alone never imply compatibility."""
    return {"schema_version": 1, "feature_version": FEATURE_VERSION,
            "feature_algorithm_version": ALGORITHM_VERSION,
            "feature_order": FEATURE_ORDER, "source_mode": SOURCE_MODE,
            "preparation_manifest_sha256": MANIFEST_SHA256,
            "scheduled_rounds": "unknown", "debut_columns": "deferred",
            "orientation": ORIENTATION}
