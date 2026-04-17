"""Train an ensemble model blending LightGBM v2, LogReg, XGBoost, and Elo.

Two strategies:
  1. Simple weighted blend — grid-search weights in 0.1 increments on val set.
  2. Stacking variant (fallback) — logistic regression meta-learner on
     out-of-fold predictions using temporal CV folds.

Decision rule: ensemble must achieve log loss < best_single - 0.003 to be
promoted as production candidate.

Usage:
    python modeling/train_ensemble.py
"""

from __future__ import annotations

import json
import sys
import warnings
from datetime import datetime, timezone
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss as sk_log_loss

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modeling.data import (
    load_bout_data, temporal_split, rolling_cv,
    FEATURE_COLS, FEATURE_COLS_V2, describe_splits,
)
from modeling.evaluate import compute_metrics, calibration_table, print_report
from modeling.artifacts import load_model, latest_artifact
from features.debut_prior import compute_debut_priors, apply_debut_features
from warehouse.db import get_connection


MODEL_NAME = "ensemble"
DECISION_THRESHOLD = 0.003  # ensemble must beat best single by this margin
WEIGHT_STEP = 0.1


def _elo_proba(diff_elo: np.ndarray) -> np.ndarray:
    """Convert Elo difference to win probability via logistic formula."""
    return 1.0 / (1.0 + np.power(10, -diff_elo / 400.0))


def _generate_weight_tuples(
    n_models: int,
    step: float = 0.1,
) -> list[tuple[float, ...]]:
    """Generate all weight tuples summing to 1.0 in increments of step."""
    steps = int(round(1.0 / step))
    combos = []
    for parts in product(range(steps + 1), repeat=n_models):
        if sum(parts) == steps:
            combos.append(tuple(p * step for p in parts))
    return combos


def _blend_probs(
    probs: list[np.ndarray],
    weights: tuple[float, ...],
) -> np.ndarray:
    """Weighted average of probability arrays."""
    result = np.zeros_like(probs[0])
    for p, w in zip(probs, weights):
        result += w * p
    return result


def weighted_blend_search(
    val_probs: list[np.ndarray],
    y_val: np.ndarray,
    model_names: list[str],
) -> tuple[tuple[float, ...], dict]:
    """Grid search over weight combinations on validation set.

    Returns:
        (best_weights, all_results) sorted by log loss.
    """
    combos = _generate_weight_tuples(len(model_names), WEIGHT_STEP)
    print(f"\n── Weighted blend: searching {len(combos)} weight combinations ──")

    results: list[tuple[tuple[float, ...], float]] = []
    for weights in combos:
        blended = _blend_probs(val_probs, weights)
        ll = float(sk_log_loss(y_val, blended))
        results.append((weights, ll))

    results.sort(key=lambda x: x[1])

    # Print top 10
    print(f"\n  Top 10 weight configurations (by val log loss):")
    header = "  " + "  ".join(f"{n:>8s}" for n in model_names) + "   log_loss"
    print(header)
    print("  " + "  ".join("─" * 8 for _ in model_names) + "  ────────")
    for weights, ll in results[:10]:
        row = "  " + "  ".join(f"{w:>8.1f}" for w in weights) + f"   {ll:.4f}"
        if weights == results[0][0]:
            row += " <-"
        print(row)

    best_weights = results[0][0]
    print(f"\n  Best weights: {dict(zip(model_names, best_weights))}")

    all_results = {str(w): ll for w, ll in results}
    return best_weights, all_results


def stacking_meta_learner(
    folds: list[tuple[pd.DataFrame, pd.DataFrame]],
    models: list,
    model_feature_cols: list[list[str]],
    model_names: list[str],
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
) -> tuple[np.ndarray, np.ndarray, LogisticRegression]:
    """Train a logistic regression meta-learner on out-of-fold predictions.

    Uses temporal CV folds to generate OOF predictions, then trains a
    meta-learner that maps stacked predictions to final probability.

    Returns:
        (val_probs, test_probs, meta_model)
    """
    print(f"\n── Stacking: training meta-learner on {len(folds)} temporal folds ──")

    # Collect OOF predictions for each model
    n_train = len(train_df)
    oof_matrix = np.full((n_train, len(models) + 1), np.nan)  # +1 for Elo
    oof_labels = np.full(n_train, np.nan)

    for fold_idx, (fold_train, fold_val) in enumerate(folds):
        # Find indices of fold_val rows in the full train_df
        val_indices = train_df.index.isin(fold_val.index)

        for m_idx, (model, feat_cols) in enumerate(zip(models, model_feature_cols)):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                probs = model.predict_proba(fold_val[feat_cols])[:, 1]
            oof_matrix[val_indices, m_idx] = probs

        # Elo component
        oof_matrix[val_indices, -1] = _elo_proba(fold_val["diff_elo"].values)
        oof_labels[val_indices] = fold_val["label"].values

    # Drop rows that were never in any validation fold
    valid_mask = ~np.isnan(oof_labels)
    X_meta = oof_matrix[valid_mask]
    y_meta = oof_labels[valid_mask].astype(int)
    print(f"  OOF rows: {valid_mask.sum()} / {n_train}")

    # Train meta-learner
    meta = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000, random_state=42)
    meta.fit(X_meta, y_meta)

    print(f"  Meta-learner coefficients: {dict(zip(model_names + ['elo'], meta.coef_[0]))}")
    print(f"  Meta-learner intercept: {meta.intercept_[0]:.4f}")

    # Generate val and test predictions
    val_stack = np.column_stack([
        model.predict_proba(val_df[feat_cols])[:, 1]
        for model, feat_cols in zip(models, model_feature_cols)
    ] + [_elo_proba(val_df["diff_elo"].values)])

    test_stack = np.column_stack([
        model.predict_proba(test_df[feat_cols])[:, 1]
        for model, feat_cols in zip(models, model_feature_cols)
    ] + [_elo_proba(test_df["diff_elo"].values)])

    val_probs = meta.predict_proba(val_stack)[:, 1]
    test_probs = meta.predict_proba(test_stack)[:, 1]

    return val_probs, test_probs, meta


def main() -> None:
    models_dir = Path(__file__).resolve().parent.parent / "models"

    # ── Load component models ────────────────────────────────────────
    print("Loading component models...")

    lgbm_v2_dir = latest_artifact(models_dir, "lgbm_v2")
    xgb_dir = latest_artifact(models_dir, "xgb")
    logreg_dir = latest_artifact(models_dir, "logreg")

    if not all([lgbm_v2_dir, xgb_dir, logreg_dir]):
        missing = []
        if not lgbm_v2_dir: missing.append("lgbm_v2")
        if not xgb_dir: missing.append("xgb")
        if not logreg_dir: missing.append("logreg")
        print(f"  ERROR: Missing model artifacts: {missing}")
        print(f"  Run train_lgbm_v2, train_xgb, and train_logreg first.")
        sys.exit(1)

    lgbm_v2_model, lgbm_v2_meta = load_model(lgbm_v2_dir)
    xgb_model, xgb_meta = load_model(xgb_dir)
    logreg_model, logreg_meta = load_model(logreg_dir)

    lgbm_v2_features = lgbm_v2_meta["feature_cols"]
    xgb_features = xgb_meta["feature_cols"]
    logreg_features = logreg_meta["feature_cols"]

    print(f"  LightGBM v2: {lgbm_v2_dir.name} ({len(lgbm_v2_features)} features)")
    print(f"  XGBoost:     {xgb_dir.name} ({len(xgb_features)} features)")
    print(f"  LogReg:      {logreg_dir.name} ({len(logreg_features)} features)")

    # ── Load data (v2 superset covers all models) ────────────────────
    print("\nLoading bout data...")
    conn = get_connection()
    df = load_bout_data(conn, feature_cols=FEATURE_COLS_V2)
    conn.close()

    train, val, test = temporal_split(df)
    describe_splits(train, val, test)

    # Apply debut priors (needed by lgbm_v2 and xgb)
    print("\nComputing debut priors from training data...")
    priors = compute_debut_priors(train)
    train = apply_debut_features(train, priors)
    val = apply_debut_features(val, priors)
    test = apply_debut_features(test, priors)

    y_val = val["label"].values
    y_test = test["label"].values

    # ── Generate predictions from each component ─────────────────────
    print("\n── Component model predictions ───────────────────────────────")

    val_probs_lgbm = lgbm_v2_model.predict_proba(val[lgbm_v2_features])[:, 1]
    val_probs_xgb = xgb_model.predict_proba(val[xgb_features])[:, 1]
    val_probs_logreg = logreg_model.predict_proba(val[logreg_features])[:, 1]
    val_probs_elo = _elo_proba(val["diff_elo"].values)

    test_probs_lgbm = lgbm_v2_model.predict_proba(test[lgbm_v2_features])[:, 1]
    test_probs_xgb = xgb_model.predict_proba(test[xgb_features])[:, 1]
    test_probs_logreg = logreg_model.predict_proba(test[logreg_features])[:, 1]
    test_probs_elo = _elo_proba(test["diff_elo"].values)

    # Print individual model val performance
    model_names = ["lgbm_v2", "xgb", "logreg", "elo"]
    val_probs_all = [val_probs_lgbm, val_probs_xgb, val_probs_logreg, val_probs_elo]
    test_probs_all = [test_probs_lgbm, test_probs_xgb, test_probs_logreg, test_probs_elo]

    print(f"\n  {'Model':<15s} {'Val LL':>8s}  {'Test LL':>8s}  {'Test Acc':>8s}  {'Test AUC':>8s}")
    print(f"  {'─'*15} {'─'*8}  {'─'*8}  {'─'*8}  {'─'*8}")
    best_single_test_ll = float("inf")
    best_single_name = ""
    for name, vp, tp in zip(model_names, val_probs_all, test_probs_all):
        v_ll = float(sk_log_loss(y_val, vp))
        t_metrics = compute_metrics(y_test, tp)
        print(f"  {name:<15s} {v_ll:>8.4f}  {t_metrics['log_loss']:>8.4f}  "
              f"{t_metrics['accuracy']:>8.4f}  {t_metrics['roc_auc']:>8.4f}")
        if t_metrics["log_loss"] < best_single_test_ll:
            best_single_test_ll = t_metrics["log_loss"]
            best_single_name = name

    print(f"\n  Best single model: {best_single_name} (test LL = {best_single_test_ll:.4f})")
    target_ll = best_single_test_ll - DECISION_THRESHOLD
    print(f"  Ensemble target: < {target_ll:.4f}")

    # ── Strategy 1: Simple weighted blend ────────────────────────────
    best_weights, blend_results = weighted_blend_search(
        val_probs_all, y_val, model_names,
    )

    blend_val_probs = _blend_probs(val_probs_all, best_weights)
    blend_test_probs = _blend_probs(test_probs_all, best_weights)

    blend_val_metrics = compute_metrics(y_val, blend_val_probs)
    blend_test_metrics = compute_metrics(y_test, blend_test_probs)

    print_report(blend_val_metrics, f"{MODEL_NAME} (blend) — Validation")
    print_report(blend_test_metrics, f"{MODEL_NAME} (blend) — Test")

    blend_passes = blend_test_metrics["log_loss"] < target_ll

    # ── Strategy 2: Stacking (if blend doesn't pass) ─────────────────
    stack_test_metrics = None
    stack_meta = None
    if not blend_passes:
        print(f"\n  Blend log loss {blend_test_metrics['log_loss']:.4f} >= target {target_ll:.4f}")
        print(f"  Trying stacking variant...")

        folds = rolling_cv(train)
        models = [lgbm_v2_model, xgb_model, logreg_model]
        model_feat_cols = [lgbm_v2_features, xgb_features, logreg_features]

        stack_val_probs, stack_test_probs, stack_meta = stacking_meta_learner(
            folds, models, model_feat_cols,
            ["lgbm_v2", "xgb", "logreg"],
            train, val, test,
        )

        stack_val_metrics = compute_metrics(y_val, stack_val_probs)
        stack_test_metrics = compute_metrics(y_test, stack_test_probs)

        print_report(stack_val_metrics, f"{MODEL_NAME} (stacked) — Validation")
        print_report(stack_test_metrics, f"{MODEL_NAME} (stacked) — Test")

    # ── Decision ─────────────────────────────────────────────────────
    print(f"\n── Decision ──────────────────────────────────────────────────")

    # Pick the best ensemble variant
    final_test_metrics = blend_test_metrics
    final_val_metrics = blend_val_metrics
    ensemble_type = "blend"
    final_weights = best_weights

    if stack_test_metrics and stack_test_metrics["log_loss"] < blend_test_metrics["log_loss"]:
        final_test_metrics = stack_test_metrics
        final_val_metrics = stack_val_metrics
        ensemble_type = "stacked"

    passes_decision = final_test_metrics["log_loss"] < target_ll
    status = "PROMOTED" if passes_decision else "NO MEANINGFUL LIFT"

    print(f"  Best ensemble type: {ensemble_type}")
    print(f"  Ensemble test log loss: {final_test_metrics['log_loss']:.4f}")
    print(f"  Best single test log loss: {best_single_test_ll:.4f}")
    print(f"  Target (best - {DECISION_THRESHOLD}): {target_ll:.4f}")
    print(f"  Decision: {status}")

    # ── Calibration table ────────────────────────────────────────────
    final_test_probs = blend_test_probs if ensemble_type == "blend" else stack_test_probs
    print(f"\n── Calibration table (test set, {ensemble_type}) ────────────────")
    cal = calibration_table(y_test, final_test_probs)
    print(cal.to_string(index=False))

    # ── Comparison ───────────────────────────────────────────────────
    print(f"\n── Final comparison ──────────────────────────────────────────")
    print(f"  {'Model':<30s} {'Log Loss':>10s} {'Accuracy':>10s} {'AUC':>10s} {'ECE':>10s}")
    print(f"  {'─'*30}  {'─'*10}  {'─'*10}  {'─'*10}  {'─'*10}")
    for name, tp in zip(model_names, test_probs_all):
        m = compute_metrics(y_test, tp)
        print(f"  {name:<30s} {m['log_loss']:>10.4f}  {m['accuracy']:>10.4f}  "
              f"{m['roc_auc']:>10.4f}  {m['ece']:>10.4f}")
    print(f"  {'ensemble (' + ensemble_type + ')':<30s} {final_test_metrics['log_loss']:>10.4f}  "
          f"{final_test_metrics['accuracy']:>10.4f}  {final_test_metrics['roc_auc']:>10.4f}  "
          f"{final_test_metrics['ece']:>10.4f}")

    # ── Save artifact ────────────────────────────────────────────────
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    artifact_dir = models_dir / MODEL_NAME / timestamp
    artifact_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "model_name": MODEL_NAME,
        "ensemble_type": ensemble_type,
        "decision": status,
        "component_models": {
            "lgbm_v2": str(lgbm_v2_dir),
            "xgb": str(xgb_dir),
            "logreg": str(logreg_dir),
            "elo": "diff_elo logistic formula",
        },
        "best_single_model": best_single_name,
        "best_single_test_logloss": best_single_test_ll,
        "target_logloss": target_ll,
        "train_rows": len(train),
        "val_rows": len(val),
        "test_rows": len(test),
        "val_date": str(val["event_date"].min().date()),
        "test_date": str(test["event_date"].min().date()),
    }

    if ensemble_type == "blend":
        metadata["blend_weights"] = dict(zip(model_names, best_weights))
    else:
        metadata["stacking_coefficients"] = dict(
            zip(["lgbm_v2", "xgb", "logreg", "elo"],
                stack_meta.coef_[0].tolist())
        )
        metadata["stacking_intercept"] = float(stack_meta.intercept_[0])

    (artifact_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, default=str), encoding="utf-8",
    )

    metrics = {"val": final_val_metrics, "test": final_test_metrics}
    (artifact_dir / "metrics.json").write_text(
        json.dumps(metrics, indent=2, default=str), encoding="utf-8",
    )

    print(f"\n  Artifact saved to: {artifact_dir}")


if __name__ == "__main__":
    main()
