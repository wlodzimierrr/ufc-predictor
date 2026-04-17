"""Train XGBoost for UFC fight prediction as an ensemble partner.

Loads v2 features with debut priors, tunes (max_depth, min_child_weight,
reg_lambda) via rolling CV (27 configs), applies Platt scaling, and saves
the artifact.

XGBoost handles NaN natively — no imputation needed.

Usage:
    python modeling/train_xgb.py
"""

from __future__ import annotations

import sys
import warnings
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modeling.data import (
    load_bout_data, temporal_split, rolling_cv,
    FEATURE_COLS_V2, describe_splits,
)
from modeling.evaluate import compute_metrics, calibration_table, print_report
from modeling.calibrate import calibrate_platt
from modeling.artifacts import save_model
from features.debut_prior import compute_debut_priors, apply_debut_features
from warehouse.db import get_connection


MODEL_NAME = "xgb"

BASE_PARAMS: dict = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "max_depth": 4,
    "learning_rate": 0.02,
    "n_estimators": 500,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 50,
    "reg_lambda": 1.0,
    "random_state": 42,
    "verbosity": 0,
}

TUNE_GRID = {
    "max_depth": [3, 4, 6],
    "min_child_weight": [20, 50, 100],
    "reg_lambda": [0.1, 1.0, 5.0],
}

EARLY_STOPPING_ROUNDS = 50


def _cv_evaluate(
    folds: list[tuple[pd.DataFrame, pd.DataFrame]],
    params: dict,
    feature_cols: list[str],
) -> tuple[float, float, list[float]]:
    """Run rolling CV and return (mean_ll, std_ll, fold_losses)."""
    fold_losses = []
    for train_df, val_df in folds:
        p = {**params}
        n_est = p.pop("n_estimators")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model = xgb.XGBClassifier(
                n_estimators=n_est,
                early_stopping_rounds=EARLY_STOPPING_ROUNDS,
                **p,
            )
            model.fit(
                train_df[feature_cols], train_df["label"].values,
                eval_set=[(val_df[feature_cols], val_df["label"].values)],
                verbose=False,
            )

        y_prob = model.predict_proba(val_df[feature_cols])[:, 1]
        ll = compute_metrics(val_df["label"].values, y_prob)["log_loss"]
        fold_losses.append(ll)

    return float(np.mean(fold_losses)), float(np.std(fold_losses)), fold_losses


def tune_hyperparams(
    folds: list[tuple[pd.DataFrame, pd.DataFrame]],
    feature_cols: list[str],
) -> tuple[dict, dict]:
    """Grid search over XGBoost hyperparameters using rolling CV.

    Returns:
        (best_params, all_results) where all_results maps str(combo) -> {mean, std}.
    """
    keys = list(TUNE_GRID.keys())
    combos = list(product(*TUNE_GRID.values()))
    all_results: dict[str, dict] = {}

    print(f"\n── Tuning {len(combos)} configurations x {len(folds)} folds ──────────")

    for i, combo in enumerate(combos):
        overrides = dict(zip(keys, combo))
        params = {**BASE_PARAMS, **overrides}

        mean_ll, std_ll, _ = _cv_evaluate(folds, params, feature_cols)
        all_results[str(combo)] = {"mean": mean_ll, "std": std_ll}

        if (i + 1) % 10 == 0:
            print(f"  ... {i + 1}/{len(combos)} configs evaluated")

    ranked = sorted(all_results.items(), key=lambda x: x[1]["mean"])

    # Print top 10
    print(f"\n  Top 10 configurations (by mean CV log loss):")
    print(f"  {'max_depth':>10s}  {'min_child_w':>11s}  {'reg_lambda':>10s}  "
          f"{'mean_ll':>8s}  {'std_ll':>7s}")
    print(f"  {'─'*10}  {'─'*11}  {'─'*10}  {'─'*8}  {'─'*7}")
    for combo_str, stats in ranked[:10]:
        combo = eval(combo_str)  # noqa: S307 — safe, we built the string
        marker = " <-" if combo_str == ranked[0][0] else ""
        print(f"  {combo[0]:>10d}  {combo[1]:>11d}  {combo[2]:>10.1f}  "
              f"{stats['mean']:>8.4f}  {stats['std']:>7.4f}{marker}")

    best_combo_str = ranked[0][0]
    best_combo = eval(best_combo_str)  # noqa: S307
    best_params = dict(zip(keys, best_combo))

    print(f"\n  Best: max_depth={best_params['max_depth']}, "
          f"min_child_weight={best_params['min_child_weight']}, "
          f"reg_lambda={best_params['reg_lambda']}")

    return best_params, all_results


def print_feature_importance(
    model: xgb.XGBClassifier,
    feature_names: list[str],
    top_n: int = 20,
) -> None:
    """Print top feature importances by gain."""
    importances = model.feature_importances_
    pairs = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)

    print(f"\n── Top {top_n} feature importances (gain) ──────────────────────")
    max_imp = pairs[0][1] if pairs else 1
    for name, imp in pairs[:top_n]:
        bar_len = int(40 * imp / max_imp) if max_imp > 0 else 0
        print(f"  {name:<45s} {imp:>8.4f}  {'█' * bar_len}")


def main() -> None:
    # ── Load data ────────────────────────────────────────────────────
    print("Loading bout data (v2 features)...")
    conn = get_connection()
    df = load_bout_data(conn, feature_cols=FEATURE_COLS_V2)
    conn.close()
    print(f"  Loaded {len(df):,} labeled bouts ({len(FEATURE_COLS_V2)} features)")

    # ── Temporal split ───────────────────────────────────────────────
    train, val, test = temporal_split(df)
    describe_splits(train, val, test)

    # ── Apply debut priors ───────────────────────────────────────────
    print("\nComputing debut priors from training data...")
    priors = compute_debut_priors(train)
    train = apply_debut_features(train, priors)
    val = apply_debut_features(val, priors)
    test = apply_debut_features(test, priors)

    debut_cols = ["debut_prior_win_prob_f1", "debut_height_adv", "debut_reach_adv"]
    feature_cols = FEATURE_COLS_V2 + debut_cols
    print(f"  Total features: {len(feature_cols)} ({len(FEATURE_COLS_V2)} v2 + {len(debut_cols)} debut)")

    # ── Rolling CV for hyperparameter tuning ─────────────────────────
    folds = rolling_cv(train)
    print(f"\n  Rolling CV: {len(folds)} folds from training data")
    best_overrides, tune_results = tune_hyperparams(folds, feature_cols)

    # ── Train final model ────────────────────────────────────────────
    print("\n── Training final model ──────────────────────────────────────")
    final_params = {**BASE_PARAMS, **best_overrides}
    n_est = final_params.pop("n_estimators")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model = xgb.XGBClassifier(
            n_estimators=n_est,
            early_stopping_rounds=EARLY_STOPPING_ROUNDS,
            **final_params,
        )
        model.fit(
            train[feature_cols], train["label"].values,
            eval_set=[(val[feature_cols], val["label"].values)],
            verbose=False,
        )

    best_iter = model.best_iteration
    print(f"  Trained with best_iteration={best_iter} (of max {n_est})")

    # ── Evaluate (uncalibrated) ──────────────────────────────────────
    y_val = val["label"].values
    y_prob_val = model.predict_proba(val[feature_cols])[:, 1]
    val_metrics_uncal = compute_metrics(y_val, y_prob_val)

    y_test = test["label"].values
    y_prob_test = model.predict_proba(test[feature_cols])[:, 1]
    test_metrics_uncal = compute_metrics(y_test, y_prob_test)

    print_report(val_metrics_uncal, f"{MODEL_NAME} (uncalibrated) — Validation")
    print_report(test_metrics_uncal, f"{MODEL_NAME} (uncalibrated) — Test")

    # ── Platt scaling calibration ────────────────────────────────────
    print("\n── Platt scaling calibration ─────────────────────────────────")
    y_prob_test_cal = calibrate_platt(y_prob_val, y_val, y_prob_test)
    test_metrics_cal = compute_metrics(y_test, y_prob_test_cal)

    y_prob_val_cal = calibrate_platt(y_prob_val, y_val, y_prob_val)
    val_metrics_cal = compute_metrics(y_val, y_prob_val_cal)

    print_report(val_metrics_cal, f"{MODEL_NAME} (calibrated) — Validation")
    print_report(test_metrics_cal, f"{MODEL_NAME} (calibrated) — Test")

    # ── Calibration table ────────────────────────────────────────────
    print("\n── Calibration table (test set, calibrated) ──────────────────")
    cal = calibration_table(y_test, y_prob_test_cal)
    print(cal.to_string(index=False))

    # ── Feature importance ───────────────────────────────────────────
    print_feature_importance(model, feature_cols)

    # ── Comparison ───────────────────────────────────────────────────
    print(f"\n── Comparison ────────────────────────────────────────────────")
    print(f"  {'Model':<35s} {'Log Loss':>10s} {'Accuracy':>10s} {'AUC':>10s} {'ECE':>10s}")
    print(f"  {'─'*35}  {'─'*10}  {'─'*10}  {'─'*10}  {'─'*10}")
    print(f"  {MODEL_NAME + ' (uncalibrated)':<35s} {test_metrics_uncal['log_loss']:>10.4f}  "
          f"{test_metrics_uncal['accuracy']:>10.4f}  {test_metrics_uncal['roc_auc']:>10.4f}  "
          f"{test_metrics_uncal['ece']:>10.4f}")
    print(f"  {MODEL_NAME + ' (calibrated)':<35s} {test_metrics_cal['log_loss']:>10.4f}  "
          f"{test_metrics_cal['accuracy']:>10.4f}  {test_metrics_cal['roc_auc']:>10.4f}  "
          f"{test_metrics_cal['ece']:>10.4f}")
    print(f"  {'LightGBM v2 (ref)':<35s} {'0.6374':>10s}  {'—':>10s}  {'—':>10s}  {'—':>10s}")

    # ── Save artifact ────────────────────────────────────────────────
    models_dir = Path(__file__).resolve().parent.parent / "models"
    metadata = {
        "model_name": MODEL_NAME,
        "feature_cols": feature_cols,
        "feature_version": 2,
        "debut_priors_applied": True,
        "train_rows": len(train),
        "val_rows": len(val),
        "test_rows": len(test),
        "val_date": str(val["event_date"].min().date()),
        "test_date": str(test["event_date"].min().date()),
        "hyperparameters": {
            **final_params,
            "n_estimators": n_est,
            "best_iteration": best_iter,
            "early_stopping_rounds": EARLY_STOPPING_ROUNDS,
        },
        "tune_results": tune_results,
    }
    metrics = {
        "val_uncalibrated": val_metrics_uncal,
        "test_uncalibrated": test_metrics_uncal,
        "val_calibrated": val_metrics_cal,
        "test_calibrated": test_metrics_cal,
    }
    artifact_dir = save_model(model, metadata, models_dir, metrics=metrics)
    print(f"\n  Artifact saved to: {artifact_dir}")


if __name__ == "__main__":
    main()
