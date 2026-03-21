"""Train a LightGBM model for UFC fight prediction.

Loads bout_features via modeling/data.py, tunes key hyperparameters via
rolling CV, evaluates on val/test sets, and saves the artifact.

LightGBM handles NaN natively — no imputation needed.

Usage:
    python modeling/train_lgbm.py
"""

from __future__ import annotations

import sys
import warnings
from itertools import product
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modeling.data import load_bout_data, temporal_split, rolling_cv, FEATURE_COLS, describe_splits
from modeling.evaluate import compute_metrics, calibration_table, print_report
from modeling.artifacts import save_model
from warehouse.db import get_connection


MODEL_NAME = "lgbm"

# Conservative defaults — small dataset (~8k rows, 29 features)
BASE_PARAMS: dict = {
    "objective": "binary",
    "metric": "binary_logloss",
    "verbosity": -1,
    "num_leaves": 31,
    "learning_rate": 0.05,
    "n_estimators": 500,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "min_child_samples": 20,
    "random_state": 42,
}

# Tuning grid — kept small to avoid overfitting on noisy CV folds
TUNE_GRID = {
    "num_leaves": [15, 31, 63],
    "learning_rate": [0.02, 0.05, 0.1],
    "min_child_samples": [10, 20, 50],
}


def tune_hyperparams(
    folds: list[tuple[pd.DataFrame, pd.DataFrame]],
) -> tuple[dict, dict]:
    """Grid search over key hyperparameters using rolling CV folds.

    Returns:
        (best_params, all_results) where all_results maps param_tuple → mean log loss.
    """
    keys = list(TUNE_GRID.keys())
    combos = list(product(*TUNE_GRID.values()))
    all_results: dict[tuple, float] = {}

    print(f"\n── Tuning {len(combos)} configurations × {len(folds)} folds ──────────")

    for combo in combos:
        overrides = dict(zip(keys, combo))
        fold_losses = []

        for train_df, val_df in folds:
            X_train = train_df[FEATURE_COLS]
            y_train = train_df["label"].values
            X_val = val_df[FEATURE_COLS]
            y_val = val_df["label"].values

            params = {**BASE_PARAMS, **overrides}
            n_est = params.pop("n_estimators")

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                model = lgb.LGBMClassifier(n_estimators=n_est, **params)
                model.fit(
                    X_train, y_train,
                    eval_set=[(X_val, y_val)],
                    callbacks=[lgb.early_stopping(50, verbose=False)],
                )

            y_prob = model.predict_proba(X_val)[:, 1]
            ll = compute_metrics(y_val, y_prob)["log_loss"]
            fold_losses.append(ll)

        mean_ll = float(np.mean(fold_losses))
        all_results[combo] = mean_ll

    # Find best
    best_combo = min(all_results, key=all_results.get)
    best_params = dict(zip(keys, best_combo))

    # Print top 10
    ranked = sorted(all_results.items(), key=lambda x: x[1])
    print(f"\n  Top 10 configurations (by mean CV log loss):")
    print(f"  {'num_leaves':>10s}  {'lr':>6s}  {'min_child':>10s}  {'log_loss':>10s}")
    print(f"  {'─'*10}  {'─'*6}  {'─'*10}  {'─'*10}")
    for combo, ll in ranked[:10]:
        marker = " ←" if combo == best_combo else ""
        print(f"  {combo[0]:>10d}  {combo[1]:>6.3f}  {combo[2]:>10d}  {ll:>10.4f}{marker}")

    print(f"\n  Best: num_leaves={best_params['num_leaves']}, "
          f"lr={best_params['learning_rate']}, "
          f"min_child_samples={best_params['min_child_samples']}")

    return best_params, {str(k): v for k, v in all_results.items()}


def print_feature_importance(model: lgb.LGBMClassifier, feature_names: list[str], top_n: int = 20) -> None:
    """Print top feature importances by gain."""
    importances = model.feature_importances_
    pairs = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)

    print(f"\n── Top {top_n} feature importances (gain) ──────────────────────")
    max_imp = pairs[0][1] if pairs else 1
    for name, imp in pairs[:top_n]:
        bar_len = int(40 * imp / max_imp) if max_imp > 0 else 0
        print(f"  {name:<45s} {imp:>8.0f}  {'█' * bar_len}")


def main() -> None:
    # ── Load data ────────────────────────────────────────────────────
    print("Loading bout data...")
    conn = get_connection()
    df = load_bout_data(conn)
    conn.close()
    print(f"  Loaded {len(df):,} labeled bouts")

    # ── Temporal split ───────────────────────────────────────────────
    train, val, test = temporal_split(df)
    describe_splits(train, val, test)

    # ── Rolling CV for hyperparameter tuning ─────────────────────────
    folds = rolling_cv(train)
    print(f"\n  Rolling CV: {len(folds)} folds from training data")
    best_overrides, tune_results = tune_hyperparams(folds)

    # ── Train final model on full training set with early stopping ───
    print("\n── Training final model ──────────────────────────────────────")
    final_params = {**BASE_PARAMS, **best_overrides}
    n_est = final_params.pop("n_estimators")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model = lgb.LGBMClassifier(n_estimators=n_est, **final_params)
        model.fit(
            train[FEATURE_COLS], train["label"].values,
            eval_set=[(val[FEATURE_COLS], val["label"].values)],
            callbacks=[lgb.early_stopping(50, verbose=False)],
        )

    best_iter = model.best_iteration_
    print(f"  Trained with best_iteration={best_iter} (of max {n_est})")

    # ── Evaluate ─────────────────────────────────────────────────────
    y_val = val["label"].values
    y_prob_val = model.predict_proba(val[FEATURE_COLS])[:, 1]
    val_metrics = compute_metrics(y_val, y_prob_val)

    y_test = test["label"].values
    y_prob_test = model.predict_proba(test[FEATURE_COLS])[:, 1]
    test_metrics = compute_metrics(y_test, y_prob_test)

    print_report(val_metrics, f"{MODEL_NAME} — Validation")
    print_report(test_metrics, f"{MODEL_NAME} — Test")

    # ── Calibration table ────────────────────────────────────────────
    print("\n── Calibration table (test set) ──────────────────────────────")
    cal = calibration_table(y_test, y_prob_test)
    print(cal.to_string(index=False))

    # ── Feature importance ───────────────────────────────────────────
    print_feature_importance(model, FEATURE_COLS)

    # ── Save artifact ────────────────────────────────────────────────
    models_dir = Path(__file__).resolve().parent.parent / "models"
    metadata = {
        "model_name": MODEL_NAME,
        "feature_cols": FEATURE_COLS,
        "feature_version": 1,
        "train_rows": len(train),
        "val_rows": len(val),
        "val_date": str(val["event_date"].min().date()),
        "test_date": str(test["event_date"].min().date()),
        "hyperparameters": {**BASE_PARAMS, **best_overrides, "n_estimators": n_est, "best_iteration": best_iter},
        "tune_results": tune_results,
    }
    metrics = {"val": val_metrics, "test": test_metrics}
    artifact_dir = save_model(model, metadata, models_dir, metrics=metrics)
    print(f"\n  Artifact saved to: {artifact_dir}")


if __name__ == "__main__":
    main()
