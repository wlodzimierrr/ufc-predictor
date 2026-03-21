"""Train a regularized logistic regression for UFC fight prediction.

Loads bout_features via modeling/data.py, imputes + standardizes features,
tunes regularization strength C via rolling CV, evaluates on val/test sets,
and saves the artifact via modeling/artifacts.py.

Usage:
    python modeling/train_logreg.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modeling.data import load_bout_data, temporal_split, rolling_cv, FEATURE_COLS, describe_splits
from modeling.evaluate import compute_metrics, calibration_table, print_report
from modeling.artifacts import save_model
from warehouse.db import get_connection


MODEL_NAME = "logreg"
C_GRID = [0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0]


def _make_pipeline(C: float) -> Pipeline:
    """Build an impute → scale → logistic regression pipeline."""
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(
            C=C,
            l1_ratio=0,
            solver="lbfgs",
            max_iter=1000,
            random_state=42,
        )),
    ])


def tune_C(folds: list[tuple[pd.DataFrame, pd.DataFrame]]) -> tuple[float, dict]:
    """Select the best C by mean CV log loss across rolling folds.

    Returns:
        (best_C, results_dict) where results_dict maps C → list of fold log losses.
    """
    results: dict[float, list[float]] = {c: [] for c in C_GRID}

    for fold_idx, (train_df, val_df) in enumerate(folds):
        X_train = train_df[FEATURE_COLS].values
        y_train = train_df["label"].values
        X_val = val_df[FEATURE_COLS].values
        y_val = val_df["label"].values

        for c in C_GRID:
            pipe = _make_pipeline(c)
            pipe.fit(X_train, y_train)
            y_prob = pipe.predict_proba(X_val)[:, 1]
            ll = compute_metrics(y_val, y_prob)["log_loss"]
            results[c].append(ll)

    # Print CV results
    print("\n── Rolling CV results (log loss) ─────────────────────────────")
    print(f"  {'C':>8s}  ", end="")
    for i in range(len(folds)):
        print(f"  Fold {i}", end="")
    print("     Mean")
    print(f"  {'─'*8}  ", end="")
    for _ in folds:
        print(f"  {'─'*6}", end="")
    print(f"  {'─'*8}")

    best_c = min(C_GRID, key=lambda c: np.mean(results[c]))
    for c in C_GRID:
        marker = " ←" if c == best_c else ""
        vals = results[c]
        mean = np.mean(vals)
        print(f"  {c:>8.3f}  ", end="")
        for v in vals:
            print(f"  {v:.4f}", end="")
        print(f"  {mean:>8.4f}{marker}")

    print(f"\n  Best C = {best_c}")
    return best_c, results


def print_coefficients(pipe: Pipeline, feature_names: list[str]) -> None:
    """Print feature coefficients sorted by absolute value."""
    coefs = pipe.named_steps["clf"].coef_[0]
    pairs = sorted(zip(feature_names, coefs), key=lambda x: abs(x[1]), reverse=True)

    print("\n── Feature coefficients (sorted by |coef|) ───────────────────")
    for name, coef in pairs:
        bar = "+" if coef > 0 else "-"
        print(f"  {name:<45s} {coef:>+8.4f}  {bar * min(int(abs(coef) * 20), 40)}")


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

    # ── Rolling CV for C tuning ──────────────────────────────────────
    folds = rolling_cv(train)
    print(f"\n  Rolling CV: {len(folds)} folds from training data")
    best_c, cv_results = tune_C(folds)

    # ── Train final model on full training set ───────────────────────
    print("\n── Training final model ──────────────────────────────────────")
    pipe = _make_pipeline(best_c)
    X_train = train[FEATURE_COLS].values
    y_train = train["label"].values
    pipe.fit(X_train, y_train)
    print(f"  Trained on {len(train):,} rows with C={best_c}")

    # ── Evaluate ─────────────────────────────────────────────────────
    X_val = val[FEATURE_COLS].values
    y_val = val["label"].values
    y_prob_val = pipe.predict_proba(X_val)[:, 1]
    val_metrics = compute_metrics(y_val, y_prob_val)

    X_test = test[FEATURE_COLS].values
    y_test = test["label"].values
    y_prob_test = pipe.predict_proba(X_test)[:, 1]
    test_metrics = compute_metrics(y_test, y_prob_test)

    print_report(val_metrics, f"{MODEL_NAME} — Validation")
    print_report(test_metrics, f"{MODEL_NAME} — Test")

    # ── Calibration table ────────────────────────────────────────────
    print("\n── Calibration table (test set) ──────────────────────────────")
    cal = calibration_table(y_test, y_prob_test)
    print(cal.to_string(index=False))

    # ── Coefficients ─────────────────────────────────────────────────
    print_coefficients(pipe, FEATURE_COLS)

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
        "hyperparameters": {"C": best_c, "l1_ratio": 0, "solver": "lbfgs"},
        "cv_results": {str(c): float(np.mean(v)) for c, v in cv_results.items()},
    }
    metrics = {"val": val_metrics, "test": test_metrics}
    artifact_dir = save_model(pipe, metadata, models_dir, metrics=metrics)
    print(f"\n  Artifact saved to: {artifact_dir}")


if __name__ == "__main__":
    main()
