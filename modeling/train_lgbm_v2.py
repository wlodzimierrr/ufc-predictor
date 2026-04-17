"""Train LightGBM v2 on the expanded feature set with debut priors.

Two-step process:
  Step 1 — Baseline check: retrain with Phase 4's best config on v2 features.
           If log loss improves by >= 0.005 over Phase 4 (0.6523), proceed to tuning.
  Step 2 — Focused tuning: 108-config grid search via rolling CV.
           Reject configs where fold std > 0.02.

After tuning, runs a feature selection pass: drops features with < 1% of total
gain, retrains, and keeps the trimmed model if log loss is unchanged or better.

Usage:
    python modeling/train_lgbm_v2.py
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

from modeling.data import (
    load_bout_data, temporal_split, rolling_cv,
    FEATURE_COLS, FEATURE_COLS_V2, describe_splits,
)
from modeling.evaluate import compute_metrics, calibration_table, print_report
from modeling.artifacts import save_model
from features.debut_prior import compute_debut_priors, apply_debut_features
from warehouse.db import get_connection


MODEL_NAME = "lgbm_v2"

# Phase 4 reference
PHASE4_TEST_LOGLOSS = 0.6523
IMPROVEMENT_THRESHOLD = 0.005

# Phase 4's best config — used as baseline for step 1
PHASE4_BEST: dict = {
    "objective": "binary",
    "metric": "binary_logloss",
    "verbosity": -1,
    "num_leaves": 15,
    "learning_rate": 0.02,
    "n_estimators": 500,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "min_child_samples": 50,
    "random_state": 42,
}

# Focused tuning grid (Step 2)
TUNE_GRID = {
    "num_leaves": [10, 15, 20, 31],
    "learning_rate": [0.01, 0.02, 0.05],
    "min_child_samples": [30, 50, 80],
    "reg_lambda": [0.0, 1.0, 5.0],
}

# Max fold std to accept a config
MAX_FOLD_STD = 0.02


def _train_and_eval(
    params: dict,
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    feature_cols: list[str],
) -> tuple[lgb.LGBMClassifier, dict]:
    """Train a LightGBM model and return (model, test_metrics-style dict)."""
    p = {**params}
    n_est = p.pop("n_estimators")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model = lgb.LGBMClassifier(n_estimators=n_est, **p)
        model.fit(
            train_df[feature_cols], train_df["label"].values,
            eval_set=[(val_df[feature_cols], val_df["label"].values)],
            callbacks=[lgb.early_stopping(50, verbose=False)],
        )
    return model


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
            model = lgb.LGBMClassifier(n_estimators=n_est, **p)
            model.fit(
                train_df[feature_cols], train_df["label"].values,
                eval_set=[(val_df[feature_cols], val_df["label"].values)],
                callbacks=[lgb.early_stopping(50, verbose=False)],
            )

        y_prob = model.predict_proba(val_df[feature_cols])[:, 1]
        ll = compute_metrics(val_df["label"].values, y_prob)["log_loss"]
        fold_losses.append(ll)

    return float(np.mean(fold_losses)), float(np.std(fold_losses)), fold_losses


def tune_hyperparams(
    folds: list[tuple[pd.DataFrame, pd.DataFrame]],
    feature_cols: list[str],
) -> tuple[dict, dict]:
    """Grid search over focused hyperparameter space using rolling CV.

    Rejects configs with fold std > MAX_FOLD_STD.

    Returns:
        (best_params, all_results) where all_results maps str(combo) → {mean, std}.
    """
    keys = list(TUNE_GRID.keys())
    combos = list(product(*TUNE_GRID.values()))
    all_results: dict[str, dict] = {}

    print(f"\n── Step 2: Tuning {len(combos)} configurations x {len(folds)} folds ──")

    for i, combo in enumerate(combos):
        overrides = dict(zip(keys, combo))
        params = {**PHASE4_BEST, **overrides}

        mean_ll, std_ll, _ = _cv_evaluate(folds, params, feature_cols)
        all_results[str(combo)] = {"mean": mean_ll, "std": std_ll}

        if (i + 1) % 20 == 0:
            print(f"  ... {i + 1}/{len(combos)} configs evaluated")

    # Filter by stability, then sort by mean
    stable = {k: v for k, v in all_results.items() if v["std"] <= MAX_FOLD_STD}
    if not stable:
        print(f"  WARNING: No configs with fold std <= {MAX_FOLD_STD}. "
              f"Using all configs.")
        stable = all_results

    ranked = sorted(stable.items(), key=lambda x: x[1]["mean"])

    # Print top 10
    print(f"\n  Top 10 configurations (stable, by mean CV log loss):")
    print(f"  {'num_leaves':>10s}  {'lr':>6s}  {'min_child':>10s}  {'reg_lambda':>10s}  "
          f"{'mean_ll':>8s}  {'std_ll':>7s}")
    print(f"  {'─'*10}  {'─'*6}  {'─'*10}  {'─'*10}  {'─'*8}  {'─'*7}")
    for combo_str, stats in ranked[:10]:
        combo = eval(combo_str)  # noqa: S307 — safe, we built the string
        marker = " <-" if combo_str == ranked[0][0] else ""
        print(f"  {combo[0]:>10d}  {combo[1]:>6.3f}  {combo[2]:>10d}  {combo[3]:>10.1f}  "
              f"{stats['mean']:>8.4f}  {stats['std']:>7.4f}{marker}")

    # Extract best config
    best_combo_str = ranked[0][0]
    best_combo = eval(best_combo_str)  # noqa: S307
    best_params = dict(zip(keys, best_combo))

    print(f"\n  Best: num_leaves={best_params['num_leaves']}, "
          f"lr={best_params['learning_rate']}, "
          f"min_child_samples={best_params['min_child_samples']}, "
          f"reg_lambda={best_params['reg_lambda']}")

    return best_params, all_results


def feature_importance_analysis(
    model: lgb.LGBMClassifier,
    feature_names: list[str],
    top_n: int = 20,
) -> tuple[list[tuple[str, float]], list[str]]:
    """Analyze feature importances. Return (ranked_pairs, low_gain_features).

    low_gain_features: features contributing < 1% of total gain.
    """
    importances = model.feature_importances_
    pairs = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)
    total_gain = sum(imp for _, imp in pairs)

    print(f"\n── Top {top_n} feature importances (gain) ──────────────────────")
    max_imp = pairs[0][1] if pairs else 1
    for name, imp in pairs[:top_n]:
        pct = 100.0 * imp / total_gain if total_gain > 0 else 0
        bar_len = int(40 * imp / max_imp) if max_imp > 0 else 0
        print(f"  {name:<45s} {imp:>8.0f} ({pct:>5.1f}%)  {'█' * bar_len}")

    # Identify low-gain features
    threshold = 0.01 * total_gain
    low_gain = [name for name, imp in pairs if imp < threshold]

    if low_gain:
        print(f"\n  Features with < 1% of total gain ({len(low_gain)}):")
        for name in low_gain:
            imp = dict(pairs)[name]
            pct = 100.0 * imp / total_gain if total_gain > 0 else 0
            print(f"    {name:<45s} {imp:>8.0f} ({pct:>5.1f}%)")

    return pairs, low_gain


def feature_selection_pass(
    train: pd.DataFrame,
    val: pd.DataFrame,
    test: pd.DataFrame,
    params: dict,
    full_feature_cols: list[str],
    low_gain_features: list[str],
    full_test_ll: float,
) -> tuple[list[str], lgb.LGBMClassifier, dict, dict, bool]:
    """Retrain without low-gain features. Keep trimmed model if log loss <= full model.

    Returns:
        (final_cols, final_model, val_metrics, test_metrics, was_trimmed)
    """
    if not low_gain_features:
        print("\n── Feature selection: no low-gain features to drop ──")
        return full_feature_cols, None, None, None, False

    trimmed_cols = [c for c in full_feature_cols if c not in low_gain_features]
    print(f"\n── Feature selection pass ──────────────────────────────────")
    print(f"  Dropping {len(low_gain_features)} features, retraining with {len(trimmed_cols)}")

    p = {**params}
    n_est = p.pop("n_estimators")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        trimmed_model = lgb.LGBMClassifier(n_estimators=n_est, **p)
        trimmed_model.fit(
            train[trimmed_cols], train["label"].values,
            eval_set=[(val[trimmed_cols], val["label"].values)],
            callbacks=[lgb.early_stopping(50, verbose=False)],
        )

    y_prob_val = trimmed_model.predict_proba(val[trimmed_cols])[:, 1]
    trimmed_val = compute_metrics(val["label"].values, y_prob_val)

    y_prob_test = trimmed_model.predict_proba(test[trimmed_cols])[:, 1]
    trimmed_test = compute_metrics(test["label"].values, y_prob_test)

    print(f"  Full model test log loss:    {full_test_ll:.4f}")
    print(f"  Trimmed model test log loss: {trimmed_test['log_loss']:.4f}")

    if trimmed_test["log_loss"] <= full_test_ll + 1e-4:
        print(f"  -> Keeping trimmed model ({len(trimmed_cols)} features)")
        return trimmed_cols, trimmed_model, trimmed_val, trimmed_test, True
    else:
        print(f"  -> Trimmed model is worse, keeping full model ({len(full_feature_cols)} features)")
        return full_feature_cols, None, None, None, False


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

    # Add debut prior columns to feature list
    debut_cols = ["debut_prior_win_prob_f1", "debut_height_adv", "debut_reach_adv"]
    feature_cols = FEATURE_COLS_V2 + debut_cols
    print(f"  Total features: {len(feature_cols)} ({len(FEATURE_COLS_V2)} v2 + {len(debut_cols)} debut)")

    # ── Step 1: Baseline check ───────────────────────────────────────
    print(f"\n── Step 1: Baseline check (Phase 4 config on v2 features) ──")
    print(f"  Phase 4 test log loss: {PHASE4_TEST_LOGLOSS:.4f}")

    model_baseline = _train_and_eval(PHASE4_BEST, train, val, feature_cols)

    y_prob_test = model_baseline.predict_proba(test[feature_cols])[:, 1]
    baseline_test = compute_metrics(test["label"].values, y_prob_test)

    y_prob_val = model_baseline.predict_proba(val[feature_cols])[:, 1]
    baseline_val = compute_metrics(val["label"].values, y_prob_val)

    print_report(baseline_val, f"{MODEL_NAME} baseline — Validation")
    print_report(baseline_test, f"{MODEL_NAME} baseline — Test")

    improvement = PHASE4_TEST_LOGLOSS - baseline_test["log_loss"]
    print(f"\n  Improvement over Phase 4: {improvement:+.4f} "
          f"({'PASS' if improvement >= IMPROVEMENT_THRESHOLD else 'BELOW THRESHOLD'})")

    # ── Step 2: Focused tuning (conditional) ─────────────────────────
    if improvement >= IMPROVEMENT_THRESHOLD:
        folds = rolling_cv(train)
        print(f"\n  Rolling CV: {len(folds)} folds from training data")
        best_overrides, tune_results = tune_hyperparams(folds, feature_cols)
        final_params = {**PHASE4_BEST, **best_overrides}
    else:
        print(f"\n  Skipping focused tuning (improvement {improvement:.4f} < {IMPROVEMENT_THRESHOLD})")
        print(f"  Investigating feature gains...")

        # Report which new features have zero or near-zero gain
        importances = model_baseline.feature_importances_
        pairs = sorted(zip(feature_cols, importances), key=lambda x: x[1], reverse=True)
        total = sum(imp for _, imp in pairs)
        new_features = [c for c in feature_cols if c not in FEATURE_COLS]
        print(f"\n  New feature gains:")
        for name in new_features:
            imp = dict(pairs).get(name, 0)
            pct = 100.0 * imp / total if total > 0 else 0
            print(f"    {name:<45s} {imp:>8.0f} ({pct:>5.1f}%)")

        final_params = PHASE4_BEST
        tune_results = {}

    # ── Train final model ────────────────────────────────────────────
    print("\n── Training final model ──────────────────────────────────────")
    final_model = _train_and_eval(final_params, train, val, feature_cols)

    best_iter = final_model.best_iteration_
    n_est = final_params.get("n_estimators", 500)
    print(f"  Trained with best_iteration={best_iter} (of max {n_est})")

    # ── Evaluate ─────────────────────────────────────────────────────
    y_val = val["label"].values
    y_prob_val = final_model.predict_proba(val[feature_cols])[:, 1]
    val_metrics = compute_metrics(y_val, y_prob_val)

    y_test = test["label"].values
    y_prob_test = final_model.predict_proba(test[feature_cols])[:, 1]
    test_metrics = compute_metrics(y_test, y_prob_test)

    print_report(val_metrics, f"{MODEL_NAME} — Validation")
    print_report(test_metrics, f"{MODEL_NAME} — Test")

    # ── Calibration table ────────────────────────────────────────────
    print("\n── Calibration table (test set) ──────────────────────────────")
    cal = calibration_table(y_test, y_prob_test)
    print(cal.to_string(index=False))

    # ── Feature importance + selection ───────────────────────────────
    ranked_pairs, low_gain = feature_importance_analysis(final_model, feature_cols)

    final_feature_cols = feature_cols
    dropped_features: list[str] = []

    if low_gain:
        (final_feature_cols, trimmed_model, trimmed_val,
         trimmed_test, was_trimmed) = feature_selection_pass(
            train, val, test, final_params,
            feature_cols, low_gain, test_metrics["log_loss"],
        )
        if was_trimmed:
            final_model = trimmed_model
            val_metrics = trimmed_val
            test_metrics = trimmed_test
            dropped_features = low_gain
            best_iter = final_model.best_iteration_

            # Re-evaluate and print
            print_report(val_metrics, f"{MODEL_NAME} (trimmed) — Validation")
            print_report(test_metrics, f"{MODEL_NAME} (trimmed) — Test")

    # ── Comparison row ───────────────────────────────────────────────
    print(f"\n── Comparison ────────────────────────────────────────────────")
    print(f"  {'Model':<30s} {'Log Loss':>10s} {'Accuracy':>10s} {'AUC':>10s} {'ECE':>10s}")
    print(f"  {'─'*30}  {'─'*10}  {'─'*10}  {'─'*10}  {'─'*10}")
    print(f"  {'Phase 4 LightGBM (v1)':<30s} {PHASE4_TEST_LOGLOSS:>10.4f}  {'—':>10s}  {'—':>10s}  {'—':>10s}")
    print(f"  {MODEL_NAME:<30s} {test_metrics['log_loss']:>10.4f}  "
          f"{test_metrics['accuracy']:>10.4f}  {test_metrics['roc_auc']:>10.4f}  "
          f"{test_metrics['ece']:>10.4f}")

    # ── Save artifact ────────────────────────────────────────────────
    models_dir = Path(__file__).resolve().parent.parent / "models"
    metadata = {
        "model_name": MODEL_NAME,
        "feature_cols": final_feature_cols,
        "feature_version": 2,
        "debut_priors_applied": True,
        "train_rows": len(train),
        "val_rows": len(val),
        "test_rows": len(test),
        "val_date": str(val["event_date"].min().date()),
        "test_date": str(test["event_date"].min().date()),
        "hyperparameters": {
            **final_params,
            "best_iteration": best_iter,
        },
        "phase4_test_logloss": PHASE4_TEST_LOGLOSS,
        "dropped_features": dropped_features,
        "tune_results": tune_results,
    }
    metrics = {"val": val_metrics, "test": test_metrics}
    artifact_dir = save_model(final_model, metadata, models_dir, metrics=metrics)
    print(f"\n  Artifact saved to: {artifact_dir}")


if __name__ == "__main__":
    main()
