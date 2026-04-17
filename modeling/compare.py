"""Model comparison report — evaluates all models on the same test set.

Loads baselines, Phase 4 and Phase 5 model artifacts, and calibrated
variants. Produces a unified metrics table, overlay calibration plot,
and a production model selection record.

Usage:
    python modeling/compare.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modeling.data import (
    load_bout_data, temporal_split,
    FEATURE_COLS, FEATURE_COLS_V2, describe_splits,
)
from modeling.baselines import coin_flip_baseline, favorite_baseline, elo_baseline
from modeling.evaluate import compute_metrics, calibration_table, compare_models
from modeling.calibrate import assess_calibration, calibrate_isotonic, calibrate_platt
from modeling.artifacts import load_model, latest_artifact
from features.debut_prior import compute_debut_priors, apply_debut_features
from warehouse.db import get_connection


MODELS_DIR = Path(__file__).resolve().parent.parent / "models"


def main() -> None:
    # ── Load data (v2 superset) ─────────────────────────────────────
    print("Loading bout data (v2 features)...")
    conn = get_connection()
    df = load_bout_data(conn, feature_cols=FEATURE_COLS_V2)
    conn.close()

    train, val, test = temporal_split(df)
    describe_splits(train, val, test)

    # Apply debut priors for v2 models
    print("\nComputing debut priors from training data...")
    priors = compute_debut_priors(train)
    train = apply_debut_features(train, priors)
    val = apply_debut_features(val, priors)
    test = apply_debut_features(test, priors)

    y_val = val["label"].values
    y_test = test["label"].values

    results: dict[str, dict] = {}
    predictions: dict[str, np.ndarray] = {}

    # ── Baselines ───────────────────────────────────────────────────
    print("\n── Evaluating baselines ──────────────────────────────────────")
    for name, prob_fn in [
        ("Coin flip", lambda d: coin_flip_baseline(d)),
        ("Favorite (hard)", lambda d: favorite_baseline(d)),
        ("Favorite (soft)", lambda d: favorite_baseline(d, soft=True)),
        ("Elo baseline", lambda d: elo_baseline(d)),
    ]:
        y_prob = prob_fn(test)
        predictions[name] = y_prob
        results[name] = compute_metrics(y_test, y_prob)
        print(f"  {name}: log_loss={results[name]['log_loss']:.4f}")

    # ── Phase 4 models (v1 features) ────────────────────────────────
    print("\n── Evaluating Phase 4 models ─────────────────────────────────")
    for model_name, display_name in [("logreg", "Logistic Reg"), ("lgbm", "LightGBM")]:
        artifact_dir = latest_artifact(MODELS_DIR, model_name)
        if artifact_dir is None:
            print(f"  {display_name}: no artifact found, skipping")
            continue

        model, meta = load_model(artifact_dir)
        feat_cols = meta.get("feature_cols", FEATURE_COLS)

        y_prob = model.predict_proba(test[feat_cols])[:, 1]
        predictions[display_name] = y_prob
        results[display_name] = compute_metrics(y_test, y_prob)
        print(f"  {display_name}: log_loss={results[display_name]['log_loss']:.4f}")

        # Calibrated variant (isotonic on val set)
        y_prob_val = model.predict_proba(val[feat_cols])[:, 1]
        cal_before = assess_calibration(y_test, y_prob)
        if cal_before["ece"] > 0.05:
            y_prob_cal = calibrate_isotonic(y_prob_val, y_val, y_prob)
            cal_name = f"{display_name} (calibrated)"
            predictions[cal_name] = y_prob_cal
            results[cal_name] = compute_metrics(y_test, y_prob_cal)
            print(f"  {cal_name}: log_loss={results[cal_name]['log_loss']:.4f}  "
                  f"(ECE {cal_before['ece']:.3f} -> {results[cal_name]['ece']:.3f})")

    # ── Phase 5 models (v2 features + debut priors) ─────────────────
    print("\n── Evaluating Phase 5 models ─────────────────────────────────")

    # LightGBM v2
    lgbm_v2_dir = latest_artifact(MODELS_DIR, "lgbm_v2")
    if lgbm_v2_dir:
        model, meta = load_model(lgbm_v2_dir)
        feat_cols = meta["feature_cols"]

        y_prob = model.predict_proba(test[feat_cols])[:, 1]
        predictions["LightGBM v2"] = y_prob
        results["LightGBM v2"] = compute_metrics(y_test, y_prob)
        print(f"  LightGBM v2: log_loss={results['LightGBM v2']['log_loss']:.4f}")

        # Calibrated
        y_prob_val = model.predict_proba(val[feat_cols])[:, 1]
        cal_before = assess_calibration(y_test, y_prob)
        if cal_before["ece"] > 0.08:
            y_prob_cal = calibrate_platt(y_prob_val, y_val, y_prob)
            predictions["LightGBM v2 (calibrated)"] = y_prob_cal
            results["LightGBM v2 (calibrated)"] = compute_metrics(y_test, y_prob_cal)
            print(f"  LightGBM v2 (calibrated): log_loss="
                  f"{results['LightGBM v2 (calibrated)']['log_loss']:.4f}  "
                  f"(ECE {cal_before['ece']:.3f} -> "
                  f"{results['LightGBM v2 (calibrated)']['ece']:.3f})")

    # XGBoost
    xgb_dir = latest_artifact(MODELS_DIR, "xgb")
    if xgb_dir:
        model, meta = load_model(xgb_dir)
        feat_cols = meta["feature_cols"]

        y_prob = model.predict_proba(test[feat_cols])[:, 1]
        predictions["XGBoost"] = y_prob
        results["XGBoost"] = compute_metrics(y_test, y_prob)
        print(f"  XGBoost: log_loss={results['XGBoost']['log_loss']:.4f}")

        # Platt-calibrated
        y_prob_val = model.predict_proba(val[feat_cols])[:, 1]
        y_prob_cal = calibrate_platt(y_prob_val, y_val, y_prob)
        predictions["XGBoost (calibrated)"] = y_prob_cal
        results["XGBoost (calibrated)"] = compute_metrics(y_test, y_prob_cal)
        print(f"  XGBoost (calibrated): log_loss="
              f"{results['XGBoost (calibrated)']['log_loss']:.4f}")

    # Ensemble
    ensemble_dir = latest_artifact(MODELS_DIR, "ensemble")
    if ensemble_dir:
        meta_path = ensemble_dir / "metadata.json"
        if meta_path.exists():
            ensemble_meta = json.loads(meta_path.read_text())
            metrics_path = ensemble_dir / "metrics.json"
            if metrics_path.exists():
                ensemble_metrics = json.loads(metrics_path.read_text())
                ens_type = ensemble_meta.get("ensemble_type", "blend")
                ens_name = f"Ensemble ({ens_type})"
                results[ens_name] = ensemble_metrics["test"]
                print(f"  {ens_name}: log_loss={ensemble_metrics['test']['log_loss']:.4f}"
                      f"  (from saved metrics)")

    # ── Comparison table ─────────────────────────────────────────────
    print("\n── Model comparison (test set) ───────────────────────────────")
    df_compare = compare_models(results)
    print(df_compare.to_string())

    # ── Save report ──────────────────────────────────────────────────
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = MODELS_DIR / "comparison_report.txt"
    with open(report_path, "w") as f:
        f.write("Model Comparison Report — Phase 4 + Phase 5\n")
        f.write("=" * 70 + "\n\n")
        f.write(df_compare.to_string())
        f.write("\n\nSorted by Log Loss (ascending = better)\n")
    print(f"\n  Report saved to: {report_path}")

    # ── Production model selection ───────────────────────────────────
    _select_production_model(results, df_compare)

    # ── Overlay calibration plot ─────────────────────────────────────
    _save_calibration_plot(predictions, y_test, MODELS_DIR / "calibration_comparison.png")


def _select_production_model(results: dict[str, dict], df_compare: pd.DataFrame) -> None:
    """Select the best model and write production_model.json."""
    # Filter to trained models only (exclude baselines)
    baseline_names = {"Coin flip", "Favorite (hard)", "Favorite (soft)", "Elo baseline"}
    trained = {k: v for k, v in results.items() if k not in baseline_names}

    if not trained:
        print("\n  No trained models to select from.")
        return

    # Sort by log loss
    ranked = sorted(trained.items(), key=lambda x: x[1]["log_loss"])
    best_name, best_metrics = ranked[0]
    runner_up_name = ranked[1][0] if len(ranked) > 1 else "none"

    # Map display name to artifact path
    name_to_artifact = {
        "Logistic Reg": "logreg",
        "Logistic Reg (calibrated)": "logreg",
        "LightGBM": "lgbm",
        "LightGBM (calibrated)": "lgbm",
        "LightGBM v2": "lgbm_v2",
        "LightGBM v2 (calibrated)": "lgbm_v2",
        "XGBoost": "xgb",
        "XGBoost (calibrated)": "xgb",
    }

    artifact_name = name_to_artifact.get(best_name, best_name.lower().replace(" ", "_"))
    artifact_dir = latest_artifact(MODELS_DIR, artifact_name)
    artifact_path = str(artifact_dir) if artifact_dir else "N/A"

    # Rationale
    if "calibrated" in best_name.lower():
        rationale = (f"{best_name} selected: lowest test log loss ({best_metrics['log_loss']:.4f}) "
                     f"with post-hoc calibration improving ECE to {best_metrics['ece']:.4f}.")
    else:
        rationale = (f"{best_name} selected: lowest test log loss ({best_metrics['log_loss']:.4f}) "
                     f"among all Phase 4 and Phase 5 models.")

    selection = {
        "selected_model": best_name,
        "artifact_path": artifact_path,
        "test_log_loss": best_metrics["log_loss"],
        "test_brier": best_metrics["brier_score"],
        "test_auc": best_metrics["roc_auc"],
        "test_ece": best_metrics["ece"],
        "rationale": rationale,
        "runner_up": runner_up_name,
        "selected_at": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
    }

    selection_path = MODELS_DIR / "production_model.json"
    selection_path.write_text(json.dumps(selection, indent=2), encoding="utf-8")

    print(f"\n── Production model selection ────────────────────────────────")
    print(f"  Selected: {best_name}")
    print(f"  Log Loss: {best_metrics['log_loss']:.4f}")
    print(f"  AUC:      {best_metrics['roc_auc']:.4f}")
    print(f"  ECE:      {best_metrics['ece']:.4f}")
    print(f"  Runner-up: {runner_up_name}")
    print(f"  Written to: {selection_path}")


def _save_calibration_plot(
    predictions: dict[str, np.ndarray],
    y_test: np.ndarray,
    save_path: Path,
) -> None:
    """Save an overlay reliability diagram with all models."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 1, figsize=(10, 8), height_ratios=[3, 1],
                             sharex=True, gridspec_kw={"hspace": 0.05})
    ax_cal, ax_hist = axes

    ax_cal.plot([0, 1], [0, 1], "k--", alpha=0.3, label="Perfect")

    colors = plt.cm.tab10(np.linspace(0, 1, len(predictions)))
    for (name, y_prob), color in zip(predictions.items(), colors):
        cal = calibration_table(y_test, y_prob)
        valid = cal.dropna(subset=["mean_predicted"])
        ece = assess_calibration(y_test, y_prob)["ece"]
        ax_cal.plot(
            valid["mean_predicted"], valid["mean_actual"],
            "o-", label=f"{name} (ECE={ece:.3f})", color=color, markersize=5, alpha=0.8,
        )

    ax_cal.set_ylabel("Actual win rate")
    ax_cal.set_title("Calibration Comparison — All Models (Phase 4 + 5)")
    ax_cal.legend(loc="upper left", fontsize=7)
    ax_cal.set_xlim(-0.02, 1.02)
    ax_cal.set_ylim(-0.02, 1.02)

    # Histogram of best model predictions
    last_name = list(predictions.keys())[-1]
    ax_hist.hist(predictions[last_name], bins=50, color="#3498db", alpha=0.7, edgecolor="white")
    ax_hist.set_xlabel("Predicted probability")
    ax_hist.set_ylabel("Count")

    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Calibration plot saved to: {save_path}")


if __name__ == "__main__":
    main()
