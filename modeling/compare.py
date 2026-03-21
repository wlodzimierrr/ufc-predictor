"""Model comparison report — evaluates all models on the same test set.

Loads baselines, trained model artifacts, and optionally calibrated
variants, then produces a side-by-side metrics table and overlay
calibration plot.

Usage:
    python modeling/compare.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modeling.data import load_bout_data, temporal_split, FEATURE_COLS, describe_splits
from modeling.baselines import coin_flip_baseline, favorite_baseline, elo_baseline
from modeling.evaluate import compute_metrics, calibration_table, compare_models
from modeling.calibrate import assess_calibration, calibrate_isotonic
from modeling.artifacts import load_model, latest_artifact
from warehouse.db import get_connection


MODELS_DIR = Path(__file__).resolve().parent.parent / "models"


def _predict_artifact(artifact_dir: Path, X: np.ndarray | pd.DataFrame) -> np.ndarray:
    """Load a saved model artifact and return predictions on X."""
    model, _ = load_model(artifact_dir)
    return model.predict_proba(X)[:, 1]


def main() -> None:
    # ── Load data ────────────────────────────────────────────────────
    print("Loading bout data...")
    conn = get_connection()
    df = load_bout_data(conn)
    conn.close()

    train, val, test = temporal_split(df)
    describe_splits(train, val, test)

    y_test = test["label"].values
    X_test = test[FEATURE_COLS].apply(pd.to_numeric, errors="coerce")

    # ── Collect predictions ──────────────────────────────────────────
    results: dict[str, dict] = {}
    predictions: dict[str, np.ndarray] = {}

    # Baselines
    print("\n── Evaluating baselines ──────────────────────────────────────")
    for name, prob_fn in [
        ("Coin flip", lambda df: coin_flip_baseline(df)),
        ("Favorite (hard)", lambda df: favorite_baseline(df)),
        ("Favorite (soft)", lambda df: favorite_baseline(df, soft=True)),
        ("Elo baseline", lambda df: elo_baseline(df)),
    ]:
        y_prob = prob_fn(test)
        predictions[name] = y_prob
        results[name] = compute_metrics(y_test, y_prob)
        print(f"  {name}: log_loss={results[name]['log_loss']:.4f}")

    # Trained models
    print("\n── Evaluating trained models ─────────────────────────────────")
    for model_name, display_name in [("logreg", "Logistic Reg"), ("lgbm", "LightGBM")]:
        artifact_dir = latest_artifact(MODELS_DIR, model_name)
        if artifact_dir is None:
            print(f"  {display_name}: no artifact found, skipping")
            continue

        y_prob = _predict_artifact(artifact_dir, X_test)
        predictions[display_name] = y_prob
        results[display_name] = compute_metrics(y_test, y_prob)
        print(f"  {display_name}: log_loss={results[display_name]['log_loss']:.4f}")

        # Calibrated variant (isotonic on val set)
        y_val = val["label"].values
        X_val = val[FEATURE_COLS].apply(pd.to_numeric, errors="coerce")
        y_prob_val = _predict_artifact(artifact_dir, X_val)

        cal_before = assess_calibration(y_test, y_prob)
        if cal_before["ece"] > 0.05:
            y_prob_cal = calibrate_isotonic(y_prob_val, y_val, y_prob)
            cal_name = f"{display_name} (calibrated)"
            predictions[cal_name] = y_prob_cal
            results[cal_name] = compute_metrics(y_test, y_prob_cal)
            print(f"  {cal_name}: log_loss={results[cal_name]['log_loss']:.4f}  "
                  f"(ECE {cal_before['ece']:.3f} → {results[cal_name]['ece']:.3f})")

    # ── Comparison table ─────────────────────────────────────────────
    print("\n── Model comparison (test set) ───────────────────────────────")
    df_compare = compare_models(results)
    print(df_compare.to_string())

    # ── Save report ──────────────────────────────────────────────────
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = MODELS_DIR / "comparison_report.txt"
    with open(report_path, "w") as f:
        f.write("Model Comparison Report\n")
        f.write("=" * 70 + "\n\n")
        f.write(df_compare.to_string())
        f.write("\n\nSorted by Log Loss (ascending = better)\n")
    print(f"\n  Report saved to: {report_path}")

    # ── Overlay calibration plot ─────────────────────────────────────
    _save_calibration_plot(predictions, y_test, MODELS_DIR / "calibration_comparison.png")


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
    ax_cal.set_title("Calibration Comparison — All Models")
    ax_cal.legend(loc="upper left", fontsize=8)
    ax_cal.set_xlim(-0.02, 1.02)
    ax_cal.set_ylim(-0.02, 1.02)

    # Histogram of LightGBM predictions (or last model)
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
