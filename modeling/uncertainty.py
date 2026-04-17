"""Uncertainty flagging and confidence band analysis.

Flags predictions in the uncertain probability band and provides
analysis of what makes those fights structurally different.

Usage:
    python modeling/uncertainty.py
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modeling.data import (
    load_bout_data, temporal_split, FEATURE_COLS_V2, describe_splits,
)
from modeling.evaluate import compute_metrics
from modeling.artifacts import load_model, latest_artifact
from features.debut_prior import compute_debut_priors, apply_debut_features
from warehouse.db import get_connection


# ── Public API ───────────────────────────────────────────────────────────────

def flag_uncertain(
    y_prob: np.ndarray,
    low: float = 0.40,
    high: float = 0.60,
) -> np.ndarray:
    """Return boolean mask for predictions in the uncertain band."""
    y_prob = np.asarray(y_prob, dtype=float)
    return (y_prob >= low) & (y_prob <= high)


def confidence_tier(y_prob: np.ndarray) -> np.ndarray:
    """Assign confidence tier labels to predictions.

    Returns:
        Array of strings: "high", "medium", or "toss-up".
    """
    y_prob = np.asarray(y_prob, dtype=float)
    tiers = np.full(len(y_prob), "medium", dtype=object)
    tiers[flag_uncertain(y_prob, 0.40, 0.60)] = "toss-up"
    tiers[(y_prob >= 0.70) | (y_prob <= 0.30)] = "high"
    return tiers


def uncertainty_report(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    model,
    X: pd.DataFrame,
    feature_names: list[str],
) -> dict:
    """Analyze the uncertain prediction band.

    Returns a dict with metrics, SHAP analysis, and feature distribution
    comparison between uncertain and confident predictions.
    """
    y_true = np.asarray(y_true, dtype=int)
    y_prob = np.asarray(y_prob, dtype=float)

    uncertain_mask = flag_uncertain(y_prob)
    confident_mask = ~uncertain_mask

    tiers = confidence_tier(y_prob)
    tier_counts = {t: int((tiers == t).sum()) for t in ["high", "medium", "toss-up"]}

    report: dict = {
        "total_fights": len(y_true),
        "tier_distribution": tier_counts,
    }

    # ── Metrics by tier ──────────────────────────────────────────────
    for tier_name, mask in [("uncertain", uncertain_mask), ("confident", confident_mask)]:
        if mask.sum() < 2:
            report[f"{tier_name}_metrics"] = {"count": int(mask.sum())}
            continue

        m = compute_metrics(y_true[mask], y_prob[mask])
        m["count"] = int(mask.sum())
        m["mean_predicted"] = float(y_prob[mask].mean())
        m["mean_actual"] = float(y_true[mask].mean())
        report[f"{tier_name}_metrics"] = m

    # ── SHAP analysis on uncertain subset ────────────────────────────
    try:
        import shap

        X_uncertain = X.loc[uncertain_mask, feature_names]

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X_uncertain)

        # For binary classification, shap_values may be a list
        if isinstance(shap_values, list):
            shap_values = shap_values[1]

        mean_abs_shap = np.abs(shap_values).mean(axis=0)
        shap_pairs = sorted(
            zip(feature_names, mean_abs_shap),
            key=lambda x: x[1], reverse=True,
        )
        report["shap_top5_uncertain"] = [
            {"feature": name, "mean_abs_shap": float(val)}
            for name, val in shap_pairs[:5]
        ]

    except ImportError:
        report["shap_top5_uncertain"] = "shap not installed"
    except Exception as e:
        report["shap_top5_uncertain"] = f"SHAP failed: {e}"

    # ── Feature distribution comparison ──────────────────────────────
    dist_comparison = []
    for feat in feature_names:
        if feat not in X.columns:
            continue
        vals_unc = X.loc[uncertain_mask, feat].dropna()
        vals_conf = X.loc[confident_mask, feat].dropna()

        if len(vals_unc) < 5 or len(vals_conf) < 5:
            continue

        mean_diff = abs(float(vals_unc.mean()) - float(vals_conf.mean()))
        pooled_std = float(pd.concat([vals_unc, vals_conf]).std())
        effect_size = mean_diff / pooled_std if pooled_std > 0 else 0.0

        dist_comparison.append({
            "feature": feat,
            "uncertain_mean": float(vals_unc.mean()),
            "confident_mean": float(vals_conf.mean()),
            "effect_size": effect_size,
        })

    dist_comparison.sort(key=lambda x: x["effect_size"], reverse=True)
    report["feature_distribution_top10"] = dist_comparison[:10]

    return report


def format_report(report: dict) -> str:
    """Format the uncertainty report as human-readable text."""
    lines = []
    lines.append("=" * 70)
    lines.append("  UNCERTAINTY ANALYSIS REPORT")
    lines.append("=" * 70)

    lines.append(f"\n  Total test fights: {report['total_fights']}")
    lines.append(f"\n  Tier distribution:")
    for tier, count in report["tier_distribution"].items():
        pct = 100.0 * count / report["total_fights"]
        lines.append(f"    {tier:<10s}  {count:>5d}  ({pct:>5.1f}%)")

    for label in ["uncertain", "confident"]:
        m = report.get(f"{label}_metrics", {})
        lines.append(f"\n  {label.upper()} band metrics:")
        lines.append(f"    Count:          {m.get('count', 0)}")
        if m.get("count", 0) >= 2:
            lines.append(f"    Accuracy:       {m['accuracy']:.4f}")
            lines.append(f"    Log Loss:       {m['log_loss']:.4f}")
            lines.append(f"    Brier Score:    {m['brier_score']:.4f}")
            lines.append(f"    Mean predicted: {m['mean_predicted']:.4f}")
            lines.append(f"    Mean actual:    {m['mean_actual']:.4f}")

    # SHAP
    shap_data = report.get("shap_top5_uncertain")
    lines.append(f"\n  Top 5 SHAP features (uncertain band):")
    if isinstance(shap_data, list):
        for entry in shap_data:
            lines.append(f"    {entry['feature']:<45s}  {entry['mean_abs_shap']:.4f}")
    else:
        lines.append(f"    {shap_data}")

    # Feature distributions
    lines.append(f"\n  Feature distribution differences (uncertain vs confident):")
    lines.append(f"    {'Feature':<45s}  {'Unc mean':>10s}  {'Conf mean':>10s}  {'Effect':>8s}")
    lines.append(f"    {'─'*45}  {'─'*10}  {'─'*10}  {'─'*8}")
    for entry in report.get("feature_distribution_top10", []):
        lines.append(
            f"    {entry['feature']:<45s}  {entry['uncertain_mean']:>10.4f}  "
            f"{entry['confident_mean']:>10.4f}  {entry['effect_size']:>8.4f}"
        )

    lines.append("\n" + "=" * 70)
    return "\n".join(lines)


# ── CLI entry point ──────────────────────────────────────────────────────────

def main() -> None:
    models_dir = Path(__file__).resolve().parent.parent / "models"

    # Load best model (LightGBM v2)
    print("Loading LightGBM v2 model...")
    lgbm_dir = latest_artifact(models_dir, "lgbm_v2")
    if not lgbm_dir:
        print("ERROR: No lgbm_v2 artifact found. Run train_lgbm_v2 first.")
        sys.exit(1)

    model, meta = load_model(lgbm_dir)
    feature_cols = meta["feature_cols"]
    print(f"  Loaded from {lgbm_dir.name}")

    # Load data
    print("Loading bout data...")
    conn = get_connection()
    df = load_bout_data(conn, feature_cols=FEATURE_COLS_V2)
    conn.close()

    train, val, test = temporal_split(df)
    describe_splits(train, val, test)

    # Apply debut priors
    priors = compute_debut_priors(train)
    test = apply_debut_features(test, priors)

    y_test = test["label"].values
    y_prob = model.predict_proba(test[feature_cols])[:, 1]

    # Generate report
    print("\nRunning uncertainty analysis...")
    report = uncertainty_report(y_test, y_prob, model, test, feature_cols)

    # Format and save
    report_text = format_report(report)
    print(report_text)

    output_path = models_dir / "uncertainty_report.txt"
    output_path.write_text(report_text, encoding="utf-8")
    print(f"\n  Report saved to: {output_path}")


if __name__ == "__main__":
    main()
