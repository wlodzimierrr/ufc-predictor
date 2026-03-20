"""Evaluation utilities for UFC fight prediction models.

Computes metrics, calibration tables, and comparison reports.
All functions are model-agnostic — they take y_true and y_prob arrays.

Usage:
    from modeling.evaluate import compute_metrics, calibration_table, print_report
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)


# ── Metrics ───────────────────────────────────────────────────────────────────

def compute_metrics(y_true: np.ndarray, y_prob: np.ndarray) -> dict:
    """Compute all evaluation metrics for binary probability predictions.

    Args:
        y_true: Ground truth labels (0 or 1).
        y_prob: Predicted probability of label=1.

    Returns:
        Dict with keys: accuracy, log_loss, brier_score, roc_auc, ece.
    """
    y_true = np.asarray(y_true, dtype=int)
    y_prob = np.asarray(y_prob, dtype=float)
    y_pred = (y_prob >= 0.5).astype(int)

    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "log_loss": float(log_loss(y_true, y_prob)),
        "brier_score": float(brier_score_loss(y_true, y_prob)),
        "roc_auc": float(roc_auc_score(y_true, y_prob)),
        "ece": float(_expected_calibration_error(y_true, y_prob)),
    }


def _expected_calibration_error(
    y_true: np.ndarray, y_prob: np.ndarray, n_bins: int = 10
) -> float:
    """Expected Calibration Error: weighted average of |predicted - actual| per bin."""
    bin_edges = np.linspace(0, 1, n_bins + 1)
    ece = 0.0
    n = len(y_true)
    for lo, hi in zip(bin_edges[:-1], bin_edges[1:]):
        mask = (y_prob > lo) & (y_prob <= hi) if lo > 0 else (y_prob >= lo) & (y_prob <= hi)
        count = mask.sum()
        if count == 0:
            continue
        avg_pred = y_prob[mask].mean()
        avg_actual = y_true[mask].mean()
        ece += (count / n) * abs(avg_pred - avg_actual)
    return ece


# ── Calibration ───────────────────────────────────────────────────────────────

def calibration_table(
    y_true: np.ndarray, y_prob: np.ndarray, n_bins: int = 10
) -> pd.DataFrame:
    """Bin predictions and compute mean predicted vs actual win rate per bin.

    Returns a DataFrame with columns: bin, mean_predicted, mean_actual, count.
    """
    y_true = np.asarray(y_true, dtype=int)
    y_prob = np.asarray(y_prob, dtype=float)

    bin_edges = np.linspace(0, 1, n_bins + 1)
    rows = []
    for lo, hi in zip(bin_edges[:-1], bin_edges[1:]):
        mask = (y_prob > lo) & (y_prob <= hi) if lo > 0 else (y_prob >= lo) & (y_prob <= hi)
        count = int(mask.sum())
        if count == 0:
            rows.append({
                "bin": f"{lo:.1f}-{hi:.1f}",
                "mean_predicted": None,
                "mean_actual": None,
                "count": 0,
            })
        else:
            rows.append({
                "bin": f"{lo:.1f}-{hi:.1f}",
                "mean_predicted": float(y_prob[mask].mean()),
                "mean_actual": float(y_true[mask].mean()),
                "count": count,
            })

    return pd.DataFrame(rows)


def plot_calibration(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    model_name: str = "Model",
    save_path: str | None = None,
    ax=None,
) -> None:
    """Plot a reliability diagram with prediction histogram.

    Args:
        y_true: Ground truth labels.
        y_prob: Predicted probabilities.
        model_name: Label for the plot legend.
        save_path: If provided, save the figure to this path.
        ax: Optional matplotlib axes. If None, creates a new figure.
    """
    import matplotlib.pyplot as plt

    cal = calibration_table(y_true, y_prob)
    cal_valid = cal.dropna(subset=["mean_predicted"])

    if ax is None:
        fig, axes = plt.subplots(2, 1, figsize=(8, 7), height_ratios=[3, 1],
                                 sharex=True, gridspec_kw={"hspace": 0.05})
        ax_cal, ax_hist = axes
        own_fig = True
    else:
        ax_cal = ax
        ax_hist = None
        own_fig = False

    # Reliability diagram
    ax_cal.plot([0, 1], [0, 1], "k--", alpha=0.3, label="Perfect")
    ax_cal.plot(
        cal_valid["mean_predicted"], cal_valid["mean_actual"],
        "o-", label=model_name, markersize=6,
    )
    ax_cal.set_ylabel("Actual win rate")
    ax_cal.set_title(f"Calibration — {model_name}")
    ax_cal.legend(loc="upper left")
    ax_cal.set_xlim(-0.02, 1.02)
    ax_cal.set_ylim(-0.02, 1.02)

    # Histogram
    if ax_hist is not None:
        ax_hist.hist(y_prob, bins=50, color="#3498db", alpha=0.7, edgecolor="white")
        ax_hist.set_xlabel("Predicted probability")
        ax_hist.set_ylabel("Count")

    if save_path and own_fig:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"Saved calibration plot to {save_path}")

    if own_fig:
        plt.tight_layout()
        plt.show()


# ── Reporting ─────────────────────────────────────────────────────────────────

def print_report(metrics: dict, name: str = "Model") -> None:
    """Print a formatted single-model evaluation report."""
    print(f"\n{'─' * 50}")
    print(f"  {name}")
    print(f"{'─' * 50}")
    print(f"  Accuracy:    {metrics['accuracy']:.4f}")
    print(f"  Log Loss:    {metrics['log_loss']:.4f}")
    print(f"  Brier Score: {metrics['brier_score']:.4f}")
    print(f"  ROC AUC:     {metrics['roc_auc']:.4f}")
    print(f"  ECE:         {metrics['ece']:.4f}")
    print(f"{'─' * 50}")


def compare_models(results: dict[str, dict]) -> pd.DataFrame:
    """Build a side-by-side metric comparison table.

    Args:
        results: Dict mapping model name → metrics dict (output of compute_metrics).

    Returns:
        DataFrame with models as rows and metrics as columns, sorted by log_loss.
    """
    rows = []
    for name, metrics in results.items():
        rows.append({"model": name, **metrics})

    df = pd.DataFrame(rows).set_index("model")
    df = df.sort_values("log_loss")

    # Rename for display
    df.columns = [c.replace("_", " ").title() for c in df.columns]
    return df
