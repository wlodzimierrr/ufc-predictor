"""Probability calibration assessment and post-hoc correction.

Provides ECE/MCE assessment, reliability diagrams, and isotonic/Platt
scaling for improving model calibration.

Usage:
    from modeling.calibrate import assess_calibration, calibrate_isotonic, calibrate_platt
"""

from __future__ import annotations

import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression


# ── Assessment ───────────────────────────────────────────────────────────────

def assess_calibration(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    n_bins: int = 10,
) -> dict:
    """Compute calibration statistics: ECE, MCE, and per-bin breakdown.

    Args:
        y_true: Ground truth labels (0 or 1).
        y_prob: Predicted probability of label=1.
        n_bins: Number of equal-width bins.

    Returns:
        Dict with keys: ece, mce, n_bins, bins (list of per-bin dicts).
    """
    y_true = np.asarray(y_true, dtype=int)
    y_prob = np.asarray(y_prob, dtype=float)

    bin_edges = np.linspace(0, 1, n_bins + 1)
    bins = []
    ece = 0.0
    mce = 0.0
    n = len(y_true)

    for lo, hi in zip(bin_edges[:-1], bin_edges[1:]):
        mask = (y_prob > lo) & (y_prob <= hi) if lo > 0 else (y_prob >= lo) & (y_prob <= hi)
        count = int(mask.sum())
        if count == 0:
            bins.append({
                "bin": f"{lo:.1f}-{hi:.1f}",
                "count": 0,
                "mean_predicted": None,
                "mean_actual": None,
                "gap": None,
            })
            continue

        mean_pred = float(y_prob[mask].mean())
        mean_actual = float(y_true[mask].mean())
        gap = abs(mean_pred - mean_actual)
        weighted_gap = (count / n) * gap

        ece += weighted_gap
        mce = max(mce, gap)

        bins.append({
            "bin": f"{lo:.1f}-{hi:.1f}",
            "count": count,
            "mean_predicted": mean_pred,
            "mean_actual": mean_actual,
            "gap": gap,
        })

    return {
        "ece": float(ece),
        "mce": float(mce),
        "n_bins": n_bins,
        "bins": bins,
    }


# ── Calibration methods ─────────────────────────────────────────────────────

def calibrate_isotonic(
    y_prob_train: np.ndarray,
    y_true_train: np.ndarray,
    y_prob_test: np.ndarray,
) -> np.ndarray:
    """Fit isotonic regression on training probabilities and transform test probabilities.

    Args:
        y_prob_train: Predicted probabilities on the calibration set.
        y_true_train: True labels for the calibration set.
        y_prob_test: Predicted probabilities to be recalibrated.

    Returns:
        Recalibrated probabilities for the test set, clipped to [0, 1].
    """
    ir = IsotonicRegression(out_of_bounds="clip")
    ir.fit(np.asarray(y_prob_train, dtype=float), np.asarray(y_true_train, dtype=int))
    calibrated = ir.predict(np.asarray(y_prob_test, dtype=float))
    return np.clip(calibrated, 0.0, 1.0)


def calibrate_platt(
    y_prob_train: np.ndarray,
    y_true_train: np.ndarray,
    y_prob_test: np.ndarray,
) -> np.ndarray:
    """Fit Platt scaling (logistic regression on log-odds) and transform test probabilities.

    Args:
        y_prob_train: Predicted probabilities on the calibration set.
        y_true_train: True labels for the calibration set.
        y_prob_test: Predicted probabilities to be recalibrated.

    Returns:
        Recalibrated probabilities for the test set.
    """
    y_prob_train = np.asarray(y_prob_train, dtype=float)
    y_true_train = np.asarray(y_true_train, dtype=int)
    y_prob_test = np.asarray(y_prob_test, dtype=float)

    # Convert to log-odds, clipping to avoid inf
    eps = 1e-8
    train_clipped = np.clip(y_prob_train, eps, 1 - eps)
    test_clipped = np.clip(y_prob_test, eps, 1 - eps)

    log_odds_train = np.log(train_clipped / (1 - train_clipped)).reshape(-1, 1)
    log_odds_test = np.log(test_clipped / (1 - test_clipped)).reshape(-1, 1)

    lr = LogisticRegression(C=1e10, solver="lbfgs", max_iter=1000)
    lr.fit(log_odds_train, y_true_train)
    calibrated = lr.predict_proba(log_odds_test)[:, 1]
    return calibrated


# ── Plotting ─────────────────────────────────────────────────────────────────

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

    cal = assess_calibration(y_true, y_prob)

    # Extract non-empty bins
    preds = [b["mean_predicted"] for b in cal["bins"] if b["count"] > 0]
    actuals = [b["mean_actual"] for b in cal["bins"] if b["count"] > 0]

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
    ax_cal.plot(preds, actuals, "o-", label=f"{model_name} (ECE={cal['ece']:.3f})", markersize=6)
    ax_cal.set_ylabel("Actual win rate")
    ax_cal.set_title(f"Calibration — {model_name}")
    ax_cal.legend(loc="upper left")
    ax_cal.set_xlim(-0.02, 1.02)
    ax_cal.set_ylim(-0.02, 1.02)

    # Histogram
    if ax_hist is not None:
        ax_hist.hist(np.asarray(y_prob), bins=50, color="#3498db", alpha=0.7, edgecolor="white")
        ax_hist.set_xlabel("Predicted probability")
        ax_hist.set_ylabel("Count")

    if save_path and own_fig:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"Saved calibration plot to {save_path}")

    if own_fig:
        plt.tight_layout()
        plt.close(fig)


# ── Convenience ──────────────────────────────────────────────────────────────

def print_calibration_report(name: str, cal: dict) -> None:
    """Print a formatted calibration assessment."""
    print(f"\n  {name}:")
    print(f"    ECE = {cal['ece']:.4f}    MCE = {cal['mce']:.4f}")
    print(f"    {'Bin':<10s}  {'Count':>6s}  {'Predicted':>10s}  {'Actual':>8s}  {'Gap':>6s}")
    print(f"    {'─'*10}  {'─'*6}  {'─'*10}  {'─'*8}  {'─'*6}")
    for b in cal["bins"]:
        if b["count"] == 0:
            print(f"    {b['bin']:<10s}  {0:>6d}{'':>10s}{'':>8s}{'':>6s}")
        else:
            print(f"    {b['bin']:<10s}  {b['count']:>6d}  {b['mean_predicted']:>10.4f}  "
                  f"{b['mean_actual']:>8.4f}  {b['gap']:>6.4f}")
