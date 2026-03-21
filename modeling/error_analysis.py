"""Error analysis — segment model predictions to find systematic weaknesses.

Breaks down test-set performance by weight class, title fights, debut
fighters, era, and confidence bucket.

Usage:
    python modeling/error_analysis.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modeling.data import load_bout_data, temporal_split, FEATURE_COLS, describe_splits
from modeling.evaluate import compute_metrics
from modeling.artifacts import load_model, latest_artifact
from warehouse.db import get_connection


MODELS_DIR = Path(__file__).resolve().parent.parent / "models"


# ── Segment analysis helpers ─────────────────────────────────────────────────

def _segment_metrics(y_true: np.ndarray, y_prob: np.ndarray, mask: np.ndarray) -> dict | None:
    """Compute metrics for a boolean mask subset. Returns None if <5 samples."""
    if mask.sum() < 5:
        return None
    return compute_metrics(y_true[mask], y_prob[mask])


def analyze_by_weight_class(test: pd.DataFrame, y_prob: np.ndarray) -> list[dict]:
    """Per-division accuracy and log loss."""
    y_true = test["label"].values
    rows = []
    for wc in sorted(test["weight_class"].dropna().unique()):
        mask = (test["weight_class"] == wc).values
        m = _segment_metrics(y_true, y_prob, mask)
        if m:
            rows.append({"segment": wc, "n": int(mask.sum()), **m})
    return sorted(rows, key=lambda r: r["log_loss"])


def analyze_by_title(test: pd.DataFrame, y_prob: np.ndarray) -> list[dict]:
    """Title fights vs non-title."""
    y_true = test["label"].values
    rows = []
    for label, name in [(1, "Title fight"), (0, "Non-title")]:
        mask = (test["is_title_fight"] == label).values
        m = _segment_metrics(y_true, y_prob, mask)
        if m:
            rows.append({"segment": name, "n": int(mask.sum()), **m})
    return rows


def analyze_by_debut(test: pd.DataFrame, y_prob: np.ndarray) -> list[dict]:
    """Debut fighters: both debuting, one debuting, neither."""
    y_true = test["label"].values
    rows = []

    both = (test["both_debuting"] == 1).values
    m = _segment_metrics(y_true, y_prob, both)
    if m:
        rows.append({"segment": "Both debuting", "n": int(both.sum()), **m})

    # One debuting: career_fights diff is extreme (one has 0 fights)
    one = (~both) & (
        (test["diff_career_fights"].apply(pd.to_numeric, errors="coerce").abs() > 5).values
        | test["diff_career_fights"].isna().values
    )
    m = _segment_metrics(y_true, y_prob, one)
    if m:
        rows.append({"segment": "Experience mismatch", "n": int(one.sum()), **m})

    neither = (~both) & (~one)
    m = _segment_metrics(y_true, y_prob, neither)
    if m:
        rows.append({"segment": "Both experienced", "n": int(neither.sum()), **m})

    return rows


def analyze_by_era(test: pd.DataFrame, y_prob: np.ndarray) -> list[dict]:
    """Split by year."""
    y_true = test["label"].values
    test_years = test["event_date"].dt.year
    rows = []
    for year in sorted(test_years.unique()):
        mask = (test_years == year).values
        m = _segment_metrics(y_true, y_prob, mask)
        if m:
            rows.append({"segment": str(year), "n": int(mask.sum()), **m})
    return rows


def analyze_by_confidence(y_true: np.ndarray, y_prob: np.ndarray) -> list[dict]:
    """Accuracy per confidence bucket (distance from 0.5)."""
    conf = np.abs(y_prob - 0.5)
    edges = [0.0, 0.05, 0.10, 0.15, 0.20, 0.30, 0.50]
    rows = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        mask = (conf >= lo) & (conf < hi) if hi < 0.50 else (conf >= lo) & (conf <= hi)
        m = _segment_metrics(y_true, y_prob, mask)
        prob_label = f"p={0.5-hi:.2f}–{0.5-lo:.2f} / {0.5+lo:.2f}–{0.5+hi:.2f}"
        if m:
            rows.append({"segment": prob_label, "n": int(mask.sum()), **m})
    return rows


# ── Reporting ────────────────────────────────────────────────────────────────

def _print_segment_table(title: str, rows: list[dict], file=None) -> None:
    """Print a formatted segment table."""
    def out(s):
        print(s)
        if file:
            file.write(s + "\n")

    out(f"\n{'─' * 70}")
    out(f"  {title}")
    out(f"{'─' * 70}")
    if not rows:
        out("  (no data)")
        return

    out(f"  {'Segment':<35s} {'N':>5s}  {'Acc':>6s}  {'LogLoss':>8s}  {'Brier':>6s}  {'AUC':>6s}  {'ECE':>6s}")
    out(f"  {'─'*35} {'─'*5}  {'─'*6}  {'─'*8}  {'─'*6}  {'─'*6}  {'─'*6}")
    for r in rows:
        out(f"  {r['segment']:<35s} {r['n']:>5d}  {r['accuracy']:>6.3f}  "
            f"{r['log_loss']:>8.4f}  {r['brier_score']:>6.3f}  {r['roc_auc']:>6.3f}  {r['ece']:>6.3f}")


def main() -> None:
    # ── Load data ────────────────────────────────────────────────────
    print("Loading bout data...")
    conn = get_connection()
    df = load_bout_data(conn)
    conn.close()

    _, val, test = temporal_split(df)
    print(f"  Test set: {len(test):,} fights")

    # ── Load best model (LightGBM) ──────────────────────────────────
    artifact_dir = latest_artifact(MODELS_DIR, "lgbm")
    if artifact_dir is None:
        print("No LightGBM artifact found. Run train_lgbm.py first.")
        sys.exit(1)

    model, meta = load_model(artifact_dir)
    X_test = test[FEATURE_COLS].apply(pd.to_numeric, errors="coerce")
    y_test = test["label"].values
    y_prob = model.predict_proba(X_test)[:, 1]

    model_name = meta.get("model_name", "lgbm")
    print(f"  Model: {model_name} from {artifact_dir.name}")

    # ── Run all segment analyses ─────────────────────────────────────
    report_path = MODELS_DIR / "error_analysis.txt"
    with open(report_path, "w") as f:
        header = f"Error Analysis — {model_name}"
        print(f"\n{'=' * 70}")
        print(f"  {header}")
        print(f"{'=' * 70}")
        f.write(f"{header}\n{'=' * 70}\n")

        # Overall
        overall = compute_metrics(y_test, y_prob)
        _print_segment_table("Overall", [{"segment": "All fights", "n": len(y_test), **overall}], f)

        _print_segment_table("By Weight Class", analyze_by_weight_class(test, y_prob), f)
        _print_segment_table("Title vs Non-Title", analyze_by_title(test, y_prob), f)
        _print_segment_table("Debut Fighters", analyze_by_debut(test, y_prob), f)
        _print_segment_table("By Year", analyze_by_era(test, y_prob), f)
        _print_segment_table("By Confidence Bucket", analyze_by_confidence(y_test, y_prob), f)

    print(f"\n  Report saved to: {report_path}")


if __name__ == "__main__":
    main()
