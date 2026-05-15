"""Retroactively score and compare all completed UFC events.

This report uses the current production model against historical bout_features
and compares predictions to actual winners. Features are pre-fight snapshots,
but the model itself is the current production artifact, so this is a
production-model retro score rather than a walk-forward retrain per event.

Usage:
    python modeling/backtest_past_events.py
    # or: make review_all_events
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from features.debut_prior import apply_debut_features, compute_debut_priors
from modeling.artifacts import load_model
from modeling.calibrate import calibrate_platt
from modeling.data import load_bout_data
from modeling.uncertainty import confidence_tier, flag_uncertain
from warehouse.db import get_connection

REPO_ROOT = Path(__file__).resolve().parent.parent
PRODUCTION_MODEL = REPO_ROOT / "models" / "production_model.json"
DEFAULT_OUT_DIR = REPO_ROOT / "models" / "backtests"


def _load_production_model():
    if not PRODUCTION_MODEL.exists():
        raise FileNotFoundError(f"Production model pointer not found: {PRODUCTION_MODEL}")

    with open(PRODUCTION_MODEL, encoding="utf-8") as f:
        prod = json.load(f)

    artifact_path = Path(prod["artifact_path"])
    model, metadata = load_model(artifact_path)
    return prod, artifact_path, model, metadata


def _load_fight_metadata(conn) -> pd.DataFrame:
    """Load event/fighter/result metadata for resolved fights."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT f.fight_id::text,
                   f.event_id::text,
                   e.event_name,
                   e.event_date,
                   f.fighter_1_id::text,
                   f.fighter_2_id::text,
                   f.winner_fighter_id::text,
                   f1.full_name AS fighter_1_name,
                   f2.full_name AS fighter_2_name,
                   fw.full_name AS actual_winner_name,
                   f.weight_class,
                   f.result_type
            FROM fights f
            JOIN events e ON e.event_id = f.event_id
            JOIN fighters f1 ON f1.fighter_id = f.fighter_1_id
            JOIN fighters f2 ON f2.fighter_id = f.fighter_2_id
            LEFT JOIN fighters fw ON fw.fighter_id = f.winner_fighter_id
            WHERE f.result_type = 'win'
              AND f.winner_fighter_id IS NOT NULL
            ORDER BY e.event_date ASC, e.event_name ASC
        """)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=cols)
    if not df.empty:
        df["event_date"] = pd.to_datetime(df["event_date"])
    return df


def _prepare_feature_frame(conn, metadata: dict) -> tuple[pd.DataFrame, list[str]]:
    """Load bout features and add Python-only features used by the model."""
    feature_cols = metadata["feature_cols"]
    db_feature_cols = [c for c in feature_cols if not c.startswith("debut_")]

    df = load_bout_data(conn, feature_cols=db_feature_cols)

    if metadata.get("debut_priors_applied"):
        val_date = metadata.get("val_date")
        train_for_priors = df[df["event_date"] < val_date] if val_date else df
        priors = compute_debut_priors(train_for_priors)
        df = apply_debut_features(df, priors)

    for col in feature_cols:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df, feature_cols


def _calibrate_probabilities(model, metadata: dict, df: pd.DataFrame, feature_cols: list[str], raw_prob: np.ndarray) -> np.ndarray:
    """Apply the same Platt calibration path used by upcoming scoring."""
    val_date = metadata.get("val_date")
    test_date = metadata.get("test_date")
    if not val_date or not test_date:
        return raw_prob

    val = df[(df["event_date"] >= val_date) & (df["event_date"] < test_date)]
    val = val.dropna(subset=["label"])
    if val.empty:
        return raw_prob

    y_prob_val = model.predict_proba(val[feature_cols].values)[:, 1]
    return calibrate_platt(y_prob_val, val["label"].values, raw_prob)


def score_all_past_events(conn) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Score historical fights and return fight-level and event-level reports."""
    prod, artifact_path, model, metadata = _load_production_model()
    features, feature_cols = _prepare_feature_frame(conn, metadata)
    metadata_df = _load_fight_metadata(conn)

    if features.empty or metadata_df.empty:
        return pd.DataFrame(), pd.DataFrame(), {
            "model_name": prod.get("selected_model", metadata.get("model_name", "")),
            "model_artifact": str(artifact_path),
        }

    raw_prob = model.predict_proba(features[feature_cols].values)[:, 1]
    cal_prob = _calibrate_probabilities(model, metadata, features, feature_cols, raw_prob)

    scored = features[[
        "fight_id", "fighter_1_id", "fighter_2_id", "event_date", "weight_class", "label",
    ]].copy()
    scored["predicted_prob_f1"] = raw_prob
    scored["calibrated_prob_f1"] = cal_prob
    scored["predicted_label"] = (scored["calibrated_prob_f1"] >= 0.5).astype(int)
    scored["predicted_correct"] = scored["predicted_label"] == scored["label"]
    scored["predicted_prob_winner"] = np.where(
        scored["predicted_label"] == 1,
        scored["calibrated_prob_f1"],
        1.0 - scored["calibrated_prob_f1"],
    )
    scored["confidence_tier"] = confidence_tier(scored["calibrated_prob_f1"].values)
    scored["is_uncertain"] = flag_uncertain(scored["calibrated_prob_f1"].values)

    report = scored.merge(
        metadata_df.drop(columns=["event_date", "weight_class", "fighter_1_id", "fighter_2_id"]),
        on="fight_id",
        how="inner",
    )
    report["predicted_winner_id"] = np.where(
        report["predicted_label"] == 1,
        report["fighter_1_id"],
        report["fighter_2_id"],
    )
    report["predicted_winner_name"] = np.where(
        report["predicted_label"] == 1,
        report["fighter_1_name"],
        report["fighter_2_name"],
    )
    report["actual_label"] = report["label"]
    report["model_name"] = prod.get("selected_model", metadata.get("model_name", ""))
    report["model_artifact"] = str(artifact_path)
    report["scored_at"] = datetime.now(timezone.utc).isoformat()

    event_summary = _summarize_events(report)
    return report, event_summary, {
        "model_name": report["model_name"].iloc[0],
        "model_artifact": str(artifact_path),
    }


def _summarize_events(report: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (event_id, event_name, event_date), grp in report.groupby(["event_id", "event_name", "event_date"], sort=True):
        y_true = grp["actual_label"].astype(int).values
        y_prob = grp["calibrated_prob_f1"].astype(float).values
        correct = grp["predicted_correct"].astype(bool)

        row = {
            "event_id": event_id,
            "event_name": event_name,
            "event_date": event_date,
            "n_fights": int(len(grp)),
            "correct": int(correct.sum()),
            "accuracy": float(accuracy_score(y_true, (y_prob >= 0.5).astype(int))),
            "log_loss": float(log_loss(y_true, y_prob, labels=[0, 1])),
            "brier_score": float(brier_score_loss(y_true, y_prob)),
            "high_count": int((grp["confidence_tier"] == "high").sum()),
            "medium_count": int((grp["confidence_tier"] == "medium").sum()),
            "toss_up_count": int((grp["confidence_tier"] == "toss-up").sum()),
        }

        for tier in ["high", "medium", "toss-up"]:
            mask = grp["confidence_tier"] == tier
            key = tier.replace("-", "_")
            row[f"{key}_accuracy"] = float(grp.loc[mask, "predicted_correct"].mean()) if mask.any() else np.nan

        rows.append(row)

    return pd.DataFrame(rows).sort_values("event_date", ascending=False)


def _print_report(fight_report: pd.DataFrame, event_summary: pd.DataFrame, context: dict) -> None:
    if fight_report.empty:
        print("No completed fights with model-ready features found.")
        return

    y_true = fight_report["actual_label"].astype(int).values
    y_prob = fight_report["calibrated_prob_f1"].astype(float).values
    correct = fight_report["predicted_correct"].astype(bool)

    print("\nPast Event Prediction Comparison")
    print(f"  Model:       {context['model_name']}")
    print(f"  Artifact:    {context['model_artifact']}")
    print(f"  Events:      {len(event_summary):,}")
    print(f"  Fights:      {len(fight_report):,}")
    print(f"  Accuracy:    {correct.mean():.1%}  ({int(correct.sum())}/{len(fight_report)})")
    print(f"  Log Loss:    {log_loss(y_true, y_prob, labels=[0, 1]):.4f}")
    print(f"  Brier Score: {brier_score_loss(y_true, y_prob):.4f}")

    print("\nMost recent completed events:")
    cols = ["event_date", "event_name", "n_fights", "correct", "accuracy", "log_loss"]
    recent = event_summary[cols].head(10).copy()
    recent["accuracy"] = recent["accuracy"].map(lambda v: f"{v:.1%}")
    recent["log_loss"] = recent["log_loss"].map(lambda v: f"{v:.4f}")
    print(recent.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict and compare all completed historical events.")
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Directory for CSV outputs. Default: models/backtests",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    try:
        fight_report, event_summary, context = score_all_past_events(conn)
    finally:
        conn.close()

    fight_path = out_dir / "past_event_predictions.csv"
    event_path = out_dir / "past_event_summary.csv"
    fight_report.to_csv(fight_path, index=False)
    event_summary.to_csv(event_path, index=False)

    _print_report(fight_report, event_summary, context)
    print(f"\nSaved fight-level report: {fight_path}")
    print(f"Saved event-level report: {event_path}")


if __name__ == "__main__":
    main()
