"""Batch scoring for upcoming UFC fights.

Loads the production model, scores upcoming fight features, applies Platt
calibration (re-fit on validation data), attaches confidence tiers, and
saves predictions to CSV.

Usage:
    python modeling/score_upcoming.py
    # or: make score_upcoming
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modeling.artifacts import load_model
from modeling.calibrate import calibrate_platt
from modeling.data import load_bout_data
from modeling.uncertainty import confidence_tier, flag_uncertain
from features.debut_prior import compute_debut_priors, apply_debut_features
from warehouse.db import get_connection

REPO_ROOT = Path(__file__).resolve().parent.parent
PRODUCTION_MODEL = REPO_ROOT / "models" / "production_model.json"


def _load_production_info() -> dict:
    """Load production model pointer."""
    with open(PRODUCTION_MODEL) as f:
        return json.load(f)


def _fit_calibrator(model, metadata: dict, conn):
    """Re-fit Platt calibration on validation data.

    Returns (y_prob_val, y_val) so we can use calibrate_platt at scoring time.
    """
    feature_cols = metadata["feature_cols"]
    val_date = metadata.get("val_date")
    test_date = metadata.get("test_date")

    if not val_date or not test_date:
        return None, None

    # Debut columns are computed at runtime, not stored in the DB
    db_feature_cols = [c for c in feature_cols if not c.startswith("debut_")]
    df = load_bout_data(conn, feature_cols=db_feature_cols)

    # Apply debut priors if the model used them
    if metadata.get("debut_priors_applied"):
        train_for_priors = df[df["event_date"] < val_date]
        priors = compute_debut_priors(train_for_priors)
        df = apply_debut_features(df, priors)

    val = df[(df["event_date"] >= val_date) & (df["event_date"] < test_date)].copy()
    val = val.dropna(subset=["label"])

    if len(val) == 0:
        return None, None

    X_val = val[feature_cols].values
    y_val = val["label"].values

    y_prob_val = model.predict_proba(X_val)[:, 1]
    return y_prob_val, y_val


def score_upcoming(conn) -> pd.DataFrame:
    """Score all upcoming fights and return predictions DataFrame."""
    prod = _load_production_info()
    artifact_path = Path(prod["artifact_path"])

    print(f"Loading production model: {prod['selected_model']}")
    print(f"  artifact: {artifact_path}")
    model, metadata = load_model(artifact_path)
    feature_cols = metadata["feature_cols"]

    # Load upcoming features
    upcoming_csv = REPO_ROOT / "models" / "upcoming" / "upcoming_features.csv"
    if not upcoming_csv.exists():
        print("  No upcoming features found. Run: make build_upcoming_features")
        return pd.DataFrame()

    df = pd.read_csv(upcoming_csv)
    if df.empty:
        print("  No upcoming fights to score.")
        return pd.DataFrame()

    print(f"  Loaded {len(df)} upcoming fight(s)")

    # Verify feature columns
    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        print(f"  warn  missing features (will be NaN): {missing}")
        for c in missing:
            df[c] = np.nan

    X = df[feature_cols].values

    # Raw predictions
    y_prob_raw = model.predict_proba(X)[:, 1]

    # Platt calibration
    print("Fitting Platt calibration on validation data ...")
    y_prob_val, y_val = _fit_calibrator(model, metadata, conn)
    if y_prob_val is not None:
        y_prob_cal = calibrate_platt(y_prob_val, y_val, y_prob_raw)
        print(f"  calibration applied (fit on {len(y_val)} val samples)")
    else:
        print("  warn  could not fit calibrator, using raw probabilities")
        y_prob_cal = y_prob_raw

    # Confidence tiers
    tiers = confidence_tier(y_prob_cal)
    uncertain = flag_uncertain(y_prob_cal)

    # Look up fighter names
    fighter_names = _get_fighter_names(conn, df)

    # Build predictions DataFrame
    predictions = pd.DataFrame({
        "fight_id": df["fight_id"],
        "event_date": df.get("event_date"),
        "fighter_1_id": df["fighter_1_id"],
        "fighter_2_id": df["fighter_2_id"],
        "fighter_1_name": [fighter_names.get(fid, "Unknown") for fid in df["fighter_1_id"]],
        "fighter_2_name": [fighter_names.get(fid, "Unknown") for fid in df["fighter_2_id"]],
        "weight_class": df.get("weight_class"),
        "predicted_prob_f1": y_prob_raw,
        "calibrated_prob_f1": y_prob_cal,
        "confidence_tier": tiers,
        "is_uncertain": uncertain,
        "model_name": prod["selected_model"],
        "model_artifact": str(artifact_path),
        "scored_at": datetime.now(timezone.utc).isoformat(),
    })

    return predictions


def _get_fighter_names(conn, df: pd.DataFrame) -> dict[str, str]:
    """Look up fighter names by ID."""
    all_ids = set(df["fighter_1_id"].tolist() + df["fighter_2_id"].tolist())
    if not all_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute("SELECT fighter_id::text, full_name FROM fighters")
        return {str(r[0]): r[1] for r in cur.fetchall() if str(r[0]) in all_ids}


def _print_card(predictions: pd.DataFrame) -> None:
    """Print formatted fight card with predictions."""
    if predictions.empty:
        return

    # Sort by confidence (most confident first)
    preds = predictions.sort_values("calibrated_prob_f1", key=lambda x: abs(x - 0.5), ascending=False)

    print(f"\n{'═' * 80}")
    print(f"  UFC FIGHT PREDICTIONS")
    print(f"  Model: {preds.iloc[0]['model_name']}")
    print(f"  Scored: {preds.iloc[0]['scored_at'][:19]}")
    print(f"{'═' * 80}\n")

    tier_order = {"high": 0, "medium": 1, "toss-up": 2}
    preds = preds.sort_values("confidence_tier", key=lambda x: x.map(tier_order))

    current_tier = None
    for _, row in preds.iterrows():
        tier = row["confidence_tier"]
        if tier != current_tier:
            current_tier = tier
            label = {"high": "HIGH CONFIDENCE", "medium": "MEDIUM CONFIDENCE", "toss-up": "TOSS-UP"}
            print(f"  ── {label.get(tier, tier.upper())} {'─' * 50}")

        prob = row["calibrated_prob_f1"]
        f1 = row["fighter_1_name"]
        f2 = row["fighter_2_name"]
        wc = row["weight_class"] or ""

        # Visual bar
        bar_len = 30
        f1_len = int(round(prob * bar_len))
        f2_len = bar_len - f1_len
        bar = "█" * f1_len + "░" * f2_len

        if prob >= 0.5:
            fav = f1
            fav_prob = prob
        else:
            fav = f2
            fav_prob = 1 - prob

        print(f"    {f1:<25s} vs  {f2:<25s}  [{wc}]")
        print(f"    {bar}  {prob:.1%} — {fav} ({fav_prob:.1%})")
        print()


def main() -> None:
    conn = get_connection()
    try:
        predictions = score_upcoming(conn)

        if predictions.empty:
            print("\nNo predictions generated.")
            return

        # Save predictions
        event_dates = predictions["event_date"].dropna().unique()
        out_dir = REPO_ROOT / "models" / "predictions"
        for ed in event_dates:
            date_dir = out_dir / str(ed)[:10]
            date_dir.mkdir(parents=True, exist_ok=True)
            mask = predictions["event_date"] == ed
            predictions[mask].to_csv(date_dir / "predictions.csv", index=False)
            print(f"  Saved predictions to {date_dir / 'predictions.csv'}")

        # If no event dates, save to a generic location
        if len(event_dates) == 0:
            out_dir.mkdir(parents=True, exist_ok=True)
            predictions.to_csv(out_dir / "predictions.csv", index=False)
            print(f"  Saved predictions to {out_dir / 'predictions.csv'}")

        _print_card(predictions)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
