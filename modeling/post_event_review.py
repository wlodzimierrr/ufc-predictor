"""Post-event prediction accuracy review.

Compares saved predictions against actual results after an event completes.
Tracks accuracy over time to detect model drift.

Usage:
    python modeling/post_event_review.py --event "UFC 315"
    # or: make review_event EVENT="UFC 315"
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, log_loss, brier_score_loss

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.db import get_connection

REPO_ROOT = Path(__file__).resolve().parent.parent
PREDICTIONS_DIR = REPO_ROOT / "models" / "predictions"
PREDICTION_LOG = REPO_ROOT / "models" / "prediction_log.csv"
PRODUCTION_MODEL = REPO_ROOT / "models" / "production_model.json"


def _retroactive_predictions(event_id: str, event_name: str, date_str: str, conn) -> pd.DataFrame | None:
    """Score completed fights retroactively using bout_features + production model.

    bout_features are computed from pre-fight data only (verified by leakage
    tests), so these predictions are equivalent to what we would have generated
    before the event.
    """
    from modeling.artifacts import load_model
    from modeling.calibrate import calibrate_platt
    from modeling.data import load_bout_data
    from modeling.uncertainty import confidence_tier, flag_uncertain
    from features.debut_prior import compute_debut_priors, apply_debut_features

    if not PRODUCTION_MODEL.exists():
        print("  No production model found — cannot score retroactively.")
        return None

    with open(PRODUCTION_MODEL) as f:
        prod = json.load(f)

    model, metadata = load_model(prod["artifact_path"])
    feature_cols = metadata["feature_cols"]

    # Load bout features for this event's fights
    db_feature_cols = [c for c in feature_cols if not c.startswith("debut_")]
    cols_sql = ", ".join(f"bf.{c}" for c in db_feature_cols)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT bf.fight_id::text, bf.fighter_1_id::text, bf.fighter_2_id::text,
                   bf.event_date, bf.weight_class, bf.label, {cols_sql}
            FROM bout_features bf
            JOIN fights f ON bf.fight_id = f.fight_id
            WHERE f.event_id = %s
        """, (event_id,))
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

    if not rows:
        print(f"  No bout_features found for {event_name}.")
        print("  Run: make build_features  (to rebuild the feature table)")
        return None

    df = pd.DataFrame(rows, columns=col_names)
    for c in feature_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    bool_cols = ["is_title_fight", "is_orthodox_vs_southpaw", "both_debuting",
                 "f1_is_southpaw", "f2_is_southpaw"]
    for c in bool_cols:
        if c in df.columns:
            df[c] = df[c].astype(int)

    # Apply debut priors if needed
    if metadata.get("debut_priors_applied"):
        val_date = metadata.get("val_date")
        all_bout = load_bout_data(conn, feature_cols=db_feature_cols)
        train_for_priors = all_bout[all_bout["event_date"] < val_date] if val_date else all_bout
        priors = compute_debut_priors(train_for_priors)
        df = apply_debut_features(df, priors)

    # Fill any missing feature columns with NaN
    for c in feature_cols:
        if c not in df.columns:
            df[c] = np.nan

    X = df[feature_cols].values
    y_prob_raw = model.predict_proba(X)[:, 1]

    # Platt calibration
    val_date = metadata.get("val_date")
    test_date = metadata.get("test_date")
    if val_date and test_date:
        all_bout = load_bout_data(conn, feature_cols=db_feature_cols)
        if metadata.get("debut_priors_applied"):
            train_for_priors = all_bout[all_bout["event_date"] < val_date]
            p = compute_debut_priors(train_for_priors)
            all_bout = apply_debut_features(all_bout, p)
        val = all_bout[(all_bout["event_date"] >= val_date) & (all_bout["event_date"] < test_date)]
        val = val.dropna(subset=["label"])
        if len(val) > 0:
            y_prob_val = model.predict_proba(val[feature_cols].values)[:, 1]
            y_prob_cal = calibrate_platt(y_prob_val, val["label"].values, y_prob_raw)
        else:
            y_prob_cal = y_prob_raw
    else:
        y_prob_cal = y_prob_raw

    # Look up fighter names
    all_ids = set(df["fighter_1_id"].tolist() + df["fighter_2_id"].tolist())
    with conn.cursor() as cur:
        cur.execute("SELECT fighter_id::text, full_name FROM fighters")
        name_map = {str(r[0]): r[1] for r in cur.fetchall() if str(r[0]) in all_ids}

    preds = pd.DataFrame({
        "fight_id": df["fight_id"],
        "event_date": df.get("event_date"),
        "fighter_1_id": df["fighter_1_id"],
        "fighter_2_id": df["fighter_2_id"],
        "fighter_1_name": [name_map.get(fid, "Unknown") for fid in df["fighter_1_id"]],
        "fighter_2_name": [name_map.get(fid, "Unknown") for fid in df["fighter_2_id"]],
        "weight_class": df.get("weight_class"),
        "calibrated_prob_f1": y_prob_cal,
        "confidence_tier": confidence_tier(y_prob_cal),
        "is_uncertain": flag_uncertain(y_prob_cal),
        "model_name": prod["selected_model"],
    })
    preds["event_id"] = event_id
    preds["event_name"] = event_name
    preds["event_date_str"] = date_str
    preds["retroactive"] = True

    print(f"  Scored {len(preds)} fight(s) retroactively using production model")
    return preds


def _find_predictions(event_name: str, conn) -> pd.DataFrame | None:
    """Find saved predictions for a specific event, or score retroactively."""
    # Look up event date
    with conn.cursor() as cur:
        cur.execute("""
            SELECT event_id::text, event_name, event_date
            FROM events
            WHERE LOWER(event_name) LIKE %s
            ORDER BY event_date DESC
            LIMIT 1
        """, (f"%{event_name.lower()}%",))
        row = cur.fetchone()

    if not row:
        print(f"  Event not found: {event_name}")
        return None

    event_id, ename, event_date = str(row[0]), row[1], row[2]
    date_str = str(event_date)[:10]

    # Try saved predictions first
    pred_path = PREDICTIONS_DIR / date_str / "predictions.csv"
    if pred_path.exists():
        preds = pd.read_csv(pred_path)
        # Check if saved predictions cover the full card
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM fights
                WHERE event_id = %s AND result_type = 'win'
            """, (event_id,))
            n_completed = cur.fetchone()[0]

        if len(preds) >= n_completed or n_completed == 0:
            preds["event_id"] = event_id
            preds["event_name"] = ename
            preds["event_date_str"] = date_str
            preds["retroactive"] = False
            return preds

        print(f"  Saved predictions cover {len(preds)}/{n_completed} completed fights — scoring remaining retroactively")

    # Fall back to retroactive scoring
    retro = _retroactive_predictions(event_id, ename, date_str, conn)
    if retro is not None:
        # Merge with any saved predictions (saved take priority)
        if pred_path.exists():
            saved = pd.read_csv(pred_path)
            saved["event_id"] = event_id
            saved["event_name"] = ename
            saved["event_date_str"] = date_str
            saved["retroactive"] = False
            # Keep saved predictions for fights that have them, use retro for the rest
            saved_ids = set(saved["fight_id"])
            retro_new = retro[~retro["fight_id"].isin(saved_ids)]
            return pd.concat([saved, retro_new], ignore_index=True)
        return retro

    return None


def _get_actual_results(conn, fight_ids: list[str]) -> dict[str, dict]:
    """Get actual fight results from the warehouse."""
    if not fight_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT fight_id::text, fighter_1_id::text, fighter_2_id::text,
                   winner_fighter_id::text, result_type
            FROM fights
            WHERE fight_id::text = ANY(%s)
        """, (fight_ids,))
        results = {}
        for r in cur.fetchall():
            fid = str(r[0])
            winner = str(r[3]) if r[3] else None
            result_type = r[4]

            if result_type == "win" and winner == str(r[1]):
                label = 1
            elif result_type == "win" and winner == str(r[2]):
                label = 0
            else:
                label = None  # draw, NC, or still upcoming

            results[fid] = {"winner_id": winner, "result_type": result_type, "label": label}
        return results


def review_event(event_name: str) -> dict | None:
    """Review prediction accuracy for a completed event."""
    conn = get_connection()
    try:
        preds = _find_predictions(event_name, conn)
        if preds is None or preds.empty:
            return None

        fight_ids = preds["fight_id"].tolist()
        actuals = _get_actual_results(conn, fight_ids)
    finally:
        conn.close()

    if not actuals:
        print(f"  No actual results found for these fights. Event may not be completed yet.")
        return None

    # Join predictions with actuals
    results = []
    for _, row in preds.iterrows():
        fid = row["fight_id"]
        actual = actuals.get(fid)
        if actual is None or actual["label"] is None:
            continue

        prob = float(row["calibrated_prob_f1"])
        label = actual["label"]
        predicted_winner = 1 if prob >= 0.5 else 0
        correct = predicted_winner == label

        results.append({
            "fight_id": fid,
            "fighter_1": row.get("fighter_1_name", "?"),
            "fighter_2": row.get("fighter_2_name", "?"),
            "weight_class": row.get("weight_class", ""),
            "calibrated_prob_f1": prob,
            "confidence_tier": row.get("confidence_tier", ""),
            "actual_label": label,
            "predicted_correct": correct,
        })

    if not results:
        print("  No resolved fights with predictions to compare.")
        return None

    rdf = pd.DataFrame(results)
    y_true = rdf["actual_label"].values
    y_prob = rdf["calibrated_prob_f1"].values

    accuracy = accuracy_score(y_true, (y_prob >= 0.5).astype(int))
    ll = log_loss(y_true, y_prob)
    brier = brier_score_loss(y_true, y_prob)

    # Accuracy by tier
    tier_stats = {}
    for tier in ["high", "medium", "toss-up"]:
        mask = rdf["confidence_tier"] == tier
        if mask.sum() > 0:
            tier_acc = rdf.loc[mask, "predicted_correct"].mean()
            tier_stats[tier] = {"count": int(mask.sum()), "accuracy": float(tier_acc)}

    event_name_str = preds.iloc[0].get("event_name", event_name)
    event_date = preds.iloc[0].get("event_date_str", "")

    # Print report
    n_retro = int(preds.get("retroactive", pd.Series(dtype=bool)).sum()) if "retroactive" in preds.columns else 0
    retro_note = f"  ({n_retro} scored retroactively)" if n_retro else ""

    print(f"\n{'═' * 70}")
    print(f"  POST-EVENT REVIEW: {event_name_str}")
    print(f"  Date: {event_date}    Fights reviewed: {len(rdf)}{retro_note}")
    print(f"{'═' * 70}\n")

    print(f"  Overall Metrics:")
    print(f"    Accuracy:    {accuracy:.1%}  ({int(rdf['predicted_correct'].sum())}/{len(rdf)})")
    print(f"    Log Loss:    {ll:.4f}")
    print(f"    Brier Score: {brier:.4f}")

    if tier_stats:
        print(f"\n  Accuracy by Confidence Tier:")
        for tier in ["high", "medium", "toss-up"]:
            if tier in tier_stats:
                ts = tier_stats[tier]
                print(f"    {tier:<10s}  {ts['accuracy']:.1%}  ({ts['count']} fights)")

    print(f"\n  {'Fighter 1':<22s} {'Fighter 2':<22s} {'Prob':>5s} {'Tier':<8s} {'Result':<10s}")
    print(f"  {'─' * 22} {'─' * 22} {'─' * 5} {'─' * 8} {'─' * 10}")
    for _, row in rdf.iterrows():
        prob = row["calibrated_prob_f1"]
        mark = "✓" if row["predicted_correct"] else "✗"
        actual = "F1 won" if row["actual_label"] == 1 else "F2 won"
        print(f"  {row['fighter_1']:<22s} {row['fighter_2']:<22s} "
              f"{prob:>5.1%} {row['confidence_tier']:<8s} {actual:<7s} {mark}")

    print()

    # Build summary for logging
    summary = {
        "event_name": event_name_str,
        "event_date": event_date,
        "n_fights": len(rdf),
        "accuracy": accuracy,
        "log_loss": ll,
        "brier_score": brier,
        "tier_stats": tier_stats,
        "model_name": preds.iloc[0].get("model_name", ""),
    }

    # Append to prediction log
    _append_to_log(summary)

    # If we have enough events, show rolling trend
    _print_rolling_trend()

    return summary


def _append_to_log(summary: dict) -> None:
    """Append event summary to the prediction log CSV."""
    log_exists = PREDICTION_LOG.exists()

    with open(PREDICTION_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "event_name", "event_date", "n_fights", "accuracy",
            "log_loss", "brier_score", "model_name",
        ])
        if not log_exists:
            writer.writeheader()

        writer.writerow({
            "event_name": summary["event_name"],
            "event_date": summary["event_date"],
            "n_fights": summary["n_fights"],
            "accuracy": f"{summary['accuracy']:.4f}",
            "log_loss": f"{summary['log_loss']:.4f}",
            "brier_score": f"{summary['brier_score']:.4f}",
            "model_name": summary["model_name"],
        })

    print(f"  Appended to {PREDICTION_LOG}")


def _print_rolling_trend() -> None:
    """Print rolling accuracy trend if we have enough events."""
    if not PREDICTION_LOG.exists():
        return

    log = pd.read_csv(PREDICTION_LOG)
    if len(log) < 3:
        return

    print(f"\n  ── Rolling Prediction Trend ({len(log)} events) ──")
    print(f"  {'Event':<30s} {'Acc':>6s} {'LL':>7s} {'Brier':>7s}")
    print(f"  {'─' * 30} {'─' * 6} {'─' * 7} {'─' * 7}")
    for _, row in log.iterrows():
        print(f"  {row['event_name']:<30s} {float(row['accuracy']):>6.1%} "
              f"{float(row['log_loss']):>7.4f} {float(row['brier_score']):>7.4f}")

    # Rolling averages
    print(f"\n  Cumulative: accuracy={log['accuracy'].astype(float).mean():.1%}  "
          f"log_loss={log['log_loss'].astype(float).mean():.4f}  "
          f"brier={log['brier_score'].astype(float).mean():.4f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Post-event prediction review")
    parser.add_argument("--event", required=True, help="Event name to review")
    args = parser.parse_args()

    review_event(args.event)


if __name__ == "__main__":
    main()
