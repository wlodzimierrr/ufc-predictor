"""Post-event prediction accuracy review.

Compares saved predictions against actual results after an event completes.
Tracks accuracy over time to detect model drift.

Usage:
    python modeling/post_event_review.py --event "UFC 315"
    # or: make review_event EVENT="UFC 315"
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
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


def _normalize_name(value: object) -> str:
    """Normalize fighter names enough to match manual cards to scraped cards."""
    text = "" if pd.isna(value) else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9\s]", " ", text.lower())
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _same_fighter_name(left: object, right: object) -> bool:
    left_norm = _normalize_name(left)
    right_norm = _normalize_name(right)
    if not left_norm or not right_norm:
        return False
    if left_norm == right_norm:
        return True

    left_tokens = set(left_norm.split())
    right_tokens = set(right_norm.split())
    smaller, larger = (
        (left_tokens, right_tokens)
        if len(left_tokens) <= len(right_tokens)
        else (right_tokens, left_tokens)
    )
    # Handles small source-name differences like "Jose Miguel Delgado" vs
    # "Jose Delgado" without accepting a last-name-only match.
    return len(smaller) >= 2 and smaller.issubset(larger)


def _label_for_actual_row(actual: dict, pred_f1_name: object, pred_f2_name: object) -> int | None:
    """Return the winner label in prediction order for a matched actual fight."""
    if actual["result_type"] != "win" or actual["winner_id"] is None:
        return None

    direct = (
        _same_fighter_name(pred_f1_name, actual["fighter_1_name"])
        and _same_fighter_name(pred_f2_name, actual["fighter_2_name"])
    )
    swapped = (
        _same_fighter_name(pred_f1_name, actual["fighter_2_name"])
        and _same_fighter_name(pred_f2_name, actual["fighter_1_name"])
    )

    if direct:
        if actual["winner_id"] == actual["fighter_1_id"]:
            return 1
        if actual["winner_id"] == actual["fighter_2_id"]:
            return 0
    if swapped:
        if actual["winner_id"] == actual["fighter_2_id"]:
            return 1
        if actual["winner_id"] == actual["fighter_1_id"]:
            return 0
    return None


def _get_actual_results(conn, event_id: str, preds: pd.DataFrame) -> dict[str, dict]:
    """Get actual results, falling back to event-level fighter-name matching.

    Catch-up prediction cards can use temporary/manual fight IDs. Once official
    scraped results arrive, UFCStats fight IDs replace those rows, so exact
    fight_id matching alone undercounts reviewed fights.
    """
    if preds.empty:
        return {}

    fight_ids = [str(fid) for fid in preds["fight_id"].dropna().tolist()]

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                f.fight_id::text,
                f.fighter_1_id::text,
                f.fighter_2_id::text,
                f1.full_name,
                f2.full_name,
                f.winner_fighter_id::text,
                f.result_type,
                f.is_title_fight,
                f.is_interim_title,
                f.scheduled_rounds,
                f.finish_method,
                f.finish_round,
                f.finish_time_seconds
            FROM fights f
            JOIN fighters f1 ON f1.fighter_id = f.fighter_1_id
            JOIN fighters f2 ON f2.fighter_id = f.fighter_2_id
            WHERE f.fight_id::text = ANY(%s)
        """, (fight_ids,))
        results = {}
        for r in cur.fetchall():
            fid = str(r[0])
            winner = str(r[5]) if r[5] else None
            result_type = r[6]

            if result_type == "win" and winner == str(r[1]):
                label = 1
            elif result_type == "win" and winner == str(r[2]):
                label = 0
            else:
                label = None  # draw, NC, or still upcoming

            results[fid] = {
                "winner_id": winner,
                "result_type": result_type,
                "label": label,
                "matched_by": "fight_id",
                "actual_fight_id": fid,
                "fighter_1_name": r[3],
                "fighter_2_name": r[4],
                "is_title_fight": r[7],
                "is_interim_title": r[8],
                "scheduled_rounds": r[9],
                "finish_method": r[10],
                "finish_round": r[11],
                "finish_time_seconds": r[12],
            }

        cur.execute("""
            SELECT
                f.fight_id::text,
                f.fighter_1_id::text,
                f.fighter_2_id::text,
                f1.full_name,
                f2.full_name,
                f.winner_fighter_id::text,
                f.result_type,
                f.is_title_fight,
                f.is_interim_title,
                f.scheduled_rounds,
                f.finish_method,
                f.finish_round,
                f.finish_time_seconds
            FROM fights f
            JOIN fighters f1 ON f1.fighter_id = f.fighter_1_id
            JOIN fighters f2 ON f2.fighter_id = f.fighter_2_id
            WHERE f.event_id = %s
              AND f.result_type = 'win'
        """, (event_id,))
        event_actuals = [
            {
                "fight_id": str(r[0]),
                "fighter_1_id": str(r[1]),
                "fighter_2_id": str(r[2]),
                "fighter_1_name": r[3],
                "fighter_2_name": r[4],
                "winner_id": str(r[5]) if r[5] else None,
                "result_type": r[6],
                "is_title_fight": r[7],
                "is_interim_title": r[8],
                "scheduled_rounds": r[9],
                "finish_method": r[10],
                "finish_round": r[11],
                "finish_time_seconds": r[12],
            }
            for r in cur.fetchall()
        ]

    for _, pred in preds.iterrows():
        pred_fight_id = str(pred["fight_id"])
        if results.get(pred_fight_id, {}).get("label") is not None:
            continue

        for actual in event_actuals:
            label = _label_for_actual_row(
                actual,
                pred.get("fighter_1_name", ""),
                pred.get("fighter_2_name", ""),
            )
            if label is None:
                continue

            results[pred_fight_id] = {
                "winner_id": actual["winner_id"],
                "result_type": actual["result_type"],
                "label": label,
                "matched_by": "event_fighter_names",
                "actual_fight_id": actual["fight_id"],
                "fighter_1_name": actual["fighter_1_name"],
                "fighter_2_name": actual["fighter_2_name"],
                "is_title_fight": actual["is_title_fight"],
                "is_interim_title": actual["is_interim_title"],
                "scheduled_rounds": actual["scheduled_rounds"],
                "finish_method": actual["finish_method"],
                "finish_round": actual["finish_round"],
                "finish_time_seconds": actual["finish_time_seconds"],
            }
            break

    return results


def review_event(event_name: str) -> dict | None:
    """Review prediction accuracy for a completed event."""
    conn = get_connection()
    try:
        preds = _find_predictions(event_name, conn)
        if preds is None or preds.empty:
            return None

        event_id = str(preds.iloc[0].get("event_id", ""))
        actuals = _get_actual_results(conn, event_id, preds)
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
        fighter_1_name = row.get("fighter_1_name", "?")
        fighter_2_name = row.get("fighter_2_name", "?")

        results.append({
            "fight_id": fid,
            "actual_fight_id": actual.get("actual_fight_id", fid),
            "fighter_1_id": row.get("fighter_1_id"),
            "fighter_2_id": row.get("fighter_2_id"),
            "fighter_1": fighter_1_name,
            "fighter_2": fighter_2_name,
            "weight_class": row.get("weight_class", ""),
            "predicted_prob_f1": row.get("predicted_prob_f1"),
            "calibrated_prob_f1": prob,
            "predicted_label": predicted_winner,
            "predicted_winner_name": fighter_1_name if predicted_winner == 1 else fighter_2_name,
            "confidence_tier": row.get("confidence_tier", ""),
            "is_uncertain": row.get("is_uncertain"),
            "actual_label": label,
            "actual_winner_name": fighter_1_name if label == 1 else fighter_2_name,
            "result_type": actual.get("result_type"),
            "is_title_fight": actual.get("is_title_fight"),
            "is_interim_title": actual.get("is_interim_title"),
            "scheduled_rounds": actual.get("scheduled_rounds"),
            "finish_method": actual.get("finish_method"),
            "finish_round": actual.get("finish_round"),
            "finish_time_seconds": actual.get("finish_time_seconds"),
            "predicted_correct": correct,
            "model_name": row.get("model_name", ""),
            "model_artifact": row.get("model_artifact", ""),
            "scored_at": row.get("scored_at"),
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
    scored_at_source = (
        preds["scored_at"]
        if "scored_at" in preds.columns
        else pd.Series(pd.NaT, index=preds.index)
    )
    scored_at = pd.to_datetime(scored_at_source, errors="coerce", utc=True)
    event_date_ts = pd.to_datetime(event_date, errors="coerce", utc=True)
    before_event = bool(
        not scored_at.isna().all()
        and pd.notna(event_date_ts)
        and (scored_at.dt.date < event_date_ts.date()).all()
    )
    review_type = (
        "database_scored_at_before_event"
        if before_event
        else "catchup_scored_before_result_load"
    )

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
        "review_type": review_type,
        "first_scored_at": scored_at.min().isoformat() if not scored_at.isna().all() else None,
        "last_scored_at": scored_at.max().isoformat() if not scored_at.isna().all() else None,
    }

    # Append to prediction log
    _append_to_log(summary)
    _upsert_review_summary(summary)
    _upsert_review_fights(summary, rdf, str(preds.iloc[0].get("event_id", "")))

    # If we have enough events, show rolling trend
    _print_rolling_trend()

    return summary


def _append_to_log(summary: dict) -> None:
    """Upsert event summary into the prediction log CSV."""
    row = {
        "event_name": summary["event_name"],
        "event_date": summary["event_date"],
        "n_fights": summary["n_fights"],
        "accuracy": f"{summary['accuracy']:.4f}",
        "log_loss": f"{summary['log_loss']:.4f}",
        "brier_score": f"{summary['brier_score']:.4f}",
        "model_name": summary["model_name"],
    }

    if PREDICTION_LOG.exists():
        log = pd.read_csv(PREDICTION_LOG)
        same_event = (
            (log["event_name"].astype(str) == str(row["event_name"]))
            & (log["event_date"].astype(str) == str(row["event_date"]))
        )
        log = log.loc[~same_event]
    else:
        log = pd.DataFrame(columns=row.keys())

    log = pd.concat([log, pd.DataFrame([row])], ignore_index=True)
    log["_event_date_sort"] = pd.to_datetime(log["event_date"], errors="coerce")
    log = log.sort_values("_event_date_sort").drop(columns=["_event_date_sort"])
    log.to_csv(PREDICTION_LOG, index=False)

    print(f"  Updated {PREDICTION_LOG}")


def _upsert_review_summary(summary: dict) -> None:
    """Store reviewed event summaries for dashboard views."""
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO reviewed_prediction_events (
                        event_name,
                        event_date,
                        review_type,
                        model_name,
                        n_predicted_fights,
                        correct,
                        accuracy,
                        log_loss,
                        brier_score,
                        first_scored_at,
                        last_scored_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_name, event_date, review_type)
                    DO UPDATE SET
                        model_name = EXCLUDED.model_name,
                        n_predicted_fights = EXCLUDED.n_predicted_fights,
                        correct = EXCLUDED.correct,
                        accuracy = EXCLUDED.accuracy,
                        log_loss = EXCLUDED.log_loss,
                        brier_score = EXCLUDED.brier_score,
                        first_scored_at = EXCLUDED.first_scored_at,
                        last_scored_at = EXCLUDED.last_scored_at,
                        reviewed_at = now()
                """, (
                    summary["event_name"],
                    summary["event_date"],
                    summary["review_type"],
                    summary["model_name"],
                    summary["n_fights"],
                    int(round(summary["n_fights"] * summary["accuracy"])),
                    float(summary["accuracy"]),
                    float(summary["log_loss"]),
                    float(summary["brier_score"]),
                    summary.get("first_scored_at"),
                    summary.get("last_scored_at"),
                ))
        print("  Updated reviewed_prediction_events")
    finally:
        conn.close()


def _to_none(value):
    if pd.isna(value):
        return None
    return value


def _upsert_review_fights(summary: dict, reviewed: pd.DataFrame, event_id: str) -> None:
    """Store fight-level reviewed rows for dashboard home-page summaries."""
    if reviewed.empty:
        return

    rows = []
    for _, row in reviewed.iterrows():
        rows.append((
            event_id or None,
            summary["event_name"],
            summary["event_date"],
            summary["review_type"],
            row["fight_id"],
            row.get("actual_fight_id"),
            _to_none(row.get("fighter_1_id")),
            _to_none(row.get("fighter_2_id")),
            row.get("fighter_1"),
            row.get("fighter_2"),
            _to_none(row.get("weight_class")),
            bool(row.get("is_title_fight")) if pd.notna(row.get("is_title_fight")) else None,
            bool(row.get("is_interim_title")) if pd.notna(row.get("is_interim_title")) else None,
            int(row.get("scheduled_rounds")) if pd.notna(row.get("scheduled_rounds")) else None,
            _to_none(row.get("scored_at")),
            _to_none(row.get("predicted_prob_f1")),
            float(row.get("calibrated_prob_f1")),
            int(row.get("predicted_label")),
            row.get("predicted_winner_name"),
            row.get("confidence_tier"),
            bool(row.get("is_uncertain")) if pd.notna(row.get("is_uncertain")) else None,
            int(row.get("actual_label")),
            row.get("actual_winner_name"),
            row.get("result_type"),
            row.get("finish_method"),
            int(row.get("finish_round")) if pd.notna(row.get("finish_round")) else None,
            int(row.get("finish_time_seconds")) if pd.notna(row.get("finish_time_seconds")) else None,
            bool(row.get("predicted_correct")),
            row.get("model_name"),
            row.get("model_artifact"),
        ))

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO reviewed_prediction_fights (
                        event_id,
                        event_name,
                        event_date,
                        review_type,
                        fight_id,
                        actual_fight_id,
                        fighter_1_id,
                        fighter_2_id,
                        fighter_1_name,
                        fighter_2_name,
                        weight_class,
                        is_title_fight,
                        is_interim_title,
                        scheduled_rounds,
                        scored_at,
                        predicted_prob_f1,
                        calibrated_prob_f1,
                        predicted_label,
                        predicted_winner_name,
                        confidence_tier,
                        is_uncertain,
                        actual_label,
                        actual_winner_name,
                        result_type,
                        finish_method,
                        finish_round,
                        finish_time_seconds,
                        correct,
                        model_name,
                        model_artifact
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (event_date, review_type, fight_id)
                    DO UPDATE SET
                        event_id = EXCLUDED.event_id,
                        event_name = EXCLUDED.event_name,
                        actual_fight_id = EXCLUDED.actual_fight_id,
                        fighter_1_id = EXCLUDED.fighter_1_id,
                        fighter_2_id = EXCLUDED.fighter_2_id,
                        fighter_1_name = EXCLUDED.fighter_1_name,
                        fighter_2_name = EXCLUDED.fighter_2_name,
                        weight_class = EXCLUDED.weight_class,
                        is_title_fight = EXCLUDED.is_title_fight,
                        is_interim_title = EXCLUDED.is_interim_title,
                        scheduled_rounds = EXCLUDED.scheduled_rounds,
                        scored_at = EXCLUDED.scored_at,
                        predicted_prob_f1 = EXCLUDED.predicted_prob_f1,
                        calibrated_prob_f1 = EXCLUDED.calibrated_prob_f1,
                        predicted_label = EXCLUDED.predicted_label,
                        predicted_winner_name = EXCLUDED.predicted_winner_name,
                        confidence_tier = EXCLUDED.confidence_tier,
                        is_uncertain = EXCLUDED.is_uncertain,
                        actual_label = EXCLUDED.actual_label,
                        actual_winner_name = EXCLUDED.actual_winner_name,
                        result_type = EXCLUDED.result_type,
                        finish_method = EXCLUDED.finish_method,
                        finish_round = EXCLUDED.finish_round,
                        finish_time_seconds = EXCLUDED.finish_time_seconds,
                        correct = EXCLUDED.correct,
                        model_name = EXCLUDED.model_name,
                        model_artifact = EXCLUDED.model_artifact,
                        reviewed_at = now()
                """, rows)
        print("  Updated reviewed_prediction_fights")
    finally:
        conn.close()


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
