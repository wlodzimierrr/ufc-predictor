"""UFC fight prediction CLI.

Scores upcoming fights using the production model and displays predictions
with confidence tiers and visual probability bars.

Usage:
    python predict.py                      # score all upcoming events
    python predict.py --event "UFC 315"    # score a specific event
    python predict.py --next               # score only the next event
    python predict.py --format json        # machine-readable output
    # or: make predict
"""

from __future__ import annotations

import argparse
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from modeling.score_upcoming import score_upcoming, _print_card
from modeling.decisions import DECISION_FIELDS, attach_decisions, json_safe, write_prediction_csv
from warehouse.db import get_connection, upsert

REPO_ROOT = Path(__file__).resolve().parent


def _filter_event(predictions: pd.DataFrame, event_name: str) -> pd.DataFrame:
    """Filter predictions to a specific event by name."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT event_id::text, event_name FROM events
                WHERE LOWER(event_name) LIKE %s
            """, (f"%{event_name.lower()}%",))
            matches = cur.fetchall()
    finally:
        conn.close()

    if not matches:
        print(f"No event found matching '{event_name}'")
        return pd.DataFrame()

    event_ids = {r[0] for r in matches}

    # Join fight_id → event_id
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT fight_id::text, event_id::text FROM fights")
            fight_events = {r[0]: r[1] for r in cur.fetchall()}
    finally:
        conn.close()

    mask = predictions["fight_id"].apply(lambda fid: fight_events.get(fid) in event_ids)
    return predictions[mask].copy()


def _filter_next(predictions: pd.DataFrame) -> pd.DataFrame:
    """Filter to only the next upcoming event."""
    if "event_date" not in predictions.columns or predictions.empty:
        return predictions
    dates = predictions["event_date"].dropna().unique()
    if len(dates) == 0:
        return predictions
    next_date = min(dates)
    return predictions[predictions["event_date"] == next_date].copy()


def _output_json(predictions: pd.DataFrame) -> None:
    """Output predictions as JSON."""
    records = []
    for _, row in attach_decisions(predictions).iterrows():
        records.append({
            "fight_id": row["fight_id"],
            "fighter_1": row["fighter_1_name"],
            "fighter_2": row["fighter_2_name"],
            "weight_class": row.get("weight_class"),
            "predicted_prob_f1": float(row["predicted_prob_f1"]),
            "calibrated_prob_f1": float(row["calibrated_prob_f1"]),
            "calibrated_prob_f2": 1 - float(row["calibrated_prob_f1"]),
            "confidence_tier": row["confidence_tier"],
            "is_uncertain": bool(row["is_uncertain"]),
            "model": row["model_name"],
            "scored_at": row["scored_at"],
            **{field: row[field] for field in (*DECISION_FIELDS, "decision_origin")},
        })
    print(json.dumps(json_safe(records), indent=2, allow_nan=False))


def _database_rows(predictions: pd.DataFrame) -> list[dict]:
    """Persist precise model values alongside the compatible rounded columns."""
    columns = [
        "fight_id", "event_date", "fighter_1_id", "fighter_2_id", "fighter_1_name",
        "fighter_2_name", "weight_class", "predicted_prob_f1", "calibrated_prob_f1",
        "confidence_tier", "is_uncertain", "model_name", "model_artifact", "scored_at",
        *DECISION_FIELDS, "decision_origin",
    ]
    records = []
    for _, row in attach_decisions(predictions).iterrows():
        record = json_safe({column: row.get(column) for column in columns})
        record["event_date"] = str(row["event_date"])[:10] if pd.notna(row["event_date"]) else None
        for column in ("predicted_prob_f1", "calibrated_prob_f1"):
            record[column] = float(row[column])
            record[column + "_full"] = record[column]
        records.append(record)
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="UFC fight predictions")
    parser.add_argument("--event", type=str, help="Score a specific event by name")
    parser.add_argument("--next", action="store_true", dest="next_event",
                        help="Score only the next upcoming event")
    parser.add_argument("--format", choices=["table", "json"], default="table",
                        help="Output format (default: table)")
    args = parser.parse_args()

    conn = get_connection()
    try:
        # JSON stdout must contain only the payload, including scoring progress.
        if args.format == "json":
            with redirect_stdout(sys.stderr):
                predictions = score_upcoming(conn)
        else:
            predictions = score_upcoming(conn)
    finally:
        conn.close()

    if predictions.empty:
        if args.format == "json":
            _output_json(predictions)
            return
        print("\nNo predictions available.")
        print("To generate predictions:")
        print("  1. make load_upcoming       # load upcoming fights")
        print("  2. make build_upcoming_features  # build features")
        print("  3. make predict             # score fights")
        return

    # Apply filters
    if args.event:
        if args.format == "json":
            with redirect_stdout(sys.stderr):
                predictions = _filter_event(predictions, args.event)
        else:
            predictions = _filter_event(predictions, args.event)
    elif args.next_event:
        predictions = _filter_next(predictions)

    if predictions.empty:
        if args.format == "json":
            _output_json(predictions)
            return
        print("\nNo predictions match the filter.")
        return

    # Save predictions to CSV
    out_dir = REPO_ROOT / "models" / "predictions"
    event_dates = predictions["event_date"].dropna().unique()
    for ed in event_dates:
        date_dir = out_dir / str(ed)[:10]
        date_dir.mkdir(parents=True, exist_ok=True)
        mask = predictions["event_date"] == ed
        write_prediction_csv(predictions[mask], date_dir / "predictions.csv")

    # Save predictions to database
    conn = get_connection()
    try:
        db_rows = _database_rows(predictions)
        with conn:
            n = upsert(conn, "predictions", db_rows, pk_columns=["fight_id", "scored_at"])
        print(f"\n  Saved {n} prediction(s) to database", file=sys.stderr if args.format == "json" else sys.stdout)
    finally:
        conn.close()

    # Output
    if args.format == "json":
        _output_json(predictions)
    else:
        _print_card(predictions)


if __name__ == "__main__":
    main()
