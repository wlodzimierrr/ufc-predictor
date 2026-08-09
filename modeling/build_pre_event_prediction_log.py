"""Build an honest log of predictions made before event day.

By default this script reads the warehouse ``predictions`` table, because it
preserves older scoring runs even when local CSV files are regenerated. It keeps
the latest prediction per fight where ``scored_at::date < event_date``.

If the database is unavailable, it can fall back to saved prediction files under
``models/predictions/<event_date>/predictions.csv`` when there is local evidence
that the prediction existed before event day. It intentionally excludes
retroactive backtests.

Outputs:
    data/reports/pre_event_prediction_fights.csv
    data/reports/pre_event_prediction_events.csv

Usage:
    python modeling/build_pre_event_prediction_log.py
    # or: make pre_event_log
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from warehouse.csv_utils import iter_data_rows
from warehouse.db import get_connection

REPO_ROOT = Path(__file__).resolve().parent.parent
PREDICTIONS_DIR = REPO_ROOT / "models" / "predictions"
PREDICTION_LOG = REPO_ROOT / "models" / "prediction_log.csv"
FIGHTS_CSV = REPO_ROOT / "data" / "fights.csv"
EVENTS_CSV = REPO_ROOT / "data" / "events.csv"
DEFAULT_FIGHT_LOG = REPO_ROOT / "data" / "reports" / "pre_event_prediction_fights.csv"
DEFAULT_EVENT_LOG = REPO_ROOT / "data" / "reports" / "pre_event_prediction_events.csv"


def _load_database_predictions() -> pd.DataFrame:
    conn = get_connection()
    try:
        query = """
            SELECT
                event_id::text,
                event_name,
                event_date,
                fight_id::text,
                fighter_1_name,
                fighter_2_name,
                weight_class,
                scored_at,
                pre_event_evidence,
                ''::text AS prediction_dir_mtime,
                ''::text AS prediction_file,
                predicted_prob_f1,
                calibrated_prob_f1,
                predicted_label,
                predicted_winner_name,
                confidence_tier,
                is_uncertain,
                actual_label,
                actual_winner_name,
                resolved,
                correct,
                model_name,
                model_artifact
            FROM pre_event_prediction_fights
            ORDER BY event_date, event_name, fight_id
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()


def _build_fight_log_from_database() -> pd.DataFrame:
    preds = _load_database_predictions()
    if preds.empty:
        return pd.DataFrame()

    preds["event_date"] = pd.to_datetime(preds["event_date"], errors="coerce").dt.date
    preds["scored_at"] = pd.to_datetime(preds["scored_at"], errors="coerce", utc=True)

    columns = [
        "event_id",
        "event_name",
        "event_date",
        "fight_id",
        "fighter_1_name",
        "fighter_2_name",
        "weight_class",
        "scored_at",
        "pre_event_evidence",
        "prediction_dir_mtime",
        "prediction_file",
        "predicted_prob_f1",
        "calibrated_prob_f1",
        "predicted_label",
        "predicted_winner_name",
        "confidence_tier",
        "is_uncertain",
        "actual_label",
        "actual_winner_name",
        "resolved",
        "correct",
        "model_name",
        "model_artifact",
    ]
    return preds[columns].sort_values(["event_date", "event_name", "fight_id"])


def _read_csv_rows(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.DataFrame(iter_data_rows(path))


def _load_saved_predictions(predictions_dir: Path) -> pd.DataFrame:
    frames = []
    for path in sorted(predictions_dir.glob("*/predictions.csv")):
        df = pd.read_csv(path)
        if df.empty:
            continue
        prediction_dir_mtime = pd.to_datetime(path.parent.stat().st_mtime, unit="s", utc=True)
        df["prediction_file"] = str(path.relative_to(REPO_ROOT))
        df["prediction_dir_mtime"] = prediction_dir_mtime
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    preds = pd.concat(frames, ignore_index=True)
    preds["event_date"] = pd.to_datetime(preds["event_date"], errors="coerce").dt.date
    preds["scored_at"] = pd.to_datetime(preds["scored_at"], errors="coerce", utc=True)
    preds["scored_date"] = preds["scored_at"].dt.date
    preds["prediction_dir_date"] = preds["prediction_dir_mtime"].dt.date
    scored_before = preds["scored_date"] < preds["event_date"]
    dir_before = preds["prediction_dir_date"] < preds["event_date"]
    preds["made_before_event"] = scored_before | dir_before
    preds["pre_event_evidence"] = np.select(
        [scored_before, dir_before],
        ["scored_at_before_event", "prediction_dir_before_event"],
        default="not_pre_event",
    )
    return preds[preds["made_before_event"]].copy()


def _load_actuals() -> pd.DataFrame:
    fights = _read_csv_rows(FIGHTS_CSV)
    events = _read_csv_rows(EVENTS_CSV)

    if fights.empty:
        return pd.DataFrame()

    actuals = fights[[
        "scraped_at",
        "fight_id",
        "event_id",
        "fighter_1_id",
        "fighter_2_id",
        "fighter_1_outcome",
        "fighter_2_outcome",
        "event_status",
    ]].copy()
    actuals["scraped_at"] = pd.to_datetime(actuals["scraped_at"], errors="coerce", utc=True)
    actuals = actuals.sort_values(["fight_id", "scraped_at"]).drop_duplicates(
        "fight_id",
        keep="last",
    )

    actuals["actual_label"] = np.select(
        [
            actuals["fighter_1_outcome"].eq("W") & actuals["fighter_2_outcome"].eq("L"),
            actuals["fighter_1_outcome"].eq("L") & actuals["fighter_2_outcome"].eq("W"),
        ],
        [1, 0],
        default=np.nan,
    )
    actuals["resolved"] = actuals["actual_label"].notna()

    if not events.empty:
        events["scraped_at"] = pd.to_datetime(events["scraped_at"], errors="coerce", utc=True)
        events = events.sort_values(["event_id", "scraped_at"]).drop_duplicates(
            "event_id",
            keep="last",
        )
        event_cols = ["event_id", "name", "date_formatted"]
        actuals = actuals.merge(events[event_cols], on="event_id", how="left")
        actuals = actuals.rename(columns={
            "name": "event_name",
            "date_formatted": "actual_event_date",
        })

    return actuals


def _load_fighter_names() -> dict[str, str]:
    fighters = _read_csv_rows(REPO_ROOT / "data" / "fighters.csv")
    if fighters.empty:
        return {}
    fighters["scraped_at"] = pd.to_datetime(fighters["scraped_at"], errors="coerce", utc=True)
    fighters = fighters.sort_values(["fighter_id", "scraped_at"]).drop_duplicates(
        "fighter_id",
        keep="last",
    )
    return dict(zip(fighters["fighter_id"].astype(str), fighters["full_name"].astype(str)))


def _mark_replaced_fights(review: pd.DataFrame, actuals: pd.DataFrame) -> pd.DataFrame:
    """Label predictions whose original bout was replaced by another same-event bout."""
    if review.empty or actuals.empty:
        return review

    completed = actuals[actuals["resolved"].eq(True)].copy()
    if completed.empty:
        return review

    fighter_names = _load_fighter_names()
    replacements: dict[int, str] = {}
    unresolved = review["actual_label"].isna()
    for idx, row in review[unresolved].iterrows():
        predicted_fighters = {str(row.get("fighter_1_id")), str(row.get("fighter_2_id"))}
        candidates = completed[
            completed["event_id"].astype(str).eq(str(row.get("event_id")))
            & ~completed["fight_id"].astype(str).eq(str(row.get("fight_id")))
            & (
                completed["fighter_1_id"].astype(str).isin(predicted_fighters)
                | completed["fighter_2_id"].astype(str).isin(predicted_fighters)
            )
        ].copy()
        if candidates.empty:
            continue

        candidates["_overlap_count"] = (
            candidates["fighter_1_id"].astype(str).isin(predicted_fighters).astype(int)
            + candidates["fighter_2_id"].astype(str).isin(predicted_fighters).astype(int)
        )
        replacement = candidates.sort_values(
            ["_overlap_count", "scraped_at"],
            ascending=[False, False],
        ).iloc[0]

        predicted_names = {
            str(row.get("fighter_1_id")): str(row.get("fighter_1_name")),
            str(row.get("fighter_2_id")): str(row.get("fighter_2_name")),
        }

        def _fighter_name(fighter_id: str) -> str:
            return predicted_names.get(fighter_id) or fighter_names.get(fighter_id) or fighter_id

        f1_name = _fighter_name(str(replacement["fighter_1_id"]))
        f2_name = _fighter_name(str(replacement["fighter_2_id"]))
        replacements[idx] = f"Fighter changed: {f1_name} vs {f2_name}"

    if replacements:
        for idx, label in replacements.items():
            review.at[idx, "actual_winner_name"] = label
            review.at[idx, "resolved"] = False
            review.at[idx, "correct"] = pd.NA

    return review


def _build_fight_log(predictions_dir: Path) -> pd.DataFrame:
    preds = _load_saved_predictions(predictions_dir)
    if preds.empty:
        return pd.DataFrame()

    actuals = _load_actuals()
    if actuals.empty:
        return pd.DataFrame()

    # Keep the latest saved prediction before event day for each fight.
    preds = preds.sort_values(["fight_id", "scored_at"]).drop_duplicates(
        "fight_id",
        keep="last",
    )

    review = preds.merge(actuals, on="fight_id", how="left", suffixes=("", "_actual"))
    review["predicted_label"] = (review["calibrated_prob_f1"].astype(float) >= 0.5).astype(int)
    review["predicted_winner_name"] = np.where(
        review["predicted_label"].eq(1),
        review["fighter_1_name"],
        review["fighter_2_name"],
    )
    review["actual_winner_name"] = np.select(
        [
            review["actual_label"].eq(1),
            review["actual_label"].eq(0),
        ],
        [
            review["fighter_1_name"],
            review["fighter_2_name"],
        ],
        default="Pending / no W-L result",
    )
    resolved_mask = review["resolved"].eq(True) & review["actual_label"].notna()
    review["correct"] = pd.Series(pd.NA, index=review.index, dtype="object")
    review.loc[resolved_mask, "correct"] = review.loc[resolved_mask, "predicted_label"].eq(
        review.loc[resolved_mask, "actual_label"].astype(int),
    ).to_numpy()
    review = _mark_replaced_fights(review, actuals)

    columns = [
        "event_id",
        "event_name",
        "event_date",
        "fight_id",
        "fighter_1_name",
        "fighter_2_name",
        "weight_class",
        "scored_at",
        "pre_event_evidence",
        "prediction_dir_mtime",
        "prediction_file",
        "predicted_prob_f1",
        "calibrated_prob_f1",
        "predicted_label",
        "predicted_winner_name",
        "confidence_tier",
        "is_uncertain",
        "actual_label",
        "actual_winner_name",
        "resolved",
        "correct",
        "model_name",
        "model_artifact",
    ]
    return review[[c for c in columns if c in review.columns]].sort_values(
        ["event_date", "event_name", "fight_id"],
    )


def _build_event_log(fight_log: pd.DataFrame) -> pd.DataFrame:
    resolved = fight_log[fight_log["resolved"].eq(True) & fight_log["actual_label"].notna()].copy()
    if resolved.empty:
        return pd.DataFrame()

    rows = []
    group_cols = ["event_id", "event_name", "event_date", "model_name"]
    for keys, grp in resolved.groupby(group_cols, dropna=False, sort=True):
        event_id, event_name, event_date, model_name = keys
        y_true = grp["actual_label"].astype(int).values
        y_prob = grp["calibrated_prob_f1"].astype(float).values
        y_pred = grp["predicted_label"].astype(int).values
        correct = grp["correct"].astype(bool)
        scored_at_min = pd.to_datetime(grp["scored_at"], errors="coerce", utc=True).min()
        scored_at_max = pd.to_datetime(grp["scored_at"], errors="coerce", utc=True).max()

        row = {
            "event_id": event_id,
            "event_name": event_name,
            "event_date": event_date,
            "model_name": model_name,
            "pre_event_evidence": ",".join(sorted(grp["pre_event_evidence"].dropna().unique())),
            "n_predicted_fights": int(len(grp)),
            "correct": int(correct.sum()),
            "accuracy": float(accuracy_score(y_true, y_pred)),
            "log_loss": float(log_loss(y_true, y_prob, labels=[0, 1])),
            "brier_score": float(brier_score_loss(y_true, y_prob)),
            "first_scored_at": scored_at_min.isoformat() if pd.notna(scored_at_min) else "",
            "last_scored_at": scored_at_max.isoformat() if pd.notna(scored_at_max) else "",
            "high_count": int((grp["confidence_tier"] == "high").sum()),
            "medium_count": int((grp["confidence_tier"] == "medium").sum()),
            "toss_up_count": int((grp["confidence_tier"] == "toss-up").sum()),
        }

        for tier in ["high", "medium", "toss-up"]:
            mask = grp["confidence_tier"] == tier
            key = tier.replace("-", "_")
            row[f"{key}_accuracy"] = (
                float(grp.loc[mask, "correct"].astype(bool).mean()) if mask.any() else np.nan
            )

        rows.append(row)

    return pd.DataFrame(rows).sort_values("event_date", ascending=False)


def _load_reviewed_event_log(existing_event_log: pd.DataFrame) -> pd.DataFrame:
    """Load manually reviewed pre-event summaries from models/prediction_log.csv.

    ``prediction_log.csv`` is produced by the post-event review workflow. It is
    event-level only, so it complements the fight-level saved prediction files
    when older prediction CSVs were overwritten or partially regenerated.
    """
    if not PREDICTION_LOG.exists():
        return pd.DataFrame()

    log = pd.read_csv(PREDICTION_LOG)
    if log.empty:
        return pd.DataFrame()

    existing_keys = set()
    if not existing_event_log.empty:
        existing_keys = set(
            zip(
                existing_event_log["event_name"].astype(str),
                existing_event_log["event_date"].astype(str),
            ),
        )

    rows = []
    for _, row in log.iterrows():
        key = (str(row["event_name"]), str(row["event_date"]))
        if key in existing_keys:
            continue

        n_fights = int(row["n_fights"])
        accuracy = float(row["accuracy"])
        rows.append({
            "event_id": "",
            "event_name": row["event_name"],
            "event_date": row["event_date"],
            "model_name": row.get("model_name", ""),
            "pre_event_evidence": "reviewed_prediction_log",
            "n_predicted_fights": n_fights,
            "correct": int(round(n_fights * accuracy)),
            "accuracy": accuracy,
            "log_loss": float(row["log_loss"]),
            "brier_score": float(row["brier_score"]),
            "first_scored_at": "",
            "last_scored_at": "",
            "high_count": np.nan,
            "medium_count": np.nan,
            "toss_up_count": np.nan,
            "high_accuracy": np.nan,
            "medium_accuracy": np.nan,
            "toss_up_accuracy": np.nan,
        })

    return pd.DataFrame(rows)


def build_logs(
    predictions_dir: Path = PREDICTIONS_DIR,
    source: str = "auto",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if source not in {"auto", "db", "csv"}:
        raise ValueError("source must be one of: auto, db, csv")

    fight_log = pd.DataFrame()
    if source in {"auto", "db"}:
        try:
            fight_log = _build_fight_log_from_database()
        except Exception as exc:
            if source == "db":
                raise
            print(f"Database prediction log unavailable, falling back to CSV files: {exc}")

    if fight_log.empty and source in {"auto", "csv"}:
        fight_log = _build_fight_log(predictions_dir)

    event_log = _build_event_log(fight_log) if not fight_log.empty else pd.DataFrame()
    reviewed_log = _load_reviewed_event_log(event_log)
    if not reviewed_log.empty:
        event_log = pd.concat([event_log, reviewed_log], ignore_index=True)
        event_log["_event_date_sort"] = pd.to_datetime(event_log["event_date"], errors="coerce")
        event_log = event_log.sort_values("_event_date_sort", ascending=False).drop(
            columns=["_event_date_sort"],
        )
    return fight_log, event_log


def main() -> None:
    parser = argparse.ArgumentParser(description="Build log of real pre-event predictions.")
    parser.add_argument("--predictions-dir", default=str(PREDICTIONS_DIR))
    parser.add_argument("--fight-log", default=str(DEFAULT_FIGHT_LOG))
    parser.add_argument("--event-log", default=str(DEFAULT_EVENT_LOG))
    parser.add_argument(
        "--source",
        choices=["auto", "db", "csv"],
        default="auto",
        help="Prediction source. Default: auto (database, then CSV fallback).",
    )
    args = parser.parse_args()

    fight_log, event_log = build_logs(Path(args.predictions_dir), source=args.source)

    fight_path = Path(args.fight_log)
    event_path = Path(args.event_log)
    fight_path.parent.mkdir(parents=True, exist_ok=True)
    event_path.parent.mkdir(parents=True, exist_ok=True)

    fight_log.to_csv(fight_path, index=False)
    event_log.to_csv(event_path, index=False)

    print(f"Saved fight-level pre-event log: {fight_path}")
    print(f"Saved event-level pre-event log: {event_path}")
    print(f"Pre-event predicted fights: {len(fight_log):,}")
    print(f"Resolved predicted fights: {int(fight_log['resolved'].sum()) if not fight_log.empty else 0:,}")
    print(f"Resolved events: {len(event_log):,}")

    if not event_log.empty:
        accuracy = event_log["correct"].sum() / event_log["n_predicted_fights"].sum()
        print(f"Overall resolved accuracy: {accuracy:.1%}")


if __name__ == "__main__":
    main()
