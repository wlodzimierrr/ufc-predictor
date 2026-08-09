"""Build feature vectors for upcoming fights.

Uses the same pipeline as historical fights but targets only fights with
result_type='upcoming'. Uses today's date as the cutoff for fighter snapshots
so all features reflect current form.

The output DataFrame has the same columns as the production model expects,
ensuring no train/serve skew.

Usage:
    python features/build_upcoming.py
    # or: make build_upcoming_features
"""

from __future__ import annotations

import json
import argparse
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from features.data_loader import load_all_data
from features.history import build_fighter_index, get_history
from features.elo import compute_all_elos
from features.snapshot import build_fighter_snapshot
from features.pipeline import _bout_to_row
from features.debut_prior import compute_debut_priors, apply_debut_features
from modeling.data import load_bout_data
from warehouse.db import get_connection

REPO_ROOT = Path(__file__).resolve().parent.parent
PRODUCTION_MODEL = REPO_ROOT / "models" / "production_model.json"


def _load_feature_cols() -> list[str]:
    """Load feature columns from the production model metadata."""
    with open(PRODUCTION_MODEL) as f:
        prod = json.load(f)
    artifact_path = Path(prod["artifact_path"])
    meta_path = artifact_path / "metadata.json"
    with open(meta_path) as f:
        meta = json.load(f)
    return meta["feature_cols"]


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def build_upcoming_features(
    conn,
    *,
    from_date: date | None = None,
    to_date: date | None = None,
    include_past: bool = False,
) -> pd.DataFrame:
    """Build feature vectors for all upcoming fights.

    Returns a DataFrame with the same columns as the production model expects,
    plus identifiers (fight_id, fighter_1_id, fighter_2_id, event_date, weight_class).
    """
    print("Loading warehouse data ...", flush=True)
    data = load_all_data(conn)

    # Filter to genuinely upcoming fights by default. For catch-up prediction
    # runs, include_past lets us score locally-known upcoming rows whose event
    # dates have passed but whose results have not been loaded yet.
    today = date.today()
    upcoming = []
    for fight in data.fights:
        event = data.event_by_id.get(fight["event_id"], {})
        if fight.get("result_type") != "upcoming":
            continue
        event_date = fight.get("event_date")
        if event_date is None:
            continue
        if not include_past and event_date < today:
            continue
        if from_date and event_date < from_date:
            continue
        if to_date and event_date > to_date:
            continue
        if event.get("event_status") != "upcoming" and not (include_past and event_date <= today):
            continue
        upcoming.append(fight)

    if not upcoming:
        print("  No upcoming fights found in the warehouse.")
        return pd.DataFrame()

    print(f"  Found {len(upcoming)} upcoming fight(s)")

    # We need ALL fights (not just upcoming) to compute Elo and build history
    print("Building fighter index ...", flush=True)
    fighter_index = build_fighter_index(data)
    print(f"  indexed {len(fighter_index):,} fighters")

    print("Computing Elo ratings ...", flush=True)
    elos = compute_all_elos(data.fights)
    print(f"  rated {len(elos):,} fights")

    print("Building upcoming fight features ...", flush=True)
    bout_rows = []
    for fight in upcoming:
        f1_id = fight["fighter_1_id"]
        f2_id = fight["fighter_2_id"]
        fight_id = fight["fight_id"]

        f1_row = data.fighter_by_id.get(f1_id, {})
        f2_row = data.fighter_by_id.get(f2_id, {})

        snapshot_date = min(today, fight["event_date"])

        h1 = get_history(fighter_index, f1_id, snapshot_date)
        h2 = get_history(fighter_index, f2_id, snapshot_date)

        snap_1 = build_fighter_snapshot(
            f1_row, h1, snapshot_date, elos, fighter_index,
            fighter_id=f1_id, fight_id=fight_id,
        )
        snap_2 = build_fighter_snapshot(
            f2_row, h2, snapshot_date, elos, fighter_index,
            fighter_id=f2_id, fight_id=fight_id,
        )

        bout_rows.append(_bout_to_row(fight, snap_1, snap_2))

    df = pd.DataFrame(bout_rows)
    print(f"  built {len(df)} bout feature row(s)")

    # Apply debut priors (compute from training data)
    feature_cols = _load_feature_cols()
    has_debut_cols = any("debut_" in c for c in feature_cols)

    if has_debut_cols:
        print("Computing debut priors from training data ...", flush=True)
        # Debut columns are computed at runtime, not stored in the DB
        db_feature_cols = [c for c in feature_cols if not c.startswith("debut_")]
        train_df = load_bout_data(conn, feature_cols=db_feature_cols)
        # Training data: everything before the val_date
        with open(PRODUCTION_MODEL) as f:
            prod = json.load(f)
        artifact_path = Path(prod["artifact_path"])
        with open(artifact_path / "metadata.json") as f:
            meta = json.load(f)
        val_date = meta.get("val_date")
        if val_date:
            train_df = train_df[train_df["event_date"] < val_date]
        priors = compute_debut_priors(train_df)
        df = apply_debut_features(df, priors)
        print(f"  debut priors applied")

    # Verify columns match
    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        print(f"  warn  missing feature columns (will be NaN): {missing}")
        for c in missing:
            df[c] = None

    extra = [c for c in df.columns if c not in feature_cols
             and c not in ("fight_id", "fighter_1_id", "fighter_2_id",
                           "event_date", "weight_class", "is_title_fight",
                           "scheduled_rounds", "label", "feature_version",
                           "computed_at", "both_debuting",
                           "f1_is_southpaw", "f2_is_southpaw",
                           "weight_class_rank",
                           "is_orthodox_vs_southpaw")]
    if extra:
        print(f"  info  extra columns (not in model): {extra[:10]}")

    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Build feature vectors for upcoming fights.")
    parser.add_argument("--from-date", help="Earliest event date to include, YYYY-MM-DD")
    parser.add_argument("--to-date", help="Latest event date to include, YYYY-MM-DD")
    parser.add_argument(
        "--include-past",
        action="store_true",
        help="Include upcoming rows whose event_date is before today.",
    )
    args = parser.parse_args()

    conn = get_connection()
    try:
        df = build_upcoming_features(
            conn,
            from_date=_parse_date(args.from_date),
            to_date=_parse_date(args.to_date),
            include_past=args.include_past,
        )
        out_dir = REPO_ROOT / "models" / "upcoming"
        out_path = out_dir / "upcoming_features.csv"
        if df.empty:
            if out_path.exists():
                out_path.unlink()
                print(f"  Removed stale upcoming features file: {out_path}")
            print("\nNo upcoming features to save.")
            return

        # Save to CSV
        out_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
        print(f"\nSaved {len(df)} row(s) to {out_path}")

        # Also print a summary
        feature_cols = _load_feature_cols()
        print(f"\nFeature columns: {len(feature_cols)} expected, "
              f"{sum(1 for c in feature_cols if c in df.columns)} present")
        print(f"NaN counts in feature columns:")
        for col in feature_cols:
            if col in df.columns:
                n_na = df[col].isna().sum()
                if n_na > 0:
                    print(f"  {col}: {n_na}/{len(df)}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
