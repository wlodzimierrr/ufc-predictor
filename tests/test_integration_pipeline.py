"""End-to-end integration test for the prediction pipeline.

Inserts a synthetic upcoming fight between two known fighters, builds features,
scores the fight, and verifies the output. Cleans up after itself using a
transaction rollback.

Usage:
    python -m pytest tests/test_integration_pipeline.py -v
    # or: make test_integration
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent
PRODUCTION_MODEL = REPO_ROOT / "models" / "production_model.json"


def _db_available():
    """Check if the database is accessible."""
    try:
        from warehouse.db import get_connection
        conn = get_connection()
        conn.close()
        return True
    except Exception:
        return False


def _production_model_exists():
    """Check if production model artifact exists."""
    if not PRODUCTION_MODEL.exists():
        return False
    with open(PRODUCTION_MODEL) as f:
        prod = json.load(f)
    artifact = Path(prod["artifact_path"]) / "model.joblib"
    return artifact.exists()


skip_no_db = pytest.mark.skipif(
    not _db_available(),
    reason="Database not available"
)

skip_no_model = pytest.mark.skipif(
    not _production_model_exists(),
    reason="Production model not found"
)


def _pick_two_experienced_fighters(conn) -> tuple[dict, dict]:
    """Find two fighters with >= 10 fights each for a realistic test."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT f.fighter_id::text, f.full_name,
                   COUNT(*) as fight_count
            FROM fighters f
            JOIN fights fi ON (f.fighter_id = fi.fighter_1_id OR f.fighter_id = fi.fighter_2_id)
            WHERE fi.result_type = 'win'
            GROUP BY f.fighter_id, f.full_name
            HAVING COUNT(*) >= 10
            ORDER BY COUNT(*) DESC
            LIMIT 2
        """)
        rows = cur.fetchall()

    if len(rows) < 2:
        pytest.skip("Need at least 2 fighters with 10+ fights")

    return (
        {"fighter_id": rows[0][0], "full_name": rows[0][1], "fights": rows[0][2]},
        {"fighter_id": rows[1][0], "full_name": rows[1][1], "fights": rows[1][2]},
    )


def _pick_upcoming_event(conn) -> dict | None:
    """Find an existing upcoming event, or return None."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT event_id::text, event_name, event_date
            FROM events
            WHERE event_status = 'upcoming'
            ORDER BY event_date ASC
            LIMIT 1
        """)
        row = cur.fetchone()

    if row:
        return {"event_id": row[0], "event_name": row[1], "event_date": row[2]}
    return None


@skip_no_db
@skip_no_model
class TestIntegrationPipeline:
    """Integration test: insert synthetic fight -> build features -> score -> verify."""

    def test_full_pipeline(self):
        from warehouse.db import get_connection

        conn = get_connection()
        synthetic_event_id = None
        synthetic_fight_id = str(uuid.uuid4())

        try:
            # ── Step 1: Find two experienced fighters ──
            f1, f2 = _pick_two_experienced_fighters(conn)
            print(f"\n  Fighters: {f1['full_name']} ({f1['fights']} fights) vs "
                  f"{f2['full_name']} ({f2['fights']} fights)")

            # ── Step 2: Create synthetic upcoming event + fight ──
            event = _pick_upcoming_event(conn)
            if event:
                event_id = event["event_id"]
                print(f"  Using existing upcoming event: {event['event_name']}")
            else:
                # Create a synthetic event
                synthetic_event_id = str(uuid.uuid4())
                event_id = synthetic_event_id
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO events (event_id, event_name, event_date, event_status, source_url)
                        VALUES (%s, %s, %s, 'upcoming', 'https://test.local/integration')
                    """, (synthetic_event_id, "Integration Test Event", date.today()))
                conn.commit()
                print(f"  Created synthetic event: {synthetic_event_id}")

            # Insert synthetic fight
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fights (fight_id, event_id, fighter_1_id, fighter_2_id,
                                       result_type, weight_class, is_title_fight,
                                       scheduled_rounds, source_url, scraped_at)
                    VALUES (%s, %s, %s, %s, 'upcoming', 'middleweight', false,
                            3, 'https://test.local/integration', %s)
                """, (synthetic_fight_id, event_id, f1["fighter_id"], f2["fighter_id"],
                      datetime.now(timezone.utc)))
            conn.commit()
            print(f"  Inserted synthetic fight: {synthetic_fight_id}")

            # ── Step 3: Build features for upcoming fights ──
            from features.build_upcoming import build_upcoming_features
            features_df = build_upcoming_features(conn)

            assert not features_df.empty, "Feature builder returned empty DataFrame"
            assert synthetic_fight_id in features_df["fight_id"].values, \
                "Synthetic fight not in feature output"
            print(f"  Features built: {len(features_df)} row(s)")

            # Save features for scoring
            out_dir = REPO_ROOT / "models" / "upcoming"
            out_dir.mkdir(parents=True, exist_ok=True)
            features_df.to_csv(out_dir / "upcoming_features.csv", index=False)

            # ── Step 4: Score ──
            from modeling.score_upcoming import score_upcoming
            predictions = score_upcoming(conn)

            assert not predictions.empty, "Scoring returned empty DataFrame"
            print(f"  Predictions: {len(predictions)} row(s)")

            # ── Step 5: Verify prediction output ──
            required_cols = [
                "fight_id", "fighter_1_name", "fighter_2_name",
                "predicted_prob_f1", "calibrated_prob_f1",
                "confidence_tier", "is_uncertain", "model_name",
            ]
            for col in required_cols:
                assert col in predictions.columns, f"Missing column: {col}"

            # Check probability bounds
            assert (predictions["predicted_prob_f1"] >= 0).all(), "Raw prob < 0"
            assert (predictions["predicted_prob_f1"] <= 1).all(), "Raw prob > 1"
            assert (predictions["calibrated_prob_f1"] >= 0).all(), "Cal prob < 0"
            assert (predictions["calibrated_prob_f1"] <= 1).all(), "Cal prob > 1"

            # Check confidence tier values
            valid_tiers = {"high", "medium", "toss-up"}
            actual_tiers = set(predictions["confidence_tier"].unique())
            assert actual_tiers.issubset(valid_tiers), \
                f"Invalid tiers: {actual_tiers - valid_tiers}"

            # Check model name matches production model
            with open(PRODUCTION_MODEL) as f:
                prod = json.load(f)
            assert predictions.iloc[0]["model_name"] == prod["selected_model"], \
                "Model name doesn't match production model"

            # Find our synthetic fight in predictions
            our_pred = predictions[predictions["fight_id"] == synthetic_fight_id]
            assert len(our_pred) == 1, \
                f"Expected 1 prediction for synthetic fight, got {len(our_pred)}"

            prob = float(our_pred.iloc[0]["calibrated_prob_f1"])
            tier = our_pred.iloc[0]["confidence_tier"]
            print(f"\n  Synthetic fight prediction:")
            print(f"    {f1['full_name']} vs {f2['full_name']}")
            print(f"    P(F1 wins) = {prob:.3f}  [{tier}]")
            print(f"\n  Integration test PASSED")

        finally:
            # ── Cleanup: remove synthetic data ──
            conn.rollback()  # clear any failed transaction state
            with conn.cursor() as cur:
                cur.execute("DELETE FROM fights WHERE fight_id = %s", (synthetic_fight_id,))
                if synthetic_event_id:
                    cur.execute("DELETE FROM events WHERE event_id = %s", (synthetic_event_id,))
            conn.commit()
            conn.close()

            # Clean up the features CSV (restore to empty if it was)
            features_path = REPO_ROOT / "models" / "upcoming" / "upcoming_features.csv"
            if features_path.exists():
                features_path.unlink()

            print("  Cleanup complete")


@skip_no_db
@skip_no_model
class TestPipelineComponents:
    """Smoke tests for individual pipeline components."""

    def test_production_model_loads(self):
        from modeling.artifacts import load_model
        with open(PRODUCTION_MODEL) as f:
            prod = json.load(f)
        model, metadata = load_model(prod["artifact_path"])
        assert model is not None
        assert "feature_cols" in metadata
        assert len(metadata["feature_cols"]) > 0

    def test_feature_cols_consistent(self):
        """Production model feature_cols should match v2 + debut priors."""
        with open(PRODUCTION_MODEL) as f:
            prod = json.load(f)
        with open(Path(prod["artifact_path"]) / "metadata.json") as f:
            meta = json.load(f)
        feature_cols = meta["feature_cols"]
        assert len(feature_cols) == 50, f"Expected 50 features, got {len(feature_cols)}"

    def test_confidence_tier_function(self):
        import numpy as np
        from modeling.uncertainty import confidence_tier
        probs = np.array([0.1, 0.35, 0.5, 0.65, 0.9])
        tiers = confidence_tier(probs)
        assert tiers[0] == "high"      # 0.1
        assert tiers[1] == "medium"    # 0.35
        assert tiers[2] == "toss-up"   # 0.5
        assert tiers[3] == "medium"    # 0.65
        assert tiers[4] == "high"      # 0.9

    def test_calibrate_platt_roundtrip(self):
        import numpy as np
        from modeling.calibrate import calibrate_platt
        np.random.seed(42)
        y_prob = np.random.uniform(0.2, 0.8, 100)
        y_true = (y_prob > 0.5).astype(int)
        calibrated = calibrate_platt(y_prob, y_true, y_prob)
        assert len(calibrated) == len(y_prob)
        assert (calibrated >= 0).all()
        assert (calibrated <= 1).all()
