# Phase 6: Inference Pipeline & Evaluation Closeout

The original Phase 6 (evaluation/calibration) is largely complete from Phases 4-5:
calibration module, error analysis, uncertainty flagging, and production model selection
are all done. This phase merges the remaining evaluation closeout with the inference
pipeline (original Phase 7) to deliver end-to-end predictions for upcoming UFC cards.

---

## T6.1 — Evaluation Closeout

#### T6.1.1 Phase 4-5 combined notebook
- **Description:** Create a single Jupyter notebook that presents the full modeling story:
  feature evolution (v1 → v2), model comparison, calibration analysis, uncertainty bands,
  and production model selection. This replaces separate Phase 5 and Phase 6 notebooks.
- **Status:** DONE
- **Acceptance Criteria:**
  - `notebooks/03_phase5_modeling.ipynb` covers:
    - v1 vs v2 feature set comparison (29 → 50 features)
    - Model leaderboard table (all models, sorted by log loss)
    - Calibration overlay plot (all models on same reliability diagram)
    - Feature importance comparison (LightGBM v2 vs XGBoost)
    - Uncertainty band analysis visualization (tier distribution, accuracy by tier)
    - Debut prior analysis (why 0.5 base, distribution shift finding)
    - Production model selection rationale
  - Uses saved model artifacts and metrics — does not retrain
  - All plots have titles, labels, and legends
- **Dependencies:** Phase 5 complete
- **Complexity:** S
- **Risk:** Low

---

## T6.2 — Upcoming Fight Ingestion

#### T6.2.1 Upcoming fights scraper update
- **Description:** The events spider already scrapes upcoming events from ufcstats.com
  and tags them with `event_status="upcoming"`. But the warehouse loader (`load_events.py`)
  currently filters to completed events only. Update the loader to also persist upcoming
  events, and add a job to extract the fight card (fighter matchups) for upcoming events.
- **Status:** DONE
- **Acceptance Criteria:**
  - `warehouse/load_events.py` loads both completed and upcoming events. Upcoming events
    are identifiable via `event_status = 'upcoming'` in the events table.
  - `warehouse/load_upcoming_fights.py` provides:
    - Scrapes fight card from upcoming event pages (fighter matchups, weight class,
      scheduled rounds, title fight flag)
    - Resolves fighter names to `fighter_id` via the fighters table
    - Inserts into `fights` table with `result_type = 'upcoming'` (or a new
      `upcoming_fights` table — decide based on schema cleanliness)
    - Handles fighters not yet in the warehouse (new UFC signees) — logs warnings,
      inserts fighter stubs if possible
  - `make load_upcoming` Makefile target
  - Idempotent — re-running updates the card without duplicating rows
- **Dependencies:** Phase 1 scraper, Phase 2 warehouse
- **Complexity:** M
- **Risk:** Medium — ufcstats.com upcoming page structure may differ from completed events.
  Fighter name resolution can fail for new signees not yet in the fighters table.

#### T6.2.2 Upcoming fight feature builder
- **Description:** Build feature vectors for upcoming fights using the same pipeline as
  historical fights. The feature pipeline is already cutoff-aware — it just needs a
  wrapper that targets upcoming fights specifically.
- **Status:** DONE
- **Acceptance Criteria:**
  - `features/build_upcoming.py` provides:
    - Loads all warehouse data (same as `pipeline.py`)
    - For each upcoming fight, builds fighter snapshots using today's date as cutoff
    - Computes bout-level features (diffs, ratios, matchup)
    - Applies debut priors from the saved training priors
    - Returns a DataFrame with the same columns as `bout_features`
    - Saves to `upcoming_features` table or CSV
  - Feature columns match the production model's `metadata.json:feature_cols` exactly
  - `make build_upcoming_features` Makefile target
- **Dependencies:** T6.2.1, Phase 3 feature pipeline
- **Complexity:** S
- **Risk:** Low — reuses existing feature code. Main risk is column mismatch between
  pipeline output and model expectations.

---

## T6.3 — Batch Scoring Pipeline

#### T6.3.1 Batch scoring job
- **Description:** Load the production model, score upcoming fights, apply calibration,
  attach confidence tiers, and persist predictions.
- **Status:** DONE
- **Acceptance Criteria:**
  - `modeling/score_upcoming.py` provides:
    - Loads production model from `models/production_model.json` → artifact path
    - Loads upcoming fight features from T6.2.2
    - Generates predictions (`predict_proba`)
    - Applies Platt calibration (using saved calibration from training)
    - Attaches `confidence_tier` ("high" / "medium" / "toss-up") and `is_uncertain` flag
    - Outputs a predictions DataFrame with columns:
      `fight_id, event_date, fighter_1_name, fighter_2_name, weight_class,
       predicted_prob_f1, calibrated_prob_f1, confidence_tier, is_uncertain,
       model_name, model_artifact, scored_at`
    - Saves to `models/predictions/<event_date>/predictions.csv` and prints to console
  - Console output: formatted card with predictions, sorted by confidence
  - `make score_upcoming` Makefile target
- **Dependencies:** T6.2.2, Phase 5 production model
- **Complexity:** S
- **Risk:** Low

#### T6.3.2 Prediction CLI
- **Description:** Simple command-line interface to score a specific upcoming event or
  the next card. Wraps the batch scoring job with user-friendly output.
- **Status:** DONE
- **Acceptance Criteria:**
  - `predict.py` (repo root) provides:
    - `python predict.py` — scores all upcoming events
    - `python predict.py --event "UFC 315"` — scores a specific event by name
    - `python predict.py --next` — scores only the next upcoming event
    - Output: formatted table with fighter names, predicted probability, confidence tier,
      and a visual bar for probability
    - Supports `--format json` for machine-readable output
    - Supports `--explain` to show top-3 SHAP features for each fight
  - Does not require database access for scoring (loads from saved features CSV)
  - `make predict` Makefile target
- **Dependencies:** T6.3.1
- **Complexity:** S
- **Risk:** Low

---

## T6.4 — Post-Event Review

#### T6.4.1 Post-event accuracy tracker
- **Description:** After an event completes, compare predictions to actual results.
  Track prediction accuracy over time to detect model drift.
- **Status:** DONE
- **Acceptance Criteria:**
  - `modeling/post_event_review.py` provides:
    - Loads saved predictions for a completed event
    - Joins with actual results from the warehouse
    - Computes: accuracy, log loss, brier score, calibration by tier
    - Appends results to `models/prediction_log.csv` (cumulative tracker)
    - Prints formatted comparison: prediction vs actual for each fight, with
      correct/incorrect markers
  - `make review_event EVENT="UFC 315"` Makefile target
  - If prediction log has >= 3 events, prints rolling accuracy trend
- **Dependencies:** T6.3.1, Phase 2 warehouse
- **Complexity:** S
- **Risk:** Low

---

## Dependency Graph

```
Phase 5 (complete)
    |
    +-- T6.1.1 (notebook) — independent, can run anytime
    |
    +-- T6.2.1 (upcoming fight ingestion)
    |       |
    |   T6.2.2 (upcoming feature builder) <-- T6.2.1
    |       |
    |   T6.3.1 (batch scoring) <-- T6.2.2
    |       |
    |       +-- T6.3.2 (prediction CLI) <-- T6.3.1
    |       |
    |       +-- T6.4.1 (post-event review) <-- T6.3.1
    |
```

**Critical path:** T6.2.1 → T6.2.2 → T6.3.1 → T6.3.2

---

## Suggested Execution Order

| Day | Tickets |
|---|---|
| 1 | T6.1.1 (notebook), T6.2.1 (upcoming ingestion) — parallel |
| 2 | T6.2.2 (feature builder), T6.3.1 (batch scoring) |
| 3 | T6.3.2 (prediction CLI), T6.4.1 (post-event review) — parallel |

---

## Success Criteria for Phase 6

Phase 6 is successful when:

1. **End-to-end prediction:** `make predict` produces calibrated predictions with
   confidence tiers for the next upcoming UFC card.
2. **Reproducibility:** the prediction pipeline uses the exact same features and model
   as the Phase 5 production model — no train/serve skew.
3. **Honest output:** predictions include confidence tiers so analysts know which
   predictions to trust and which are toss-ups.
4. **Review loop:** post-event review can track prediction accuracy over time.

---

## What is NOT in Phase 6

| Item | Rationale |
|---|---|
| **Dashboard / visualization** | Deferred to Phase 8. CLI output is sufficient for now. |
| **API endpoint** | Deferred. CLI + CSV output covers analyst needs. API adds deployment complexity that isn't justified yet. |
| **Model retraining automation** | Not needed until prediction log shows drift. Manual retraining via `make train_all_v2` is sufficient. |
| **Bayesian / sequence models** | Deferred from Phase 5. Not justified until dataset grows or current model shows degradation. |
