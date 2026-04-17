# Phase 7: Visualization, Documentation & Integration Testing

Phases 1-6 delivered: warehouse, features, modeling, calibration, and inference pipeline.
Phase 7 closes the project with documentation, analyst-facing notebooks, and an
integration test to validate the full end-to-end flow.

---

## T7.1 — Documentation

#### T7.1.1 Model card
- **Description:** Formal model card documenting the production model: data sources,
  feature set, training methodology, evaluation results, known limitations, bias
  analysis (by weight class, debut status, era), and deployment context.
- **Status:** DONE
- **Acceptance Criteria:**
  - `docs/model_card.md` covers:
    - Model identity (name, version, artifact path, training date)
    - Training data summary (row counts, date range, temporal splits)
    - Feature set (v2 + debut priors, 50 columns with descriptions)
    - Training methodology (XGBoost config, Platt calibration, early stopping)
    - Evaluation metrics (log loss, AUC, Brier, ECE on test set)
    - Calibration analysis (reliability diagram reference, ECE by bin)
    - Known limitations (toss-up band accuracy, debut fighter uncertainty,
      era sensitivity, no injury/camp data)
    - Bias analysis (accuracy by weight class, title vs non-title, debut vs veteran)
    - Intended use and out-of-scope uses
  - References saved artifacts in `models/` — does not duplicate raw numbers
- **Dependencies:** Phase 6 complete
- **Complexity:** S
- **Risk:** Low

#### T7.1.2 Data dictionary
- **Description:** Single reference document covering all warehouse tables, columns,
  feature definitions, and enum values. Serves as the canonical schema reference
  for anyone working with the data.
- **Status:** DONE
- **Acceptance Criteria:**
  - `docs/data_dictionary.md` covers:
    - All warehouse tables (events, fighters, fights, fight_stats_aggregate,
      fighter_snapshots, bout_features) with column-level descriptions
    - Data types, nullability, and foreign key relationships
    - Enum values for: result_type, event_status, weight_class, stance,
      finish_method, confidence_tier
    - Feature column definitions for both v1 (29 cols) and v2 (50 cols) sets
    - Debut prior feature explanations
  - Generated from actual schema (DDL files + code), not guesswork
- **Dependencies:** Phase 6 complete
- **Complexity:** S
- **Risk:** Low

---

## T7.2 — Analyst Notebooks

#### T7.2.1 Prediction notebook
- **Description:** Interactive Jupyter notebook that runs the prediction pipeline for
  upcoming fights and presents results with visualizations. Designed for analysts
  to explore predictions before a card.
- **Status:** DONE
- **Acceptance Criteria:**
  - `notebooks/04_upcoming_predictions.ipynb` provides:
    - Loads upcoming fight features and scores them with the production model
    - Formatted fight card table with fighter names, probabilities, confidence tiers
    - Horizontal bar chart showing predicted probabilities per fight
    - Confidence tier distribution (pie/bar chart)
    - Feature contribution breakdown for top 3 most confident and top 3 toss-up fights
      (using SHAP or feature importance)
    - Calibration context: overlay upcoming prediction distribution on historical
      reliability diagram
  - Works with no upcoming data (shows placeholder message)
  - Works with live upcoming data when available
- **Dependencies:** T6.3.1 (batch scoring)
- **Complexity:** M
- **Risk:** Low — reuses existing scoring code. Main risk is no upcoming data to demo.

#### T7.2.2 Post-event review notebook
- **Description:** Reusable Jupyter notebook template for reviewing prediction accuracy
  after an event completes. Produces visual comparison of predictions vs actuals
  with calibration and tier analysis.
- **Status:** DONE
- **Acceptance Criteria:**
  - `notebooks/05_post_event_review.ipynb` provides:
    - Parameterized by event name (cell variable at top)
    - Loads saved predictions and joins with actual results from warehouse
    - Per-fight comparison table: predicted probability, actual winner, correct/incorrect
    - Accuracy summary: overall, by confidence tier, by weight class
    - Calibration plot: predicted vs actual for this event overlaid on historical curve
    - Surprise analysis: biggest upsets (high confidence wrong predictions)
    - Rolling accuracy trend (if prediction_log.csv has >= 3 events)
  - Graceful handling when no predictions exist for the event
- **Dependencies:** T6.4.1 (post-event review)
- **Complexity:** S
- **Risk:** Low

---

## T7.3 — Operations & Testing

#### T7.3.1 Runbook update
- **Description:** Update the project runbook with Phase 6 inference pipeline
  documentation: how to run predictions end-to-end, troubleshooting common issues,
  and the post-event update workflow.
- **Status:** DONE
- **Acceptance Criteria:**
  - `docs/runbook.md` updated with:
    - Inference pipeline section: `make predict_pipeline` end-to-end flow
    - Step-by-step: load upcoming → build features → score → review
    - Troubleshooting: no upcoming events, missing features, column mismatch,
      calibration warnings
    - Post-event workflow: re-scrape → reload → review → (optional) retrain
    - Model update workflow: when to retrain, how to promote a new production model
    - Quick reference table of all Makefile targets with descriptions
- **Dependencies:** Phase 6 complete
- **Complexity:** S
- **Risk:** Low

#### T7.3.2 End-to-end integration test
- **Description:** Script that validates the full prediction pipeline works correctly
  by inserting a synthetic upcoming fight, building features, scoring it, and
  verifying the output. Cleans up after itself.
- **Status:** DONE
- **Acceptance Criteria:**
  - `tests/test_integration_pipeline.py` provides:
    - Inserts a synthetic upcoming event + fight into the warehouse (two known
      fighters with history)
    - Runs feature builder for upcoming fights
    - Runs batch scoring
    - Verifies: predictions CSV exists, has correct columns, probabilities are
      in [0, 1], confidence tier is assigned, model name matches production model
    - Cleans up: removes synthetic event/fight from warehouse after test
    - Can run via `make test_integration`
  - Does NOT modify real data or retrain models
  - Skips gracefully if database is not available
- **Dependencies:** Phase 6 complete
- **Complexity:** M
- **Risk:** Medium — requires careful cleanup to avoid polluting the warehouse.
  Use a transaction rollback or explicit DELETE for cleanup.

---

## Dependency Graph

```
Phase 6 (complete)
    |
    +-- T7.1.1 (model card) — independent
    |
    +-- T7.1.2 (data dictionary) — independent
    |
    +-- T7.2.1 (prediction notebook) — independent
    |
    +-- T7.2.2 (review notebook) — independent
    |
    +-- T7.3.1 (runbook update) — independent
    |
    +-- T7.3.2 (integration test) — independent
```

All tickets are independent of each other and can be executed in any order or in parallel.

---

## Suggested Execution Order

| Day | Tickets |
|---|---|
| 1 | T7.1.1 (model card), T7.1.2 (data dictionary), T7.3.1 (runbook update) — parallel |
| 2 | T7.2.1 (prediction notebook), T7.2.2 (review notebook) — parallel |
| 3 | T7.3.2 (integration test) |

---

## Success Criteria for Phase 7

Phase 7 is successful when:

1. **Documentation:** a new contributor can understand the system from model card +
   data dictionary + runbook without reading source code.
2. **Analyst workflow:** notebooks provide visual, interactive access to predictions
   and post-event review.
3. **Confidence:** integration test proves the end-to-end pipeline works and catches
   regressions.

---

## What is NOT in Phase 7

| Item | Rationale |
|---|---|
| **Web dashboard (Streamlit/Dash)** | CLI + notebooks + Power BI cover analyst needs. Web app adds deployment complexity not justified yet. |
| **API endpoint** | Same as Phase 6 — CLI + CSV is sufficient. |
| **Model retraining automation** | Manual retraining via `make train_all_v2` is sufficient until prediction log shows drift. |
| **Bayesian / sequence models** | Research track — not justified until current model shows degradation. |
| **Betting market comparison** | Requires acquiring odds data (out of scope for current data sources). |
