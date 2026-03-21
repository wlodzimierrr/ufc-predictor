# Phase 4 Execution Tickets

Phase 4 in [ufc-predictor.md](ufc-predictor.md) is `Baseline Modeling`. This file turns
that scope into a real implementation backlog.

**Goal:** Train, evaluate, and compare models that produce calibrated pre-fight win
probabilities from the `bout_features` table. Establish a reproducible training pipeline
with time-based validation, calibration checks, and model explainability.

**Architecture decisions:**
- All models consume `bout_features` rows directly — no re-querying the warehouse at
  training time. The feature pipeline (Phase 3) is the single source of truth.
- Temporal splits only — never random. Models are evaluated on fights they could not have
  seen during training.
- Model artifacts, metrics, and predictions are persisted for reproducibility and comparison.
- Calibration matters more than raw accuracy. This is a probability estimation system, not
  a classification system.
- LightGBM is the primary production candidate. Logistic regression and Elo are baselines
  for comparison. All three are trained and compared on the same splits.

**Data baseline (from Phase 3 handoff):**
- 8,550 bout feature rows (label: 1 = fighter_1 wins, 0 = fighter_2 wins, NULL = draw/NC)
- ~8,400 labeled rows after excluding draws/NCs (150 NULL labels)
- Features: 29 bout-level columns (20 diffs + 5 ratios + 4 matchup/metadata)
- Temporal range: 1993–2026 (~32 years of UFC history)
- Core-diff completeness: 54.8% overall, 58.5% for non-debut bouts
- 537 both-debuting bouts (6.3%) with all core diffs NULL

---

## T4.1 — Training Infrastructure

#### T4.1.1 Data loading and temporal split module ✅ DONE
- **Description:** Build a module that loads `bout_features` from the database, excludes
  unlabeled rows (draws/NCs), selects model-ready feature columns, and produces temporal
  train/validation/test splits.
- **Status:** DONE
- **Acceptance Criteria:**
  - `modeling/data.py` provides:
    - `load_bout_data(conn) -> DataFrame` — loads bout_features, drops NULL labels, casts
      types, returns a pandas DataFrame sorted by event_date.
    - `FEATURE_COLS: list[str]` — the canonical list of feature columns used for training
      (excludes IDs, dates, label, metadata not used as features).
    - `temporal_split(df, val_date, test_date) -> (train, val, test)` — splits by
      `event_date < val_date`, `val_date <= event_date < test_date`, `event_date >= test_date`.
    - `rolling_cv(df, n_folds, min_train_years, val_months) -> list[(train, val)]` —
      generates time-ordered folds with expanding training windows and fixed-length
      validation windows. Each fold's validation period is later than the previous fold's.
  - Default split dates: test holdout = last 2 years of data, validation = 2 years before
    that. These are configurable, not hardcoded.
  - Both-debuting bouts (all diffs NULL) are included but flagged — models must handle them.
  - Unit tests in `modeling/tests/test_data.py` verify:
    - No temporal leakage (no future fights in training set).
    - Fold ordering is strictly chronological.
    - Label distribution is reported per split.
    - NULL handling: feature columns with NULLs are preserved (not silently dropped).
- **Dependencies:** Phase 3 complete
- **Complexity:** M
- **Risk:** Low
- **Notes:** LightGBM handles NaN natively. For logistic regression, a preprocessing step
  will impute or drop NaN columns — that belongs in the model-specific trainer, not here.

#### T4.1.2 Evaluation framework ✅ DONE
- **Description:** Build reusable evaluation utilities that compute all required metrics and
  produce comparison reports.
- **Status:** DONE
- **Acceptance Criteria:**
  - `modeling/evaluate.py` provides:
    - `compute_metrics(y_true, y_prob) -> dict` — returns accuracy, log loss, Brier score,
      ROC AUC, and expected calibration error (ECE, 10 bins).
    - `calibration_table(y_true, y_prob, n_bins=10) -> DataFrame` — bins predictions by
      predicted probability, shows mean predicted vs actual win rate per bin, and bin count.
    - `print_report(metrics, name) -> None` — formatted console output.
    - `compare_models(results: dict[str, dict]) -> DataFrame` — side-by-side metric
      comparison table across models.
  - Calibration plot function (matplotlib): reliability diagram with identity line, bins,
    and histogram of prediction counts.
  - All metrics are computed on the same held-out set for fair comparison.
  - Unit tests in `modeling/tests/test_evaluate.py` with synthetic data verify metric
    computation matches known values.
- **Dependencies:** None (pure utility)
- **Complexity:** S
- **Risk:** Low

#### T4.1.3 Model persistence and artifact management ✅ DONE
- **Description:** Define a standard format for saving trained models, metadata, and
  evaluation results.
- **Status:** DONE
- **Acceptance Criteria:**
  - `modeling/artifacts.py` provides:
    - `save_model(model, metadata, path)` — persists the model object (joblib/pickle),
      a `metadata.json` (feature list, split dates, hyperparameters, training row count,
      feature_version), and `metrics.json` (evaluation results on val and test sets).
    - `load_model(path) -> (model, metadata)` — loads a saved model and its metadata.
  - Artifact directory structure: `models/<model_name>/<timestamp>/` containing
    `model.joblib`, `metadata.json`, `metrics.json`.
  - `metadata.json` includes enough information to reproduce the training run.
  - Unit tests verify round-trip save/load.
- **Dependencies:** None
- **Complexity:** S
- **Risk:** Low
- **Notes:** Keep this simple. No MLflow or experiment tracking servers — just files on disk.

---

## T4.2 — Baselines

#### T4.2.1 Naive baselines
- **Description:** Implement simple heuristic baselines that require no training. These set
  the floor for model performance.
- **Status:** DONE
- **Acceptance Criteria:**
  - `modeling/baselines.py` provides:
    - `favorite_baseline(df) -> array` — predicts fighter_1 wins if
      `diff_career_win_rate > 0`, else fighter_2 wins. Returns P(fighter_1 wins) as 1.0 or
      0.0 (hard predictions) and a soft version using the raw win rate difference mapped
      to [0.3, 0.7] range.
    - `coin_flip_baseline(df) -> array` — returns 0.5 for every fight (calibration
      reference point).
    - `elo_baseline(df) -> array` — converts `diff_elo` to win probability using the
      standard logistic formula: `P = 1 / (1 + 10^(-diff_elo / 400))`.
  - Each baseline is evaluated on the test set using the evaluation framework.
  - Results are saved as a baseline comparison row in the model comparison table.
  - Tests verify output shapes and probability bounds [0, 1].
- **Dependencies:** T4.1.1, T4.1.2
- **Complexity:** S
- **Risk:** Low
- **Notes:** The Elo baseline uses the pre-computed `diff_elo` from bout_features (already
  built in Phase 3). No additional Elo computation needed.

---

## T4.3 — Model Training

#### T4.3.1 Logistic regression trainer
- **Description:** Train a regularized logistic regression as the interpretable baseline
  model.
- **Status:** TODO
- **Acceptance Criteria:**
  - `modeling/train_logreg.py` provides:
    - Loads data via `modeling/data.py`, applies temporal split.
    - Preprocessing: imputes NaN with median (fit on train only), standardizes features
      (fit on train only). Preprocessing pipeline is persisted with the model.
    - Trains `LogisticRegression(penalty='l2', solver='lbfgs', max_iter=1000)`.
    - Tunes regularization strength `C` via rolling CV (T4.1.1), selecting by log loss.
    - Evaluates on validation and test sets using `modeling/evaluate.py`.
    - Saves model artifact via `modeling/artifacts.py`.
  - Script is runnable: `python modeling/train_logreg.py`
  - Console output shows: split sizes, best C, CV log loss per fold, final test metrics,
    calibration table.
  - Feature coefficients are printed (sorted by absolute value) for interpretability.
  - `make train_logreg` Makefile target.
- **Dependencies:** T4.1.1, T4.1.2, T4.1.3
- **Complexity:** M
- **Risk:** Low
- **Notes:** Logistic regression requires complete features (no NaN). The preprocessing
  pipeline handles this. Expect moderate performance — this is the interpretability anchor.

#### T4.3.2 LightGBM trainer
- **Description:** Train a LightGBM gradient-boosted tree model as the primary production
  candidate.
- **Status:** TODO
- **Acceptance Criteria:**
  - `modeling/train_lgbm.py` provides:
    - Loads data via `modeling/data.py`, applies temporal split.
    - No imputation needed — LightGBM handles NaN natively.
    - Default hyperparameters: `objective='binary'`, `metric='binary_logloss'`,
      `num_leaves=31`, `learning_rate=0.05`, `n_estimators=500`, `early_stopping_rounds=50`,
      `feature_fraction=0.8`, `bagging_fraction=0.8`, `bagging_freq=5`.
    - Tunes key hyperparameters (`num_leaves`, `learning_rate`, `min_child_samples`) via
      rolling CV, selecting by log loss.
    - Evaluates on validation and test sets using `modeling/evaluate.py`.
    - Saves model artifact via `modeling/artifacts.py`.
  - Script is runnable: `python modeling/train_lgbm.py`
  - Console output shows: split sizes, best hyperparameters, CV log loss per fold, final
    test metrics, calibration table, top-20 feature importances (gain).
  - `make train_lgbm` Makefile target.
- **Dependencies:** T4.1.1, T4.1.2, T4.1.3
- **Complexity:** M
- **Risk:** Medium — hyperparameter tuning on small rolling CV folds can be noisy.
- **Notes:** With ~8,400 labeled rows and ~29 features, overfitting is a real concern.
  Conservative defaults (shallow trees, regularization) are intentional. Early stopping
  on validation loss is critical.

---

## T4.4 — Calibration and Explainability

#### T4.4.1 Probability calibration
- **Description:** Assess and optionally improve the calibration of model probabilities.
- **Status:** TODO
- **Acceptance Criteria:**
  - `modeling/calibrate.py` provides:
    - `assess_calibration(y_true, y_prob, n_bins=10) -> dict` — returns ECE, MCE (max
      calibration error), and per-bin stats.
    - `plot_calibration(y_true, y_prob, model_name, save_path)` — saves a reliability
      diagram (predicted vs actual) with a histogram of prediction counts.
    - `calibrate_isotonic(y_prob_train, y_true_train, y_prob_test) -> array` — fits
      isotonic regression on validation set, transforms test set probabilities.
    - `calibrate_platt(y_prob_train, y_true_train, y_prob_test) -> array` — fits Platt
      scaling on validation set, transforms test set probabilities.
  - Calibration is assessed for each model (logreg, LightGBM, Elo baseline).
  - If a model's ECE > 0.05, apply isotonic or Platt scaling and report the improvement.
  - Calibration plots saved to `models/<model_name>/<timestamp>/calibration.png`.
  - Tests verify that perfectly calibrated synthetic predictions have ECE ≈ 0.
- **Dependencies:** T4.3.1, T4.3.2
- **Complexity:** S
- **Risk:** Low
- **Notes:** Logistic regression is usually well-calibrated by default. LightGBM often
  benefits from post-hoc calibration. Elo probabilities are analytically calibrated but
  may drift on UFC-specific data.

#### T4.4.2 Feature importance and SHAP analysis
- **Description:** Compute and visualize feature importances and SHAP values for the
  LightGBM model.
- **Status:** TODO
- **Acceptance Criteria:**
  - `modeling/explain.py` provides:
    - `feature_importance_plot(model, feature_names, save_path)` — bar chart of top-20
      features by LightGBM gain, saved as PNG.
    - `shap_summary(model, X_test, feature_names, save_path)` — SHAP beeswarm plot for
      the test set, saved as PNG.
    - `shap_single_fight(model, X_row, feature_names) -> dict` — returns SHAP values for
      a single prediction (for analyst-facing explanations).
  - Output saved to `models/<model_name>/<timestamp>/importance.png` and `shap_summary.png`.
  - Console output lists top-10 features by SHAP mean absolute value.
  - Tests verify SHAP values sum approximately to the model output (additivity check).
- **Dependencies:** T4.3.2
- **Complexity:** S
- **Risk:** Low — SHAP for tree models is fast (TreeExplainer).
- **Notes:** Only computed for LightGBM. Logistic regression coefficients already serve as
  interpretability for that model.

---

## T4.5 — Comparison and Analysis

#### T4.5.1 Model comparison report
- **Description:** Produce a single comparison report across all models and baselines on
  the same test set.
- **Status:** TODO
- **Acceptance Criteria:**
  - `modeling/compare.py` provides:
    - Loads all saved model artifacts from `models/`.
    - Evaluates each model on the same test set (loaded via `modeling/data.py`).
    - Produces a comparison table:
      ```
      Model              Accuracy  Log Loss  Brier   AUC    ECE
      ─────────────────  ────────  ────────  ──────  ─────  ─────
      Coin flip          0.500     0.693     0.250   0.500  0.000
      Favorite baseline  0.XXX     X.XXX     X.XXX   X.XXX  X.XXX
      Elo baseline       0.XXX     X.XXX     X.XXX   X.XXX  X.XXX
      Logistic Reg       0.XXX     X.XXX     X.XXX   X.XXX  X.XXX
      LightGBM           0.XXX     X.XXX     X.XXX   X.XXX  X.XXX
      LightGBM (calib)   0.XXX     X.XXX     X.XXX   X.XXX  X.XXX
      ```
    - Produces overlay calibration plot (all models on one reliability diagram).
    - Saves report to `models/comparison_report.txt` and plot to `models/calibration_comparison.png`.
  - Script is runnable: `python modeling/compare.py`
  - `make compare_models` Makefile target.
- **Dependencies:** T4.2.1, T4.3.1, T4.3.2, T4.4.1
- **Complexity:** S
- **Risk:** Low

#### T4.5.2 Error analysis
- **Description:** Analyze model errors by segment to identify systematic weaknesses.
- **Status:** TODO
- **Acceptance Criteria:**
  - `modeling/error_analysis.py` provides:
    - Segments test set predictions by:
      - **Weight class:** per-division accuracy and log loss.
      - **Title fights vs non-title:** does the model perform differently on high-profile fights?
      - **Debut fighters:** fights where one or both fighters are debuting (`both_debuting`,
        or `diff_career_fights` indicating one debut).
      - **Era:** split by decade or 5-year blocks to detect drift.
      - **Confidence buckets:** bin by predicted probability (0.5–0.6, 0.6–0.7, ..., 0.9–1.0)
        and show accuracy per bucket.
    - Prints a formatted report for each segment.
    - Saves to `models/error_analysis.txt`.
  - Script is runnable: `python modeling/error_analysis.py`
  - `make error_analysis` Makefile target.
- **Dependencies:** T4.3.2, T4.5.1
- **Complexity:** S
- **Risk:** Low
- **Notes:** This is informational analysis, not a pass/fail gate. It identifies where the
  model is weakest so Phase 5 (if any) can focus improvements.

---

## T4.N — Notebooks

#### T4.N.1 Data exploration notebook ✅ DONE
- **Description:** Interactive Jupyter notebook for exploring the bout_features dataset,
  feature distributions, missingness, correlations, and temporal splits.
- **Status:** DONE
- **Acceptance Criteria:**
  - `notebooks/01_data_exploration.ipynb` runs top-to-bottom with kernel restart + run all.
  - Covers: dataset overview, label distribution over time, missingness analysis, feature
    histograms, feature-to-label correlations, inter-feature correlation heatmap,
    distributions by outcome, temporal split visualization, rolling CV fold diagram,
    Elo baseline preview, and modeling takeaways.
- **Dependencies:** T4.1.1
- **Complexity:** S
- **Risk:** Low

#### T4.N.2 Model comparison notebook
- **Description:** Interactive notebook that trains all models, displays metrics, calibration
  plots, SHAP analysis, and error breakdowns in a single reproducible document.
- **Status:** TODO
- **Acceptance Criteria:**
  - `notebooks/02_model_comparison.ipynb` runs top-to-bottom.
  - Covers:
    - Trains or loads all model artifacts (baselines, logistic regression, LightGBM).
    - Side-by-side metric table (accuracy, log loss, Brier, AUC, ECE).
    - Overlay calibration plot (all models on one reliability diagram).
    - LightGBM feature importance bar chart and SHAP beeswarm plot.
    - Error analysis segments: weight class, title fights, debut fighters, confidence buckets.
    - Single-fight prediction example with SHAP waterfall explanation.
  - Someone unfamiliar with the codebase can open this notebook and understand how well
    the models work, where they fail, and why they make specific predictions.
- **Dependencies:** T4.5.1, T4.5.2, T4.4.2
- **Complexity:** M
- **Risk:** Low
- **Notes:** This is the presentation layer — it consumes the scripts from T4.2–T4.5 rather
  than duplicating their logic. Import from modeling modules, don't copy-paste.

---

## T4.6 — Closeout

#### T4.6.1 Makefile targets and Phase 4 runbook
- **Description:** Add modeling commands to the Makefile and document the Phase 4 workflow
  in the runbook.
- **Status:** TODO
- **Acceptance Criteria:**
  - `Makefile` gains targets:
    - `train_logreg` — trains logistic regression
    - `train_lgbm` — trains LightGBM
    - `compare_models` — runs model comparison report
    - `error_analysis` — runs error analysis
    - `train_all` — full sequence: train_logreg → train_lgbm → compare_models
  - `docs/runbook.md` gains a **Phase 4 Baseline Modeling** section covering:
    - How to train each model
    - How to compare models
    - How to generate predictions for a new fight
    - Known limitations and failure modes
  - **Phase 4 handoff checklist** documents what Phase 5 (deployment/inference) can rely on:
    - Best model identified and persisted
    - Calibration assessed
    - Error analysis reviewed
    - Comparison report available
- **Dependencies:** T4.5.1, T4.5.2
- **Complexity:** S
- **Risk:** Low

---

## Dependency Graph

```
Phase 3 (complete)
    │
    ├── T4.1.1 (data + splits)──┬── T4.2.1 (baselines)─────────────────┐
    │       │                   │                                       │
    │    T4.N.1 (EDA notebook)  │                                       │
    │                           │                                       │
    ├── T4.1.2 (eval framework)─┤── T4.3.1 (logreg)────┐               │
    │                           │                       │               │
    └── T4.1.3 (artifacts)──────┤── T4.3.2 (LightGBM)──┤               │
                                │       │               │               │
                                │       │         T4.4.1 (calibration)──┤
                                │       │                               │
                                │       └── T4.4.2 (SHAP)              │
                                │                                       │
                                └───────────────────── T4.5.1 (compare)─┤
                                                            │           │
                                                       T4.5.2 (errors) │
                                                            │           │
                                                       T4.N.2 (model notebook)
                                                            │
                                                       T4.6.1 (closeout)
```

**Critical path:** T4.1.1 → T4.3.2 → T4.4.1 → T4.5.1 → T4.6.1

---

## Suggested Execution Order

| Day | Tickets |
|---|---|
| 1 | T4.1.1, T4.1.2, T4.1.3, T4.N.1 (infrastructure + EDA notebook) |
| 2 | T4.2.1 (baselines), T4.3.1 (logistic regression) |
| 3 | T4.3.2 (LightGBM) |
| 4 | T4.4.1 (calibration), T4.4.2 (SHAP) |
| 5 | T4.5.1 (comparison), T4.5.2 (error analysis), T4.N.2 (model notebook), T4.6.1 (closeout) |

---

## Risk Register

| Risk | Severity | Mitigation |
|---|---|---|
| Overfitting on ~8,400 rows with 29 features | Medium | Conservative hyperparameters, early stopping, rolling CV |
| Temporal shift (UFC meta changes over 32 years) | Medium | Rolling CV across eras; error analysis by decade |
| Debut fighters with all-NULL diffs | Low | LightGBM handles NaN; logistic regression imputes; `both_debuting` flag available |
| Label imbalance (fighter_1 wins 64.3% of labeled rows) | Low | Not severe; monitor calibration in the 0.3–0.5 range |
| LightGBM not installed | Low | Add to requirements; pip install lightgbm |
| SHAP computation slow on full test set | Low | Subsample to 500 rows for SHAP analysis |
