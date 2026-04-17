# Phase 5 Execution Tickets

Phase 5 in [ufc-predictor.md](ufc-predictor.md) is `Advanced Modeling`. This file turns
that scope into a real implementation backlog.

**Goal:** Extract meaningful lift over the Phase 4 calibrated baselines through feature
expansion, feature selection, a second GBDT implementation, and ensemble blending. Produce
a production model selection artifact that unblocks the Phase 7 inference pipeline.

**Architecture decisions:**
- Feature set v2 is a superset of v1 — existing training infrastructure in `modeling/` is
  reused without modification. New features land in `features/` and the `bout_features`
  pipeline.
- All new models are evaluated on the same holdout test set as Phase 4 for fair comparison.
- Phase 4 LightGBM (calibrated) and Logistic Regression (calibrated) are the benchmarks to
  beat. The metric used for primary comparison is log loss on the test set.
- Model artifacts follow the same `models/<model_name>/<timestamp>/` format established in
  Phase 4.
- Research spikes (Bayesian, Sequence LSTM) are deferred to Phase 6. Phase 5 focuses on
  high-ROI production work.

**Phase 4 handoff — what Phase 5 inherits:**
- Best model: Logistic Reg (calibrated) — test log loss **0.6396**, accuracy 63.5%, AUC 0.684
- LightGBM (calibrated): test log loss **0.6426**, accuracy 62.1%, AUC 0.680
- 29 features (20 diffs + 5 ratios + 4 matchup/metadata), feature_version=1
- Key weaknesses identified from error analysis:
  - **Both-debuting bouts** (n=25): accuracy 36%, log loss 0.871 — near-random predictions
  - **Title fights** (n=48): accuracy 56.2%, log loss 0.723 — model underperforms here
  - **Light heavyweight** AUC 0.506 — division-level model barely better than random
  - **Featherweight / women's bantamweight / welterweight**: log loss 0.662–0.682 vs 0.640 overall
  - **Confidence band 0.55–0.65**: accuracy 47.1–58.5% — model is overconfident in this range
    (427 fights, 42% of the test set)
  - **2024 era** slightly worse than 2025–2026 (temporal drift still mild)

**Key insight from Phase 4:** `career.py` already computes many features (defense rates,
absorption, finish-method breakdown, title fight counts) that are never surfaced as diff
columns in `bout.py`. Half of the "v2 features" are a wiring task, not a computation task.

---

## T5.1 — Feature Improvement

#### T5.1.1 Expanded feature set (v2)
- **Description:** Wire existing career.py features into bout.py as diff/ratio columns,
  add genuinely new feature families (trends, physical ratios, activity), and produce a
  `feature_version=2` snapshot in `bout_features`.
- **Status:** DONE
- **Acceptance Criteria:**
  - **Wiring existing career.py features** — these are already computed in the snapshot but
    not surfaced as bout-level diffs. Add `_safe_diff()` calls in `bout.py` for:
    - `diff_career_ko_rate` (from `ko_tko_win_rate`)
    - `diff_career_sub_rate` (from `sub_win_rate`)
    - `diff_career_decision_rate` (from `dec_win_rate`)
    - `diff_career_sig_strikes_absorbed_pm` (from `career_sig_strikes_absorbed_per_min`)
    - `diff_career_sig_strike_defense` (from `career_sig_strike_defense`)
    - `diff_career_takedown_defense` (from `career_takedown_defense`)
    - `diff_title_fight_count` (from `title_fights`)
  - **Genuinely new features** — require new computation or new snapshot fields:
    - **Trend features** (highest expected value): for each of the last-5 fights, compute
      a per-fight feature vector (sig_strikes_landed_pm, takedown_accuracy, control_rate,
      win). Then compute `slope_sig_strikes_last5`, `slope_td_accuracy_last5`,
      `slope_control_rate_last5` (OLS slope over fight index), and `std_sig_strikes_last5`,
      `std_td_accuracy_last5` (per-fight volatility). Expose as diffs in bout.py.
      These capture momentum — is the fighter improving or declining?
    - **Reach-to-height ratio:** `diff_reach_height_ratio` (reach_cm / height_cm, difference
      between fighters). New computation in `physical.py`.
    - **Five-round experience:** `diff_five_round_fights` (count of scheduled 5-round bouts
      before this fight). New counter in `career.py`.
    - **Recent activity rate:** `diff_fights_per_year_last3` (bouts per calendar year over
      the last 3 fights window). New computation in `rolling.py`.
    - **Stance detail:** add `f1_is_southpaw`, `f2_is_southpaw` individual flags alongside
      the existing `is_orthodox_vs_southpaw` interaction flag (richer signal for tree models).
    - **Weight class encoding:** ordinal `weight_class_rank` (flyweight=1 ... heavyweight=9,
      women's divisions mapped separately) for models that cannot use categorical columns.
  - `features/snapshot.py` FEATURE_VERSION bumped to 2 for new rows.
  - `modeling/data.py` gains `FEATURE_COLS_V2: list[str]` — the canonical v2 column list.
  - Leakage tests extended to cover all new columns.
  - Data completeness table updated in `docs/feature-catalog.md`.
- **Dependencies:** Phase 3 complete, T4.1.1
- **Complexity:** S-M (wiring is S, trend features are M)
- **Risk:** Low for wiring. Medium for trend features — fighters with < 5 fights will have
  NULL slopes, which is fine (LightGBM handles NaN natively).
- **Notes:** The wiring subtask should take under an hour — do not overcomplicate it. Trend
  features are the highest-value new addition and should get the most attention.

#### T5.1.2 Debut fighter prior module
- **Description:** Both-debuting bouts (n=25 in test, ~6% of all fights) produce near-random
  predictions because all diff features are NULL. Build an informative prior for debut
  fighters drawn from the training distribution.
- **Status:** DONE
- **Acceptance Criteria:**
  - `features/debut_prior.py` provides:
    - `compute_debut_priors(train_df) -> dict` — computes, from training fights involving
      at least one debuting fighter, the empirical win rate by:
      - Weight class.
      - Stance (orthodox vs southpaw debutant).
      - Physical advantage: reach and height above/below weight-class median.
    - `apply_debut_features(bout_df, priors) -> DataFrame` — for rows where
      `both_debuting=True` or one fighter is debuting, fills the following columns:
      - `debut_prior_win_prob_f1`: prior probability fighter_1 wins based on physical
        profile and weight class base rates from training data.
      - `debut_reach_adv`: reach_cm difference standardized by weight-class std.
      - `debut_height_adv`: height_cm difference standardized by weight-class std.
  - The prior is fit on training data only — never on validation or test. Priors are
    serialized alongside model artifacts so inference can apply the same prior.
  - Both-debuting bouts in the test set are re-evaluated with the prior features included.
    Report shows log loss before and after the prior on this subgroup (target: improve
    from 0.871 toward the overall 0.652 baseline).
  - Unit tests verify: prior columns are non-null for debut bouts, prior fit only uses
    training rows, prior probabilities are within [0.3, 0.7].
- **Dependencies:** T5.1.1, T4.1.1
- **Complexity:** M
- **Risk:** Low — the prior is simple; the risk is data sparsity for some weight classes.
  Fall back to a global prior if a weight-class bucket has fewer than 20 training examples.
- **Notes:** Expected overall test set impact is small (~0.003 log loss from 25 bouts), but
  this is the right thing to do for production correctness. Every scored fight should have
  non-degenerate predictions.

---

## T5.2 — Advanced LightGBM

#### T5.2.1 LightGBM v2 — expanded features and focused tuning
- **Description:** Retrain LightGBM on feature_version=2 with debut priors applied.
  Phase 4's best hyperparameters are the starting point — only run a focused search if
  new features change the optimal configuration.
- **Status:** DONE
- **Acceptance Criteria:**
  - `modeling/train_lgbm_v2.py` follows the same structure as `modeling/train_lgbm.py` but:
    - Loads `FEATURE_COLS_V2` from `modeling/data.py`.
    - Applies debut priors from `features/debut_prior.py` before training.
    - **Step 1 — baseline check:** retrain with Phase 4's best config
      (`num_leaves=15, lr=0.02, min_child_samples=50`) on v2 features. Compare test log
      loss to Phase 4 LightGBM (0.6523). If v2 features improve log loss by ≥ 0.005,
      proceed to focused tuning. If not, investigate which new features have zero gain and
      document findings.
    - **Step 2 — focused tuning** (only if step 1 shows improvement):
      - `num_leaves`: [10, 15, 20, 31]
      - `learning_rate`: [0.01, 0.02, 0.05]
      - `min_child_samples`: [30, 50, 80]
      - `reg_lambda`: [0.0, 1.0, 5.0]
      Total combos: 4 × 3 × 3 × 3 = 108 via rolling CV, select by mean log loss.
      Report fold-level variance alongside mean — reject configs where fold std > 0.02.
    - After best config selected, refit on train+validation with early stopping on a
      held-out slice of the validation set.
  - **Feature selection pass:** after training, identify features with < 1% of total gain.
    Retrain once without them and compare. If log loss is unchanged or better, persist the
    trimmed model. Document which features were dropped and why.
  - Saves artifact to `models/lgbm_v2/<timestamp>/`.
  - Console output: best config, CV log loss, feature importances (top 20 by gain),
    dropped features (if any), comparison row.
  - `make train_lgbm_v2` Makefile target.
- **Dependencies:** T5.1.1, T5.1.2, T4.1.1, T4.1.2, T4.1.3
- **Complexity:** M
- **Risk:** Medium — more features on the same data can overfit. The feature selection pass
  mitigates this. Early stopping remains critical.

#### T5.2.2 Ensemble model — blend LightGBM v2, Logistic Regression, XGBoost, and Elo
- **Description:** Build a weighted blend of the best models. Ensembles improve calibration
  and reduce variance without adding features. This is the most likely source of lift.
- **Status:** DONE (no meaningful lift — documented)
- **Acceptance Criteria:**
  - `modeling/train_ensemble.py` provides:
    - **Simple weighted blend:** grid-search weights over (lgbm_v2_weight, logreg_weight,
      xgb_weight, elo_weight) summing to 1.0 in 0.1 increments. Select weights by log loss
      on the validation set. Use itertools to generate valid weight tuples.
    - Evaluates final ensemble on the test set and appends to the comparison report.
    - Saves blend weights as a JSON artifact in `models/ensemble/<timestamp>/`.
    - If blend does not improve over the best single model, try a **stacking variant:**
      logistic regression meta-learner on out-of-fold predictions from LightGBM v2 +
      Logistic Regression + XGBoost using the same temporal CV folds from T4.1.1.
  - Console output: best weights, validation log loss per top-10 weight configuration, and
    final test metrics.
  - `make train_ensemble` Makefile target.
  - **Decision rule:** if the ensemble does not achieve log loss < best_single − 0.003,
    document as "no meaningful lift" and do not promote as production candidate.
- **Dependencies:** T5.2.1, T5.3.1, T4.3.1
- **Complexity:** M
- **Risk:** Low for weighted blend. Stacking adds leakage risk if CV folds are not strictly
  temporal — assert this explicitly in tests.
- **Notes:** The Elo component uses `diff_elo` via the standard logistic formula — no
  separate model artifact needed. Just compute `1 / (1 + 10^(-diff_elo / 400))` on the
  test feature matrix.

---

## T5.3 — XGBoost Benchmark

#### T5.3.1 XGBoost trainer and comparison
- **Description:** Train XGBoost as a second gradient boosting implementation. XGBoost and
  LightGBM make different split decisions and regularize differently, making them better
  ensemble partners than LightGBM + Random Forest. XGBoost also handles NaN natively.
- **Status:** DONE
- **Acceptance Criteria:**
  - `modeling/train_xgb.py` provides:
    - Loads `FEATURE_COLS_V2`, applies debut priors. No imputation needed (XGBoost handles
      NaN natively).
    - Default hyperparameters: `objective='binary:logistic'`, `eval_metric='logloss'`,
      `max_depth=4`, `learning_rate=0.02`, `n_estimators=500`, `subsample=0.8`,
      `colsample_bytree=0.8`, `min_child_weight=50`, `reg_lambda=1.0`, `random_state=42`,
      `early_stopping_rounds=50`.
    - Tunes (`max_depth`, `min_child_weight`, `reg_lambda`) via rolling CV:
      - `max_depth`: [3, 4, 6]
      - `min_child_weight`: [20, 50, 100]
      - `reg_lambda`: [0.1, 1.0, 5.0]
      Total combos: 27. Select by mean log loss.
    - Applies Platt scaling calibration post-training (same process as T4.4.1).
    - Saves artifact to `models/xgb/<timestamp>/`.
    - Appends calibrated and uncalibrated rows to the comparison report.
  - Console output: best hyperparameters, CV log loss, test metrics, top-20 feature
    importances (gain).
  - `make train_xgb` Makefile target.
- **Dependencies:** T5.1.1, T5.1.2, T4.1.1, T4.1.2, T4.1.3
- **Complexity:** S
- **Risk:** Low — same workflow as LightGBM, different library. XGBoost is a mature,
  well-documented package.
- **Notes:** XGBoost's primary value is ensemble diversity. If it scores within 0.01 log
  loss of LightGBM v2, it is a good blend partner. If it significantly underperforms,
  drop it from the ensemble.

---

## T5.4 — Confidence Band Analysis

#### T5.4.1 Uncertainty flagging for the 0.55–0.65 band
- **Description:** 42% of test set fights fall in the 0.40–0.65 predicted probability range
  with 47–58% accuracy. The model is overconfident here. Instead of trying to force better
  predictions in a region where fights are genuinely unpredictable, add an uncertainty flag
  and SHAP analysis to understand why.
- **Status:** DONE
- **Acceptance Criteria:**
  - `modeling/uncertainty.py` provides:
    - `flag_uncertain(y_prob, low=0.40, high=0.60) -> array[bool]` — returns True for
      predictions in the uncertain band.
    - `uncertainty_report(y_true, y_prob, model, X, feature_names) -> dict` — for fights
      in the uncertain band:
      - Compute accuracy, log loss, and calibration.
      - Run SHAP on just this subgroup. Report top-5 features driving uncertain predictions.
      - Compare feature distributions (uncertain vs confident fights) to identify what's
        structurally different.
    - Report saved to `models/uncertainty_report.txt`.
  - Predictions from the scoring pipeline (Phase 7) should include an `is_uncertain: bool`
    flag and a `confidence_tier` label ("high" / "medium" / "toss-up") based on the
    predicted probability band. This metadata helps analysts interpret outputs honestly.
  - `make uncertainty_analysis` Makefile target.
- **Dependencies:** T5.2.1
- **Complexity:** S
- **Risk:** Low
- **Notes:** This is not a modeling fix — it is an honest acknowledgement that some fights
  are genuinely unpredictable with available data. Flagging toss-ups is more useful than
  emitting a false-precision 0.53 probability. This artifact directly feeds Phase 7's
  prediction output format.

---

## T5.5 — Closeout

#### T5.5.1 Phase 5 full comparison report and model selection
- **Description:** Update the comparison report and calibration plots to include all Phase 5
  models, and make an explicit production model selection decision.
- **Status:** DONE
- **Acceptance Criteria:**
  - `modeling/compare.py` updated to load and evaluate all model artifacts across Phase 4
    and Phase 5 in a single unified table.
  - Comparison report at `models/comparison_report.txt` updated with rows for:
    `LightGBM v2`, `LightGBM v2 (calibrated)`, `XGBoost`, `XGBoost (calibrated)`,
    `Ensemble`, alongside all Phase 4 rows.
  - Overlay calibration plot regenerated at `models/calibration_comparison.png` to include
    all models.
  - A **model selection record** is written to `models/production_model.json` containing:
    ```json
    {
      "selected_model": "<model_name>",
      "artifact_path": "models/<name>/<timestamp>/",
      "test_log_loss": 0.XXXX,
      "test_brier": 0.XXXX,
      "test_auc": 0.XXXX,
      "test_ece": 0.XXXX,
      "rationale": "<1-2 sentences>",
      "runner_up": "<model_name>",
      "selected_at": "YYYYMMDDTHHMMSSZ"
    }
    ```
  - `make compare_models` target regenerates this report end-to-end.
  - `docs/runbook.md` gains a **Phase 5 Advanced Modeling** section covering:
    - How to retrain LightGBM v2 and XGBoost
    - How to regenerate the ensemble
    - The production model selection rationale
    - Phase 7 prerequisites (what is now available for scoring and evaluation)
- **Dependencies:** T5.2.1, T5.2.2, T5.3.1, T5.4.1
- **Complexity:** S
- **Risk:** Low

#### T5.5.2 Makefile targets for Phase 5
- **Description:** Add make targets for all Phase 5 scripts.
- **Status:** DONE
- **Acceptance Criteria:**
  - `Makefile` gains targets:
    - `train_lgbm_v2` — trains LightGBM v2 on feature set v2
    - `train_xgb` — trains XGBoost
    - `train_ensemble` — trains ensemble model
    - `uncertainty_analysis` — runs uncertainty band analysis
    - `train_all_v2` — full sequence: `train_lgbm_v2` -> `train_xgb` -> `train_ensemble` ->
      `compare_models`
  - Each target prints a one-line description when run with `make help`.
- **Dependencies:** T5.2.1, T5.3.1, T5.2.2
- **Complexity:** S
- **Risk:** Low

---

## Dependency Graph

```
Phase 4 (complete)
    |
    +-- T5.1.1 (feature set v2: wire existing + trends + physical) ------+
    |       |                                                            |
    |   T5.1.2 (debut prior) ---+                                       |
    |                           |                                       |
    +-- T5.2.1 (LightGBM v2) <-- T5.1.1 + T5.1.2                       |
    |       |                                                           |
    |       +-- T5.4.1 (uncertainty analysis) <-- T5.2.1                |
    |       |                                                           |
    |   T5.2.2 (ensemble) <-- T5.2.1 + T5.3.1 + T4.3.1                 |
    |                                                                   |
    +-- T5.3.1 (XGBoost) <-- T5.1.1 + T5.1.2 --------------------------+
    |
    +-- T5.5.1 (comparison + model selection) <-- T5.2.1 + T5.2.2 + T5.3.1 + T5.4.1
            |
        T5.5.2 (Makefile)
```

**Critical path:** T5.1.1 -> T5.1.2 -> T5.2.1 -> T5.2.2 -> T5.5.1

---

## Suggested Execution Order

| Day | Tickets |
|---|---|
| 1 | T5.1.1 (wire existing features + add trends + physical ratios) |
| 2 | T5.1.2 (debut prior), then retrain: T5.2.1 (LightGBM v2 baseline check) |
| 3 | T5.2.1 (focused tuning + feature selection), T5.3.1 (XGBoost) -- parallel |
| 4 | T5.2.2 (ensemble), T5.4.1 (uncertainty analysis) -- parallel |
| 5 | T5.5.1 (comparison + model selection), T5.5.2 (Makefile) |

---

## Success Criteria for Phase 5

Phase 5 is successful if at least one of the following is true:

1. **Meaningful lift:** the best Phase 5 model achieves test log loss <= **0.635** (>= 0.005
   improvement over calibrated LogReg at 0.6396). Primary criterion.
2. **Debut improvement:** both-debuting bout log loss drops from 0.871 to <= 0.750 with the
   debut prior (T5.1.2).
3. **Production readiness:** `models/production_model.json` exists with a clear selection
   rationale, the uncertainty flag is implemented, and Phase 7 inference work is unblocked.

Phase 5 is **not** considered a failure if log loss does not improve, as long as:
- The feature expansion and selection process is documented.
- The uncertainty analysis provides actionable insight for the prediction output format.
- The production model is formally selected and the decision is recorded.

---

## Deferred to Phase 6

The following items from the original Phase 5 plan are deferred. They are research spikes
with high risk, high complexity, and uncertain returns. They should only be pursued after
the inference pipeline (Phase 7) is operational and there is appetite for further model
improvement.

| Item | Rationale for deferral |
|---|---|
| **Bayesian hierarchical model** (was T5.4.1) | The debut prior (T5.1.2) addresses the sparse-fighter problem more cheaply. A hierarchical model won't conjure signal from missing data. Pursue if T5.1.2 is insufficient and analyst demand justifies the complexity. |
| **Sequence model (LSTM/GRU)** (was T5.5.1 Option B) | 8,400 rows is too small for a sequence model to generalise. The trend/slope features (T5.1.1) capture the momentum signal more robustly. Revisit only if the dataset grows significantly or a simpler variant shows > 0.005 log loss lift. |
| **Random Forest benchmark** (was T5.3.1) | RF requires imputation, needs calibration, and is unlikely to beat GBDT on this dataset. XGBoost provides more ensemble diversity with less overhead. |

---

## Risk Register

| Risk | Severity | Mitigation |
|---|---|---|
| v2 wired features add columns with high missingness for pre-2008 fights | Low | LightGBM handles NaN natively; audit completeness before training |
| Trend features (slope) are NULL for fighters with < 5 fights | Low | Expected; LightGBM handles NaN; these features only fire for experienced fighters |
| Feature selection removes useful features | Low | Compare log loss with and without dropped features; keep both artifacts |
| Focused tuning still overfits on small rolling CV folds | Medium | Report fold-level variance; reject configs with fold std > 0.02 |
| Debut prior leaks validation distribution | Low | Assert prior is fit only on training rows; serialize alongside model artifact |
| Ensemble provides marginal lift with added complexity | Low | Set explicit threshold (0.003 improvement); otherwise do not promote |
| XGBoost not installed in environment | Low | Add to requirements; `pip install xgboost` |
