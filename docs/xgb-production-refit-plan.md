# Pre-April 2026 XGBoost refit specification

This is a preparation specification, not authorization to fit or deploy. Phase
3B must explicitly accept `retrospective_git_pre_cutoff` or supply stronger
versioned sources and rerun preflight. Strict historical replay is unavailable.

## Fixed inputs and source interpretation

Use `configs/xgb_refit_pre_april_2026.toml`, the isolated snapshot at
`data/experiments/phase3a_pre_april_2026_git_1f477d3_metadata_safe/`, and its manifest hash
`42e8e1661409b2ca758a4e18775f9816865439448fbce0d88af68dda22325671`.
Knowledge cutoff is `2026-03-31T12:09:04.077607+00:00`; eligible labels require
`event_date < "2026-03-31"`. The source is Git commit
`1f477d3ddc87b123d0099025b669e728e7881a34`, recorded March 18 at 12:43:50 UTC.
It provides 8,400 eligible wins through March 7. It does not include every
currently known pre-cutoff bout. The exact archival coverage is the experiment's
eligible population; do not augment it from today's warehouse without review.

This source gives repository evidence that its exact CSV values existed before
the experiment cutoff. It does not prove historical availability before each
1994–2026 bout. Profiles, corrected results/statistics and matchup metadata
retain this limitation. Git timestamps are not independent trusted timestamps.
The candidate is a chronological retrospective experiment with a source pinned
before the global knowledge cutoff, not a certified historical deployment replay.

The snapshot stores the production's ordered 50 inputs (47 raw v2 features plus
three deferred debut columns), fighter orientation, event identity/date, label
and version. Deferred debut columns are NaN until preprocessing for a specific
training partition. Do not pass these placeholder columns directly to fitting.
Legitimate remaining NaNs go to XGBoost without row removal or global imputation.
Differences are fighter 1 minus fighter 2; label 1 means fighter 1 wins.
Algorithm version is `v2_date_frozen_elo_schedule_unknown_v1`, distinct from the production's
old within-date Elo behavior despite using the same feature names/version 2.

The archived `num_rounds` column is the post-event finish round: its parser uses
the same `Round:` selector as `finish_round`. It is never accepted as scheduled
rounds. All 8,400 target schedules remain NaN, and five-round-history experience
is unknown when a prior schedule is unknown (7,870 missing differences). Actual
elapsed time is recorded separately and consumed only in strictly earlier
histories for rate denominators. No scheduled-round inference from target
outcomes or title status is introduced. The earlier preliminary snapshot without
the `_metadata_safe` suffix is marked REJECTED and fails the current input guard.

Before any learned fit, call `validate_refit_config`, load the exact snapshot
through `load_prepared_snapshot` with the pinned manifest SHA-256 and explicit
source mode, compare configuration/code/package hashes, and call
`assert_no_holdout_fights` on that operation's labels. The guard excludes all
166 original/alternate/precautionary IDs from fitting, tuning, early stopping
and calibration. Never load frozen outcomes or joined comparison evidence into
the training/preprocessing path. Whole dates and events stay in a partition.

## A. Select settings using older development data

All development labels precede March 31, 2025. Use these expanding windows;
`folds.json` records every fight ID in each partition and checksums the lists.

| Fold | Train event dates | Development validation dates, inclusive start/exclusive end | Train / validation rows |
|---|---|---|---:|
| dev_2022 | before 2022-03-31 | [2022-03-31, 2023-03-31) | 6,414 / 502 |
| dev_2023 | before 2023-03-31 | [2023-03-31, 2024-03-31) | 6,916 / 519 |
| dev_2024 | before 2024-03-31 | [2024-03-31, 2025-03-31) | 7,435 / 506 |

Use `prepare_fold_inputs` separately for each partition. It computes debut
height/reach normalization from that fold's training rows and applies those
same priors to train and validation. Any later-added preprocessing must follow
the same rule. Save each fold's priors with training-ID hash. The legacy trainer
computes priors before rolling CV; do not use that entrypoint for this experiment.

Evaluate the 27 combinations of depth [3,4,6], minimum child weight [20,50,100]
and lambda [0.1,1,5]. Fixed settings: logistic objective, log-loss evaluation,
learning rate 0.02, subsample/column subsample 0.8, seed 42, one thread and
`tree_method="hist"`. Each development fit permits at most 500 rounds and
early stopping patience 50 on its own development validation partition.

Select the configuration by row-weighted development log loss; resolve exact
ties lexicographically by (depth, minimum child weight, lambda). For that winning
configuration, select rounds as
`floor(median(best_iteration + 1 across its three development folds) + 0.5)`.
Save settings, all development losses, selected round count, fold provenance
and prior hashes in `selection.json`. These are selection diagnostics, not
unbiased performance estimates. Do not borrow the old artifact's best iteration.

## B. Generate final-12-month expanding-window OOF probabilities

Freeze A's settings and round count before OOF generation. Do not use an OOF
prediction window for hyperparameter selection or early stopping, including
selection through aggregate OOF performance.

| Fold | Train event dates | OOF prediction dates, inclusive start/exclusive end | Train / OOF rows |
|---|---|---|---:|
| oof_1 | before 2025-03-31 | [2025-03-31, 2025-06-30) | 7,941 / 129 |
| oof_2 | before 2025-06-30 | [2025-06-30, 2025-09-30) | 8,070 / 126 |
| oof_3 | before 2025-09-30 | [2025-09-30, 2025-12-31) | 8,196 / 128 |
| oof_4 | before 2025-12-31 | [2025-12-31, 2026-03-31) | 8,324 / 76 |

Each base fit uses all earlier eligible rows and that fold's independently fit
debut priors. Fit exactly the selected number of rounds, with no early-stopping
evaluation set. Within a prediction window, a later row's histories may use
earlier resolved fights, while the window's learner remains fixed; feature
histories and fitting memberships are different inputs. No target/same-date
results enter histories or Elo. The pinned source's observation-time limitation
still applies to retrospective chronology.

Produce exactly 459 OOF rows, one per eligible bout in
[2025-03-31,2026-03-31). March 14/21/28 labels absent from the source are outside
this eligible population, not fictitious predictions. Save `oof.csv` with fight
ID, event ID/date, original orientation, label, raw probability, fold, selected
settings/round hash, training membership hash and prior hash. Save
`oof_manifest.json`, fold priors and checksums. `validate_oof_inputs` rejects
missing/duplicate membership, wrong folds, changed orientation/labels, invalid
probabilities and single-class calibration inputs before any calibrator fit.

## C. Fit and freeze Platt calibration

After B's membership/provenance passes preflight, fit one logistic regression
on OOF log odds, clipping raw probabilities to [1e-8,1−1e-8], with C=1e10,
lbfgs and maximum 1,000 iterations. Save the fitted estimator and clipping
contract as `calibrator.joblib` plus metadata. Require both label classes.
The old `calibrate_platt` convenience function returns transformed values and
discards its estimator; implement estimator persistence in Phase 3B.

OOF base metrics measure fixed-development-selected retrospective OOF behavior.
Metrics after fitting Platt on these same rows are **calibration-fit diagnostics**.
There is no separate independent calibrated validation set in this design.
Neither development diagnostics nor calibrated OOF fit diagnostics justify a
claim of unbiased final-model validation or automatic promotion.

## D. Refit the final base learner on every eligible row

Fit final debut priors on all 8,400 eligible rows, guarded against holdouts,
and save `final_debut_priors.json` independently from every fold prior. Apply
those priors to the final training frame. Fit XGBoost using A's selected
settings and round count, with no early stopping and no retained two-year
validation/test reservation. The final learner includes every eligible row
through March 7, 2026, rather than stopping in 2022 as the current artifact does.

Use a new isolated candidate root
`data/experiments/phase3b_xgb_pre_april_2026/`; refuse overwrite. Save base
learner, fitted calibrator, final priors, ordered features/versions, source and
training manifests/hashes, knowledge/event cutoffs, selection settings/rounds,
fold/OOF lineage, package versions and `probability_band_v1`. Required filenames
are enumerated in the TOML. Include all component hashes in `checksums.json`.
Do not write a production pointer or promote automatically.

Future inference must load the frozen fitted calibrator and final saved priors.
It must not recompute either from the live warehouse. Validate versions and
feature order before scoring; missing components must fail clearly.

## Later comparison at original scoring instants

The 108 original pre-event forecasts remain the fixed probability baseline;
their outcomes have been inspected and they are not an unseen test set. Do not
recompute old probabilities using the current warehouse or a new old-model
calibrator. The historical 146 remain qualified mixed evidence, unchanged.

For each later candidate comparison row, use the original prediction's
`scored_at`, event/fighter identities and orientation. Restrict result/stat
inputs to versions evidenced available at or before that instant. A fight
weeks after scoring must not admit intervening results. With date-only result
evidence, exclude the UTC scoring day as well as the target day;
`reconstruct_at_scored_at` caps both histories and Elo at
`min(event_date, UTC(scored_at).date())` and uses a label-free Elo probe.
It deliberately labels availability `unverified_at_scored_at`; it is not a
strict source-version selector.

Earlier resolved fights, including excluded fitting identities, may update a
later forecast's histories when independent source evidence establishes their
information was available by that later scoring instant. This does not put
their labels into base fitting, tuning, early stopping or calibration. Do not
use frozen outcome files to develop features. Source-version availability and
mutable matchup/profile fields for these forecasts still need review before
comparison; the March archive alone cannot supply later resolved histories.
