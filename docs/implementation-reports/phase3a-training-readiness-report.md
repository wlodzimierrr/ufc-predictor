# Phase 3A: historical lineage and training readiness

Repository: `/home/wlodzimierrr/ufc-data`. Date: 2026-10-01. All timestamps below are UTC.

## 1. Preparation status

**Preparation completed; no model fitting authorized or performed.** Implemented archive-based training preparation, source/warehouse audit tooling, temporal fixes, snapshot/fold/calibration-input guards, a draft TOML configuration and the production-refit specification. Existing user changes were preserved. No applicable `AGENTS.md` was found in the repository or ancestor directories.

Read the Phase 1b/2 reports, accepted manifests, holdout guard, NO PICK contract, XGBoost metadata/model card, and relevant source, feature, training and calibration code/tests. The completed identity recovery and eight catch-up mappings were not reinvestigated.

## 2. Separate readiness findings

**Strict historical replay: NOT READY.** Exact versions available before every historical bout, and at every original forecast scoring instant, cannot be established. Current scrape dates and late feature computation dates do not independently demonstrate leakage. Old event dates do not establish historical availability either.

**Chronological retrospective reconstruction: READY FOR REVIEW in a specific archive mode.** Git commit `1f477d3ddc87b123d0099025b669e728e7881a34`, recorded at `2026-03-18T12:43:50+00:00`, preserves exact CSV inputs before the fixed global knowledge cutoff. The validated snapshot uses these versions, excludes target/future/same-date histories and Elo, and removes a demonstrated finish-round-as-scheduled-round defect. It has 8,400 eligible rows through March 7, 2026.

Recommend `retrospective_git_pre_cutoff`, with unknown scheduled rounds and explicit coverage limitations. This is stronger than using today's mutable warehouse values, but cannot support claims of certified pre-bout availability, strict replay, complete current pre-cutoff coverage, or unbiased performance on the already-inspected 108 forecasts. Git dates are repository evidence, not independent trusted timestamps. **This report does not authorize that mode for Phase 3B; the planning decision must explicitly accept it or supply stronger sources.**

## 3. Source and knowledge-lineage evidence

Event date means when the bout happened. Observation time means evidence that a particular version existed. Scrape time means when a source was fetched. Computation time means when features were regenerated. These are separate facts.

The warehouse audit used a read-only repeatable-read transaction at `2026-10-01T16:09:42.695816+00:00`, PostgreSQL 17.9, database timezone UTC. Actual schemas expose `scraped_at` on source tables and `computed_at` on feature tables; they provide no versioned observation/validity history. Upserts replace values and retain the greatest scrape timestamp.

| Evidence/input | Available timestamps and recovered versions | Coverage and limitations | Refit consumption |
|---|---|---|---|
| Results and labels | Event date; source `scraped_at`; March 12 and March 18 Git versions. March 18 fight scrapes span Feb 19–Mar 13. | Archive: 8,550 bouts, 8,400 wins, ending Mar 7. Current pre-cutoff warehouse: 8,613 bouts, 8,459 wins. Exact archive version predates global cutoff; per-bout availability remains unverified. One shared historical result differs today. | Derive labels from archived winner ID in archived fighter orientation; never substitute current outcomes. |
| Aggregate and round statistics | Source scrape time; Git blobs; fetch-manifest times/content hashes. No warehouse version history. | March archive: 17,102 aggregate rows, including 40 unjoinable rows for 20 absent bouts; 19 archived bouts lack aggregates. Round archive: 40,320 rows. Current warehouse: 17,386 aggregate and 41,514 round rows, scraped in August. | Join archived aggregates only to archived bouts/participants. Round statistics are audited but unused by current features. Preserve sparse bouts. |
| Fighter profiles: DOB, height, reach, stance | Source scrape time; exact archived CSV; no effective-date history. Archived profiles scraped Feb 19. | 4,452 archived profiles; 4,522 current warehouse profiles. Comparing common IDs at warehouse numeric precision finds 8 DOB, 20 height, 17 reach and 12 stance changes. These demonstrate mutability, not when each change became known. | Use archived profiles; age is computed at target date. Historical pre-bout correctness/availability remains an assumption. |
| Matchup metadata, titles and scheduled rounds | Event date and fight scrape time; archived bout type/title text. | All 8,550 archived `num_rounds` values equal `finish_round`. Both parser selectors read post-event `Round:`. Thus `num_rounds` is not evidence of scheduled rounds. Title/weight metadata has no pre-bout version history. | Retain archived title/weight metadata with availability qualification. Set scheduled rounds unknown; never infer them from target outcomes. |
| Elo and opponent strength | Derived from dated resolved bouts; no independent historical observation time. | History filtering alone did not protect Elo: persisted snapshots have 65 fighter/date groups with differing same-date ratings. Opponent histories are separately filtered before each prior bout. | Freeze Elo for each entire date, then apply that date's updates. Use only earlier opponent histories and pre-bout Elo. |
| Career, rolling, decay and trend inputs | Prior-bout dates plus result/statistic source versions; computation time is separate. | Existing `get_history` uses strict `<`; same-date target bouts are excluded. Missing statistics and inherited rate formulas remain measurement limitations. | Reconstruct from archived prior bouts. Reconstructed elapsed duration is separate from unknown schedules and used only for prior-bout rates. |
| Debut-prior normalization | Training membership and computation time; no persistent production prior bundle. | Old training computes priors before rolling CV; live inference recomputes priors. Neutral base prior is 0.5; height/reach normalization depends on training rows. | Defer all three prior columns in the snapshot. Compute normalization separately for each fold and again for final refit; persist final priors separately. |
| `fighter_snapshots` | `as_of_date`, feature version, `computed_at`; one row per fighter/bout, overwritten on refresh. | 17,888 current rows, computed Aug 16; no historical versions. Late computation alone is not leakage, but inputs and same-day Elo are uncertified. | Do not consume stored snapshots. Rebuild isolated rows. |
| `bout_features` | Event date, feature version, `computed_at`; one overwritten row per bout. | 8,944 current rows; 8,613 pre-cutoff, version 2, with 8,459 labels. All computed Aug 16. Full-table legacy label check finds 34 discrepancies; pre-cutoff check finds zero. Stored schedules inherit the concrete parser defect. | Do not use current stored features for this candidate. Optional loader cutoffs/exclusions execute in SQL before labels are fetched. |

Bounded searches inspected CSV Git history and only raw paths named by successful pre-cutoff fetch records. Of those paths, current content still matches a pre-cutoff hash for 3 event listings, 0 event pages, 0 fighter pages and 2 fight pages. Raw storage is overwritten by identity; its manifest alone cannot restore missing bodies. This is partial evidence, not a complete replay archive. No dependency-directory or broad forensic inventory was performed.

## 4. Temporal defects and fixes

1. **Concrete target leakage:** `warehouse.transform` maps post-event `num_rounds` into `scheduled_rounds`. New archive preparation overrides that mapping with unknown schedules. All 8,400 target schedules remain NaN; five-round-experience differences are unknown for 7,870 rows, rather than falsely zero. An optional history duration field retains observed finish-round/clock information for strictly earlier bouts. Rate modules consume it without exposing target duration. Existing 300-second round arithmetic remains a measurement assumption for nonstandard historical formats.
2. **Same-date Elo:** ratings are now frozen before all bouts on a date; date updates are applied afterward. A repeated tournament fighter cannot acquire another same-day bout's result in its target features. Ordering is deterministic by date/fight ID.
3. **Forecast-time Elo:** the legacy upcoming builder caps histories at today's/event date but obtains Elo at a future bout's position in the full warehouse. New pure `reconstruct_at_scored_at` caps both histories and Elo at the earlier of event date and original UTC scoring date, excludes that entire day, and uses a label-free Elo probe. It still explicitly reports source availability as unverified.
4. **Preprocessing/calibration lifecycle:** fold-specific normalization helpers and the specification replace global pre-CV normalization for this experiment. Future inference must load saved final priors and a frozen calibrator. Existing production scorer behavior was not changed or run.

The initial preparation snapshot, created before the schedule defect was discovered, is explicitly marked rejected at `data/experiments/phase3a_pre_april_2026_git_1f477d3/REJECTED.json`. The current loader refuses it before fitting. It was never fit or scored. Only the replacement `_metadata_safe` snapshot is a candidate input.

## 5. Eligible rows and exclusions

Fixed knowledge cutoff: `2026-03-31T12:09:04.077607+00:00`. Exclusive event cutoff: `event_date < "2026-03-31"`; March 31 is excluded.

| Archive selection | Rows |
|---|---:|
| Source bouts | 8,550 |
| Eligible binary wins | 8,400 |
| Draws excluded from fitting | 62 |
| No-contests excluded from fitting | 88 |
| On/after-cutoff rows in this archive | 0 |
| Held-out IDs encountered in this archive | 0 |
| Upcoming rows in this archive | 0 |

All 166 original, verified alternate and precautionary IDs are nevertheless loaded, excluded and checked by `assert_no_holdout_fights` before a training frame is returned and in fold/calibration preflight. Draw/NC bouts may supply legitimate earlier history but never fitting labels.

Eligible rows span **1994-03-11–2026-03-07**, across **764 events / 759 dates**; labels are **5,401 fighter-1 wins / 2,999 fighter-2 wins**. Archived orientation is preserved without reordering or winner-based relabeling. All **530 both-debuting rows** remain. Duplicate fight IDs and duplicate event/unordered-fighter identities, invalid labels, missing identifiers, infinite/non-numeric values and unexpected schemas/versions fail preflight.

Coverage is explicitly narrower than today's warehouse: 63 warehouse bouts are absent from the archive—40 resolved bouts on March 14/21/28 (39 wins, one draw), 22 other historical resolved bouts, and one upcoming integration-test record. One shared archived win is now a different result; it was not silently replaced. These differences explain the net 59-win count gap. The archive retains 99.3% of the current win count; no large subset was discarded to imply strict certification.

Missingness is recorded per feature. Examples: 121 missing age differences, 24 height, 1,034 reach, 2,142 career-win-rate and 5,311 takedown-trend differences. Scheduled rounds have 8,400 missing values; all three debut columns are deliberately deferred on all rows. Missing statistics do not cause bout deletion. Existing formulas' treatment of absent statistics is documented as a measurement limitation rather than redesigned here.

## 6. Snapshot, certification and hashes

Validated candidate directory: `data/experiments/phase3a_pre_april_2026_git_1f477d3_metadata_safe/`.

Certification: `chronological_retrospective_only`; `strict_replay_ready = false`; `phase3b_mode_approval_required = true`. Feature version is 2 with algorithm `v2_date_frozen_elo_schedule_unknown_v1`. The 50-column feature order exactly matches production metadata.

| Candidate file | SHA-256 |
|---|---|
| `manifest.json` | `42e8e1661409b2ca758a4e18775f9816865439448fbce0d88af68dda22325671` |
| `training.csv` | `e53cd2f253985290ca2e03185fd4b67ee67c209473f595d5fda3cecbb34e764c` |
| `folds.json` | `c425c98ff96f7e4dba4505e1215ace017587fef3d6a41f8996ad5ce726b615d1` |
| `sources/events.csv` | `df2d02bbadbe5b293bcb33c08f0ae4095b4ba4398cd3f8884032da61be5ad81f` |
| `sources/fighters.csv` | `ecce91a1aa2fb024d0f9c8eb489e9d9b5ddb0663f2f745ce089c8bad5461ce12` |
| `sources/fights.csv` | `3c89061dc315a640f29ddfd260b2b23afc1e48faed5106de0cbe461558e90ff1` |
| `sources/fight_stats.csv` | `b20c0e07aefff66f749476a3acd8abc57c0ef6cdc09d545c05fe7fe068d93324` |

The manifest records source Git blobs/hashes, cutoff, row/date/event counts, exclusion reasons, feature order, missingness, orientation/labels, package versions and code/configuration hashes. Publication refuses existing or protected destinations and makes generated files read-only. A complete independent rebuild produced byte-identical data, source, fold and snapshot manifests. A separate `build-receipt.json` records computation at `2026-10-01T16:20:35.353937+00:00`–`16:20:37.782047+00:00`; that time is not historical availability.

Audit directory: `data/audits/phase3a_pre_april_2026/`. It contains the source/SQL capture, supplemental parser/archival evidence, independent rebuild verification, preservation baseline/check and both holdout validation outputs. `lineage-evidence.json` SHA-256 is `1ca52a359ac5feb9d1fe95bdbd9c7874c8f217984311c369d6811a458f29f9ee`.

## 7. Exact refit procedure

Configuration: `configs/xgb_refit_pre_april_2026.toml`. Supporting specification: `docs/xgb-production-refit-plan.md`. Every fold's complete train/prediction fight-ID membership and membership hash is in `folds.json`. Train uses all eligible dates strictly before the prediction window start; intervals below include their start and exclude their end. Dates/events are never split.

| Stage/fold | Training dates before | Prediction/selection window | Train / window rows |
|---|---|---|---:|
| Development 2022 | 2022-03-31 | [2022-03-31, 2023-03-31) | 6,414 / 502 |
| Development 2023 | 2023-03-31 | [2023-03-31, 2024-03-31) | 6,916 / 519 |
| Development 2024 | 2024-03-31 | [2024-03-31, 2025-03-31) | 7,435 / 506 |
| OOF 1 | 2025-03-31 | [2025-03-31, 2025-06-30) | 7,941 / 129 |
| OOF 2 | 2025-06-30 | [2025-06-30, 2025-09-30) | 8,070 / 126 |
| OOF 3 | 2025-09-30 | [2025-09-30, 2025-12-31) | 8,196 / 128 |
| OOF 4 | 2025-12-31 | [2025-12-31, 2026-03-31) | 8,324 / 76 |

**A. Selection:** Evaluate the draft's 27 depth/child-weight/lambda configurations using only development folds. Fixed learning rate 0.02, subsample/column subsample 0.8, seed 42, one thread, histogram trees. Development fits may use 500 maximum rounds and patience 50. Choose row-weighted development log loss, with lexicographic parameter tie-break. Select the winning configuration's round count as `floor(median(best_iteration + 1 across development folds) + 0.5)`. Fit normalization separately inside each development training partition. Save selection results and prior/membership hashes. No selection has been performed in Phase 3A.

**B. OOF:** Freeze A's settings/round count. Fit one expanding learner per OOF fold with that fold's training-only normalization. No OOF-window early stopping or tuning. Generate exactly **459** eligible OOF rows over the final 12 pre-cutoff months. Validate exact coverage, folds, orientation, labels, probabilities and both calibration label classes before any calibrator fit. Save OOF probabilities and fold/prior lineage.

**C. Calibration:** Fit one Platt logistic regression on clipped OOF log odds, epsilon 1e-8, C=1e10, lbfgs, maximum 1,000 iterations. Save the fitted estimator and clipping contract. Metrics on those same calibrated rows are **calibration-fit diagnostics**, not independent validation. No independent calibrated validation population is reserved in this design.

**D. Final refit:** Fit and save final debut priors separately using all 8,400 eligible rows. Fit the final base learner on every eligible row with A's fixed settings and round count, without early stopping or the old two-year validation/test reservation. Its eligible data ends in 2026, rather than retaining the production learner's 2022 training endpoint.

The isolated future bundle must contain base learner, frozen fitted calibrator, final priors, feature order/version/algorithm, training/source manifests and hashes, knowledge/event cutoffs, selected settings/rounds, fold/OOF lineage, package versions, checksums and unchanged `probability_band_v1`. Inference must load these components and fail if missing; it must not refit them from the live warehouse. Candidate output root is `data/experiments/phase3b_xgb_pre_april_2026/`, with no automatic promotion.

## 8. Remaining assumptions and blockers

- Complete per-bout/per-scoring-instant source-version evidence is missing. Archived profile and corrected-statistic values are retrospective inputs, even though their exact archive predates the global cutoff.
- Scheduled rounds cannot be recovered from the archived CSV contract. The safe candidate keeps them unknown. Adding verified schedules or missing March/other bouts requires a reviewed source augmentation and a new snapshot/manifest.
- Historical rate arithmetic, missing-statistic handling and source fighter ordering retain measurement/representation limitations. No unrelated feature redesign was introduced.
- The existing stored feature tables and production inference's live calibration/prior rebuilding are unsuitable as this experiment's input/bundle lifecycle. They were not refreshed or repaired.
- For later comparison on the 108 forecasts, construct each feature vector at its original `scored_at`, with original identities/orientation and source versions available by that instant. A prediction weeks before its event must exclude intervening results. The new helper enforces date chronology but does not certify missing source availability.
- Earlier resolved fights may update later forecast histories when their information was available by that later scoring time, including identities excluded from fitting. Their labels remain excluded from learner selection, fitting, early stopping and calibration. Obtain history inputs independently; do not open frozen outcome files for feature development.

The 108 original forecasts remain the frozen baseline and are not an unseen test set. Do not recompute the old model's probabilities with today's warehouse/calibrator. The 146 historical records remain qualified mixed evidence; their catch-up identity questions were not reopened.

## 9. Files created and modified

Created `features/replay.py`, `modeling/refit_preflight.py`, `tools/prepare_xgb_refit_data.py`, `tools/audit_training_lineage.py`, `features/tests/test_replay.py`, `modeling/tests/test_refit_preflight.py`, `modeling/tests/test_refit_sql.py`, the TOML, supporting specification and this report; also the isolated candidate/audit artifacts and rejected-preliminary marker described above.

Modified `features/elo.py`, `features/history.py`, `features/career.py`, `features/rolling.py`, `features/decay.py`, `features/opponent.py`, `features/physical.py`, `modeling/data.py`, and the explanatory same-day comment in `features/tests/test_leakage.py`. Legacy research command defaults remain unchanged. Holdout guard, accepted artifacts, decision policy, dashboard and warehouse schema were not modified.

## 10. Tests and results

Final focused suite: **306 passed, 0 failed, 0 skipped**, including a disposable PostgreSQL SQL-filter test. It ran the new replay/preflight/SQL tests, existing history/Elo/opponent/career/rolling/decay/physical/debut tests, and existing modeling data/holdout/holdout-recovery tests.

Coverage includes exclusive March 31 filtering before SQL label fetch; original/alternate exclusions; no outcome-file access from preparation; target/future/same-day feature invariance; scoring-time Elo/history caps; mutable-profile limitations; rejected finish-round schedules; prior-only elapsed durations; temporal fold ordering, whole events/dates and complete coverage; training-only fold normalization; deterministic manifests/full rebuild; checksum tampering; missing/unverified availability classification; and strict-mode failure before source loading or fitting.

The broader legacy `test_snapshot_bout.py` yielded **15 passed / 19 failed** because it expects obsolete v1 feature names/version. The identical 19 failures were reproduced using the original HEAD Elo implementation; they are pre-existing and were not repaired through an unrelated feature rewrite.

The existing live leakage suite ran with `PGOPTIONS='-c default_transaction_read_only=on'`: **10 passed / 1 failed**, reporting 34 persisted label discrepancies across the full table. A separate read-only pre-cutoff query found **zero** label discrepancies. No warehouse repair followed. Its older checks do not certify scheduled-round semantics or same-date Elo; new adversarial tests cover those defects.

Calibration tests were inspected but learned-calibrator fitting tests were not executed. The existing live integration test that writes warehouse records and overwrites upcoming features was not run. Mutating SQL verification used a disposable local PostgreSQL cluster with a synthetic table, then stopped the server. No live migrations were applied. Scoped `git diff --check` passed.

## 11. Preservation checks

All **176 protected existing files** match their initial SHA-256 values, including existing models/predictions, holdouts, report/evidence files, migration files and user-modified inputs. No new files were created under `models/`. Both existing offline holdout validators returned **VALID**, with 166 exclusions each; their outputs are preserved in the new audit directory. No accepted artifact was regenerated.

Production pointer SHA-256 remains `ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa`. The preservation baseline and `verification.json` contain the full file list/check results.

## 12. Scope confirmation

No XGBoost, calibrator or other learned-model fitting; candidate scoring/performance evaluation; production refresh; live migration; warehouse result repair; artifact promotion; production-pointer/dashboard edit; commit; or push occurred. Synthetic normalization tests and synthetic probability-input validation exercised preflight only. Existing probabilities, outcomes, frozen populations and Phase 1/2 evidence remain unchanged.

## 13. Recommended Phase 3B scope and exact inputs

First return this report for an explicit planning decision on `retrospective_git_pre_cutoff`, its unknown schedules and incomplete archival coverage. Strict replay remains unavailable. If that mode is accepted, use:

1. This report and `docs/xgb-production-refit-plan.md`.
2. `configs/xgb_refit_pre_april_2026.toml` and its pinned source commit/cutoffs.
3. Only the `_metadata_safe` snapshot, manifest SHA-256 `42e8e1661409b2ca758a4e18775f9816865439448fbce0d88af68dda22325671`, its source hashes and exact fold memberships.
4. `modeling/refit_preflight.py` guards, the unchanged 166-ID holdout guard, both accepted manifests and `docs/no-pick-contract.md`.
5. The new audit evidence, missingness, package versions and code/configuration hashes.

Phase 3B should implement the isolated candidate fitting/bundle persistence entrypoint and then execute A–D only under the next session's authorization. Validate snapshot/configuration and operation-specific fitting memberships before every learned fit. Save development/fold/final priors separately and persist the fitted calibrator. Do not use the legacy trainer/scorer entrypoints, reject the preliminary snapshot, and do not promote automatically. Later scoring/comparison on the 108 original forecasts needs its own reviewed scoring-time source treatment; retain their original probabilities as baseline.
