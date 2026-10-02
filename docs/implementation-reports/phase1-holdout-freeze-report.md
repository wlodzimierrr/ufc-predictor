# Phase 1: holdout recovery, audit, and freeze

**Status: BLOCKED**

Audit date: 2026-10-01. Repository: `/home/wlodzimierrr/ufc-data`.

## 1. Executive summary

Recovered an existing historical report containing 146 unique resolved prediction records that reproduce **81 correct overall, 55 uncertain / 22 correct, and 91 actionable / 59 correct**. Every original prediction is independently corroborated by warehouse history, including its scoring timestamp, raw and calibrated probabilities, confidence tier, uncertainty flag, and model artifact.

However, this candidate has **44 high-confidence fights / 30 correct**, using the required inclusive rule `calibrated_prob_f1 <= 0.30 or >= 0.70`. It does not reproduce the required **57 / 39**. None of 26 evaluated source/selection combinations satisfies all invariants. Matching three groups does not establish that this was the exact analysis input.

The candidate also contains **38 catch-up predictions scored after event day**. It cannot be described as a wholly prospective cohort. The freeze is blocked pending the original analysis evidence or an explicit correction to the inconsistent invariant and a decision about these exceptions.

Only recovery evidence, read-only validation tooling, an outcome-isolated exclusion helper, tests, and documentation were added. No substitute holdout, accepted manifest, training run, or candidate scoring run was created.

## 2. Cohort definition and selection rule

No accepted cohort has been frozen.

The strongest recovered candidate comes from Git blob:

`e7a6e34bb35e015d91975f182572613ad4d5b48e`

An unreachable tree, `54783dabfba46427ace3bb64e74c2402b8fe4a7b`, identifies its filename as `pre_event_prediction_fights.csv`. The original 211-row report was preserved byte-for-byte under the evidence directory so Git garbage collection cannot destroy this evidence. There is no established source commit, report creation timestamp, or original analysis session for this blob.

Candidate selection is precisely:

1. Read this specific immutable source blob, not the regenerated current report.
2. Keep existing rows where `2026-04-01 <= event_date < 2026-09-01` and original `resolved == True`.
3. Preserve the original records; do not filter by correctness, confidence, or desired counts.

This yields 146 rows / 146 unique fight IDs across 16 events, dated **2026-04-04 through 2026-08-22**. The snapshot has 169 April–August rows; 23 were unresolved and excluded. There are no duplicate candidate fight IDs.

An independent warehouse reconciliation reproduces the same metrics: restore explicit `reviewed_prediction_fights` rows over the current view by `fight_id`, then restrict to April 1 through August 22 and resolved results. The full April–August reconciliation instead gives 154 / 86 after adding eight resolved August 29 fights. This is evidence explaining snapshot drift, not authorization to replace the requested cohort.

## 3. Sources examined and candidate reconciliation

The machine-readable inventory in `phase1-holdout-evidence/recovery-audit.json` records **83 source-inventory entries and 26 evaluated candidate rules**, including filenames, available source hashes, Git object IDs, warehouse queries, and counts. It is the exhaustive source list for the automated inventory.

Investigated sources included:

- Current `data/reports/pre_event_prediction_fights.csv` and event summaries.
- Older `models/pre_event_prediction_fights.csv`, event summaries, and `models/prediction_log.csv`.
- All 30 saved `models/predictions/*/predictions.csv` files, with original timestamps.
- `data/fights.csv`, `data/events.csv`, fighter identity data, repository manifests, refresh logs, cached-card recovery code, and catch-up review code.
- `data/reports/canceled_fight_predictions_backup_20260912.csv`; its predictions concern September, outside this cohort.
- Other existing betting, policy, market-benchmark, and research report CSVs. They contain derived market/betting rows, incomplete fight coverage, or other evaluation populations; they do not establish the original 146 calibrated prediction records.
- Existing retroactive backtest files and model evaluation reports; these do not establish this original cohort.
- All six notebooks, including the saved-prediction and post-event review notebooks; no original 146-fight analysis reference was found in cell source, and the saved-prediction review has no saved outputs.
- Relevant report history on all available Git refs, reflogs and stash, plus **366 unreachable blobs**. Eleven unreachable fight-report blobs were evaluated. No stash was available. No repository database dump or additional prediction backup establishing all invariants was found.
- Accessible warehouse `predictions`, `pre_event_prediction_fights`, `reviewed_prediction_fights`, `reviewed_prediction_events`, `pre_event_prediction_events`, and relevant `fights`/`events` records. The warehouse contains 571 prediction records for 288 fights. Audit access used read-only repeatable-read transactions; its timezone is UTC.

Counts below are **fights / correct**, with probability bands computed from calibrated probabilities. Report selections use April–August resolved rows unless otherwise specified. Short Git IDs identify full IDs in the machine-readable inventory.

| Source / rule | Total | Uncertain | Actionable | High confidence |
|---|---:|---:|---:|---:|
| Required invariants | 146 / 81 | 55 / 22 | 91 / 59 | 57 / 39 |
| Current report | 142 / 79 | 53 / 21 | 89 / 58 | 41 / 28 |
| Older `models/` fight report | 38 / 20 | 17 / 6 | 21 / 14 | 7 / 4 |
| Git commit `6e5c0cd6` report | 133 / 74 | 51 / 20 | 82 / 54 | 40 / 28 |
| Git commit `4af6c2f6` report | 122 / 69 | 48 / 19 | 74 / 50 | 36 / 25 |
| Unreachable `2b206b01` | 84 / 45 | 39 / 14 | 45 / 31 | 15 / 11 |
| Unreachable `4b8de841` | 68 / 37 | 32 / 13 | 36 / 24 | 9 / 6 |
| Unreachable `55f8f96c` | 141 / 79 | 53 / 21 | 88 / 58 | 40 / 28 |
| Unreachable `5863fb45` | 141 / 79 | 53 / 21 | 88 / 58 | 40 / 28 |
| Unreachable `77f7fbd4` | 65 / 33 | 30 / 9 | 35 / 24 | 11 / 7 |
| Unreachable `7853cde6` | 74 / 40 | 34 / 13 | 40 / 27 | 12 / 8 |
| Unreachable `807dd47c` | 52 / 31 | 22 / 12 | 30 / 19 | 8 / 5 |
| Unreachable `c868d96d` | 65 / 36 | 30 / 13 | 35 / 23 | 9 / 6 |
| Unreachable `e434058c` | 84 / 45 | 39 / 14 | 45 / 31 | 15 / 11 |
| **Unreachable `e7a6e34b`: strongest candidate** | **146 / 81** | **55 / 22** | **91 / 59** | **44 / 30** |
| Unreachable `fa9f8840` | 110 / 61 | 42 / 16 | 68 / 45 | 34 / 23 |
| Saved CSVs + current outcomes, latest per fight | 132 / 73 | 57 / 22 | 75 / 51 | 26 / 19 |
| Saved CSVs, scored before event day | 39 / 19 | 14 / 3 | 25 / 16 | 11 / 8 |
| Saved CSVs, scored on/after event day | 93 / 54 | 43 / 19 | 50 / 35 | 15 / 11 |
| Warehouse current view | 142 / 79 | 53 / 21 | 89 / 58 | 41 / 28 |
| Warehouse reviewed rows only | 51 / 31 | 13 / 7 | 38 / 24 | 25 / 16 |
| Warehouse history, earliest per fight | 142 / 81 | 61 / 26 | 81 / 55 | 25 / 19 |
| Warehouse history, latest per fight | 142 / 80 | 62 / 25 | 80 / 55 | 26 / 19 |
| Warehouse strict pre-event history, earliest | 104 / 55 | 44 / 16 | 60 / 39 | 20 / 14 |
| Warehouse strict pre-event history, latest | 104 / 55 | 44 / 16 | 60 / 39 | 20 / 14 |
| Warehouse view with reviewed results restored | 154 / 86 | 57 / 23 | 97 / 63 | 44 / 30 |
| Same reconciliation, through August 22 | 146 / 81 | 55 / 22 | 91 / 59 | 44 / 30 |

Saved CSV outcome joins use the prediction's fighter orientation, not the current result row's positional label. Several saved files were overwritten with later scoring runs; their current contents cannot substitute for original warehouse history.

## 4. Data provenance audit

`candidate-provenance-audit.json` identifies every candidate fight by ID, names, original timestamp, available fighter IDs, provenance, reviewed actual-fight mapping, and current resolution state. All 146 scoring identities and original prediction fields match warehouse `predictions` history.

| Candidate provenance | Rows | Timing |
|---|---:|---|
| `database_scored_at_before_event` | 108 | Scored before event day |
| `catchup_scored_before_result_load` | 38 | Scored after event day |
| All candidate rows | 146 | 108 before / 0 same-day / 38 after |

The 38 after-event exceptions are explicitly:

| Event date / card | Rows | Original scored_at, UTC |
|---|---:|---|
| July 18: Du Plessis vs. Usman | 12 | `2026-08-08T18:05:36.577481+00:00` |
| August 1: Medic vs. Rodriguez | 14 | `2026-08-08T18:05:36.577481+00:00` |
| August 8: Gamrot vs. Salkilld | 12 | `2026-08-09T11:44:27.480069+00:00` |

“Scored before result load” is a recorded review assertion. It does not establish pre-event scoring or independently prove that result information was unavailable to the scoring workflow. The candidate cannot be called entirely prospective. No additional row in this candidate is explicitly marked as retroactive, but the 38 catch-up rows remain post-event records.

Twenty reviewed records map the original prediction `fight_id` to a different actual fight ID: ten on July 18 and ten on August 1. These are recorded identity mappings; they must not be silently treated as proof of unchanged bout identity. Their mappings are included in the row audit.

The original candidate has zero unresolved outcomes. Twelve candidate August 22 rows are now unresolved in the current warehouse view after their underlying results were reset to `upcoming`: Carli Judice–Jeisla Chaves, Kennedy Nzechukwu–Shamil Gaziev, MarQuel Mederos–Mason Jones, Marcio Barbosa–Ryan Kuse, Reinier de Ridder–Roman Dolidze, Chris Padilla–Nasrat Haqparast, Wes Schultz–Jackson McVey, Gauge Young–Stan Dorsainvil, Serghei Spivac–Vitor Petrino, Jamall Emmers–Lerryan Douglas, Anthony Hernandez–Gregory Rodrigues, and Shanelle Dyer–Elise Reed. Their historical outcomes remain corroborated by reviewed records; this session did not repair warehouse results.

The snapshot's 23 excluded April–August rows comprise eight explicit replacement annotations and fifteen pending/no-W-L annotations. The replaced predictions are Jafel Filho–Lucas Rocha, Tai Tuivasa–Sean Sharaf, Modestas Bukauskas–Rodolfo Bellato, Rei Tsuruya–Jesus Aguilar, Imanol Rodriguez–Matt Schnell, Iwo Baraniewski–Billy Elekana, Ode Osbourne–Cody Durden, and Kody Steele–Gauge Young. All 23 IDs and annotations are in the audit JSON. Cancellation counts cannot be certified: the historical report lacks a reliable cancellation field. Pending and replacement annotations must not be converted into invented cancellation labels.

The current report's total drift reconciles as **146 − 12 reset August 22 outcomes + 8 newly resolved August 29 fights = 142**; correctness similarly reconciles as **81 − 7 + 5 = 79**.

## 5. Proposed training cutoff

For the recovered candidate, the exact earliest relevant timestamp is:

**`2026-03-31T12:09:04.077607+00:00`**

The latest original timestamp is:

**`2026-08-16T20:25:39.927324+00:00`**

The proposed event-date filter for later training is therefore:

```python
event_date < "2026-03-31"
```

Equivalently, the latest permitted event date is March 30, 2026. Excluding the earliest scoring day avoids assuming that a same-day event/result was known by 12:09 UTC. This recommendation derives from actual original scoring history, not the April cohort start or the model artifact directory timestamp.

The cutoff remains provisional until the exact accepted cohort is established. Event dates alone do not prove historical availability of results, statistics, or refreshed features; Phase 2 must additionally audit their knowledge timestamps. Holdout IDs must be excluded independently of this date filter.

## 6. Frozen paths and SHA-256 hashes

**There are no accepted frozen data files or manifest hashes.** The dedicated directory `data/holdouts/prospective_2026_apr_aug/` contains only a blocked-status README. `predictions.csv`, `outcomes.csv`, and `manifest.json` were deliberately not published because the required invariants are not established.

Preserved recovery evidence resides under `docs/implementation-reports/phase1-holdout-evidence/`:

| Evidence file | SHA-256 |
|---|---|
| `recovered-report-e7a6e34bb35e015d91975f182572613ad4d5b48e.csv` | `8b66ee10d706061a739604aaa74a6b71fcd3b02731d534c4a134eb9243c5d5ae` |
| `recovery-audit.json` | `3511778afb79ebff2c788598408bd430724d5e9418eb38b7284a88800dad1451` |
| `candidate-provenance-audit.json` | `bef6d8aa41f98ac267186800e7c7291ea14a971b5daea715b4bf45bfab18d31d` |

The recovered source CSV is read-only and contains historical joined outcomes. It is evidence for validation/evaluation only, not a training input or an accepted holdout. The row audit was created at `2026-10-01T08:58:21.789520+00:00`.

## 7. Required-invariant reconciliation

| Group / calibrated probability rule | Required fights | Required correct | Recovered fights | Recovered correct | Recovered accuracy | Result |
|---|---:|---:|---:|---:|---:|---|
| All | 146 | 81 | 146 | 81 | 55.5% | Matches |
| Uncertain: `0.40 <= p <= 0.60` | 55 | 22 | 55 | 22 | 40.0% | Matches |
| Actionable: outside uncertain band | 91 | 59 | 91 | 59 | 64.8% | Matches |
| High confidence: `p <= 0.30 or p >= 0.70` | 57 | 39 | 44 | 30 | 68.2% | **Fails** |

Original confidence-tier labels also identify exactly 44 high-confidence rows / 30 correct. Applying the high-confidence rule to raw probabilities instead gives 53 / 35 and does not resolve the discrepancy. Thresholds, probabilities, original flags, outcomes, and required invariants were not changed to manufacture a match.

## 8. Files created or modified

All task changes are new files; no pre-existing repository file was edited:

- `modeling/holdout.py`: reusable `load_holdout_fight_ids` and `assert_no_holdout_fights`; validates accepted status, prediction checksum, identities and count; reads no outcomes and mutates no DataFrame.
- `tools/validate_prospective_holdout.py`: offline, read-only validator/evaluator enforcing the original required invariants, separate files, one-to-one joins, required fields, finite probabilities, latent correctness, provenance audit, cutoff and file hashes.
- `tools/audit_holdout_recovery.py`: reproducible source inventory and candidate reconciliation with optional read-only warehouse access; never publishes or chooses a substitute cohort.
- `modeling/tests/test_holdout.py` and `modeling/tests/test_holdout_recovery.py`: temporary synthetic contract fixtures; no synthetic records were published as holdout data.
- `data/holdouts/prospective_2026_apr_aug/README.md`.
- This report and the three evidence files listed above.

The validator supports a future version-1 `FROZEN` manifest with selection rule, sources, counts, date/timestamp ranges, proposed cutoff, artifact, metrics, SHA-256 file entries, creation time, and provenance limitations. The helper fails closed while that accepted manifest is absent. No actual training pipeline imports were added. Individual outcome joins exist only in the two audit/validation utilities.

## 9. Tests and verification

```sh
python3 -m pytest modeling/tests/test_holdout.py modeling/tests/test_holdout_recovery.py modeling/tests/test_data.py -q
```

**68 passed**: 43 new tests plus 25 existing data/split tests. Tests cover outcome-free exclusion, strings/UUIDs, padded overlaps, missing and unaccepted holdouts, both file hashes, duplicate IDs, one-to-one joins, missing fields, invalid/nonfinite probabilities, unresolved/invalid labels, recomputed correctness, band counts/correctness, inclusive boundaries, manifest metrics, unsafe cutoff, undocumented timing changes, wrong artifacts, outcome-column contamination, path escape, prediction/result orientation, and timestamp ordering.

```sh
python3 tools/validate_prospective_holdout.py
python3 tools/validate_prospective_holdout.py
```

Both runs returned **exit 1**, with byte-identical output:

```text
HOLDOUT VALIDATION FAILED: Cannot read frozen manifest: /home/wlodzimierrr/ufc-data/data/holdouts/prospective_2026_apr_aug/manifest.json
```

Both outputs have SHA-256 `8aeafaf5244596841f769f48c7057786cd9e6256d12657fc60af3e23f86d2a38`. These are verified fail-closed results, not successful holdout validation. Positive offline validation was exercised twice on temporary synthetic fixtures and returned identical results without changing any files. No frozen data hashes can be verified while the real freeze is blocked.

```sh
PGCONNECT_TIMEOUT=5 python3 tools/audit_holdout_recovery.py --warehouse
```

Two recovery audit runs produced byte-identical JSON, SHA-256 `3511778afb79ebff2c788598408bd430724d5e9418eb38b7284a88800dad1451`. Zero candidate rules matched every invariant.

SHA-256 preservation checks confirmed all **78 pre-existing user-change/untracked/model files** in the initial preservation baseline remained unchanged. New code/documentation had no whitespace diagnostics. Whole-worktree `git diff --check` reported pre-existing CSV whitespace issues; those user files were preserved.

## 10. Unresolved risks and required input

To unblock the requested freeze, supply the original analysis input or code identifying the 146 `(fight_id, scored_at)` records and supporting the **57 / 39 high-confidence result at the stated calibrated thresholds**. If that number was mistaken, explicitly correct the invariant and confirm whether the recovered `e7a6e34b` snapshot is the intended analysis population.

Also decide how the 38 post-event catch-up records should be represented. Keeping the historical 146 requires documenting it as a mixed pre-event/catch-up evaluation cohort; a strictly prospective population would be a different cohort and must not silently replace it. Confirm the reviewed bout identity mappings and historical August 22 outcomes before acceptance.

Remaining risks are the unreachable source's unknown authorship/analysis time, overwritten saved CSVs, mutable warehouse outcome state, reviewed identity remaps, incomplete cancellation evidence, post-event knowledge leakage in catch-up records, and historical feature/result availability. Checksums detect data changes but do not prevent someone from editing both data and manifest; a completed freeze should also use exclusive creation, read-only files and version-control review.

## 11. Scope confirmation

**No model training, candidate scoring, production-pointer edits, model-artifact edits, dashboard changes, confidence-policy changes, NO PICK behavior, market stacking, production promotion, or commits were performed.** Existing user changes and unrelated untracked files were preserved. `models/production_model.json` and every existing model/prediction artifact remained byte-identical. Warehouse queries were read-only; no warehouse records or views were modified.

## 12. Recommended Phase 2 inputs and scope

First resolve the conflicting high-confidence invariant and cohort/prospectivity decision, then finish Phase 1 by publishing separate outcome-free predictions, keyed outcomes, and an accepted checksummed manifest. Require successful repeated offline validation before starting model development.

Phase 2 planning should consume this report, the preserved historical source, the two audit JSON files, confirmed record identities and exceptions, the accepted manifest, and the existing XGBoost artifact metadata. Its proposed scope is to establish training-data knowledge lineage, enforce the final date cutoff and holdout-ID exclusion in later training code, and define development/evaluation splits outside the frozen population. Keep individual holdout outcomes restricted to evaluation utilities. Retraining or policy/product changes require a separate Phase 2 instruction after the freeze is accepted.
