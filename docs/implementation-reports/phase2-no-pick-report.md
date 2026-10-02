# Phase 2: versioned NO PICK decision layer

Repository: `/home/wlodzimierrr/ufc-data`. Report date: 2026-10-01.

## 1. Status

**COMPLETED** for the authorized backend, persistence-support, and reporting scope. Live deployment and the separate dashboard frontend remain future integration work.

Applicable ancestor/repository `AGENTS.md` files were checked; none was present. Existing user changes were preserved. All edits used `apply_patch`.

## 2. Decision behavior

Central API: `modeling.decisions.decide_prediction()`. Stable policy: `probability_band_v1`.

- Existing calibrated `p = P(fighter_1 wins)` in inclusive 0.40–0.60: **NO PICK**.
- `p < 0.40`: actionable fighter-2 pick, label 0.
- `p > 0.60`: actionable fighter-1 pick, label 1.

Probabilities are neither changed nor inverted. Invalid/nonfinite probabilities raise a clear error. Existing high confidence remains inclusive `p <= 0.30 or p >= 0.70`, toss-up remains inclusive 0.40–0.60, and medium covers the rest. Strongest 57 remains a separate ranking group.

Scoring DataFrames and saved CSVs carry the contract. JSON carries the fields and both calibrated fighter probabilities, with nulls and no NaN. Human output prints NO PICK with both fighters' probabilities. Scoring calculations, calibration logic, and model artifact selection are unchanged.

## 3. Fields, nulls, and legacy compatibility

| Field | Contract |
|---|---|
| `decision_status` | `pick` or `no_pick` |
| `is_actionable` | Boolean; false for NO PICK |
| `pick_label` | Nullable integer: 0 for fighter 2, 1 for fighter 1; null for NO PICK |
| `pick_winner_name` | Selected fighter's name; null for NO PICK or unavailable name |
| `uncertainty_reasons` | `['probability_band']` for NO PICK; otherwise an empty list |
| `decision_policy_version` | `probability_band_v1` |
| `decision_origin` | `recorded_at_scoring`, `derived_at_review`, or `derived_from_legacy_probability` |
| `pick_correct` | Boolean for resolved actionable picks; null for NO PICK and unresolved results |

Legacy `predicted_label`, `predicted_winner_name`, `correct`, and `predicted_correct` retain latent evaluation semantics based on `p >= 0.5`. Public actionable picks must use the new nullable pick fields.

Python uses nullable integer/boolean types; JSON uses null and arrays; CSV uses empty nullable cells and JSON arrays for reasons; SQL uses NULL and `text[]` reasons. Legacy prediction files receive metadata in memory/output without source rewrites. Legacy warehouse decisions are explicitly derived from their available stored probabilities. Old summary-only event logs cannot reconstruct decision metrics, so those fields and policy version remain null.

Catch-up evidence remains visible. Reports group pre-event evidence separately; reviews containing retroactive scoring are conservatively marked `retroactive_review`. Existing outcome identity/matching behavior was not broadened or certified by this work.

## 4. Files created and modified

Created:

- `modeling/decisions.py`
- `modeling/tests/test_decisions.py`
- `warehouse/sql/019_prediction_decisions.sql`
- `warehouse/tests/test_prediction_decisions_migration.py`
- `betting/tests/test_prediction_decision_compatibility.py`
- `docs/no-pick-contract.md`
- `docs/implementation-reports/phase2-no-pick-verification.json`
- `docs/implementation-reports/phase2-no-pick-report.md`

Modified:

- `modeling/uncertainty.py`
- `modeling/score_upcoming.py`
- `predict.py`
- `modeling/post_event_review.py`
- `modeling/build_pre_event_prediction_log.py`

## 5. Migration and deployment

New migration: `warehouse/sql/019_prediction_decisions.sql`. No already-applied migration was edited. It adds nullable decision/full-precision fields and validates future prediction/review writes. It performs no historical UPDATE or backfill.

Existing view names, column names, types, order, and latent semantics remain compatible. Additive views are:

- `prediction_decisions`
- `latest_prediction_decisions`
- `current_event_prediction_decisions`
- `pre_event_prediction_fight_decisions`
- `pre_event_prediction_event_decisions`
- `reviewed_prediction_event_decisions`

The last view exposes persisted review denominators for partially resolved cards; legacy summary-only rows identify unavailable decision metadata. Fight-derived event metrics use the available fight-row population.

For a separately authorized deployment, inspect the pending migration list, configure the deployment database connection, and run `python3 warehouse/migrate.py` before deploying these prediction/review writers or the database report reader. Applied migrations are skipped by existing tracking. Auto report mode can fall back to CSV before the new view exists; explicit database mode surfaces the missing-view error. No live migration was run here.

## 6. Probability precision

The decision input is the original float64 calibrated probability. JSON no longer rounds to four decimals; CSV retains round-trip values and readers use `float_precision='round_trip'`.

Existing `numeric(6,4)` probability columns remain unchanged. New `predicted_prob_f1_full` and `calibrated_prob_f1_full` use PostgreSQL `double precision`, preserving the Python model values. New SQL decisions use the full calibrated value when available. For example, 0.39999 selects fighter 2 even when the old numeric projection is 0.4000; 0.60001 selects fighter 1 even when that projection is 0.6000. Immediate floating-point neighbors of both boundaries were tested across Python, CSV, JSON, and SQL.

New database consumers must use `*_full` probability fields with the decision metadata. Rounded display percentages and old numeric projections are unsuitable for recomputing future decisions. Legacy warehouse precision cannot be recovered; its decisions are explicitly derived from the historical stored value. Additive review views also expose `latent_label_full` and `latent_correct_full` for evaluation without changing old view contracts.

## 7. Reporting denominators

- `total_count`: all forecasts, including pending/unresolved results.
- `resolved_count`: forecasts with resolved binary results.
- `latent_correct_count` / `latent_accuracy`: correct latent labels / resolved count.
- `actionable_count`: all forecasts outside the NO PICK band.
- `actionable_resolved_count`: resolved actionable forecasts.
- `actionable_correct_count` / `actionable_accuracy`: correct actionable picks / actionable resolved count.
- `actionable_coverage`: actionable count / total count.
- `no_pick_count` / `no_pick_share`: NO PICK count / total count.
- `threshold_high_count`: all forecasts meeting the inclusive 0.30/0.70 high threshold.
- `threshold_high_resolved_count`: resolved threshold-high forecasts.
- `threshold_high_correct_count` / `threshold_high_accuracy`: correct high labels / high resolved count.

Zero eligible results produce null accuracy. Empty populations produce null share/coverage. Pending, draw, NC, or unresolved results do not count as losses. Log loss and Brier include every resolved forecast, including NO PICK; single-class events work. Legacy event `n_fights` / `n_predicted_fights` continue to count resolved forecasts. Existing dashboard views retain their previous behavior; the additive views provide the new denominators.

## 8. Tests and results

**275 passed, 0 failed, 0 skipped**, including **28 disposable PostgreSQL tests**. The final focused suite was:

```bash
PHASE2_TEST_INITDB=/tmp/ufc-phase2-postgres-bin/extracted/usr/lib/postgresql/15/bin/initdb \
python3 -m pytest \
  modeling/tests/test_decisions.py \
  modeling/tests/test_holdout.py \
  modeling/tests/test_holdout_recovery.py \
  modeling/tests/test_calibrate.py \
  modeling/tests/test_evaluate.py \
  warehouse/tests/test_prediction_decisions_migration.py \
  betting/tests -q
```

The host initially had PostgreSQL client tools only. PostgreSQL 15.18 server binaries were downloaded/extracted under `/tmp`, without system installation. Migration tests started and stopped an isolated local cluster with its own Unix socket, exercised existing migrations plus 019, verified old view signatures and historical row preservation, and tested prediction/review persistence and report reads. No live warehouse was used. `PHASE2_TEST_INITDB` is optional when server binaries are installed normally.

Coverage includes exact 0.30/0.40/0.50/0.60/0.70 boundaries; immediate neighbors; confident picks for either fighter; unchanged probabilities/no inversion; invalid/nonfinite and empty input; JSON/CSV/SQL nulls; rounding consistency; legacy source preservation; all-NO-PICK and unresolved events; correct denominators; single-class proper scores; provenance; and unchanged betting value/staking results.

Prediction-only checks, with no frozen outcome join for policy development:

| Population | NO PICK | Actionable | Threshold high |
|---|---:|---:|---:|
| Historical, 146 records | 55 | 91 | 44 |
| Pre-event, 108 records | 46 | 62 | 23 |

Both existing offline validators returned **VALID**:

```bash
python3 tools/validate_prospective_holdout.py --holdout-dir data/holdouts/historical_2026_apr_aug
python3 tools/validate_prospective_holdout.py --holdout-dir data/holdouts/pre_event_2026_apr_aug
```

Scoped `git diff --check` passed. A full-workspace check reports existing user CSV line endings/trailing whitespace outside Phase 2; those files were preserved. The original live integration test was intentionally not run because it inserts synthetic warehouse records and overwrites/deletes `models/upcoming/upcoming_features.csv`. An isolated scorer harness with temporary paths and mocked model/calibration, plus the disposable PostgreSQL suite, covered the affected behavior safely. Production refresh/scoring and live-deployment checks were outside authorization.

## 9. Preservation

Targeted SHA-256 checks matched **all 139 protected files** before and after: 55 existing model/prediction files, 11 holdout files, 44 existing report files, seven Phase 1 evidence files, 18 applied migrations, and four Phase 1 report/guard/validator inputs. No new model files were created. Full hashes and validator summaries are recorded in `phase2-no-pick-verification.json`.

Production pointer SHA-256 remained:
`ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa`.

Frozen datasets, accepted manifests, original probabilities/outcomes, evidence, and the 166-ID exclusion guard/policy remain intact. Existing saved predictions and report CSVs were not overwritten or regenerated.

## 10. Remaining integrations

`/home/wlodzimierrr/ufc-dashboard` was not changed. Its current UI may still display latent winners. It must adopt all six decision fields, `decision_origin`, `pick_correct`, full-precision probability fields, and the new event metrics/resolved denominators. Render NO PICK using `decision_status`; select actionable winners only from `pick_label` / `pick_winner_name`. Keep both fighters' probabilities, latent evaluation metrics, and catch-up/retroactive provenance visible. Use persisted review summaries for complete coverage on partially resolved catch-up cards.

Migration 019 must be deployed before backend writers/database report reads. Betting consumers remain compatible, and existing model-versus-market value calculations, bet/pass rules, and staking policies are unchanged. Public contract and integration details are in `docs/no-pick-contract.md`.

## 11. Scope confirmation

No training, candidate scoring/evaluation, calibration-refitting change, contextual modifier, market stacking, betting-policy change, live migration/backfill, production prediction/refresh command, production-pointer change, dashboard frontend edit, commit, or push occurred. Scorer/CLI verification used isolated mocked fixtures; no production model predictions were generated. Existing user changes were retained.

## 12. Next-session inputs and recommended scope

Use the Phase 1b report, both accepted manifests and frozen populations, identity/outcome evidence, the unchanged 166-ID guard, this report, and `docs/no-pick-contract.md`.

Next work should audit historical knowledge lineage and feature/result/statistic availability before preparing a production refit. Verify information availability at historical scoring/training cutoffs, not merely event dates. The proposed training filter remains `event_date < '2026-03-31'`, excluding March 31 onward; wire and verify the existing holdout guard in later training paths before fitting anything.

The 108 original pre-event forecasts are the primary later comparison population; their outcomes have already been inspected, so they are not an unseen test set. The 146 historical records include 38 catch-up forecasts and eight unresolved identity mappings and are not a wholly prospective/clean evaluation population. Resolve or explicitly qualify those mappings, document lineage limitations, and prepare a reviewed refit plan before training or candidate comparison. This audit remains a prerequisite for retraining, but did not block the fixed NO PICK rule.
