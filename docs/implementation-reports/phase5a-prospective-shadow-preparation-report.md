# Phase 5A prospective shadow preparation — handoff

**Implementation: COMPLETE. Challenger fitting readiness: BLOCKED.
Reference-bootstrap input readiness: READY. Prospective workflow readiness:
READY on synthetic fixtures; real paired capture readiness: BLOCKED.**

The new prospective experiment is preregistered. Current source values,
separate legacy-reference bootstrap inputs, a complete considered-bout registry,
and a diagnostic challenger reconstruction are frozen. No model, normalization
prior or calibrator was fit; no real probability, forecast evaluation or
production change occurred. The historical comparison remains **STILL_BLOCKED**.
Neither this preparation nor its diagnostics establishes prospective superiority.

Work took place in `/home/wlodzimierrr/ufc-data` on October 2, 2026. No applicable
ancestor or repository `AGENTS.md` exists. Initial worktree: clean, `main` at
`6dcf84c`; verified upstream `origin/main`, fetch/push URL
`https://github.com/wlodzimierrr/ufc-predictor.git`. Fetch found local and remote
equal. The requested Phase 4A.2, Phase 3B, refit-plan, NO PICK and Git-handoff
documents were read, along with candidate contracts/loaders, corrected replay,
history/Elo, source loading/reconciliation, legacy feature generation, production
scoring, calibration and warehouse schemas. No historical recovery search or
per-bout availability audit was conducted.

## Published preparations and interpretation

Both runs are under `data/experiments/phase5a_prospective_shadow/`:

| Run | Meaning | checksums.json SHA-256 |
|---|---|---|
| `20261002_phase5a_current_capture_v1` | Original exact capture; fail-closed header-only training preparation after structural defects | `1dd7aee05e8cacfe658ea03f4b3ea1700d6850657ca302a5a232ad00e039fd64` |
| `20261002_phase5a_current_preparation_v2_blocked` | Handoff preparation, rebuilt offline from the identical capture; preserves diagnostic features and memberships while blocking fitting | `eda0256815b59a749d215fe00ec533826150bd2a8f2240b90d2baa6f8370bf56` |

The handoff run is referred to below as **v2**. Its training-manifest pin is
`e23f7635c096e54090ffd229ab547ceb9e5f26fa7b71b2eac876be70f14ebe79`.
It locks 69 component files plus the checksum root; the original run locks 68
components plus its root. Files are read-only, and publication refuses existing
run names. No incomplete marker remains. All nine source payloads are exactly
identical between runs. The second run performed **no warehouse or page read**.
`parent_capture_provenance.json` pins the original capture and explains reuse.

The first preparation stopped before feature reconstruction. To deliver useful
diagnostics without concealing defects, the second preparation reconstructs
well-formed ambiguous bout rows without coalescing identities or deleting
histories. `dataset_role=blocked_diagnostic_reconstruction`,
`fitting_eligible_rows=0`, manifest readiness false, and every fold readiness
false make that distinction explicit. `load_current_preparation` rejects the
run with `Blocked or incompatible current preparation`. The diagnostic CSV and
proposed memberships are **not accepted Phase 5B fitting inputs**.

## Capture clocks, sources and freshness

One capture transaction used `set_session(isolation_level="REPEATABLE READ",
readonly=True, autocommit=False)`. Before source queries it verified
`transaction_read_only=on` and `transaction_isolation=repeatable read`.
Database timezone: **UTC**; snapshot: `207329:207329:`.

| Clock | UTC |
|---|---|
| Capture application start | 2026-10-02 13:28:08.558979 |
| Database transaction start | 2026-10-02 13:28:08.561409 |
| Database capture start | 2026-10-02 13:28:08.577092 |
| Database capture end | 2026-10-02 13:28:09.595863 |
| Rollback/close completion | 2026-10-02 13:28:09.643397 |
| Coherent capture completion, including bounded HTTP response | 2026-10-02 13:28:09.905938 |
| Original preparation completion | 2026-10-02 13:28:12.718935 |
| Offline v2 preparation | 2026-10-02 13:33:41.993183–13:33:52.837770 |

The transaction was rolled back and closed. `source_capture_receipt.json` stores
every query, parameter, schema/column list, result count and query observation
clock. `sources/schemas.json` captures public schema types, nullability and
defaults for the four source tables and `bout_features`. No credential,
password, connection string, authorization header or cookie is published.

| Captured source | Rows | Recorded scrape range UTC | Missing scrape clock |
|---|---:|---|---:|
| `public.events` | 798 | June 1 00:03:05–September 30 18:46:41 | 1 |
| `public.fighters` | 4,522 | August 8 18:00:30–September 30 18:46:41 | 0 |
| `public.fights` | 8,992 | March 30 10:24:47.177619–September 30 18:46:41 | 0 |
| `public.fight_stats_aggregate` | 17,386 | August 8 23:39:44–August 9 03:09:44 | 0 |

Source events span March 11, 1994–November 7, 2026. Resolved results actually
end **August 29, 2026**, 34 calendar days before the training cutoff. Aggregate
statistics have not been refreshed since August 9. There are **128 past-dated
unresolved `upcoming` rows** and 35 future rows. Capture today does not make
these sources complete or fresh through today. Scrape clocks describe recorded
observations, not independently certified source availability.

The four full-table queries were `SELECT * FROM public."<table>" ORDER BY
"<primary key>"`, using their recorded allowlisted table/key names. Exact SQL
values are preserved as canonical JSON: UUID/date/time values are textual,
numeric `Decimal` values retain exact decimal strings, and the captured schema
preserves SQL types. Challenger feature inputs contain no stored `bout_features`
or `fighter_snapshots`. Contemporary values are known at capture time; their
pre-historical-bout availability is **unverified**. This is retrospective
training reconstruction, not certified replay.

One ordinary targeted request was made to
`http://www.ufcstats.com/event-details/ad3fdba28a7540cf`, the earliest future
warehouse event. Request: 13:28:09.673188 UTC; response: 13:28:09.903083 UTC.
It returned HTTP **200**, but the 2,998-byte body was an access-check response.
Its SHA-256 is `c428b6bdbfe11efd594b7eb02e014dff33c4624e9b212c5356a2bd68878849dc`.
The response `Date` header says 13:25:12 GMT and is retained as server metadata,
not substituted for our observation time. The exact body and response metadata
are in `official_sources/`. The four-request maximum stopped after this first
response; no retry, blocking bypass, profile fetch or crawler followed. Its
unparsed body resolves no title, history or matchup eligibility assertion.

## Challenger reconstruction, eligibility and defects

Exclusive training event cutoff: **2026-10-02**, the coherent capture's UTC
completion date. The entire date and every later date are excluded. The
diagnostic reconstruction has **8,558 rows, 783 events and 778 event dates**,
from March 11, 1994 through August 29, 2026. Label counts are 5,493 fighter-1
wins and 3,065 fighter-2 wins. Labels and orientation are checked against the
independently captured source, without reading frozen outcomes or joined
evaluation evidence.

All **166 exclusion IDs** are present in the source and absent from returned
binary rows. Of them, 109 have independently captured resolved results and may
appear in strictly earlier histories. That permission is distinct from using
their labels in learner fitting, selection, normalization or calibration.
Histories retain 8,829 source rows, including 65 draws and 97 NC; draws/NC are
not fitting targets. Source-wide result counts: 8,667 wins, 65 draws, 97 NC,
163 upcoming. Eligibility records cover **every one of the 8,992 source fights**.
Reason counts overlap: 166 fitting exclusions, 35 on/after cutoff, 65 draws,
97 NC, 163 unresolved/upcoming, and 8,992 blocked by the global structural gate.

Fourteen event/participant pairs have two distinct bout IDs. Twelve groups
contain IDs already excluded from fitting but can still corrupt global Elo and
opponent histories if both versions are counted. Two groups are outside the
exclusion set:

| Event date | First ID | Second ID |
|---|---|---|
| 1997-12-21 | `2c4d505e-c625-5e27-89f8-e36c2b8224b4` | `383d786b-c425-517e-a2af-d4cb19081135` |
| 2026-04-18 | `498de4bd-d781-52af-a383-802158196d2d` | `4b08f65d-db68-5091-9568-4748c4cf7318` |

These are structural identity conflicts under the current contract, not proof
that all records are erroneous: an early tournament pair could be a legitimate
rematch. Resolution needs explicit source/identity evidence and a reviewed
current-data history policy. All exact rows remain preserved. No arbitrary URL
priority, label repair, history deletion or warehouse correction was used.
The source manifest/eligibility ledger and structural issue list retain the
remaining group IDs. **Fitting readiness remains blocked even where the
conflicting IDs are themselves fitting-excluded.**

Corrected algorithm: `v2_date_frozen_elo_schedule_unknown_v1`, feature version 2,
the exact ordered 50 columns in `feature_contract.json`. Date-frozen Elo,
strictly earlier histories and opponent histories, original fighter orientation,
and legitimate sparse values remain. Actual elapsed duration is consumed only
through earlier histories. Every target/history schedule is unknown; neither
finish rounds nor title status infer schedules. The three debut columns remain
deferred; there is no learned/global preprocessing in this phase.

All 8,558 scheduled-round and each deferred-debut value are NaN. There are 537
both-debuting rows; five-round-experience differences are missing in 8,021 rows.
Other illustrative missing counts: age 123, height difference 24, reach
difference 1,036, takedown accuracy 3,271, reach/height ratio 1,038 and weight
class rank 196. Full per-column counts are in `missingness.json`. Among source
profiles, height/reach/DOB/stance are absent in 334/1,955/514/857 rows. There are
136 resolved source fights lacking at least one participant's aggregate stats.
No sparse bout is removed merely for missing stats.

Contemporary stored title booleans and statistical defaults are not historical
attestations. The unchanged corrected feature implementation also retains
legacy stance-flag semantics. These inherited missingness/default limitations
are explicit; future eligibility separately requires sourced title/profile/
experience evidence and never turns unknown experience into zero.

## Fixed Phase 5B recipe and exact proposed OOF memberships

`configs/phase5_prospective_shadow_v1.json` and
`docs/phase5-prospective-shadow-protocol-v1.md` were written before capture or
modeling. The source config SHA-256 is
`6d63abe99fa6a401df2e779c68b1e3ab8aa81fd3a3d2253058c812a644732438`;
its canonically serialized effective-config SHA-256 is
`2b92e2bc0342265734bbf09e6c5b9d4fa9cdaa6555a0fe66e6e6d0ab44eb5b20`.
The byte-frozen protocol SHA-256 is
`c5576835ef950c39ff211aa20a2fe0942dda48442f04ff6adbc535a3737c0bfe`.

Use depth **6**, child weight **100**, lambda **1.0**, **310** rounds;
logistic objective, logloss evaluation, learning rate 0.02, row/column
subsample 0.8, random seed 42, one thread, histogram trees and verbosity 0.
No new hyperparameter search, OOF early stopping or tuning is permitted.

| Fold | Expanding training before | OOF interval, inclusive start/exclusive end | Train / OOF |
|---|---|---|---:|
| oof_1 | 2025-10-02 | [2025-10-02, 2026-01-02) | 8,217 / 128 |
| oof_2 | 2026-01-02 | [2026-01-02, 2026-04-02) | 8,345 / 114 |
| oof_3 | 2026-04-02 | [2026-04-02, 2026-07-02) | 8,459 / 69 |
| oof_4 | 2026-07-02 | [2026-07-02, 2026-10-02) | 8,528 / 30 |

These **341 exact proposed OOF identities** pass chronology/binary/166-ID
filters but remain diagnostic-only until structural readiness is resolved.
Every ID list and digest is frozen in `folds.json`; no event/date is split.
Combined OOF membership hash:
`ec2eabb18c777c99126be92fe5b09cb82f399ee016d379b0a4099b7c2c6f924d`.
Final proposed 8,558-row membership hash:
`93ac5c098cf43a69d8f251dba4aec754d0d43a3862e56779069c1540b3c5b6bf`.

After a separately accepted, structurally valid preparation, Phase 5B computes
training-only preprocessing independently in each fold, fits 310 rounds per
OOF learner without tuning/early stopping, and generates exactly one raw OOF
prediction per eligible row. One persisted Platt estimator uses clipped log
odds, epsilon 1e-8, C=1e10, lbfgs and max_iter=1000. Same-row calibrated metrics
are labeled **calibration-fit diagnostics**. Final priors and learner are
independently refit on all eligible current rows. The Phase 3B loader/trainer
guards are unchanged; the new loader is `load_current_preparation`, with a
separate versioned `phase5_current_candidate_v1` contract and explicit root/
manifest pins. The diagnostic override cannot make its loader accept blocked
inputs.

## Three comparator roles and reference bootstrap

The March research control remains the complete unchanged Phase 3B candidate
trained through March 7, 2026, checksum root
`41f8c8afb8fd091fa0b04823ad9175cedcfdbf85ccf3f62d77c71e0d9219279f`.
The current challenger is a **future separate Phase 5B artifact**. The reference
is a **production-derived prospective comparator**, not a recovered historical
production probability stream.

The existing learner is `models/xgb/20260328T221117Z/model.joblib`, SHA-256
`0585077675968c2a3537ffd3fddb87a9eb610c98d36200b64dd5fe148599389a`.
Its metadata hash is
`d10a0e6fd17996edac5d6aa90b4436ade1a5d2424b7dbe2f9ed379c469c4e02a`.
Neither learner deserialization nor inference occurred. Stored reference rows
were fetched in the same read-only transaction, physically under
`reference_bootstrap/`, using the recorded metadata boundaries:
preprocessing before **2022-03-12**, calibration in
**[2022-03-12, 2024-03-09)**.

SQL applies `bf.event_date < %s`, `bf.label IS NOT NULL` and
`NOT (bf.fight_id::text = ANY(%s))` **before labels reach Python**. Left source
joins preserve missing/mismatched references for validation rather than dropping
them. Captured data include exact feature values, fight/event/fighter IDs,
labels, feature version, computed clock, source result/orientation/date and
source observation provenance. Exclusions, numeric/label validity and source
joins pass. No fitting labels from the original holdout or its alternates are
present.

There are **6,391 preprocessing and 1,009 calibration rows**, compared with
the artifact's recorded 6,376/1,007: **+15/+2**. This is a current reproduction
bootstrap, not recovery of the original training/validation row population.
Preprocessing membership hash:
`2013992e43c41c7c1825b93d3d71a84b14a3ef4e68760e4093a79f7bcf69fbf3`;
calibration membership hash:
`de0ed1872e81ab8e4a5ae77a550a94a555e178ba8e53bcf42118e2f70503d5db`.
Feature computation timestamps are August 16, 2026
20:25:09.579778–20:25:10.793139 UTC, well after the historical calibration
events. All 7,400 stored rows retain scheduled-round values: 3/1/2/5/4 in
3,717/2,132/1,237/271/43 rows respectively. This directly illustrates the
legacy finish-round/schedule defect retained in bootstrap inputs. Legacy
within-date Elo behavior, stored rounding, defaults and mutable historical
profiles also remain limitations. They were not repaired to improve the
reference or relabeled as the corrected challenger pipeline.

Production currently recomputes debut preprocessing and **fits Platt at scoring
time without persisting the estimator**. Phase 5B must freeze preprocessing
and a separately saved Platt estimator from these bootstrap inputs before any
prospective predictions or trial-outcome access. The existing learner remains
unchanged; there is no repeated recalibration or silent raw-probability fallback.
Reference-bootstrap readiness means the **captured inputs pass validation**,
not that a usable frozen reference bundle already exists.

`features/build_upcoming.py` was reviewed, SHA-256
`061a9628ab741d1525515a9cc46f22e1c959d9a5c17f79bbc32543345b4f2786`.
Its legacy snapshot reference date is `min(today,event_date)`; the corrected
forecast replay keeps the target event date for age/activity and uses a separate
observation history cap. Legacy generation also loads live sources/priors and
all-date Elo inputs. Phase 5B must pin an isolated reference feature adapter
that consumes the matched frozen capture and saved components, documents these
recipe differences, and never calls the production writer. This capture has
not implemented or run real reference feature scoring.

## Prospective registry and frozen evaluation protocol

All **35 considered bouts across six announced events** are registered, with
stable identities/orientation, announced date, capture cutoff/hash, source
metadata, recipe versions, readiness and blocking reasons. Announced dates and
counts: October 3/10/17/24/31 and November 7: **14/1/12/6/1/1**. The scope is
all captured warehouse bouts on/after cutoff; it is not a claim of worldwide
or complete-card coverage. Every considered identity is included before any
prediction. There are zero scored bouts and zero complete paired forecasts.

Every bout has unknown attested title status and unresolved experience for both
fighters. Profiles are present, but this does not independently certify complete
history or debut. Fourteen bouts fail the 24-hour rule; twenty are beyond the
14-day window; one is within the timing window but still metadata-blocked.
Reason counts overlap. Old saved production predictions supply no counterpart.

`modeling/prospective_registry.py` validates metadata-only registration/captures,
first complete pairs, common observation cutoff, input/component freeze clocks,
and conservative lead times. Original source/prediction lineage is retained
through hash-linked revisions/cancellations. `tools/append_prospective_registry.py`
publishes exclusive immutable successors from checksum-verified parents and
copies old record bytes unchanged. It refuses predictions/outcomes in the
preparation journal. Provisional identities, unknown dates/title/profile/
experience and missing counterparts remain explicit blockers.

The primary forecast is the **first complete paired capture** between 14 days
and 24 hours before the announced date's **00:00 UTC** boundary, inclusively.
Candidate and reference use the same observation cutoff, with separate recorded
feature recipes. Forecasts follow completed capture and retain the lead-time
restriction. Different information-date comparisons are separate descriptive
work. Revisions, replacements and cancellations append records and preserve
the original forecast; revised roster/date forecasts leave the primary metric
population under predeclared rules while remaining in coverage logs.

Primary comparison: calibrated current challenger versus frozen reference on
complete paired prospective binary forecasts. March control is a predeclared
secondary comparison. Primary metrics are **all-fight log loss and Brier**,
including NO PICK. Log loss clips at 1e-8; Brier uses unmodified unrounded p.
Latent class is p>=0.5. `probability_band_v1` keeps inclusive 0.40–0.60 NO PICK
and high confidence at p<=0.30 or p>=0.70.

The protocol fixes ten bins ([0,.1], then (lo,hi]), counts/means/gaps/ECE/MCE
and null empty bins. Event-cluster bootstrap uses 10,000 paired replicates,
NumPy Generator PCG64 seed 42, lexically sorted event IDs, E events sampled with
replacement with all paired rows and multiplicities retained, bout-weighted
metrics, challenger-minus-reference differences, and linear-quantile 2.5/97.5
percentile intervals. Each model's latent, actionable and high-confidence
counts, accuracy and coverage, reference-selected actionable comparisons and
jointly actionable comparisons are predeclared.

Coverage denominators explicitly distinguish all considered bouts, data-blocked/
not scored, scored NO PICK/actionable, missing inputs/counterparts, complete
pairs, unresolved results, cancellations, revisions/replacements, draws, NC and
resolved paired binary bouts. Missingness is never silently scored as a loss;
empty accuracies are null. The first formal checkpoint is the first completed
event ingestion batch with **at least 200 resolved paired binary bouts over at
least 20 events**. This is a collection target, not a power guarantee or
promotion rule. Earlier monitoring is descriptive only; there is no favorable
stopping, trial-result modification, threshold tuning or outcome-based filtering.
All real inputs, forecasts, hashes and receipts must freeze before a later
separately scoped outcome-ingestion command/session. No automatic promotion.

## Component pins, tests, rebuilds and preservation

All paths in the next table are relative to v2; `checksums.json` covers every
other component and preserves exact bytes via the repository's existing
`data/experiments/** -text` rule.

| Component | SHA-256 |
|---|---|
| `sources/events.json` | `e36c7250b9965ec8294fe9bac196f45ec96ad51624ef704e5e188b83eb868a78` |
| `sources/fighters.json` | `1cf4a351788b7de8496e1e05c8591f5cf462739a4843f44831fa7168d20a4caf` |
| `sources/fights.json` | `27738c6ca29fafa1cdde121b377607ef9683c79cfb7eacca4b96b1f166118b17` |
| `sources/fight_stats_aggregate.json` | `ec19a9e5e0b13766744e187f1d099e92c65103940ac02fefe804b4421414bcb4` |
| `sources/schemas.json` | `d4d0a931dc86598b1b2f26e5bd52347060094fe5beb64031b07bd359e431ee8a` |
| `training.csv` | `3a48ffa517e9cb4475212f507b91039c7e8d6fb08b68fee1bf90d010e4d5c33c` |
| `folds.json` | `8c8ed6c4fc1a133b942e6fb54d5ebf9d6f9247e884bf1b407d47d4f6f217ac07` |
| `eligibility.json` | `896b39adc167aded715ba5b8b89ef807edc6936c3f30a696af9d761440ce3a05` |
| `reference_bootstrap/stored_features.json` | `74d0c51a8fb3f9f206c9534944e9fc60068d1c45aa2927450a8cc9557095f2ed` |
| `reference_bootstrap/preprocessing_rows.json` | `76d1373afbb45068fa5f1e9a58faad99bcee2f77476f2ccbfb6511a3b1afc4ca` |
| `reference_bootstrap/calibration_rows.json` | `5008b460dea1e4317ff1566afb637a70e5e8eb5195c79c1b741bf8baa94f568d` |
| `reference_bootstrap/manifest.json` | `208f4c32f811f536d7b5baf68ea4be862d4e45d429d3d7994637e440aa5d1443` |
| `registry/considered_bouts.json` | `bfd61fbe1ce400f046859b88973acad380c88cc8601edde4d485d7f06f77b34c` |
| `source_manifest.json` | `38ac73d19f43929cf44f60ef39fae444667b2585a38cd2ad238dc195ce31fe40` |
| `source_capture_receipt.json` | `dafdb9b4d9b54f33a48b2f471708aa86c6d34a83b4c5f07e5f7b92708d9467eb` |
| `feature_contract.json` | `223dc3dc7946766f152302f6105f93c7782b5832f4105fba6904db881fcca336` |
| `missingness.json` | `2df50270cc7bbc304b678abe59bbd66009e580274e73864b0eb328d3e2e519f7` |
| `package_versions.json` | `bb841c0a84f659cff99b65f760be9a05bc530c00ebd1d569ab74165f0b810b51` |
| `code_versions.json` | `90ce335479ca5b4ec826f8bfb3e83624d0b56e115f3d1b6a9431991ce192f6d4` |
| `validation_results.json` | `71cc88d520bb91da891819295a88f591e628de8538d0c2078d7b165da09d2a64` |
| `run_receipt.json` | `c3a6ba01b024a4028c6c06e25545152ecb269957aa7f1441354c75d37b09016d` |

Packages: Python 3.11.2; numpy 2.4.3; pandas 3.0.1; scikit-learn 1.8.0;
xgboost 3.2.0; psycopg2-binary 2.9.11; joblib 1.5.3; pytest 9.0.2. The code
manifest pins new preparation/registry commands and all relevant reconstruction,
holdout, calibration and scorer dependencies. No old candidate guard was changed.

Final explicit safe suite: **319 passed**, including **38 new Phase 5A tests**.
It uses mocked HTTP/connections, synthetic feature data, and the existing
disposable PostgreSQL SQL-restriction test. It covers read-only verification/
failure before publication, rollback/close, cutoff/all 166 exclusions, earlier
excluded-result histories, source defects without silent deletions, draws/NC,
target/same-date/future invariance, unknown schedules/deferred debut slots,
fixed OOF memberships, challenger/reference separation, no preparation fitting/
real scoring/frozen-outcome reads, lead times/common cutoff/component clocks,
unknown metadata, complete consideration coverage, immutable revisions/
cancellations/successors, deterministic reconstruction and checksum tampering.
Compilation and whitespace checks passed.

Test-selection deviation: an earlier overly broad `features/tests` invocation
ran existing read-only live warehouse integrity tests and obsolete v1 feature
schema tests: **339 passed, 21 failed** (two live integrity failures, nineteen
legacy schema failures). This was a selection mistake. It used only SELECTs
for live integrity checks, produced no predictive outcome metrics, and was not
used as challenger feature input. It was not rerun or repaired. The known
mutating `tests/test_integration_pipeline.py` was never run. Subsequent tests
used an explicit safe file list. The test receipts disclose this deviation.

Both independently repeated v2 builds matched all **49 deterministic payloads**
byte-for-byte before publication; publication read-back, another rebuild and
the verify CLI each passed. The initial capture likewise rebuilds all 49
payloads exactly using its original committed preparation code. Every source
byte matches across runs. The real v2 loader refusal was independently checked.
Synthetic fixtures prove overwrite, incomplete-run and tampered-checksum
rejection. No real estimator loading or probability parity test was run.

Hash-only preservation checks before capture, after publication and during
handoff verify **14,515 existing files unchanged**, zero missing and zero new
protected files. The baseline hash is
`c54b194de66827ce33e92f4cad9bd7d411efbb694f5c14dc3edad86902668683`.
Coverage includes both original holdouts/probabilities, Phase 1–4 evidence and
reports, accepted preparations, the complete March candidate, both blocked
Phase 4 runs, existing models/predictions/pointer, root CSVs and 526 MB of raw
storage. Protected outcomes are hashed for preservation only, not parsed by
the preparation. Production-pointer hash remains
`ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa`.

## Remaining blockers and exact Phase 5B handoff

1. Resolve the fourteen source identity groups, including legitimate rematch
   distinctions and duplicate manual/source histories, through explicit current
   source evidence and a reviewed history contract. Do not delete ambiguous
   fights or weaken the March candidate guards. Publish a new exclusive
   structurally valid preparation and freeze its new membership/source hashes.
   Current v2 fitting and OOF calibration remain blocked; its diagnostic hashes
   must not be treated as authorization to fit.
2. Source freshness is limited to August results/statistics with 128 stale
   unresolved bouts. Report this coverage explicitly. Any later coherent
   refresh is a new capture/run with its own exclusive UTC completion-date
   cutoff, not an overwrite or relabeling of this snapshot.
3. The reference inputs are ready under the separate legacy contract. In a
   separately authorized Phase 5B, verify the recorded pointer/base/metadata and
   bootstrap hashes, use the exact 6,391/1,009 rows, compute persisted legacy
   preprocessing and one saved Platt estimator, and pin the isolated future
   reference feature adapter before any forecasts/outcomes. Preserve inherited
   defects and document the current-versus-original membership difference.
4. Obtain attested upcoming title metadata and complete-history/explicit-debut
   evidence; the ordinary official source request is blocked. Respect that
   block. Freeze matched contemporary captures and both model/component hashes
   before real predictions. All 35 consideration records remain visible and
   unscored. No production prediction can substitute for the paired reference.

Start the next session with this report, the preregistered config/protocol,
v2 checksum/training-manifest pins, exact source/eligibility/feature/missingness/
fold files, `reference_bootstrap/` inputs/provenance, registry records and tail,
package/code manifests and receipts, the unchanged March candidate and 166-ID
guard. A ready current preparation will additionally require a separately
reviewed structural resolution and a new run. Phase 5B must not silently accept
the currently blocked diagnostic data. Preserve the March research control and
freeze all future learners/preprocessing/calibrators before trial forecasting.
Outcome ingestion and any production decision remain separately scoped.

Git handoff follows `next-phase-git-handoff.md`: logical implementation,
diagnostic-preparation, and evidence/report commits, then a normal push to the
verified upstream. Exact commit hashes, push outcome and final worktree status
are reported in the final session response, without a self-referential report
update commit.
