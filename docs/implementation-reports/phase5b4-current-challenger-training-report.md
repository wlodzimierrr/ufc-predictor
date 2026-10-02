# Phase 5B.4 current challenger training and publication

**Implementation COMPLETE. Isolated challenger artifact READY. Modeling-role
eligibility READY. April relationship UNRESOLVED. Prospective forecasting
BLOCKED. Historical comparison STILL_BLOCKED. No production approval or
automatic promotion.**

Exactly four OOF XGBoost learners, one Platt estimator and one fresh final
XGBoost learner were fitted under this session's explicit authorization. Five
partition-specific debut prior computations used only their respective training
rows. Independent saved-component replay matched all 341 OOF probabilities and
the final preprocessing/raw/calibrated pipeline exactly. No estimator was
refitted to repair a persistence discrepancy; there were no discrepancies.

## Artifact and trust anchors

The completed challenger is exclusively published at:

`data/experiments/phase5b4_current_challenger/20261002T183215Z_phase5b4_current_challenger_v1_fixed/`

| Artifact/pin | SHA-256 |
|---|---|
| Challenger `checksums.json` | `59ffd4995946cc9ec7512f2f14caade09fd3144d2cfb3ee2d3b9b0951f5ca300` |
| Challenger `training_manifest.json` | `7330bbebd06a6f5c0233cf8892be5c1728edc8805add02b1511499bf198b0c73` |
| Final learner `models/final.json` | `42e20aceefd19bd5e795fde7bea19c9916b6fd847422563d7f262a6c2087842c` |
| Saved `calibrator.joblib` | `24cf146cc594063e006a3af2a9d85839e2e63d4c49a14e6f169c9ada916304e0` |
| Final `priors/final.json` | `60c34c9fb99f6da476872bc8f23954e255b84d74b8f40e1e1f983a2b75bfb173` |
| Full-precision `oof.csv` | `2efbc2aa8103f95a1d02f540ba6cfd61fe36f58c9c7fd098b1f243f64d6f1e85` |
| Exact `calibration_input.json` | `aac18a137e540fd6096e441e12706003210a93a88080c1bcb718bfadf816a581` |
| Verbatim session `authorization.txt` | `df392746391f5810fa608360649f06f15d87444007dc3fc8efb571394ab697e8` |
| Accepted preparation checksum root | `7d36dc2bdb2a99eb81a44424fe603a6beab74d05435e41c8a1489cea11f0bd4f` |
| Accepted preparation training manifest | `00f445f4fc93af986db5dbda5631c89181ccaa85271f643660dade66b4c9aa4b` |
| Unchanged preregistered configuration | `6d63abe99fa6a401df2e779c68b1e3ab8aa81fd3a3d2253058c812a644732438` |

There are **49 checksum-covered components plus the checksum root**, totaling
13,607,351 bytes. The largest file is the 3,283,071-byte original training
membership manifest. No incomplete marker remains. The bundle includes all
five native JSON learners and effective configurations, all five priors, the
persisted calibrator/parameters, OOF probabilities and component lineage, exact
training/fold memberships, preprocessing/calibration contracts, accepted
preparation manifests/configuration/source receipts and hashes, package/code
pins, authorization, actual execution/freeze times, tests, diagnostics,
preservation baseline and independent verification.

The preparation was consumed only through
`modeling.phase5_role_aware.load_role_aware_preparation`, with both required
external pins. Its independent reconstruction, full inventory, source/identity
evidence, role admissions, exclusions, code and packages passed before fitting.
No direct training CSV read substituted for guarded loading. The separate
trainer additionally checked the exact accepted CSV serialization hash,
preregistered fold bytes, original source identities/winner orientation and the
complete 8,992-row partition admission ledger. This retains all 166 exclusions
and canonical/alias propagation for every fitting, preprocessing, OOF and
calibration role. Accepted preparation-only status fields and preregistration
bytes remain unchanged; the new authorization receipt applies to this run.

## Populations, fixed recipe and partition provenance

The exclusive cutoff is **2026-10-02**, while the actual latest binary training
event is **2026-08-29**. All **8,558** eligible rows entered the final fit,
including the old validation/test periods; none was reserved. They represent
783 events and 778 dates, with original labels 5,493 fighter-1 wins and 3,065
fighter-2 wins. Labels and probabilities always mean fighter 1 at class 1.

Final membership hash:
`93ac5c098cf43a69d8f251dba4aec754d0d43a3862e56779069c1540b3c5b6bf`.
Combined 341-row OOF membership hash:
`ec2eabb18c777c99126be92fe5b09cb82f399ee016d379b0a4099b7c2c6f924d`.
Exact identities, dates, orientation and labels are in
`training_membership.json`; exact train/OOF IDs and hashes are in
`preparation/folds.json`. Each prior also saves ordered training IDs, their hash,
the accepted unfitted input hash, exclusive boundary and actual endpoint.

| Fold | Training end exclusive | OOF interval [start,end) | Train / OOF | Actual training endpoint |
|---|---|---|---:|---|
| oof_1 | 2025-10-02 | [2025-10-02, 2026-01-02) | 8,217 / 128 | 2025-09-27 |
| oof_2 | 2026-01-02 | [2026-01-02, 2026-04-02) | 8,345 / 114 | 2025-12-13 |
| oof_3 | 2026-04-02 | [2026-04-02, 2026-07-02) | 8,459 / 69 | 2026-03-28 |
| oof_4 | 2026-07-02 | [2026-07-02, 2026-10-02) | 8,528 / 30 | 2026-06-27 |

Every learner used exactly:

```json
{
  "objective": "binary:logistic", "eval_metric": "logloss",
  "max_depth": 6, "min_child_weight": 100, "reg_lambda": 1.0,
  "n_estimators": 310, "learning_rate": 0.02,
  "subsample": 0.8, "colsample_bytree": 0.8,
  "random_state": 42, "n_jobs": 1, "tree_method": "hist", "verbosity": 0
}
```

All fits called `fit(X, y)` on fresh estimators, with no eval set, early
stopping, feedback from evaluation labels, warm start, class/sample reweighting,
round reselection or search. Each saved learner contains exactly 310 boosted
rounds. Effective booster configuration, parameters, fit arguments, feature
names/order, binary orientation and absence of stopping/evaluation state are
verified separately.

The new lifecycle uses `modeling/phase5b4_contract_v1.py`,
`modeling/phase5b4_trainer_v1.py`, `modeling/phase5b4_bundle_v1.py`,
`modeling/phase5b4_safety.py` and
`tools/train_phase5b4_current_challenger.py`. The Phase 3B trainer/contracts and
all existing artifact-pinned code remain byte-identical. Only the unchanged
pure debut helpers and canonical feature list are reused. New partition guards
independently require the exact ordered 50 features, approved membership,
complete chronology, original labels/orientation/non-debut values, no excluded
identity/alias and no event/date split immediately before transforms and fits.
Deep copies protect accepted inputs; their serialized hash is rechecked
throughout the lifecycle.

Each fold computed neutral 0.5 debut priors and training-only physical
normalization using the existing bucket/fallback recipe. Final priors were
computed independently on all 8,558 rows at
18:32:45.046035–18:32:45.236826 UTC, after OOF generation and calibration.
Their global height/reach standard deviations are 6.462122056892313 and
8.306182183564745. They never entered OOF preprocessing. All schedules remain
unknown. Missing physical values, unknown weight classes and other legitimate
NaNs retain their existing semantics; there is no invented imputation.

## Calibration and diagnostics

Exactly 341 unique approved OOF identities were combined in fold/chronological
order, with original metadata and labels, probability-range checks, exact
coverage, train/prediction membership hashes and saved prior/learner hashes.
The persisted calibration input contains the ordered IDs/folds/labels, raw
probabilities and their explicitly clipped log odds, bound to the calibration
contract by hash. The Platt estimator used epsilon **1e-8**, **C=1e10**,
**lbfgs**, **max_iter=1000**, classes `[0,1]` and positive class fighter 1.

Platt fitted once at 18:32:44.616162–18:32:44.618267 UTC and converged in
**5 iterations**, with zero convergence warnings. Saved coefficient:
**1.4672337196210894**; intercept: **-0.45713446552549547**.
These values, dimensions, estimator settings and convergence are validated on
reload. Invalid input or non-convergence stops publication; no raw-probability
fallback exists.

| Diagnostic scope, same 341 rows | Natural-log loss | Brier |
|---|---:|---:|
| Raw OOF diagnostics | 0.6203851061052678 | 0.21654337662025586 |
| **Calibration-fit diagnostics** | 0.6046915301570098 | 0.20998497246264813 |

Log loss explicitly clips at 1e-8; Brier uses full-precision unmodified
probabilities. The second row evaluates calibration on the rows used to fit
that calibrator. Neither row is prospective performance or certified
historical replay. Neither selected a model, threshold or recipe, and neither
establishes superiority over the reference. No reference probabilities were
loaded or compared.

## Freezes, staged publication and independent verification

The unique directory was created exclusively with `INCOMPLETE`; no run was
overwritten. Immutable component files and a pinned staging inventory were
written before independent deserialization/replay. Completed inventory and
preservation/verification receipts passed before the marker was removed.
The public loader refuses incomplete runs. The trainer also refuses another
run carrying the same authorization rather than automatically retrying fits.

Actual execution began **2026-10-02T18:32:20.368396+00:00**. Component fitting
and freezing completed **18:32:48.131406 UTC**. Completion was recorded at
**18:32:56.401689 UTC**. The timestamp embedded in the run name is only its
unique directory identifier. Actual component-freeze clocks are:

| Component | Fit interval UTC | Saved-component freeze UTC |
|---|---|---|
| oof_1 | 18:32:32.264414–18:32:32.961968 | 18:32:32.965565 |
| oof_2 | 18:32:36.047618–18:32:36.531456 | 18:32:36.535143 |
| oof_3 | 18:32:39.671497–18:32:40.156476 | 18:32:40.159909 |
| oof_4 | 18:32:43.298855–18:32:43.788026 | 18:32:43.791456 |
| Platt | 18:32:44.616162–18:32:44.618267 | 18:32:44.619117 |
| Final learner | 18:32:46.965034–18:32:47.459800 | 18:32:47.462997 |

The independent staged replay began at 18:32:48.292442 UTC. Saved fold priors
reproduced every training and OOF prepared matrix hash, and separately loaded
fold learners reproduced **all 341 raw probabilities exactly**, with no
tolerance. Independently loaded final priors/learner/calibrator reproduced the
8,558-row prepared matrix and raw/calibrated probability arrays exactly against
the in-memory pipeline. Final verification hashes are:

| Verified value | SHA-256 |
|---|---|
| Prepared matrix with identities | `276f0826892403cb1280869d9ef6f033561463466bb56cfb7dfb906eb3b0902f` |
| Raw probability vector, little-endian float64 | `0c8e68ae9a6af689900f98d4216fa1ce79af2202a27453f1bebb2bd58bcc7c3f` |
| Calibrated vector, little-endian float64 | `e5379b9f109e00763a711558c6ce13ea8da12ea17310045f8171de151c18a6ba` |

A fresh CLI process repeated guarded preparation loading, completed-bundle
loading and all persistence replay. Its replay ran
18:33:48.056166–18:33:55.000723 UTC and passed the same hashes with **zero
fitting calls**. Real probability calls in both processes were confined to
approved OOF/calibration inputs and persistence verification on training rows.

The loader requires caller-supplied checksum and training-manifest pins. It
checks complete inventory, confined paths/no symlinks, every hash, accepted
preparation/configuration/source provenance, code/packages, contracts,
memberships, prior normalization and OOF/calibration lineage before estimator
deserialization. It then checks saved learner names/order/classes/rounds and
calibrator settings/fitted parameters. Loading itself neither queries sources
nor rebuilds histories, fits components or writes production outputs.

Inference requires a separately supplied actual capture/source/feature-code
provenance contract bound to the exact input matrix. It checks algorithm,
policy, orientation, preprocessing/version semantics and code hashes in
addition to column names. Training-reference source hashes remain explicitly
named as such. New inference cannot claim the old training capture; it requires
outcome-free inputs and a later actual capture. This local API does not certify
prospective title/history/timing/common-capture eligibility; those remain the
responsibility of the separately authorized later orchestration. Only
synthetic inputs exercised its future-input branch here.

## Tests, environment and preservation

The final explicit safe suite passed **79 tests, zero failed/skipped**, in
**16.75 seconds** at 18:30:34.858933–18:30:52.164821 UTC:

```text
modeling/tests/test_phase5b4_current_challenger.py
features/tests/test_debut_prior.py
```

It covers exact partition/alias/date guards, training-only priors and
independent final populations, untouched inputs/NaNs, OOF uniqueness and
lineage, fixed parameters/rounds/no eval arguments, clipping/orientation and
non-convergence, synthetic save/load parity, complete staged loading,
tampering/repinned incompatibility refusals before deserialization, actual
inference provenance binding, no load-time fits/network/warehouse/production
writes, overwrite/incomplete rejection and preservation failures. The final
suite made **one synthetic XGBoost fit and one synthetic Platt fit**, separately
instrumented from the six real estimator fits. Test fixture receipts are
explicitly synthetic and were never published as challenger artifacts.

During test development, the first collection was blocked by the no-write guard
when pytest attempted a bytecode cache. Disabling test bytecode writing fixed
that. The second run exposed a new guard incorrectly rejecting legitimate
missing weight classes and two test-only patch issues. The guard was corrected
to retain accepted null semantics, and the test patches were corrected. The
next suites passed 67, 72 and finally 79 tests as loader coverage expanded.
All these iterations preceded real fitting. There were **no deviations from
the authorized real recipe or scope**. Compilation, scoped whitespace checks
and secret-pattern review passed. No broad discovery, mutating integration,
live warehouse test or unrelated repair was run.

Recorded package versions are Python 3.11.2, NumPy 2.4.3, pandas 3.0.1,
XGBoost 3.2.0, scikit-learn 1.8.0, joblib 1.5.3, psycopg2-binary 2.9.11 and
pytest 9.0.2. `code_versions.json` pins the exact new implementation/test bytes
and unchanged reused helper/schema files; accepted preparation code/package
pins are separately retained under `preparation/`.

Initial worktree was clean on `main`, HEAD
`5debe74fdf38f073b5ee76479c4027a8d9f1256c`, upstream `origin/main`, fetch/push
remote `https://github.com/wlodzimierrr/ufc-predictor.git`. All six required
documents/configurations were read completely. Ancestor/repository/scoped
instruction searches found no applicable `AGENTS.md`. Initial `git fetch
origin` confirmed zero divergence. Git transport was the only network use.

The baseline captured at **18:07:46.429707 UTC** covers **15,469 existing
files**, excluding Git internals, dependency/cache directories and `.env`.
Its hash is
`b6fb275e3a20ca2a0619e6c4ca0e7d2558e5f0b6b70e611d871421b6b9c683d7`.
Verification before fitting, before completed publication, after publication
and after fresh-process replay found **zero changed, zero missing and zero new
protected files**. Protected outcome/model/source bytes were only hashed for
preservation. Active guards blocked network/warehouse operations, credential
reads, frozen outcome parsing and repository writes outside the challenger
root; replay additionally blocked fitting/prior computation.

The March control root
`41f8c8afb8fd091fa0b04823ad9175cedcfdbf85ccf3f62d77c71e0d9219279f`
and all 29 components, and the frozen reference root
`12c17761fe76998e49fb5e79ea8a455861e22eaf57b37071f692bab17c1ba381`
and all 10 components, passed extra hash-only checks without deserialization.
All accepted preparations/holdouts, production models/pointer/predictions,
source storage and all **35 registry records** remain byte-identical.

Durable post-publication/test-development/review receipts are separately frozen
at
`data/experiments/phase5b4_current_challenger/20261002T183650Z_phase5b4_handoff_evidence_v1/`,
checksum root
`8d16273e52ea10a16bc415b8717e0526f0aa7d083eec745237cfe0b6f492c016`.
The challenger bundle itself was never appended to or rewritten after
completion. Relevant executed commands include:

```text
git fetch origin
python3 tools/train_phase5b4_current_challenger.py test --receipt /tmp/phase5b4-current-challenger/tests-fifth.json
python3 tools/train_phase5b4_current_challenger.py preserve --baseline /tmp/phase5b4-current-challenger/preservation-baseline.json --receipt /tmp/phase5b4-current-challenger/preservation-before-fitting.json
python3 tools/train_phase5b4_current_challenger.py train --run-name 20261002T183215Z_phase5b4_current_challenger_v1_fixed --authorization /tmp/phase5b4-current-challenger/authorization.txt --baseline /tmp/phase5b4-current-challenger/preservation-baseline.json --test-receipt /tmp/phase5b4-current-challenger/tests-fifth.json --receipt /tmp/phase5b4-current-challenger/publication-receipt.json
python3 tools/train_phase5b4_current_challenger.py verify --run data/experiments/phase5b4_current_challenger/20261002T183215Z_phase5b4_current_challenger_v1_fixed --checksums-sha256 59ffd4995946cc9ec7512f2f14caade09fd3144d2cfb3ee2d3b9b0951f5ca300 --manifest-sha256 7330bbebd06a6f5c0233cf8892be5c1728edc8805add02b1511499bf198b0c73 --receipt /tmp/phase5b4-current-challenger/independent-process-replay.json
python3 tools/train_phase5b4_current_challenger.py preserve --baseline /tmp/phase5b4-current-challenger/preservation-baseline.json --receipt /tmp/phase5b4-current-challenger/preservation-after-replay.json
python3 -m py_compile modeling/phase5b4_contract_v1.py modeling/phase5b4_bundle_v1.py modeling/phase5b4_trainer_v1.py modeling/phase5b4_safety.py tools/train_phase5b4_current_challenger.py modeling/tests/test_phase5b4_current_challenger.py
git diff --check
```

The report, intended implementation/tests and both frozen artifact/evidence runs
are handed off in logical commits, followed by a normal push to the verified
upstream under `next-phase-git-handoff.md`. Exact commit hashes, push outcome and
final worktree status belong in the final response, avoiding a self-referential
report-update commit loop.

The existing global `models/` ignore rule also matches nested experiment model
directories. A new `.gitignore` scoped to the Phase 5B.4 root includes this
phase's pinned learner/configuration JSON in Git. Existing ignore files and
artifact bytes are unchanged; no ignored file was force-added.

## Remaining blockers and authorized boundaries

The April draw/upcoming identity relationship remains **UNRESOLVED**, with no
new alias, preferred occurrence, cancellation or identity evidence. Source
binary results stop **August 29**, and aggregate statistics were last refreshed
**August 9**. The October 2 capture/cutoff does not attest complete results or
experience through October 2. Retrospective reconstruction, mutable
contemporary profiles, unverified historical availability, sparse/incomplete
histories, inherited stance semantics and stored/defaulted metadata/statistics
remain limitations.

Challenger component readiness is now **READY**, but prospective forecasting
remains **BLOCKED**. All 35 registrations still lack attested title status and
complete-history/explicit-debut evidence for both participants. Fourteen fail
the 24-hour rule, twenty exceed the 14-day lead and one is timing-eligible but
metadata-blocked under the unchanged capture. That old capture precedes the
new component freezes. A separately authorized later matched capture must
provide complete metadata, common observation cutoff and physical inputs for
the distinct challenger/reference/March recipes, after all component freezes
and within the preregistered window. It requires fresh source/role validation,
honest inference provenance and append-only registry/capture orchestration.

There was **no real upcoming forecasting, historical holdout scoring,
trial-outcome ingestion, source refresh, source network request, production
change or promotion**. Historical comparison remains **STILL_BLOCKED**.
Artifact readiness alone authorizes none of those later operations.
