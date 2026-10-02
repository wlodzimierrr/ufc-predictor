# Phase 5C.1 shadow intake and orchestration contract v1

Implementation-only contract: `phase5c1_prospective_shadow_v1`, source roles
`phase5c1_independent_source_roles_v1`, inference adapter
`phase5c1_capture_receipt_inference_v2`. Real forecasts require separate Phase
5C.2 authorization and valid inputs. Historical comparison is STILL_BLOCKED.
No acquisition, fitting, outcome ingestion or production operation occurs
implicitly. The existing protocol, eligibility policy, threshold policy and
three component bundles remain unchanged.

## Operator-supplied observation package

Supply a local directory and an independently trusted SHA-256 of its exact
`capture_receipt.json`. A hash provides integrity; the operator must verify the
receipt's observation authenticity with its source authority. A local copy time
is not a new source observation. No paid provider is selected. An authoritative
export, a supplied primary-source capture with a reviewed structured export,
or a separately authorized read-only warehouse capture can supply the package.
Access-check bodies must be retained as failed evidence, never used as proof.
No website retries or source repairs are part of this implementation.

The receipt has these required fields:

- `version`: `phase5c1_independent_source_roles_v1`; `capture_id`: unique UUID;
  `synthetic`: explicit boolean. Synthetic observation clocks are simulations.
- `mode`: `authoritative_export` or `readonly_repeatable_read`; named `provider`
  and an exact `source_scope` describing everything considered, including
  incomplete pages, unresolved rows and timing-blocked bouts.
- `started_at`, `completed_at`, `observation_cutoff`: timezone-aware actual
  observation instants. Completion equals the common cutoff. Start must follow
  **all component and orchestration freezes**. For real inputs completion must
  not be in the future. Old row refresh times may remain old; never alter source
  bytes or clocks to produce changed hashes.
- `tables`: exact entries for `events`, `fighters`, `fights`,
  `fight_stats_aggregate`, `schemas`. Each has relative `body`, `sha256`, `bytes`,
  `provider`, `url`, `requested_at`, `observed_at`. Table observations lie inside
  the capture interval. Bodies are exact preserved JSON exports, not repaired
  computational projections. Field schemas must match the four accepted public
  SQL schemas. Original unchanged schema bytes may also contain the unused,
  accepted `bout_features` schema; no labels from that table are read.
- `evidence_body`, `evidence_sha256`: exact evidence-package JSON location/hash.
- `considered`: every considered source row, each with unique `source_row_id`.
  Resolved rows have `fight_id`, `event_id`, ordered `fighter_1_id`/
  `fighter_2_id`, and `event_date`. Unidentified rows retain a provisional source
  identity, original URL and missing identity/date fields. Every captured future
  announcement must appear. Missing rows block completeness. Provisional rows
  remain in the journal and are blocked individually.
- For warehouse mode, `transaction` has verified `read_only: "on"`,
  `isolation: "repeatable read"`, timezone, snapshot, exact queries/schema and
  their receipts, `rolled_back: true`, `closed: true`. Exact table hashes and
  source-observation times remain necessary.

Raw UUIDs, types, nullability, joins, finite values, statistic constraints,
result/winner states and row refresh chronology are checked before indexing.
Future resolved results block intake. Same-observation-date results never enter
histories. Only independently observed earlier win/draw/NC occurrences enter
history; unresolved non-target announcements never enter any fighter, opponent,
statistic or Elo index. All raw fight/statistic rows retain hash-bound admission
lineage. There is no fitting projection or label-loading stage.

## Evidence package schema

`version` is `phase5c1_evidence_package_v1`. `sources` maps source IDs to exact
preserved bodies with `body`, `sha256`, named `provider`, `url`, `authoritative:
true`, HTTP/export `status: 200`, `access_blocked: false`, `requested_at` and
`observed_at`. Observation must follow request and precede the common cutoff.
Unsupported/blocked/missing bodies or discrepant hashes block intake.

`assertions` is an array. Every assertion has `kind`, `source_id`, matching
`observed_at`, a structured `claim`, and `extraction` with method
`json_pointer_v1`, exact JSON pointer and `claim_sha256`. The pointer must
reproduce the **whole claim**, including identity and explicit assertion,
from the preserved source. Duplicate/conflicting assertions block. A string
saying "Bout", a missing field, a warehouse default or a generated title label
cannot supply an assertion.

Title claims include all four ordered bout/event/participant IDs, `event_date`
and a literal boolean `is_title_fight`. Both true and false need explicit
source evidence. Title text inferred from schedule/finish information fails.

Experience claims include those same target identities/date, the specific
`fighter_id`, `domain: "admitted_resolved_ufc_occurrences_v1"`,
`covered_from` on/before `1993-11-12`, `covered_before_exclusive` equal to
min(target event date, observation UTC date), `complete: true`,
`status: verified_history` or `verified_debut`, and `prior_occurrences`.
Each prior occurrence carries `fight_id`, `event_id`, `event_date`, ordered
participant IDs and `result_type`. The list must match the admitted histories
for that fighter exactly, including draws/NC and independently observed
excluded-history identities. A debut assertion requires zero actual admitted
occurrences **and** affirmative complete-domain evidence. Missing rows or an
empty warehouse query alone are not debut evidence. This domain is UFC feature
history, not a claim of complete lifetime combat-sports experience.

For an unstructured original capture, supply an authoritative signed review
export containing the explicit structured claim. Add source `review` with
named `reviewer`, `authority`, `signed_assertion_sha256` equal to the full claim
hash, `original_body`, `original_sha256`, `original_provider`, `original_url`,
`original_observed_at`, `reviewed_at`, and nonempty `spans`. Each span has exact
UTF-8 byte `start`, `end`, `text`. Original observation <= review <= structured
export observation <= cutoff. Preserve both bodies. The operator must verify
the review authority/signature externally before trusting the package receipt
pin; the code validates assertion binding and extraction, not PKI. Blocked
original pages cannot support review evidence. Review cannot invent title
status or complete history from silence. One reviewed source export should
contain one claim so its signed assertion hash is unambiguous.

Profiles must exist as structurally valid captured rows. Individual height,
reach, DOB and other optional measurements can remain null. No source repair,
imputation or zero-experience invention is permitted.

## Contemporary identity validation

The original twelve mappings and exact 1997 rematch evidence are verified from
the frozen reconciliation bundle. Each relevant mapping is checked against the
new whole row and per-participant statistic hashes. Changed observations do
not inherit an old ledger disposition blindly. They require a fresh explicit
`identity` assertion, extracted from authoritative bytes like other evidence.

An identity claim includes `relationship` (`same_occurrence` or
`distinct_occurrences`), two `fight_ids`, `event_id`, `event_date`,
`participant_ids`, exact `row_sha256` and `statistic_sha256` maps. Same-occurrence
claims also require `canonical_id` and `explicit_transition: true`. Nonprovenance
row values and complete participant-statistic values must agree. Keep the
entire canonical source row; never blend. Reversed orientation, overlapping
mappings, differently missing statistics and conflicts block. New distinct
same-event occurrences require a separately versioned adapter contract;
only the exact evidenced rematch is currently supported.

The April draw/upcoming relationship stays unresolved unless explicit new
relationship evidence is supplied and validated. The earlier draw may enter
history under its original ID; the stale announcement contributes nothing to
history. Two resolved same-event participant occurrences block even with new
IDs or fitting exclusions. No preferred result or cancellation is inferred.

## Inference compatibility and recipes

The challenger old API rejects identical training table hashes. The separate
v2 adapter requires a distinct immutable observation receipt, verified actual
post-freeze clocks, common cutoff, contemporary source hashes, validated
projection/evidence hashes, exact matrix hash, and exact component pins. The
old training capture cannot satisfy that contract. Unchanged content from a
new genuine observation is allowed. No bytes or timestamps are manipulated.

The challenger uses pinned corrected `features.forecast_replay` functions:
history cap before min(target date, UTC observation date), target event date
retained for feature reference, date-frozen Elo, unknown schedules and deferred
debut columns. Apply only saved final preprocessing and saved calibrator.
The reference uses `phase5_legacy_reference_adapter_v1`, saved preprocessing,
learner and calibrator: snapshot date at min clocks, stored schedules, legacy
sequential Elo/defaults. Contemporary admitted rows retain source order.
March uses corrected functions plus unchanged March saved components and
`CandidateBundle.predict`, including its iteration-range and recipe guards.
Its training-reference API contract is passed as a compatibility certificate;
actual contemporary receipt/projection/matrix provenance is recorded separately.
It is not relabeled as the March training snapshot.

## Stages and publication

The entry point is `python3 tools/run_phase5c1_shadow.py`. Each command returns
machine JSON with `status: READY` or `BLOCKED`; blocked exits with code 2.

| Stage | Required arguments and behavior |
|---|---|
| `verify-components` | Verify pinned roots, all components and existing code without estimators |
| `load-components` | Load all three guarded bundles; fitting and prediction forbidden |
| `freeze-contract` | `--output` new exclusive isolated Phase5C root; actual code/contract freeze |
| `validate` | `--contract --contract-pin --package --receipt-pin`; source/evidence integrity |
| `register` / `revise` | Same contract/package pins plus `--journal`; metadata only, no predictions |
| `replace` | Registration arguments plus `--source-row-id` of replaced row; new row identity |
| `cancel` | Contract pins, `--journal --source-row-id --reason`; preserve all previous records |
| `prepare` | Validated package/contract arguments; optional exclusive `--output` projection bundle |
| `check` | Package/contract arguments, explicit `--forecast-at`; all considered readiness/reasons |
| `build` | Check arguments plus exclusive `--output`; separate exact matrices, saved reference preprocessing |
| `forecast` | Package/contract arguments plus `--journal --run-name`; synthetic only in this CLI; optional synthetic `--forecast-at --without-march` |
| `verify` / `replay` | Contract pins plus `--run --run-pin`; independently rebuild exact features; replay is synthetic only |
| `synthetic-demo` | Contract pins, new isolated `--output`, optional `--run-name --without-march`; synthetic full workflow and replay |

The root journal's immutable namespace forbids mixing synthetic and real
records. Its metadata successors copy all **35 original metadata record bytes**,
pin the parent root and previous journal entry, and bind the actual fitted
challenger/component versions in new records. They do not alter original
recipe declarations. Probabilities/outcomes remain forbidden in metadata.
Run bundles separately preserve exact inputs, processed matrices, sources,
evidence, bindings, outputs, decisions and observation/forecast receipts.

POSIX exclusive file locking covers registration, candidate reservation,
prediction and staged publication. Existing run names are refused. Failed
attempts and incomplete reservations remain visible. A primary pair is complete
only when both challenger and reference succeed before the inclusive 24-hour
boundary. Missing March is explicit and never changes the primary population.
The first eligible complete-input candidate is reserved before any probability
is observed. A failed/crashed first candidate blocks automatic later selection;
operator review must retain its failure and predeclare any supplemental
comparison. Older out-of-order captures and receipt replays are refused.
Revisions invalidate original primary membership; cancellation/replacement
retains every record and output. This phase provides no favorable retry or
supplemental scoring override.

Timing is inclusive [announced date 00:00 UTC minus 14 days, minus 24 hours] at
both observation and forecast completion. Registration precedes prediction.
Calibrated full-precision p determines `probability_band_v1`: 0.40–0.60
inclusive is scored NO PICK; <=0.30 or >=0.70 is high confidence; latent
fighter-1 selection is p>=0.5. Data-blocked is never NO PICK, and missing
counterparts are never losses. No outcomes or evaluation command exists.

The explicit future `authorized_readonly_capture` API requires separately
supplied capture authorization and source identity. It verifies read-only and
repeatable-read settings before source/schema reads, records exact SQL,
transaction snapshot/timezone, query clocks, hashes, rolls back and closes.
Phase5C.1 invokes only `mocked_readonly_capture` with marked mocked connections.
The CLI exposes no live capture or real forecasting activation switch.
