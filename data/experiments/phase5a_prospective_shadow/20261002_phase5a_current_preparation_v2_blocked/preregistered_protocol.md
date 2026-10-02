# Prospective shadow protocol v1

Preregistered in Phase 5A before any new modeling. The machine contract is
`configs/phase5_prospective_shadow_v1.json`. The new trial population consists
of announced future bouts considered after registration. It does not replace
the historical 108 or 146 populations. Historical comparison remains
**STILL_BLOCKED**. No promotion is authorized.

The unchanged March research control is the complete Phase 3B bundle trained
through March 7, 2026. The current challenger will be a new Phase 5B artifact,
using this capture's separately pinned current-candidate contract. Phase 3B's
cutoff, manifest and row-count guards remain unchanged. The reference uses the
existing production learner with independently frozen legacy preprocessing
and Platt calibration. It is a **production-derived prospective reference**,
never a recovered historical production probability stream.

## Preparation and Phase 5B contract

Capture `public.events`, `public.fighters`, `public.fights` and
`public.fight_stats_aggregate` in one read-only repeatable-read transaction.
Verify read-only before queries; record schemas, queries, database timezone,
transaction and capture clocks; roll back and close. Exact captured values are
known at capture time. Their availability before each historical bout is
unverified. This is retrospective training reconstruction, not certified replay.
Source freshness and gaps must accompany every readiness claim.

Set the exclusive training cutoff to the UTC date on which coherent capture
completes. Exclude that whole day and later dates. Use corrected algorithm
`v2_date_frozen_elo_schedule_unknown_v1`, ordered 50 features, original
orientation, date-frozen Elo and strictly earlier fighter/opponent histories.
All schedules remain unknown. Prior actual elapsed duration may enter earlier
history rate calculations; finish rounds and title status never imply schedule.
Three debut columns remain deferred. Sparse statistics and legitimate NaNs
remain. Null physical fields remain missing; absent profiles are structural
defects, not empty profiles. Stored contemporary flags and defaulted statistics
retain source-provenance limitations, not historical availability certification.

All 166 original, alternate and precautionary identities are excluded before
returning any fitting labels, and from every fitting, normalization, selection
and calibration operation. Independently captured earlier results for excluded
identities may enter later chronological histories. That history permission
does not permit their labels as fitting targets. Draws and NC may enter earlier
history but are not binary targets. Invalid labels, identities, joins, dates or
numeric values block readiness; they are not silently corrected or dropped.
Every source bout receives explicit eligibility reasons.

Phase 5B must verify the new run's checksums, pinned training manifest, exact
schema/features, memberships, source and code/package versions. Use fixed
depth 6, child weight 100, lambda 1.0 and 310 rounds; base settings are in the
configuration. No further search. Four expanding OOF windows use calendar
boundaries at cutoff minus 12, 9, 6, 3 months and cutoff. Clamp the day to month
end if necessary. Freeze exact memberships now. Each fold computes its own
training-only preprocessing and uses 310 rounds without tuning or early
stopping. Generate exactly one raw OOF probability per eligible window row.
Fit one persisted Platt estimator on clipped log odds using epsilon 1e-8,
C=1e10, lbfgs, max_iter=1000 and both classes. Metrics calibrated on these same
rows are **calibration-fit diagnostics**. Independently compute final priors
and refit the learner on every eligible current row. Persist all components,
provenance and hashes. Phase 5A does none of these fits or predictions.

Reference inputs are physically separate. The production metadata records
preprocessing rows before `val_date=2022-03-12` and calibration rows in
`[2022-03-12,2024-03-09)`. Apply exclusions and date restrictions in SQL before
fetching their labels. Preserve exact stored feature values, computation and
version metadata, identities, labels and source joins. Current stored features
are permitted solely for reproducing this legacy recipe. They may retain
finish-round schedules, within-date Elo, rounded numerics, defaults and other
original pipeline defects. Do not repair them or claim pipeline equivalence.
Counts can differ from the production artifact's original counts because this
is a current bootstrap capture; record differences and missing provenance.

`modeling/score_upcoming.py` currently computes debut priors and fits Platt at
scoring time; `calibrate_platt` discards the estimator. Phase 5B must freeze and
persist the reference's preprocessing and calibrator before any prospective
forecast or trial outcome access. Keep the base learner unchanged. A missing
component blocks reference readiness; no repeated calibration or raw-probability
fallback is permitted. Pin the legacy future-feature recipe separately from
the corrected candidate pipeline and use matched contemporary source captures.

## Registry, observations and revisions

Register every considered bout before predictions, irrespective of confidence,
probability or future outcome. The initial consideration scope is every
captured warehouse bout whose announced date is on/after capture completion's
UTC date, including timing-blocked bouts. Later captures register all bouts in
their declared source scope, including incomplete pages and unresolved IDs.
Unidentified page rows receive provisional source-row identities and remain
blocked until an append-only identity resolution record is supplied. Registry
coverage describes the declared sources; it does not claim all worldwide bouts.

Preserve stable event/fight/fighter IDs, orientation, announced date, source URL,
source capture times and hashes, recipe/component versions, eligibility reasons,
forecast time and roster/date revisions. Records are immutable, exclusively
created and linked by hashes. No in-place update is permitted. Use
`tools/append_prospective_registry.py` to publish a new exclusive successor run
from a checksum-verified parent, copying parent record bytes unchanged and
appending validated metadata records. Never append files inside a frozen run.
Each successor pins the parent checksum and new journal tail. Source bodies
are exact response bytes with request/response times, status, URL and SHA-256.
Use bounded ordinary official-page reads; stop on access blocking/rate limiting.

The primary forecast uses the **first complete paired capture** in the inclusive
window from announced event date's conservative 00:00 UTC boundary minus 14
days through minus 24 hours. Registration must precede forecasting. Observation
cutoff must be identical across challenger and reference; all inputs must be
captured by that instant, and forecasts must follow it while still meeting the
lead-time rule. The March control uses that capture too. Distinct feature
pipelines and information differences remain documented. Captures with different
observation cutoffs are separate descriptive comparisons, never primary pairs.
Old saved production predictions cannot provide a matched counterpart.

Title status requires an explicit sourced true/false assertion; a warehouse
default false is unknown for future eligibility. Entire profiles must exist;
individual optional fields can remain NaN. Experience requires sourced complete
history or explicit debut evidence; absent histories do not imply zero
experience. Metadata gaps produce `data_blocked_not_scored`, never NO PICK.
Scored decisions later distinguish inclusive 0.40–0.60 `scored_no_pick`, outer
`scored_actionable`, and `missing_counterpart_prediction`. Phase 5A accepts no
real probabilities. Threshold high confidence is inclusive p<=0.30 or p>=0.70.

Date, orientation or opponent revisions append new records and preserve all
original inputs and forecasts. Forecasts for a changed roster/date retain their
original lineage and are invalidated for the primary population by the logged
revision, regardless of outcomes. A replacement is a newly registered bout.
Cancellations remain in coverage denominators. Later captures do not select a
more favorable primary forecast. Only predeclared supplemental descriptive
forecasts may be appended after the first complete pair.

## Frozen evaluation definitions

Primary: calibrated current-challenger versus frozen-reference probabilities
on complete paired prospective forecasts with resolved binary outcomes and
valid unrevised matchups. March control is a predeclared secondary comparison,
with its own explicit complete-pair denominator. Include NO PICK in log loss,
Brier and latent accuracy. Log loss clips p to [1e-8,1-1e-8]; Brier uses actual
unrounded p. Latent class is p>=0.5. Never invert or tune probabilities.

Report each model's actionable and high-confidence total/resolved/correct
counts, accuracy (correct/resolved for that subset), coverage (subset/all scored
forecasts) and paired-population coverage. Baseline-selected comparisons use
the reference's actionable subset; jointly actionable comparisons require both
models actionable. Retain NO PICK latent/proper-score behavior in all-fight
metrics. Empty denominators give null, not zero accuracy. Single-class events
are retained.

Ten calibration bins follow [0,0.1], (0.1,0.2], ..., (0.9,1]; record counts,
mean p, mean y and absolute gaps, ECE weighted by row count and MCE over nonempty
bins; empty bins have null values. Use event-cluster paired bootstrap with
10,000 replicates and NumPy Generator PCG64 seed 42, lexically sorted event
IDs. For E events sample E events with replacement, retaining every paired row
in each selected event including multiplicities. Calculate bout-weighted
metrics and challenger-minus-reference differences. Percentile 95% intervals
use quantile method linear at 0.025/0.975. Report marginal intervals too; these
are descriptive uncertainty, not automatic promotion tests.

Coverage flow starts with **all unique considered bouts** and reports separate
counts and reasons for data-blocked/not scored, complete captures awaiting
frozen models, timing blocks, scored NO PICK/actionable, missing counterparts,
unresolved results, cancellations, revisions/replacements, draws and NC.
The registry, scored rows, complete pairs, resolved pairs and resolved binary
pairs each have explicit denominators. Overlapping reason counts are labeled;
do not silently turn missingness into losses or omit blocked bouts. Proper
scores use resolved paired binary bouts; unresolved/draw/NC/cancelled/revised
forecasts remain in registry coverage and result disposition counts.

First formal checkpoint: the first completed event outcome-ingestion batch
with at least **200 resolved paired binary bouts over at least 20 events**.
This is a collection target, not a power guarantee or promotion threshold.
Before it, descriptive monitoring is allowed; no favorable stopping or
trial-based challenger modification. There is no outcome-based filtering or
threshold tuning. Any later revised experiment requires separate preregistration.

All real forecast files, exact inputs, component/model hashes and run receipts
must be frozen before outcome loading. Outcome ingestion is a later separately
scoped command/session. Phase 5A has no outcome loader or scoring entrypoint.
Neither results nor this trial authorize automatic production promotion.
