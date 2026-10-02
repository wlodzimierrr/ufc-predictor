# Phase 4A.2 source policy, version 2

Fixed after the bounded archive inspection and before any real feature
construction. Version 1 and its frozen run remain unchanged. This policy
authorizes recovery evidence; it does not authorize scoring or evaluation.

Search only the four `data/{events,fights,fighters,fight_stats}.csv` paths,
their four `scraper/data/` counterparts, `data/manifests/events_manifest.csv`,
`models/upcoming/upcoming_features.csv`, and the thirteen original event-date
`models/predictions/<event_date>/predictions.csv` paths. Inspect at most 256
relevant reachable Git trees. Record every path/tree, including absent files,
immutable blob identities, exact byte hashes, and both repository timestamps.
Inspect producer code at these trees separately and record those references.
Do not search other data, reflogs, unreachable objects, websites or warehouses.

Availability is `max(author UTC, committer UTC)`. A whole file version must
precede or equal original `scored_at`; row scrape dates cannot backdate an
ineligible file. Git/capture timestamps are qualified observational evidence,
not independently trusted timestamps. The October refresh and August 19 tree
are ineligible for every original forecast.

Within an eligible file, collapse exactly identical records. For differing
records of one identity (statistic identity is fight/fighter pair), require
valid `scraped_at` observations no later than the tree's availability. Scraper
parsers emit UTC observation times and incremental exports append revisions;
manifest producer updates `last_seen_at` when rewriting an event record.
Use the latest observation, regardless of outcomes or statistics. A tied
observation with differing records is unresolved, including an older tied
revision. Never select by row order, content hash, winner or prediction.
Retain all exact raw records, line identities, selected/rejected decisions and
reasons. Immutable Git blob references preserve the full original CSV bytes.
Blank/header-only/malformed components remain unusable.

Primary history/statistics still come from one root four-component tree.
Among eligible coherent root trees, try the newest first; conflicting coherent
trees tied at the newest availability are unresolved unless all four exact
component bytes agree. Incoherent trees and their rejection reasons stay in the
catalogue; the fixed fallback is the latest eligible coherent tree.
Do not mix fight or statistic histories across trees. Reconcile that tree
before coherence validation. If a fight references an absent event/profile,
supplement only that reference by the following fixed component rule, capped
at the primary tree availability (also no later than scoring):

* Events: search root event versions first, then scraper event versions, then
  event-manifest versions. Within the first class containing the identity,
  use its latest eligible, unambiguous reconciled record. Tied version times
  with differing records remain unresolved. A manifest maps only explicit
  event ID/name/date/URL/status and observation time; it supplies no title flag.
* Profiles: search root profile versions first, then scraper profile versions,
  with the same eligibility/ordering/tie rule. Empty fighter stubs cannot
  resolve a missing profile. No invented measurements are permitted.

Record every supplementary component/row and rejected component version.
If any reference or conflict remains unresolved, reject the entire primary
tree. Never drop a historical fight to make a tree coherent: Elo updates and
opponent histories can propagate beyond a target fighter's direct history.
Orphan statistics keep version 1's explicit unused/reporting treatment.

Target metadata uses the selected eligible tree, exact original IDs/pair/date,
and the original known weight class. Explicit recognized `Title Bout` text
may establish true title status. Generic `Weight Class Bout` CSV text has no
row-level producer attestation and remains unresolved without an eligible
hash-matched raw body. The scraper copies page titles, but cached-upcoming and
manual catch-up tools synthesize generic text; presence of parser code or
spacing between scrape times alone cannot certify non-title status. A false
value in an old feature file may likewise be a producer default, not verified
metadata. Record these distinctions explicitly.

Reuse only the already frozen, eligible version-1 raw supplements and their
capture lineage. Do not repeat the raw-body search. For any new CSV target
flag, record the original row and selected source identity. Preserve the
existing raw verified flag when CSV producer attribution is unresolved.
Original prediction files supply only allowlisted identity/time metadata;
never parse probability, confidence, correctness or outcome columns. Inspect
feature files for independently evidenced metadata only, never reuse vectors.
An absent physical profile stays missing; a recovered profile without eligible
history stays unknown experience, not a debut inferred from sparse coverage.
Inspect the preserved profile bodies' explicit fight/event/date links as
incomplete experience evidence. They cannot establish complete histories or
supply missing result/statistic revisions under this component policy.

All 108 original targets must have essential inputs resolved before any real
feature construction. Otherwise publish only `STILL_BLOCKED` recovery evidence,
with no `features.csv` or reduced completed cohort. Preserve the original
comparison protocol bytes. No completed-vector loader compatibility is claimed.
The training contract is only a compatibility reference, never scoring-source
lineage. Any eventual reconstruction must use unchanged
`v2_date_frozen_elo_schedule_unknown_v1`, original orientation/event reference
date, history/Elo/opponent cap before `min(event_date, UTC(scored_at).date())`,
entire scoring/target-day and target-result exclusion, unknown schedules and
deferred debut columns. A completed policy-v2 run would require its own checked
loader adapter and full reconstruction parity; version 1's adapter cannot
certify this incomplete evidence package.
