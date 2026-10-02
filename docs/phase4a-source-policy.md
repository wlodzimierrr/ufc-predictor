# Phase 4A source policy, version 1

This policy is fixed before any forecast feature reconstruction. Selection uses
only the original forecast identities, known event dates and scoring instants.
Probabilities, winners, confidence, outcomes and performance are excluded.

Search all reachable Git history affecting exactly `data/events.csv`,
`data/fights.csv`, `data/fighters.csv` and `data/fight_stats.csv`, with a hard
256-snapshot bound. A snapshot is one commit tree containing all four files.
Its repository availability instant is the later of author and committer time.
Require the needed CSV schema, unique identifiers/statistic pairs and event
references. Report sparse statistics and missing profiles separately. Reject
incoherent trees; never mix their components with another tree. Among eligible
trees no later than the original scoring instant, select the maximum
`(availability UTC instant, full commit SHA)` in ascending lexicographic order.
Later revisions of older bouts remain ineligible. Git dates are repository
evidence, not independently trusted timestamps.

Preserve immutable blob references, exact SHA-256 hashes and a tested resolver.
Record row scrape ranges, archive observation date, last resolved event date,
orphan statistics, historical statistic gaps and archive age for each target.
None establishes complete archive or historical website coverage. Chronology,
version availability, feature compatibility and completeness are separate.

Bounded supplementation is restricted to missing target profiles and target
matchup flags. It cannot alter any archived result/statistic history. Search only
fetch-manifest entries naming the original target fighter/fight/event raw paths.
Require a successful HTTP 200 record at or before scoring, an exact SHA-256
match to the surviving body, and matching URL-derived identity. Choose the
latest `(fetched_at UTC, content_hash, storage_path, job_run_id)`; preserve the
selected body and exact record. A manifest for an overwritten body is a gap.
Event pages may establish target title status only if an explicit matchup row
and original IDs survive; absence of a title indication is not a false flag.
No new scrape, current profile, warehouse read or ad hoc source union is allowed.
If a supplemented profile has no history in the selected coherent snapshot,
record an unresolved experience gap rather than assert zero experience from
an incomplete archive. Frozen weight class/event date remain known forecast
metadata; target title status needs explicit as-of metadata.

Before constructing any real feature row, require all 108 original targets to
have resolved essential inputs. Otherwise freeze the complete target/source
evidence and comparison protocol in a clearly incomplete BLOCKED run; publish
no `features.csv` or reduced completed cohort. Individual missing measurements
and legitimate sparse statistics can remain missing under the accepted feature
contract. Unknown title status and an absent profile are not false/empty values.

History eligibility is strictly before
`min(original event_date, UTC(original scored_at).date())`. The existing
training snapshot modules compute age/activity/rolling/decay features at the
known original event date. Capped indexes and Elo prevent intervening results
from entering those calculations. Every schedule remains unknown; the three
debut columns remain deferred. Phase 3A code and the candidate remain unchanged.
