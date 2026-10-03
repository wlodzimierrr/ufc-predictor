# Phase 5C.8 local source qualification

**Offline assessment COMPLETE. The packet is not qualified for prospective
inputs. Prospective forecasting remains BLOCKED; historical comparison remains
STILL_BLOCKED; production is unchanged.**

The preserved packet supports literal dates, participant links, profile fields,
some explicit title wording and observed history rows. It supplies no qualified
complete UFC enumeration or verified debut. Its ordinary bout labels do not
satisfy v1's explicit non-title requirement. A newly identified Duncan
profile/detail-versus-warehouse discrepancy further prevents a truthful history
claim for the initial Allen–Duncan example.

The actionable deliverable is
[the v2 proposal](../phase5-source-evidence-contract-v2-proposal.md), labelled
**PROPOSED_NOT_APPROVED**, and
[the compact existing-byte index](phase5c8-local-source-index.json).
The operator must decide whether to qualify the named source's domain,
designation, enumeration, display-order and freshness semantics with supporting
authority records, or retain v1 and supply truthful explicit evidence. No policy
approval, source certification or runtime gate change occurred here.

## Accepted context, pins and boundaries

Read completely: the Phase 5C.7 inventory report, Phase 5C.6 capture report,
Phase 5C.3 source-health report, Phase 5C.1 evidence contract v1, prospective
shadow protocol v1 and Git handoff. Ancestor and hidden repository searches
found no applicable `AGENTS.md`. Phase 5C.3 remains the established acquisition
diagnostic baseline; its pipeline audit and diagnostic JSON were not regenerated
or rewritten. The accepted identity ledger, frozen supporting HTML copies,
evidence validator and relevant capture semantics were inspected for this task.

Starting worktree was clean on `main`, HEAD
`3934290f93e82d584d9972a1312c7da441938d5e`, upstream `origin/main`.
Fetch/push remote is `https://github.com/wlodzimierrr/ufc-predictor.git`;
Git-only remote-ref verification returned that same commit. Git publication
transport is the only network use in this phase.

Abbreviations below are exact repository directories:

- **C2**: `data/experiments/phase5c1_shadow_workflow/20261003T052316Z_phase5c2_real_intake_v1/`.
- **B3**: `data/experiments/phase5b3_role_aware_preparation/20261002_phase5b3_role_aware_v2_validated/`.
- **L**: `data/raw/ufcstats/`.

The six accepted inventories verify every member and exact membership:
B3 (88), fixed Phase 5B.4 challenger (49), accepted shadow contract (3),
C2 activation (9), C2 observation (8), and C2 BLOCKED run (15). Their checksum
root pins remain, respectively:

```text
7d36dc2bdb2a99eb81a44424fe603a6beab74d05435e41c8a1489cea11f0bd4f
59ffd4995946cc9ec7512f2f14caade09fd3144d2cfb3ee2d3b9b0951f5ca300
83cf581f3f90b7c0f85fcc8063d87330843bec657b03da7f5135ddde450d7514
21672682c5a7096fdc94ac8344a90755186c4cfadd92f9cb91ee2d5e4d98adfd
e2791af3b24ea6785e816d3c19e2ccf34020ac25acad84692874050bbcefa6a8
33d8e983476c408ee7a67f785c27856d58b3a0c9a0a3ea3fc2373a5bbe61d81c
```

C2's receipt pin remains
`c09d3adf1065edda3582eabd9df6126d842bfa19e84ef7e222bddc384c246a18`;
its cutoff remains `2026-10-03T05:23:34.369563+00:00`. All nine frozen
`code_versions.json` inventories were inspected; live pins pass for B3 (37),
Phase 5B.4 (8), embedded preparation (37), shadow contract (12) and activation
(5). All 13 embedded preparation files equal their accepted originals.
No model was loaded and no protected outcome/performance file was parsed;
preservation reads those files only as opaque bytes for hashing.

## Inspected inputs and compact reference conventions

All candidate CSVs were read offline. Their exact pins are:

| Input | Data records | SHA-256 |
|---|---:|---|
| `data/events.csv` | 816 | `269ae15eb2b90224f892b5dd0f330a9aa103887e8c57380c11a938abfc993679` |
| `data/fights.csv` | 9,118 | `616f7be79c7c61ddec2aaddc4aa7d36ec0a2478106e6d10697df078406012b8c` |
| `data/fighters.csv` | 4,513 | `be329865512e24806f1d2d67a76d5484f525a9d8ed72d0a1b060cf90d404062b` |
| `data/fight_stats.csv` | 17,386 | `e4eabd760f3713d23135fbb0ac8e09c407b8b456316bb2ae7f2bfd4699a03129` |
| `data/fight_stats_by_round.csv` | 41,514 | `fd50110bccafbb1825e89f36f8b98c3bd0be36635467174098a12cb460b49689` |
| `data/manifests/fetch_manifest.csv` | 108,259 | `40587f0b8b3c736fec99a7fc4459c9ddc24577a49399b8ce969d4d6b7c5b36e3` |
| `data/manifests/events_manifest.csv` | 797 | `397e6d2bec2fa5bda5c981e1be94056a03dd4d45d5ccc2e5cdb29eb4e2297975` |
| `data/manifests/fight_stats_queue.csv` | 8,885 | `b5da7fa06b417baae1e1f855f94729e5606b1ac3d38648da95dadd929db2a450` |

Semantic inspection covers **479 surviving legacy HTML bodies** (16,615,544
bytes) and **40 absent candidate paths**, selected by all registered targets,
their event/profile identities, every surviving local event-listing file,
the accepted identity evidence, the false-NC cases and linked occurrences for
the two initial examples. Six B3 `identity_evidence/...html` copies were also
read and compared. All six equal the corresponding legacy body; none rescues
an overwritten version within this scope. All 14,290 existing legacy raw files
were hashed for preservation, without a second full acquisition audit.

The index records each surviving path, exact SHA-256/length, all manifest version
hashes and their record references, exact source URLs and original clock ranges.
`versions` entries are `[hash, fetch_records, original_clock_range]`; a version
survives at the referenced legacy path only when its hash equals that body's
measured hash. It records **1,993 successful manifest references**, of which
**468 match** the inspected surviving bodies. Each reference here has a distinct
version hash within its path. Other references remain explicitly unavailable
at that path, without a claim that no copy exists anywhere outside this scope.
All 158 failed manifest records remain indexed, including original clocks,
statuses and challenge hashes where present; legacy failure bodies were not
preserved by that writer.

Eleven present bodies have no hash-matching fetch record: nine detail bodies
and listings `event_listing_20260519.html` and `event_listing_20260904.html`.
Their contents are inspectable, but their source observation identities/clocks
are unsupported. Candidate URLs from captured rows are not substitute fetch
receipts. The index keeps them visible. `git ls-files data/raw/ufcstats` is empty;
the checked local raw paths supply no tracked Git version recovery.

For exact replay of an indexed manifest reference, use its **1-based CSV data
record number, excluding the header**, against the pinned manifest. That row
contains the exact job, URL, `fetched_at`, status and storage path. JSON pointers
are zero-based; source spans are half-open UTF-8 byte intervals. Profile row
entries `[fight_id,start,end]` reference existing row bytes rather than copying
the HTML corpus. The index is 998,315 bytes, SHA-256
`13a56b169fc9558ea053bf591cdd6e0b64549dd1d902370bb092c9f0643920d3`.
It contains audit references and limitations, not an evidence package.

Audit selection is **the surviving exact bytes for statement inspection**.
No latest/version preference supplies forecasting evidence. Overwritten hashes,
failed records and frozen copies are preserved separately. Modification times,
current copy times and current audit clocks were not used as historical
provenance. Legacy `fetched_at` is a middleware observation clock, not a request
clock, cache-origin proof or provider freshness certificate.

## Registration scope and actual claim support

Targets come from C2's existing outcome-free
`coverage/blockers_by_source_bout_fighter.json` and `capture_receipt.json.considered`,
cross-checked with all 163 `register` entries in
`journal/registry/00000164/entries.json`. Its separate `observation_received`
entry at `/163` remains referenced. All **35 original registration files** are
individually pinned and mapped to their contemporary row, including expired and
timing-blocked rows. No original registration, manual alias or unresolved row
was dropped. There are zero unidentified considered rows in this frozen packet;
unsupported identities remain unsupported rather than being removed.

The original population remains 163 considered rows, 128 expired announcements,
35 on/after October 3, 13 timing-in-window rows, 12 accepted alias representations,
and zero forecasts. Audit attention to Allen–Duncan and Chovancek–You is not a
reduced forecasting/evaluation cohort. Original blockers remain in every index
target and continue to point to the frozen coverage metadata.

| Claim | Actual packet support across all targets | Qualification disposition |
|---|---|---|
| Announced event date | 162 event bodies state the registered date; one event body is absent (Jim Miller–Andrei Arlovski, `b802f05f-fffe-4ab6-a344-cec9afbbecb8`). | Direct literal date; native-to-repository binding and UTC boundary are computational semantics. Stale observation does not establish a contemporary announcement. |
| Participant orientation | 140 surviving detail headers match registered ordered profile identities; 23 details are absent. | Direct ordered href statements plus UUID derivation. Nine present detail bodies lack matching fetch provenance. Event cards match ordered membership for 96, reverse it for 21, lack the linked occurrence for 45, and are absent for one. All 21 reversals have the same unordered membership; display-order semantics require review, not silent relabelling. |
| Title | Three details explicitly say `UFC ... Title Bout`; 137 say ordinary weight-class `... Bout`; 23 are absent. | Three direct title-wording candidates; none is a native v1 whole structured claim. Every ordinary/missing label remains UNKNOWN under v1. V2 non-title derivation needs an independently qualified exhaustive designation rule. |
| Profiles | All 277 unique participants have captured profile rows; 265 have preserved UFCStats profile bodies with matching names. Twelve have no preserved UFCStats profile body. | Captured row presence is supported; optional missing fields remain missing. Ten absent-body rows use manual fighter URLs; Lucas Armand and Bruce Whitehead use external UFC athlete URLs without bodies in this packet. Do not certify stub completeness or provider availability from those rows. |
| Occurrences | Profile tables expose 2,203 linked rows. There are 152 target/participant comparisons with differences between pre-boundary profile-resolved link sets and frozen resolved source sets. | Observed sets only: mixed domains, stale rows and unresolved identities can explain differences. This count is not 152 proven missing UFC histories. The accepted admitted counts remain visible separately from raw source counts; no history is qualified complete. |
| Debut | Some captured admitted histories are empty, including Chovancek's. | No affirmative complete-domain empty enumeration or trusted explicit UFC-debut statement. Zero rows do not qualify a debut. |

Selectors used for statement inspection: event date in the labelled
`li.b-list__box-list-item`; detail header `.b-fight-details__person-link`;
detail event `h2 a[href*="event-details/"]`; designation
`.b-fight-details__fight-title`; card `tr[data-link]` and ordered fighter hrefs;
profile `.b-content__title-highlight`, `.b-content__title-record`, labelled
`.b-list__info-box_style_small-width li`, and `table.js-fight-table tr[data-link]`.
Whitespace is collapsed only in reported literals; preserved bytes are unchanged.
Audit profile dates use the seventh table cell's event/date text and an English
abbreviated month/day/year; unparsed dates remain unknown. These counts describe
profile-linked observations, without treating their event names as a domain
classification or their participant-relative flags as admitted bout results.
UUID bindings remove only `www.` before UUID5/NAMESPACE_URL, retaining HTTP/HTTPS
differences. No source claim is attributed to a computed UUID or hash.

The three positive-title candidates are Van–Pantoja
`534791de-cebe-53cc-8da8-00afdde28596` (September 19), Silva–Cong
`87499a34-2739-59f3-9b91-3106fb69ecdc` (October 3), and Van–Taira
`f7ea20a5-40fe-5def-a14b-e5c0dccd90af` (April 11). Exact detail spans are,
respectively, `[3466,3667)`, `[3441,3650)`, `[3461,3662)`; hashes/URLs/manifest
rows are in the index. All three are timing-blocked in the frozen observation.
All 13 timing-in-window rows have ordinary bout wording, so none gains explicit
non-title support through this inspection.

## Allen–Duncan and the history-domain counterexamples

Allen–Duncan is target `/66` in the frozen coverage/registration arrays,
original registration 14, fight `6fe1d59a-6ae9-5436-bc78-767da12a4707`,
event `e213540d-2ca7-54ee-96af-4159bd741028`, ordered participants
`5d26bddf-51d3-5417-aeab-7329263c2e27` and
`364ad7d6-2d46-5e3c-a5f9-5696d76b73a9`.

| Body under L | Exact SHA-256 | Hash-matching original clock |
|---|---|---|
| `events/e213540d-2ca7-54ee-96af-4159bd741028.html` | `66b2e2cec114cdb95b4cd2328939983547cebba59938f6a8b4fcc447e103b427` | `2026-09-12T16:38:41Z` |
| `fights/6fe1d59a-6ae9-5436-bc78-767da12a4707.html` | `4d321d27f06d122ccb908aa99ad0a6cc98dc926d49c77bc664ee5378fc261ee5` | `2026-09-12T16:39:03Z` |
| `fighters/5d26bddf-51d3-5417-aeab-7329263c2e27.html` | `3976206b69a035f498eec584f2770854548e88882b1c3f01beef6046f8afc9b6` | `2026-08-08T19:15:13Z` |
| `fighters/364ad7d6-2d46-5e3c-a5f9-5696d76b73a9.html` | `be1c5c3f2871dd1d00284fca42839fe6718298213cb0645266132408f0fabd35` | `2026-08-08T19:17:38Z` |

The exact detail source URL is
`http://www.ufcstats.com/fight-details/7db1a3dac7e343e7`; the header's ordered
profile URLs end `2f181c0467965b98` and `a93f94c923c3a9cb`. All exact event/profile
source URLs and previous hash references are bound in the index. Date statement
is in event bytes `[2566,2699)`; `Middleweight Bout` is in detail bytes
`[3466,3552)`. These support direct statements, not a native non-title boolean.
The event has another September 12 hash reference whose bytes no longer occupy
the path; both references remain visible rather than choosing the later clock.

Allen's profile lists 20 resolved rows versus 19 captured admitted occurrences.
The extra row, `[67078,70041)`, explicitly names **DWCS 3.4**, July 16, 2019,
detail `552b01ec3faebd65`, computational ID
`d6faa5f7-b0b0-54e9-b74a-7db43d8db99c`. Its local detail body is absent.
Whether/how it belongs outside the admitted UFC domain requires the proposed
event classification, not subtraction to manufacture completeness.

Duncan's profile lists ten resolved rows versus nine captured admitted
occurrences. Row `[9301,12290)` states a win on July 18, 2026 against Jared
Cannonier and links `4eff5a845db17572`, ID
`53b9cada-68ba-5c11-8da4-28833cd6b5fe`. The preserved detail supports ordered
Cannonier/Duncan statuses L/W; SHA-256
`3c5f19b3612765165abd8e6a9811e0cf335b14fee7a978447778b830c0d2b089`,
matching manifest data record **106083**, `2026-08-09T11:49:41Z`.
Its linked event URL ends `f354c50b8d63d9b3`. C2 `sources/fights.json` `/2923`
and `data/fights.csv` record **8834** instead retain the same fight identity as
upcoming under event `68a758a6-bd6a-5ec7-933e-72251614d52f`. This is a disclosed
source/history and event-binding discrepancy, not an admitted tenth occurrence.
An explicit event/occurrence relationship and separately authorized source
revision are required. Neither the result nor event ID was corrected here.

Chovancek–You is target `/6`, original registration 2, fight
`129b49aa-9be9-5739-a710-4e63b5254405`, dated October 17. Its title literal is
`Bantamweight Bout` at `[3482,3568)`, not explicit non-title evidence.
Chovancek's profile hash remains
`bc4ceeaf215cbe95c006db7b8189404efd27f1c17341d3bf5e15129956e49db4`,
matching `2026-08-08T19:51:22Z`. `Record: 9-0-0` lies in `[2426,2514)`.
The sole linked row `[9314,12272)` says **win, DWCS 9.6, September 16, 2025**,
detail `f8c24b531731a43a`; it is not an upcoming row. This refines Phase 5C.3's
description of that link without changing its conclusion: no qualified resolved
UFC occurrence/debut evidence is supplied. The corresponding detail body is
absent; zero captured admitted UFC rows remains unverified debut status.

You's six profile rows versus four captured admitted occurrences include two
additional wins labelled **Road to UFC 3.5 + 3.6** (August 23, 2024,
`c936e2a815bfeea3`, `[21426,24391)`) and **Road to UFC 3.3 + 3.4** (May 19,
2024, `a52ec7298ba5e2c5`, `[24409,27365)`). Both local detail bodies are absent.
These examples establish mixed provider domains; lifetime records and link
counts cannot stand in for the contract's UFC enumeration.

## Discrepancies, v1 acceptance and outstanding evidence

The known false-NC inputs remain disclosed and unchanged:

| Fight ID / participants | Frozen pointer / CSV version records | Surviving body / matching manifest observation |
|---|---|---|
| `3c34cdee-2aa5-5cef-b467-3f5ed89b0b0f`, Gastelum–Belgaroui | C2 fights `/2112` says `nc`; local records 338, 8936, 9012 say canceled with empty outcomes | `dd66715f0dfcb6d9ddfd4b18f3b4a2a46bc6d75332efb605eedf0ac087590c21`; manifest 107156, `2026-08-16T19:52:20Z` |
| `0ed0563e-a80c-5aa5-a2c4-8c0e818a7273`, Rodriguez–Silva | C2 fights `/491` says `nc`; local records 345, 8943, 9019 say canceled with empty outcomes | `d2fd866d2a048ea99605b555d7e2b453c4b4130b1b7e4ce86f967ae4d6ebef89`; manifest 107163, `2026-08-16T19:52:28Z` |

Both raw detail headers have empty outcome statuses. These bytes corroborate
Phase 5C.3's established false-NC discrepancy; no occurrence was excluded,
reclassified or reconciled to satisfy a history assertion. The accepted strict
loader repair does not rewrite these frozen rows.

April Castaneda–Vologdin still has draw `498de4bd-d781-52af-a383-802158196d2d`
and upcoming `4b08f65d-db68-5091-9568-4748c4cf7318`, different official URLs,
and no explicit transition. Both original bytes and B3 copies retain their
accepted pins. The twelve manual aliases and exact rematch retain the existing
ledger's bounded dispositions; matching names or pairs provide no new alias
approval. Forty-five absent card occurrences do not establish cancellation.

Under **v1 now**, existing table bytes, structural captured profiles, already
accepted identity evidence and registration metadata remain available in their
accepted roles. No new title, experience, debut or identity assertion qualifies
from this audit. HTML does not reproduce the whole required structured claim;
there is no externally trusted signed review/export, source-authority decision
or complete-domain evidence. Even positive title wording cannot make the whole
package ready. C2's evidence package remains empty and its blocker status intact.

Under **proposed v2**, date/identity bindings and the three positive title
predicates could be reproducibly derived after qualification and eligible
observation. The 137 ordinary labels could map to false only with affirmative
designation semantics. Profile occurrence rows could seed an enumeration only
after domain, completeness and revision coverage are proved. Existing old
observations and known disagreements still block current qualification after
policy approval; approval is insufficient by itself.

Specific missing records/capabilities are: provider authority and role/domain
attestation; affirmative designation taxonomy; complete single-page or paginated
history/export capability with terminal/page/count/revision receipts; genuine
contemporary non-cached roster/title/profile/enumeration observations through
the required boundary; original request/cache provenance for historical anchors;
explicit Duncan and April occurrence/event transitions; evidence-backed
dispositions for the two false-NC records; and authentic profiles/identity support
for the twelve absent-body participants. The index identifies all affected
targets and unsupported paths, rather than substituting a generic blocked label.

## Smallest next step and independent code prerequisites

**One next acquisition step, only after authority/semantics decisions:** obtain
one bounded, authentic Allen–Duncan source packet from the approved authority,
using the existing immutable capture mechanism if ordinary permitted source
reads are available, or an authenticated provider export if they are not.
It must supply current event/detail designation, both current profiles, complete
in-domain occurrence enumeration through the new cutoff with completion and
revision coverage, and explicit resolution evidence for Duncan's July 18
event/occurrence discrepancy. Historical bodies may be referenced by their
existing pins; unsupported historical provenance needs genuine supporting
records. This is an evidence-obligation pilot, not selection of the trial cohort;
all 163 rows and 35 originals stay visible and later intake must register its
entire declared scope.

Prerequisites are separately authorized bounded acquisition, named source/reviewer
authority, proved enumeration capability, approved title/domain/order rules,
and predeclared exact observation selection without latest/preferred-result
choice. Do not reuse C2's capture budget or cutoff. Stop on challenge/rate limit,
unproved provider completeness, unresolved domain/identity/result conflict,
missing page/profile, unverifiable cache/origin clock or timing failure; retain
all partial/failed versions and reasons. If the source cannot provide the required
capability, stop and require a separately qualified source for that role. No
unchanged warehouse recapture is a remedy for these missing facts.

Code decisions are independent. Phase 5C.4 strict disposition, Phase 5C.5
aggregate completion selection and Phase 5C.6/7 immutable capture/resolution
repairs are already accepted and preserved. Later publication must use strict
disposition and retain rejected/canceled rows; legacy transforms may still
reproduce frozen false NC and must not be a new admission route. Cross-load stale
upsert behavior and source/parser/warehouse revision handling need separately
scoped repair before corrected live publication. A future v2 evidence adapter,
domain/scoring compatibility review and a new contract freeze require separate
implementation authorization. None of these code changes establishes provider
authority or substitutes for missing source evidence; no further scaffolding
implementation is prescribed before the operator's evidence decision.

## Offline checks, preservation and Git handoff

A bounded 128-line scratch inspection helper used the existing scraper
environment's `parsel` selectors and standard-library CSV/JSON/hash functions.
It reads these named inputs and prints audit references to stdout; it imports
no scraper engine, warehouse, feature or model API and denies socket connection/
DNS calls. It is not a new general inventory tool or runtime evidence adapter.
Scratch inspection, checks and preservation receipts are outside Git at
`/tmp/phase5c8-local-qualification/`.

Relevant checks: independently repeat the bounded extraction, compare exact
index bytes; verify published input/body hashes and all manifest reference
paths/hashes/clocks; check all target/registration identities and original pins;
verify source selectors/spans and frozen copy bindings; verify accepted artifact
members/live code pins; and compare a pre-edit hash-only preservation baseline.
The independent reference validator passed **5,704 checks**, including all
2,203 profile-row identity/spans, all manifest version references, targets,
originals and title/date selectors. Repeated extraction matched the published
index byte-for-byte. These checks passed. No broad test/model suite is needed for the three new
documentation/index artifacts; no runtime code changed.

The baseline covers **7,955 tracked and 49,710 local files**, excluding Git,
dependencies/caches, bytecode and root `.env`. Final preservation found zero
changed/missing pre-existing files and only the three intended additions:
proposal, index and this report. All raw/CSV/manifest bytes, frozen contracts,
registries, models, production files, accepted repairs and prior reports remain
unchanged. The `data/raw/ufcstats_v2/` namespace remains absent. No new real or
synthetic runtime observation, queue, projection or feature artifact was created.

No source request, crawler, refresh, warehouse connection/write, queue
regeneration, migration, real feature construction, fitting, forecasting or
outcome/performance evaluation occurred. Historical source results were examined
only for occurrence/disposition consistency. Scoped whitespace, size, secret and
content reviews passed. Publication follows the verified Git handoff: logical
commits and a normal push to `origin/main`, without history rewriting.
Commit hashes, push result and final worktree status belong in the final response.
