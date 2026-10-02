**Phase 4A.2 source recovery and reconciliation — 2026-10-02**

**Status: STILL_BLOCKED. The single bounded investigation is complete.** Two
of the original 98 affected forecasts now have their essential inputs resolved;
96 remain affected. All 108 original identities, orientations, event dates,
row order and scoring instants are preserved. No real feature vectors,
`features.csv`, reduced completed cohort, candidate probabilities or outcome
metrics were produced. Phase 4B remains unavailable under this historical
protocol.

Work was performed in `/home/wlodzimierrr/ufc-data`. No applicable ancestor or
repository `AGENTS.md` was found. The initial worktree was clean, on `main` at
`9ec2672`, with 11 existing local commits ahead of `origin/main`. Those commits
include the previous phase's subsequently committed work. Historical reports'
statements that their original sessions made no commit or push remain unchanged.

The requested Phase 4A readiness report, policy v1, Phase 3B build report, Git
handoff contract, scoring-input preparation, forecast replay, source loader,
scraper parsers/exporters, event-manifest producer and upcoming-card recovery /
loading / feature / scoring producers were reviewed before reconstruction
decisions. No real reconstruction was entered because the all-108 gate remains
closed.

The new incomplete run is:

`data/experiments/phase4a2_pre_event_2026_scoring_inputs/20261002_phase4a2_archived_asof_v2_incomplete/`

Its `checksums.json` SHA-256 is
`8d9440dd8f95b0322292f579e0f58c39dcafe39eeeb761dcfff7091e91db4c40`.
It locks 31 component files, including the same 17 previously verified raw
bodies, with exclusive publication and read-only files. Computation/publication
times are recorded in `run_receipt.json`; they never establish historical
source availability.

The search was restricted to these 23 data paths: the four root
`data/{events,fights,fighters,fight_stats}.csv` files, the four corresponding
`scraper/data/` files, `data/manifests/events_manifest.csv`,
`models/upcoming/upcoming_features.csv`, and original event-date prediction
files for April 4/11/18/25, May 2/9/16/30, June 6/14, July 11 and August 15/22
under `models/predictions/<event_date>/predictions.csv`.

Eight relevant reachable Git trees were inspected, below the hard 256-tree
bound. `source-manifest.json` enumerates **every one of the 184 data path/tree
checks**, including 69 present and 115 absent paths, exact blob IDs, full
SHA-256 hashes, schemas, raw/selected row counts, rejected components and raw
revision decisions. It also records 104 exact producer path/tree checks across
the same eight trees. Source discovery and deterministic rechecks used the same
fixed scope; there was no additional forensic expansion, unreachable-object or
reflog search. Producer-history log inspection was metadata only, not another
data-tree search. `commands.json` preserves the scope and command arguments.

Availability is the later author/committer UTC instant. The two instants agree
for these trees. These are qualified repository observations, **not independently
trusted timestamps**. Row scrape dates and manifest discovery dates never make
a later file version historically eligible.

| Exact tree | Availability UTC | Root classification under v2 | Assigned forecasts |
|---|---|---|---:|
| `0a13162ea60e0a2ede49d8d8d10b30a81683b717` | 2026-03-12 16:48:33 | Coherent, superseded | 0 |
| `1f477d3ddc87b123d0099025b669e728e7881a34` | 2026-03-18 12:43:50 | Coherent, superseded by identical root bytes in next tree | 0 |
| `c0d91b3a794494e3eddff8a38414903ed66a25e8` | 2026-03-18 12:44:03 | Coherent | 65 |
| `8bb0552fc2fa4f91e1e769c41dc999ac61b2f14b` | 2026-04-17 10:40:11 | Rejected: eight missing participant profiles remain | 0 |
| `e23dd7cf3572c41776197c1990a09f17c9d96cc9` | 2026-05-15 14:25:46 | Coherent after observation-ordered revisions | 30 |
| `4af6c2f63ebcf63e13fbdbbdcf3bce59243334e4` | 2026-08-09 12:36:40 | Coherent after observation-ordered revisions | 13 |
| `6e5c0cd6afcb360c58036eaa1c489e71f1d4bc81` | 2026-08-19 19:27:06 | Reconciles structurally; after every scored_at | 0 |
| `50f02ab5e85b1e2b9726e5ec722ea56bfcc9b83a` | 2026-10-02 09:17:36 | Post-scoring; also retains one missing participant profile | 0 |

The August 9 tree is later than the August 9 forecasts' original
`11:17:50.025384 UTC` scoring instant. Consequently those eleven forecasts use
May's eligible tree; only the thirteen August 16 forecasts use August 9's tree.
October's refreshed rows are not historical substitutes, regardless of their
event or scrape dates. The March 18 scraper-refresh tree changes the root
availability reference by 13 seconds but preserves all four selected root blobs
from policy v1 byte-for-byte.

**Lead results and reconciliation evidence.** The scraper-side event path has
three distinct blobs (3/3/15 rows). Twelve of April's missing event identities
appear in its 15-row version, which helps diagnose the rejected root archive.
The fixed reference priority actually selects fourteen earlier eligible root
event records and one eligible April event-manifest record to fill all fifteen
missing event identities (186 fight references). These are explicitly recorded
supplements, not a union of historical fights. Eight participant profiles still
cannot be supplied at April's availability, so the whole tree remains rejected.

The scraper-side fights path has three distinct blobs, each with 39 records,
no original target matchup and no history for the five specifically affected
fighters. Its April-and-later version is malformed and rejected. The scraper
fighters file is empty in all eight trees: it supplies no Gable Steveson or
supplemented-fighter profile. The two scraper statistic blobs have 78 and 440
rows. They supply no evidence that resolves these essential gaps and are not
mixed into the primary statistics. Exact blobs and rejection details survive
in the catalogue.

Five distinct event-manifest versions (777/781/791/791/797 rows) were inspected.
The April version supplies one otherwise unavailable event reference under the
fixed priority rule. Manifest `last_seen_at` is the changing observation field;
`discovered_at` is preserved by the producer and cannot attest later row content.
Manifests contain no independent target title metadata or physical profiles.

No Git tree contains any of the thirteen original prediction files or the
upcoming-feature file. The thirteen surviving local prediction files were
inspected for schema and allowlisted identity/time metadata only. They do not
contain title status. Probability, confidence, correctness and outcome values
were not extracted or used. The surviving upcoming-feature file has 35 rows
for October 3–November 7, computed September 30, and zero original target
matches. It supplies no usable historical metadata. Old vectors were not
reused; modification time was never an availability criterion.

The new fixed policy is `docs/phase4a-source-policy-v2.md`, SHA-256
`251433abdb2cef1a85a3ec52630bdb80df18704fb3f04f1b7514a0bb7325ae4f`.
Policy v1 and its frozen run were not edited. Producer code establishes that
scrape times are emitted at observation and incremental CSV exports append
revisions. This supports selecting the latest recorded observation **within an
already eligible file**. Exact duplicates collapse; conflicting tied
observations, malformed clocks and observations later than file availability
remain unresolved. No hash or row-order preference resolves conflicting content.

May has eight duplicate event identities and 71 duplicate fight identities;
August 9 has twelve duplicate fight identities. Their differing revisions have
distinct eligible observations, so these trees become coherent under v2.
Selected unique counts are respectively 781 events / 8,755 fights / 4,464
profiles / 17,182 statistics and 790 / 8,879 / 4,492 / 17,386. Raw rows,
source hashes, line identities, selected/rejected revisions and reasons are
preserved in the manifest. No real archived tie was resolved by content ordering;
synthetic conflicting ties are rejected by tests.

Primary fights/statistics always come from one root tree. Only absent
event/profile references may use the documented per-component priority and
as-of cap, with complete component/row provenance. The April repair uses that
rule for diagnostic reconciliation; no target selects April. No historical
fight identity was dropped to achieve coherence. An unresolved tree is rejected
in full because its Elo updates and opponent histories can affect more than
the immediate target fighters. Sparse statistics remain diagnostics: the
selected March/May/August archives have 19/97/129 resolved fights missing at
least one participant's statistics and 40/0/4 unused orphan statistics. Their
last resolved dates are March 7, May 9 and August 8. Archive completeness and
independent historical website availability remain uncertified.

**Title provenance and remaining gaps.** The existing scraper copies page bout
titles, but `recover_cached_upcoming_fights.py` and manual catch-up code can
synthesize a generic weight-class `Bout` string. Upcoming loading and old
feature construction can also turn absent indications into false defaults.
Presence of the parser, an apparently plausible `Bout` string, varying scrape
times or an old false feature value alone does not provide row-level attestation.
These categories were not silently certified as verified metadata.

Two selected eligible May records explicitly say `UFC Interim Heavyweight Title
Bout` and `UFC Lightweight Title Bout`. Their original June 14 target identities
are `6161d3e7-9392-515a-b766-d93e637bc6c9` and
`8e007133-02c8-53dc-a08f-b74218162735`; they resolve true title status without
using outcomes. Seven other eligible June target records have generic `Bout`
text with unresolved producer attribution. Eighty-six forecasts have no selected
target record. Together those leave 93 unknown title flags. The thirteen prior
hash-matched raw matchup flags retain their existing verified provenance.

| Essential gap | Original count | Resolved | Remaining |
|---|---:|---:|---:|
| Unknown target title status | 95 forecasts | 2 | 93 forecasts |
| Missing Gable Steveson profile | 1 forecast | 0 | 1 forecast, also missing title |
| Unknown supplemented-fighter experience | 4 fighters / 3 forecasts | 0 | 4 fighters / 3 forecasts |
| Distinct affected forecasts | 98 | 2 | 96 |

Gable Steveson is `6c80aabe-5671-5caf-a5e4-bd0f0dbf1448`, target
`949f5ffc-911a-5802-8474-79be9265aa60`, scored July 1 for July 11. The newly
inspected scraper profiles are empty; the first root profile is observed
August 8 and Git-evidenced August 9, after that scoring instant. The v1 body
overwrite gap is not repaired by its later row.

The experience gaps still concern Ryan Kuse
(`816d5d3f-ef76-591b-9979-63735a26b250`), Stan Dorsainvil
(`7a343523-cc85-5054-b924-3c49120e4861`), Terrance Chatman
(`8110cdb9-d1b1-5cb3-9f1c-8cf601304496`) and Anthony Wint
(`41d605df-9392-5afd-8366-a8187b29444c`). Targets are
`3a247061-eef3-5698-aeb4-82d62b5164c1`,
`cc88d51e-6818-5219-89a8-adcca1eccf4d` and
`e1a1f140-7576-5843-955e-f3e4e044c8f2`, scored August 16 for August 22.
Their physical profiles remain independently evidenced by the same frozen
August 16 captures, but the selected August 9 fight archive has no eligible
history for them. August 19's profile/fight refresh remains ineligible.

Inspection of the preserved Wint profile adds a prior fight identity
`de2aa100-904d-5adb-ac7a-473880cd6060`, event
`8f2f37f1-919c-5a8b-a22c-fb39b3177ce9`, explicitly dated August 11. That
reference predates scoring and reinforces why zero experience is unsafe. It
does not supply the missing reconciled historical result/statistic record under
this component policy. The other three profiles' lack of a prior link likewise
does not certify debut. `raw-profile-history-references.json` preserves identity
and date evidence without extracting results or certifying complete experience.

The complete ordered assignments and every remaining gap identity are in
`source-selection.json`; the target rows remain in `targets.csv`.

| Original event date | Original forecasts | Unknown title | Forecasts still affected |
|---|---:|---:|---:|
| 2026-04-04 | 11 | 11 | 11 |
| 2026-04-11 | 10 | 10 | 10 |
| 2026-04-18 | 7 | 7 | 7 |
| 2026-04-25 | 8 | 8 | 8 |
| 2026-05-02 | 7 | 7 | 7 |
| 2026-05-09 | 9 | 9 | 9 |
| 2026-05-16 | 7 | 7 | 7 |
| 2026-05-30 | 6 | 6 | 6 |
| 2026-06-06 | 3 | 3 | 3 |
| 2026-06-14 | 6 | 4 | 4 |
| 2026-07-11 | 10 | 10 | 10 |
| 2026-08-15 | 11 | 11 | 11 |
| 2026-08-22 | 13 | 0 | 3 |
| Total | 108 | 93 | 96 |

Twelve forecasts now have essential inputs resolved under the qualified policy.
They were not reconstructed or published as a smaller completed cohort. There
are zero newly affected forecasts. Counts of individual gaps overlap as shown
above; they are not alternative populations.

**Publication and compatibility.** Added code is
`modeling/source_reconciliation.py`, the evidence-only CLI
`tools/prepare_phase4a2_source_recovery.py`, and sixteen focused checks in
`modeling/tests/test_source_reconciliation.py`. Existing scoring preparation,
replay, training preparation, loader and candidate files are unchanged.

| Published component | SHA-256 |
|---|---|
| targets.csv | `386edd85e248be571bf97796db0b1ea7dd0d6aaca01587140dfd003ba60c152e` |
| comparison-protocol.json | `6e6c127973c9ccc15fa4bfa933d8b65b6438db1ea7a16da725171a4ebdaa0f6b` |
| source-manifest.json | `6feab86a6c60892892b52664b1af4eb3a7afe108f92bb002e35e8487895a9dd9` |
| source-selection.json | `18e36a59610cdee0af11cb3d4fd60aae6228bc31c06a29ad530e445baf1f9b9f` |
| raw-profile-history-references.json | `800ad6f96dbb41907ec3be18e8ecfd71aff646bed40c7b0fe5dd0f1a14439178` |
| local-metadata-evidence.json | `dd970d553fbee5ae6c9995d88c8affa3217e8349f9c6ee5c75cd72f92bc57733` |
| compatibility.json | `cbb2cafc64729ea14493ab1e6520222d5a8ca28898e7e4ae50b0631f371b6c0c` |
| validation_results.json | `6d40dde1519a183293ba990b9d6963cf57605d42a688e3b69680e5a22cfa7901` |
| INCOMPLETE.json | `b0b7513673f84e65690cc6572c26877e06b38a50e7f322a344194cf5b889038a` |

The compatibility status is `BLOCKED_NO_COMPLETE_SCORING_INPUT`. The candidate's
29 components, all sixteen accepted preparation-code hashes, package agreement
and feature-order contract were checked. The saved training provenance is only
a compatibility reference, not the source of new scoring rows. No loader
reference is supplied, no real vectors are certified, and the existing loader
adapter rejects this package. A separate ready-v2 adapter was not built for an
incomplete population.

`v2_date_frozen_elo_schedule_unknown_v1` remains unchanged. Any eventual
reconstruction must retain the candidate's exact 50-feature order, deferred
debut columns, unknown schedules, original orientation, histories/Elo/opponents
strictly before `min(event_date, UTC(scored_at).date())`, entire scoring-day and
target-day exclusion, original event date as the feature reference date, and
target result/winner/finish/duration exclusion. The original comparison protocol
was copied byte-for-byte; no metric, threshold, clipping, bin or bootstrap rule
changed. No final priors or calibrator were fitted or applied.

**Tests and preservation.** The final safe suite passed **284 tests, zero
failures**, in 57.96 seconds: sixteen v2 checks plus 268 existing safe regressions.
Exact arguments and the log are frozen as `commands.json` and `regressions.log`.
Tests cover eligible/ineligible versions, identical/conflicting duplicates,
observational ordering and ambiguous ties, missing references, component
priority/provenance, generated title defaults, profile/experience gaps, original
108-row coverage, deterministic evidence, refusal to overwrite and blocked
loader compatibility. Existing synthetic replay checks exercise chronology,
scoring/target-day and target-result invariance, global Elo/opponents, exact
feature order, unknown schedules, deferred columns and deterministic complete
synthetic-108 reconstruction. No real forecast vectors were constructed by tests.

The existing regression allowing newly committed post-forecast snapshots while
preventing their historical selection is retained unchanged; no six-commit
assumption was restored. Guards rejected frozen-outcome/joined-audit access,
warehouse connections, model/prior fitting or application, real candidate
prediction and real feature construction. The known mutating live integration
test, live warehouse tests and unrelated legacy failures were not run.

Two independent same-scope builds produced **26 byte-identical deterministic
payload files** before operational attachments. Publication read-back validated
all component hashes and the exact file list. Module compilation and new-file
whitespace checks passed. Actual-run overwrite and incomplete-loader attempts
were independently refused. Three early inspection-command issues (empty
pathspec, temporary stdlib filename collision and empty scraper CSV) were
corrected or recorded as unusable source evidence; no source data was repaired.

Hash-only checks before preparation, after publication and during handoff verify
**all 462 baseline files unchanged**, with zero missing files and zero new files
in protected artifact roots. This covers both holdouts and original forecast
probabilities, accepted preparation artifacts, the candidate, models/pointer,
source CSVs, prior reports and policy v1. The frozen blocked-run checksum pin
remains `ac762f369aa132fc3e83694071056cda217f436f6e4315b665c454963d4a59d0`;
the candidate checksum pin remains
`41f8c8afb8fd091fa0b04823ad9175cedcfdbf85ccf3f62d77c71e0d9219279f`.
Production-pointer SHA-256 remains
`ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa`.
No production promotion, pointer change, live migration, warehouse write, model
fitting, real candidate scoring, frozen outcome evaluation or website fetch
occurred. Git network operations are solely the authorized upstream handoff.

**Stopping recommendation.** Stop this historical recovery attempt here.
Phase 4B is unavailable under the frozen 108-forecast protocol. Supply exact
eligible archived title evidence for the 93 identified targets, including
producer attestation or hash-matched page metadata for the seven generic rows;
a surviving Gable physical profile evidenced no later than July 1 scoring; and
eligible complete experience/history or independently explicit debut evidence
for the four August profiles, including Wint's August 11 historical record and
its required references. Any unresolved history must retain global Elo and
opponent dependencies. Current websites, warehouses, old defaults or frozen
outcomes cannot fill these gaps.

Alternatively, establish a separately authorized future shadow-evaluation
track with prospectively captured complete sources and a new protocol. That
alternative was not initiated. There is no authorization to score, evaluate,
retrain, promote or weaken the essential-input gate.

Git handoff follows `next-phase-git-handoff.md`: logical policy/code/tests and
evidence/report commits, followed by a normal fast-forward push that includes
the eleven pre-existing local commits. Both fetch/push URLs and the current
upstream were verified as `https://github.com/wlodzimierrr/ufc-predictor.git`,
`origin/main`; fetch succeeded, and remote commit
`81b729a21eab4b9fa50018be21fdd7e3f3390abf` was an ancestor of local HEAD.
Exact new commit hashes, push outcome and final worktree state are recorded in
the final session handoff rather than a self-referential report-update commit.
