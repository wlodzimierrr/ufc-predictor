# Phase 5C.3 source health and evidence feasibility

**Offline diagnosis COMPLETE. Prospective evidence remains BLOCKED; historical
comparison STILL_BLOCKED; production unchanged. No forecasting readiness claimed.**

The smallest recommended next phase is a separately authorized, offline repair
of the cancelled/unknown-outcome-to-NC fallback in `warehouse/transform.py`.
It prevents invented resolved history before further intake. This alone cannot
supply title or complete-history evidence. Aggregate-statistic deduplication is
also demonstrably defective; the existing evidence contract has a separate
source-semantics problem. No ingestion repair or policy amendment was applied.

## Boundary and accepted evidence

All six requested contracts/reports/handoff documents were read completely.
Ancestor and repository searches found no applicable `AGENTS.md`. Starting
worktree was clean on `main`, HEAD
`bc2ca864bacb66d327f14e13d5ba6292c8fb4c1f`, upstream `origin/main`; fetch/push
remote verified as `https://github.com/wlodzimierrr/ufc-predictor.git`.
Git transport for the required publication is the only authorized network
activity used by this phase. Diagnosis used existing files exclusively.

Let **C2** denote
`data/experiments/phase5c1_shadow_workflow/20261003T052316Z_phase5c2_real_intake_v1/`.
The three external pins passed, as did every member and exact file inventory
of activation, observation and BLOCKED run: respectively 9, 8 and 15 members.

| C2 artifact | Verified SHA-256 |
|---|---|
| `activation/checksums.json` | `21672682c5a7096fdc94ac8344a90755186c4cfadd92f9cb91ee2d5e4d98adfd` |
| `capture_attempt/observation/capture_receipt.json` | `c09d3adf1065edda3582eabd9df6126d842bfa19e84ef7e222bddc384c246a18` |
| `journal/runs/real_intake_v1/checksums.json` | `33d8e983476c408ee7a67f785c27856d58b3a0c9a0a3ea3fc2373a5bbe61d81c` |

The common cutoff remains `2026-10-03T05:23:34.369563+00:00`. Frozen tables
contain 798 events, 4,522 fighters, 8,992 fights and 17,386 aggregate-statistic
rows. Binary-result event dates end August 29; aggregate event dates end May 30,
although aggregate refresh clocks are August 8–9. The frozen 163 announcements
include 128 past-dated rows and 35 on/after October 3. C2's existing
`coverage/coverage_summary.json` and `blockers_by_source_bout_fighter.json`
remain the bout-level blocker references: 163 missing title claims, 326 missing
participant experience claims, 13 timing-in-window rows, zero forecasts.

The compact [diagnostic summary](phase5c3-source-diagnostics.json) records exact
input hashes, counts, example body hashes/last matching fetch clocks and the
two suspect NC identities. It is descriptive output, **not an authoritative
evidence package**. Its generator is
[`tools/diagnose_phase5c3_sources.py`](../../tools/diagnose_phase5c3_sources.py).
No large capture, model or registry history was duplicated.

## Acquisition-to-warehouse lineage and established gaps

| Stage | Existing implementation and operational consequence |
|---|---|
| Discovery | `spiders/events.py`, `fights.py`: completed `?page=all` and upcoming listings, then event cards using hrefs and `tr[data-link]`. Within-run event deduplication retains the first discovery. Single-event arguments bypass listings. No enumeration/pagination completion certificate is emitted. |
| Incremental selection | `spiders/incremental.py:66` loads any parsed ID and any historically successful manifest URL. Success means captured response, irrespective of parser success, freshness or consumer. Completed-event/fight IDs are skipped; no comparison with changed upstream content can occur before a request. |
| Transition handling | Event and fight subclasses subtract known upcoming records from skip sets. Thus a known upcoming URL **can** be fetched and become completed under the same URL-derived ID. However, any completed CSV version enters the known-ID set, even if a later version is upcoming. New URL/manual IDs do not automatically replace old announcements. |
| Queues | `build_fight_stats_queue.py` reads fights, event manifest and events; event manifest status takes precedence. Filtering uses event status, not individual fight outcome. Missing events default to completed; old queue entries carry forward. Both statistics spiders prefer an existing queue over listing discovery, with no queue freshness check. Fighter queue similarly replaces A–Z discovery when present. |
| Parsing | `FightInfoParser.parse_response` derives completed/upcoming from outcomes, tolerating missing fields. `FightStatParser` requires expected table headers; statistics spiders log absent-table/parse failures without parsed rows. `FighterInfoParser` extracts profile fields, record and linked fight IDs; `transform_fighter` discards record/history links, and neither step verifies completeness. |
| Export/storage | Scraper `Makefile`: `update_*` appends (`-o`); `refresh_*` and samples replace CSVs (`-O`). `AppendSafeCsvItemExporter` prevents embedded headers, not duplicate versions. `EventsManifestPipeline` updates one event row in place. Default paths resolve to root `data/`; inspection found no default path mismatch. |
| Load | `load_events.py` combines root events CSV and event manifest. `load_fights.py` reads root fights CSV, rejects unknown event/participant references, transforms, then upserts by fight ID. `load_upcoming_fights.py` selects future upcoming events and skips existing fight IDs: it is an announcement inserter, not a completed-result refresher. Stats loader reads root aggregate/round CSVs and skips unknown fight IDs. It does not derive aggregate rows from rounds. |
| Conflict handling | `warehouse/db.py:96` keeps newest `scraped_at` per PK within a load (later file row wins ties), permitting same-ID upcoming→completed transitions. Across loads, SQL updates other fields unconditionally but retains the greatest timestamp: old inputs can overwrite newer values while the clock stays new. This is a confirmed code risk, not an observed cause established by execution logs. |
| Invocation | Scraper `update_all` runs events→fights→queues→fighters→aggregate→round; `refresh_all` bypasses incremental selection. Root `load_all` orders events→fighters→fights→stats→upcoming. `update_dashboard_data` omits scraper acquisition and stats loading. No checked-in cron/CI acquisition schedule or populated local scheduling definition was found. Make recipes do not prove which jobs actually ran. |

Spider/parser paths above are under
`scraper/UFC-Web-Scraping-main/ufc_scraper/ufc_scraper/`; queue builders and
scraper Makefile are in its parent project. Loader paths are under `warehouse/`.
`utils.get_uuid_string` uses UUID5 of the URL after removing only `www.`;
HTTP/HTTPS and changed detail URLs remain different identities. Manual loaders
use other event/participant-based keys or `manual:fight:` keys. No loader
provides a general transition/alias ledger. Statistic parsing/transformation
also has numeric-zero defaults; row presence alone is not a completeness check.

**Aggregate-statistics skip defect — established.** `CrawlFightStats` inherits
the shared manifest skip set. Metadata, aggregate and round consumers use the
same fight-detail URL; a metadata fetch can suppress aggregate extraction even
when no aggregate row exists. `fight_stats_by_round.py:56` explicitly overrides
this set to empty, so its selection depends on its own CSV.
The current queue has 8,885 entries: 192 lack aggregate rows, 72 lack round rows.
Incremental aggregate selection would request only four; **188 missing aggregates
are skipped by successful shared-manifest entries**. Round selection would
request 72. Existing rounds cover 8,813 fights through August 15; aggregates
cover 8,693 through May 30. The 120 round-only fights span June 6–August 15.
One preserved June 20 fixture (`347d3776-7af9-5de4-ac62-d97754f18b09`) successfully
produced two aggregate parser rows offline. This proves a recoverable extraction
gap for that fixture, without writing statistics or constructing features.

All **17,386** locally transformed aggregate rows equal the frozen warehouse
rows field-for-field. Both current statistic CSVs have zero fight references
absent from the capture; local fight CSVs have zero event/participant reference
rejects against captured IDs. A newer aggregate batch awaiting this loader is
not present. Four queued fight IDs are absent from the capture (listed in the
summary); statistics extracted for those IDs would be rejected by the loader
until identity/reference review.
The queue reaches September 5, not the cutoff. The saved statistics report has
149 gap rows versus the current 192; it is not a certificate of present coverage.
Its code checks ID presence, not two-sided completeness or latest successful
consumer execution. No August aggregate job log identifies exactly when the
gap arose; deduplication explains present selection failure, not the whole
chronology of the May boundary. Full refresh uses `incremental=0` and therefore
does not suffer this particular skip.

**False NC transformation — established.** `warehouse/transform.py:133` maps
every outcome combination outside upcoming-empty, W/L, L/W and D/D to `nc`.
This includes `event_status=canceled` with empty outcomes and invalid W/W.
Two September 12 frozen NC rows,
`3c34cdee-2aa5-5cef-b467-3f5ed89b0b0f` and
`0ed0563e-a80c-5aa5-a2c4-8c0e818a7273`, have local cancelled/empty-outcome
versions dated August 16. Applying the current pure transform reproduces their
**entire captured rows exactly**. The causal mapping is reproduced; the actual
load invocation/log is unavailable. Their apparent September NC coverage is
not evidence of completed September bouts. The accepted projection admits
earlier `nc` rows (`phase5c1_sources.py:133`), so structural validation alone
cannot certify this history's semantic reliability. No frozen admission, result
or model was rewritten or retrospectively re-evaluated.

**Expired announcements — heterogeneous, not a blanket refresh bug.** Of 128,
107 have HTTP sources, 20 manual sources and one `https://test.local/integration`
source. Twelve manual aliases already have the accepted canonical representation;
April remains unresolved. `recover_cached_upcoming_fights.py` constructs
announcements from cached event rows and inserts missing profiles as stubs.
`apply_manual_catchup_cards.py` constructs manual IDs and clears result fields
for active catch-up rows. Source construction and upsert-only persistence can
retain announcements absent from later cards. The local CSV has 32 IDs with
both upcoming and completed versions, showing same-ID transitions are possible;
46 captured fight IDs are absent from the current CSV, so replacing/reloading
that file is not a complete warehouse reconciliation. None of these facts
establishes cancellation, replacement or same occurrence for an unresolved pair.

**Upstream access failures — established, separate from defects.** Existing
`fetch_manifest.csv` records HTTP-200 browser-challenge failures for event
listings on September 12, 18 and 30 (four each), and 42 fight failures on
September 30. The May 30 event refresh log records a May 31 challenge and zero
items; June 9's existing targeted fight logs record 13 May 30 and 12 June 6
items. Thus partial successes and access failures coexist. No later complete
results/statistics refresh is evidenced. Binary wins ending August 29 are
consistent with acquisition blockage, unrun/incomplete jobs, changed source
identities and persistent announcements; the available logs do not assign a
unique cause. Phase 5C.2 intentionally did not run any refresh. This phase also
ran none and made no access-blocked-site retry.

**Version preservation defect — established.** `middlewares.py:314` stores
entity HTML at one UUID filename and rewrites changed bodies. Both completed
and upcoming listings use the same date-only filename, even on the same day.
Of 108,101 successful manifest references, 100,335 hashes differ from the body
now at their recorded path; 7,766 match, none are missing. These are reference
counts, not distinct lost-page counts. Synthetic reproduction proves overwrite
and listing collision. Git/frozen bundles may preserve particular older versions;
the ordinary manifest itself cannot reproduce every observation. Challenges and
HTTP failures preserve metadata/hash where available, not failed response bodies.
Any later evidence intake needs exact operator-preserved versions and truthful
clocks; a hash alone cannot reconstruct missing bytes or prove authenticity.

## Evidence-contract feasibility

Requirements are in `docs/phase5c1-shadow-evidence-contract-v1.md`, implemented
by `modeling/phase5c1_evidence.py:10` and `:75`; identity handling is in
`phase5c1_sources.py:86`. C2's evidence package has empty sources/assertions.
The following assesses available bytes, not newly qualified authority.

| Claim | Exact requirement and existing support | Necessary authority/extraction; unavailable evidence |
|---|---|---|
| Title status | Literal true/false plus ordered fight/event/participant IDs and date; whole claim reproduced by JSON pointer or authoritative signed review. Allen–Duncan's preserved detail body says `Middleweight Bout` (`…/fights/6fe1d59a-6ae9-5436-bc78-767da12a4707.html:108`). The warehouse false comes from `_extract_bout_flags`: absent text also returns false. Neither proves an affirmative non-title claim. | Provider-authoritative boolean export, or review grounded in explicit title/non-title wording or separately approved exhaustive designation semantics. Bind roster/date and source version. Missing title text remains unknown; schedules/finish rounds cannot decide it. Current source bytes provide no qualifying claim. |
| Participant history | `domain=admitted_resolved_ufc_occurrences_v1`, `covered_from<=1993-11-12`, exclusive bound min(target date, observation UTC date), `complete=true`, target/participant IDs, and exact admitted occurrence identities/results including draws/NC. Existing warehouse rows provide observed occurrences. Allen/Duncan raw profile bodies list 20/10 distinct detail links with last hash-matching August 8 fetches. | Enumerate a source's entire defined UFC domain, all pages/end markers, cancellations/revisions and identity mappings; cross-check event/detail/profile sets and results, resolve every discrepancy. Operator must establish coverage authority and boundary, not just trust the warehouse. Stale pages and false NC inputs prevent a truthful present complete-history assertion. Parsed profile record totals are not UFC-domain enumeration certificates. |
| Debut | Experience fields above with `status=verified_debut`, affirmative complete-domain support and zero admitted occurrences. Cody Chovancek's August 8 raw profile says `Record: 9-0-0` and links one upcoming bout; it supplies no resolved UFC occurrence. | A verified empty enumeration under an accepted complete UFC-domain source, or authoritative explicit UFC-debut statement with identity/boundary support. Neither lifetime record, blank statistics nor empty warehouse history verifies UFC debut. No qualifying debut evidence is supplied. |
| Identity relationships | Explicit `same_occurrence`/`distinct_occurrences`, both IDs, event/date/participants, whole-row and participant-statistic hash maps; same occurrence additionally canonical ID and explicit transition. | The frozen Phase 5B.1 reconciliation ledger, preserved card links and manual creation code support the accepted twelve aliases and exact 1997 rematch; C2 revalidated these. April's card/detail bytes corroborate participants, not an old→new transition. Changed rows require new relationship evidence; additional distinct occurrences need separate adapter approval. Matching names/pairs/URLs or latest timestamps cannot supply the missing relationship. |

Example source hashes and matching observation clocks are in the diagnostic
summary; identity evidence hashes/derivations remain in the accepted
`phase5b1-reference-and-history-reconciliation-report.md` and its frozen ledger.
Source labels, successful fetches and integrity hashes do not establish provider
authenticity, exhaustive coverage or reviewer authority.

**Operational judgment:** native HTML does not contain the contract's whole
structured claims: repository UUIDs, target-bound exhaustive histories, exclusive
boundaries, `complete=true`, or row/statistic hash maps. These require a mapping
and derivation/review step. The existing signed-review route can bind truthful
claims if an independently trusted reviewer has adequate supporting evidence;
its byte spans/signature hash validate binding, not semantic entailment or PKI.
A locally generated JSON claim, even correctly hash-bound and labelled
`authoritative=true`, is not independent evidence. Available bytes cannot
satisfy the contract now. No provider capability to export its exact schema
has been established.

### Proposed amendment for explicit review — NOT APPLIED

If operator-supplied sources cannot truthfully provide native explicit complete
claims, review a separate version permitting **reproducible derived evidence
under qualified source semantics**, while retaining unknown/incomplete blockers:

1. Qualify a named source and reviewer externally: identify authority over UFC
   occurrences/title designations, documented domain, observation authenticity,
   coverage limitations and allowed semantic predicates. Provider-specific closed
   title vocabulary must be exhaustive and affirmatively qualified before an
   explicit non-title designation can map to false. Missing text still blocks.
2. Preserve immutable bytes per request/version, URLs/statuses and genuine
   clocks. Publish extractor version, selectors/pointers/spans and complete
   derivation receipts, separately from the provider bytes. Distinguish provider
   statements from operator attestations and computational identity/hash bindings.
3. Demonstrate enumeration: archive every page/cursor, page counts/end markers,
   relevant date range and source totals when provided; check duplicate/missing
   pages, truncation and access failures. Cross-check independent event-card and
   participant/detail enumerations within the qualified domain. No unverified
   source total or missing pagination control implies completeness.
4. Map source identities/orientation/date to repository identities explicitly;
   retain unresolved rows. Reconcile every result/history mismatch, including
   false NC candidates, without preferred-result selection. Whole history must
   still match the admitted projection exactly. Identity transitions still need
   affirmative relationship support; computed hashes are lineage bindings.
5. Certify complete coverage only where the qualified semantics and checks
   establish the exact exclusive boundary. Derive UFC debut only from a verified
   empty complete enumeration. State that this is source-domain completeness,
   not unknowable worldwide/lifetime certainty. Gaps or inconsistent evidence
   remain blocked. Keep timing, outcome isolation, registration and component
   contracts unchanged; publish a new reviewed contract/freeze before later use.

Until approval and qualification, v1 applies unchanged. These checks describe
requirements, not capabilities already possessed by UFCStats or any provider.
If authority is absent, the minimum operator supply is an authentic, preserved
UFC-domain export/feed or primary-source packet, coverage/semantics attestation
from a named accountable authority, explicit title evidence for each target,
and complete participant occurrence enumeration through the proposed cutoff.
Include native IDs, roster/date/revision metadata, pagination/export completion
receipts and mapping support. April additionally needs an explicit transition
or distinct-occurrence source. An operator trust decision must identify what
authority is accepted and why; approval alone cannot make unsupported facts true.
No service purchase, provider selection or website retry is proposed.

## One recommended next phase and operator decisions

**Phase 5C.4: repair strict outcome disposition, with offline verification only.**
Prioritize this over refresh because the current transform can manufacture
resolved NC history from cancellations; refreshing unchanged or blocked sources
cannot cure that mapping. This removes one prerequisite hazard, not all gates.

- **Prerequisite/authority:** separately authorize a narrowly scoped ingestion
  code change; confirm cancelled/unknown/invalid outcomes must fail explicitly,
  remain preserved as unresolved/rejected source evidence, and never become
  admitted NC. Inspect current Git state and preserve all frozen implementations
  and artifacts. No new acquisition or warehouse write authority is needed for
  this offline repair, and none should be bundled into it.
- **Scope:** `transform_fight` and focused pure/mocked tests only; retain valid
  upcoming-empty, W/L, L/W, D/D and explicit NC/NC mappings. Reject cancelled,
  missing, contradictory and unsupported outcome combinations before upsert.
  Keep the loader's pre-upsert validation barrier; do not silently drop rejected
  rows, invent a new cancellation result enum, or rewrite the two frozen rows.
- **Acceptance:** a table-driven test preserves all valid mappings and refuses
  cancelled-empty, completed-empty, partial and contradictory pairs. A mock-only
  loader test verifies a refusal occurs before any upsert when such a row appears
  late in the input. Reproduce the two diagnostic row shapes without changing
  source bytes. Pin preservation, run only relevant offline tests, commit/report.
- **Stop:** on uncertain source outcome vocabulary, require operator semantics;
  on cancellation representation requiring schema/loader redesign, stop for a
  separately scoped decision. No live correction, new capture, feature build,
  model execution or gate change. Existing historical/trial statuses stay blocked.

The aggregate-consumer skip repair and immutable raw versioning are independently
designable from local fixtures, but are separate implementation scopes, not
extra work silently included in that next phase. Likewise, evidence-authority
qualification/contract review can proceed independently of ingestion repair.
The operator must choose native explicit export/review evidence under v1 or
explicitly review the proposed semantic amendment; neither has been authorized
or supplied here. Source refresh, corrected warehouse publication and a genuinely
new matched observation would each require later bounded authority and successful
qualification. Do not reuse C2's consumed capture budget, retrofit later evidence
to its cutoff, or preserve a timing window by backdating observations.

## Verification, preservation and Git handoff

Ten focused diagnostic tests passed using existing scraper dependencies:

```text
PYTHONDONTWRITEBYTECODE=1 scraper/UFC-Web-Scraping-main/.venv/bin/python \
  tools/tests/test_phase5c3_source_diagnostics.py
PYTHONDONTWRITEBYTECODE=1 python3 tools/diagnose_phase5c3_sources.py
```

Tests reproduce aggregate-versus-round selection, failed-only retry selection,
single-participant deduplication, upcoming refreshability, completed-version
skipping, cancelled/invalid NC mapping, raw overwrite/listing collision,
event-level queue selection and one existing offline parser fixture. Socket
connections are forbidden in the test bodies; temporary writes are isolated.
The system Python lacks Scrapy, so its existing local scraper environment was
used without installing dependencies. No live integration test, broad suite,
scraper, loader, source refresh, warehouse connection, feature construction,
fit, prediction, outcome evaluation or historical comparison ran.

Two independent diagnostic outputs matched byte-for-byte. The final inventory
and hash-only preservation checks found zero changed/missing pre-existing files
and no additions beyond this phase's four intended files. Baselines cover 7,937
tracked and 22,625 local files (Git/dependency/cache/bytecode and root `.env`
excluded); protected outcomes were hashed only, never parsed. Frozen artifacts,
original registry bytes, pipeline code and production files remain unchanged.
Scoped whitespace, content/secret and size reviews passed. Changes comprise the
read-only utility, diagnostic tests, compact JSON summary and this report.

Publication follows `next-phase-git-handoff.md`: logical commits, committed-byte
verification, then normal push to verified `origin/main`, without history
rewriting. Actual commit hashes, push result and final worktree status belong
in the final response, avoiding a self-referential report-update commit loop.
