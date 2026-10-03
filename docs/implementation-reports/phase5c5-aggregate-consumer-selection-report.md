# Phase 5C.5 aggregate-consumer incremental selection

**Implementation COMPLETE; offline verification PASSED. Prospective evidence
remains BLOCKED. Historical comparison remains STILL_BLOCKED.**

Aggregate incremental selection now ignores the shared fetch manifest and skips
only fights with unambiguous two-participant aggregate parsed output. The current
queue has **192 eligible entries instead of 4**: all **188** previously diagnosed
missing-aggregate suppressions are confirmed against unchanged input bytes and
are now eligible. No requests were issued and no statistics were recovered.

No acquisition, access-blocked-source retry, source refresh, warehouse connection,
warehouse correction, data ingestion/loading, fitting, forecasting, evaluation,
model execution or production operation occurred. No evidence-policy change
occurred. The only network activity permitted for this handoff is Git transport
for the explicitly requested publication.

## Accepted handoff, Git state and scope

The Phase 5C.4 strict-outcome report, Phase 5C.3 source-health report, complete
Phase 5C.3 diagnostic JSON and `next-phase-git-handoff.md` were read completely
before editing. Searches of the repository and its ancestors found no applicable
`AGENTS.md`.

Starting worktree was clean on `main`, HEAD
`3f3b50aa3c55dea2a076631ab2795061ccb7afa9`, with local `origin/main` at the same
commit. Fetch/push remote was verified as
`https://github.com/wlodzimierrr/ufc-predictor.git`; branch upstream configuration
is `origin`, `refs/heads/main`. There were no unrelated edits to preserve.

Accepted inventories and live code pins were verified before any repository
edit. The aggregate spider is absent from all nine frozen `code_versions.json`
files under `data/experiments/`. Its previous hash appears in historical
preservation baselines, which describe earlier repository state; none was
changed or repinned. The completed Phase 5C.4 adapter, loader wiring, tests and
report remain byte-identical. No frozen implementation required replacement.

## Exact selection and completion rules

Only `CrawlFightStats` overrides the two incremental skip-source loaders.
The shared mixin and round spider are unchanged.

1. `_load_captured_uuids()` always returns an empty set for this consumer.
   Successful `fetched`, `unchanged` and `updated` manifest records, whether
   associated with metadata, rounds or aggregates, are capture records rather
   than aggregate-completion receipts. Failed records also supply no completion.
2. In incremental mode, `_load_known_ids()` examines the aggregate consumer's
   configured `existing_csv`, defaulting to `data/fight_stats.csv`. Missing files,
   empty files, header-only files, missing required columns or duplicate header
   names produce no complete fight IDs. Non-incremental mode loads no skip IDs
   and retains every candidate URL.
3. The header must contain every field in the existing `FightStats` dataclass.
   Every required cell must be present and nonblank. Extra header columns are
   permitted but participate in duplicate/revision comparison. Overlong rows
   with unnamed extra cells are invalid; truncated rows cannot establish
   completion.
4. `fight_id` and `fighter_id` must be exact canonical lowercase, hyphenated
   UUID5 strings, as emitted by the existing parser. Blank, malformed, nil,
   differently cased or whitespace-padded identity strings are not accepted.
   The row's `fight_stat_id` must equal the existing parser identity derivation
   `get_uuid_string(fight_id + fighter_id)`.
5. The nonblank row URL must have no surrounding whitespace, and its UUID under
   the unchanged `get_uuid_string` function must equal its stated `fight_id`.
   All accepted rows for that fight must contain the **same exact URL string**.
   No new URL normalization or alias reconciliation is introduced. Existing
   utility behavior removes `www.` for identity derivation; rows differing in
   that exact spelling still cannot jointly establish completion. HTTP/HTTPS
   and different detail URLs retain their existing distinct identities.
6. Every integer statistic declared by `FightStats` must be nonempty ASCII
   decimal digits, representing a nonnegative integer. No statistics are
   synthesized, substituted, summed or transformed by selection. The check
   does not validate sporting plausibility, cross-field consistency, numeric
   ranges or whether parser-produced zeros represent missing source values.
7. Group valid rows by their stated fight ID and count **distinct fighter IDs**.
   Exactly two are required. Zero, one, repeated rows for just one fighter, or
   three or more distinct fighters cannot justify skipping. Repeated rows for
   either of two participants are acceptable only when every CSV field except
   `scraped_at` agrees exactly. Different timestamps alone do not create a
   conflict and are never used to choose a revision.
8. Any differing participant revision invalidates that fight regardless of row
   order or timestamp. Any invalid row disqualifies both its stated fight ID
   and its URL-derived fight ID where supplied, so a malformed row cannot hide
   behind an otherwise valid pair. Invalidity is retained through the entire
   file; later valid rows cannot clear it. Rows with neither usable fight identity
   nor URL provide no completion and cannot be assigned to another fight by
   inference.

The shared `get_unknown_urls()` then skips only IDs returned by this aggregate
predicate. Queue and event-card discovery call the same function, preserving
candidate order, duplicate candidate behavior and existing skip counting.

Seed priority is unchanged: **single-fight argument → existing queue → listing
fallback**. The single-fight argument retains its whitespace trimming and
direct request/callback behavior even for already complete output. An existing
empty queue still prevents listing fallback. Listing fallback still discovers
events and filters their fight links; it adds no new discovery mechanism.

Selection eligibility is a local scheduling decision. It does not authorize
acquisition or retries of sources that have denied or challenged access.

## What completion establishes, and what it cannot establish

The predicate establishes only that the current local aggregate export contains
two distinct, structurally valid participant identities for a fight, correctly
bound under the existing parser's identity scheme, with populated aggregate
fields and no observed conflicting rows. It can avoid unnecessary incremental
requests when that own-consumer output is unambiguous.

It does not verify the authoritative roster or outcomes, source authenticity,
statistical accuracy, freshness, title status, debut status, complete participant
history, complete UFC history or coverage of fights absent from discovery. The
two rows need not come from one atomic observation; clocks are not provenance
or recency certificates. Existing parser numeric-zero defaults remain a
limitation. Unassignable malformed rows cannot be linked to a valid fight without
inventing identity evidence. Conservative conflict rejection can leave a fight
eligible indefinitely after appended conflicting revisions; reviewed revision
resolution is a separate task, not automatic latest-row selection here.

## Files changed and rationale

| File | Change |
|---|---|
| `scraper/UFC-Web-Scraping-main/ufc_scraper/ufc_scraper/spiders/fight_stats.py` | Aggregate-only overrides for manifest exclusion and conservative parsed-output completion; all seed/parser/callback paths retained. |
| `scraper/UFC-Web-Scraping-main/ufc_scraper/tests/spider_tests/fight_stats_spider_test.py` | 68 synthetic offline selection cases, including network and warehouse-import guards. |
| `tools/tests/test_phase5c3_source_diagnostics.py` | Narrowly changed two active broken-behavior expectations and their names to require missing/one-sided aggregates to remain eligible; disclosed this in the module docstring. All ten tests remain. |
| `tools/compare_phase5c5_aggregate_selection.py` | Read-only, compact before/after comparison using unchanged shared-mixin methods to reproduce pre-repair selection and the repaired spider for after-selection; prints input/set hashes. Never starts a crawler or calls request callbacks. |
| This report | Detailed phase verification and Git handoff. |

The historical Phase 5C.3 report and diagnostic JSON remain unchanged. Its
read-only generator also remains unchanged and continues to describe its
original shared-mixin diagnosis; it is not repurposed to rewrite historical
evidence. No diagnostic coverage was deleted. Other defect reproductions,
including legacy false-NC behavior and raw-body overwrites, remain intact.

## Synthetic tests and offline execution

All verification used existing installed dependencies. No package installation,
crawler, live integration test, broad unrelated suite or model-fitting test ran.
Synthetic CSVs/manifests were written only to temporary directories. Listing and
event-card HTML was constructed in memory. Existing parser tests and the
preserved diagnostic raw fixture read local HTML exclusively.

Tests cover successful metadata captures with missing aggregates; round-only
output; failed-only captures; one participant; repeated one-participant rows;
two-sided output with and without consistent duplicates; malformed/blank IDs;
misbound statistic/fight/URL identities; missing/blank/nonnumeric statistics;
three-participant ambiguity; conflicting revisions in both file orders; exact
URL disagreements; missing/empty/incomplete/malformed files; incremental versus
non-incremental operation; queue and listing/event-card selection; empty-queue
priority; single-fight overrides; and unchanged shared/round-consumer behavior.

| Safe check | Result |
|---|---:|
| New aggregate selection tests | 68 passed |
| Existing event selection test | 1 passed |
| Existing aggregate and round parser tests | 2 passed |
| Existing append-safe exporter tests | 2 passed |
| Phase 5C.3 diagnostic regressions | 10 passed |
| Existing legacy transform tests | 19 passed |
| Phase 5C.4 strict adapter regressions | 285 passed |
| Phase 5C.4 mock-only loader regressions | 10 passed |
| **Total** | **397 passed** |

The 73 scraper checks completed in 0.35 seconds; the 314 outcome checks in
0.24 seconds; the ten diagnostic checks in 0.028 seconds. Four existing Scrapy
`start_requests()` deprecation warnings appeared on the listing-fallback cases;
changing that lifecycle API is outside this repair. An initial root-level pytest
import-mode choice caused collection errors because of the nested scraper
package layout; running from the scraper package resolved it without repository
configuration changes. One newly written truncated-row fixture initially placed
its identity in the timestamp column; the fixture was corrected to put it in
`fight_id`, and the focused suite passed. No unresolved test failure remains.

A temporary process-wide offline runner blocks socket connects/sends and DNS
lookup, and rejects a real `warehouse.db` import before collection. The new test
module repeats these guards and explicitly tests their refusal. Loader tests
retain their synthetic database module and mocks; real credential/database
helpers are never imported. Plugin auto-loading, dotenv loading, bytecode and
pytest cache writes were disabled. Scratch runners, preservation receipts and
comparison output are in `/tmp/phase5c5-aggregate-selection/`, outside Git.

Actual successful commands (first invocation from the scraper package directory):

```text
cd /home/wlodzimierrr/ufc-data/scraper/UFC-Web-Scraping-main/ufc_scraper
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  ../.venv/bin/python /tmp/phase5c5-aggregate-selection/offline.py pytest -q -p no:cacheprovider \
  tests/spider_tests/fight_stats_spider_test.py tests/spider_tests/events_spider_test.py \
  tests/parser_tests/fight_stats_parser_test.py tests/parser_tests/fight_stats_by_round_parser_test.py \
  tests/test_exporters.py
cd /home/wlodzimierrr/ufc-data
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  python3 /tmp/phase5c5-aggregate-selection/offline.py pytest -q -p no:cacheprovider \
  warehouse/tests/test_transform.py warehouse/tests/test_strict_fight_outcomes_v2.py \
  warehouse/tests/test_load_fights_strict_outcomes_v2.py
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 \
  scraper/UFC-Web-Scraping-main/.venv/bin/python \
  /tmp/phase5c5-aggregate-selection/offline.py diagnostics-tests
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 \
  scraper/UFC-Web-Scraping-main/.venv/bin/python \
  /tmp/phase5c5-aggregate-selection/offline.py compare \
  > /tmp/phase5c5-aggregate-selection/selection-comparison.json
PYTHONDONTWRITEBYTECODE=1 python3 /tmp/phase5c5-aggregate-selection/offline.py diagnose \
  > /tmp/phase5c5-aggregate-selection/phase5c3-diagnostic-replay.json
PYTHONDONTWRITEBYTECODE=1 python3 /tmp/phase5c5-aggregate-selection/preservation.py baseline
PYTHONDONTWRITEBYTECODE=1 python3 /tmp/phase5c5-aggregate-selection/preservation.py verify
```

The baseline command ran before any repository edit. The historical diagnostic
replay is byte-identical to the saved JSON: 10,320 bytes, SHA-256
`b2fa5a75617096ee25739fc47d868d347be356b75f390430f1fa5313530febc4`.

## Current queue/export comparison and input hashes

The comparison reads only the current queue, aggregate CSV, round CSV and shared
manifest. It constructs unscheduled Scrapy `Request` objects through the queue
selection method, without invoking their callbacks or any engine. The baseline
subclass binds the unchanged shared mixin's original ID/manifest loaders; the
after-selection uses the repaired aggregate spider. No counts are forced.

| Current input | SHA-256 |
|---|---|
| `data/manifests/fight_stats_queue.csv` | `b5da7fa06b417baae1e1f855f94729e5606b1ac3d38648da95dadd929db2a450` |
| `data/fight_stats.csv` | `e4eabd760f3713d23135fbb0ac8e09c407b8b456316bb2ae7f2bfd4699a03129` |
| `data/fight_stats_by_round.csv` | `fd50110bccafbb1825e89f36f8b98c3bd0be36635467174098a12cb460b49689` |
| `data/manifests/fetch_manifest.csv` | `40587f0b8b3c736fec99a7fc4459c9ddc24577a49399b8ce969d4d6b7c5b36e3` |

All four equal the accepted Phase 5C.3 diagnostic input hashes. The queue has
8,885 nonblank URLs and distinct URL-derived identities, with zero stated
fight-ID/URL-ID mismatches.

| Selection or coverage observation | Before | After |
|---|---:|---:|
| Incremental aggregate queue entries eligible | 4 | 192 |
| Incremental aggregate fight IDs skipped | 8,881 | 8,693 |
| Missing aggregates suppressed by shared successful captures | 188 | 0 |
| Queued fights without aggregate rows | 192 | 192 |
| Queued fights skipped without passing aggregate completion | 188 | 0 |
| Round queue entries eligible | 72 | 72 |
| Non-incremental aggregate queue entries eligible | 8,885 | 8,885 |

All 188 newly eligible missing fights were previously suppressed by successful
shared-manifest records. The four previously eligible missing fights remain
eligible. Current aggregate output contains 17,386 rows for 8,693 fight IDs;
every one has exactly two distinct participant rows and passes the predicate.
These are precisely the remaining 8,693 skipped fights and their completion
basis. There are **zero** present one-participant, blank-fight-ID, partial or
ambiguous groups under the new predicate in the actual aggregate CSV. The
missing 192 fights remain absent; synthetic partial/conflicting fixtures show
how future such records are handled without modifying real exports.

The compact comparison pins the two identity sets using SHA-256 of UTF-8 sorted
unique fight IDs, one per line with a terminal newline:

| Set | Count | SHA-256 |
|---|---:|---|
| Newly eligible, previously suppressed missing fights | 188 | `f4aea8bb7d1a016cfa4b45cfb6e3c189b5144f3f39b22688fae2d3a6148dbcfc` |
| Remaining skipped fights with own-consumer completion | 8,693 | `37c1cde07ac8de3da6d6dea5ae44732914cc8353123d035a17d0ae4c990be983` |

This changes request eligibility, not coverage. The accepted queue still ends
September 5; the 120 round-only fights and four queued identities absent from
the accepted warehouse capture retain their original context. Selection does
not grant acquisition authority or make those identities loadable/authoritative.

## Preservation verification

Pre-edit hash-only baselines cover **7,945 tracked files and 49,700 local files**.
Local hashing excludes `.git`, dependency/cache directories, bytecode and root
`.env`, as in the accepted prior phase. Protected holdout outcomes and model
components were only hashed. Final comparison found no missing pre-existing
files, no unrelated modifications, exactly two authorized existing-file changes
and the three intended additions. Source CSVs, manifests, raw captures, models,
holdouts, registries, evidence contracts and production files remain unchanged.

Exact inventories, every member hash and accepted external pins passed before
and after the repair:

| Accepted artifact inventory | Members | External checksum-root SHA-256 |
|---|---:|---|
| Phase 5B.3 `20261002_phase5b3_role_aware_v2_validated` | 88 | `7d36dc2bdb2a99eb81a44424fe603a6beab74d05435e41c8a1489cea11f0bd4f` |
| Phase 5B.4 `20261002T183215Z_phase5b4_current_challenger_v1_fixed` | 49 | `59ffd4995946cc9ec7512f2f14caade09fd3144d2cfb3ee2d3b9b0951f5ca300` |
| Phase 5C.2 `activation` | 9 | `21672682c5a7096fdc94ac8344a90755186c4cfadd92f9cb91ee2d5e4d98adfd` |
| Phase 5C.2 `capture_attempt/observation` | 8 | `e2791af3b24ea6785e816d3c19e2ccf34020ac25acad84692874050bbcefa6a8` |
| Phase 5C.2 `journal/runs/real_intake_v1` | 15 | `33d8e983476c408ee7a67f785c27856d58b3a0c9a0a3ea3fc2373a5bbe61d81c` |

Counts exclude each inventory's own `checksums.json`. The capture receipt stays
pinned to `c09d3adf1065edda3582eabd9df6126d842bfa19e84ef7e222bddc384c246a18`.
All 13 embedded preparation files equal their accepted originals. All 37
Phase 5B.3 live code pins, eight Phase 5B.4 pins and 37 embedded preparation
pins pass without amendment.

Additional preservation hashes:

| Unchanged component | SHA-256 |
|---|---|
| Shared `spiders/incremental.py` | `f4b27432de495431b02e88c43367ac45c746ffe143a01caed2d34624bba20d4c` |
| `spiders/fight_stats_by_round.py` | `7e7e78ec6978a241f4d0dd7a4e5f91b6deec856a0c90fbe8b3b5033f3378d857` |
| `warehouse/transform.py` | `616ad63cf8402718ed8f912e7a3d75ba778109bac26745304256ef08eaef5a44` |
| `warehouse/load_fights.py` | `84e9446d6dc87d78c8e7315b1b3066585449846a841b6d833871fccd744adec5` |
| `warehouse/strict_fight_outcomes_v2.py` | `aec01516c479b3b7366f1b65f3980b1f1d9d34940826cdf00101c780be8eb019` |
| Phase 5C.3 source-health report | `675273823725413bcce8ec439c12500454534357f371c25a52f9617ddd4dd86d` |
| Phase 5C.4 strict-outcome report | `fda56ae6286c44a89700d3aa56a509c27e1753c279042a4104cc2ab72e36a0d3` |

Aggregate spider SHA-256 changes from
`f7655f0bf0704d5114e936b0753cc0dc536f5300c627ff35e51d5b05fea0e606` to
`40e9b93962e7d26646a36ee29be68bdc65d6a4d553bda081a41402cc6c623528`.
Historical preservation baselines, original manifests, checksum checks and code
pin files were neither edited nor weakened.

## Remaining blockers and smallest separate next step

Queue freshness, round-statistic completeness, mutable raw-body overwrites and
listing collisions, stale warehouse upserts, existing false-NC records, aliases
and evidence contracts are outside this phase and remain unchanged. Available
statistics and outcomes retain their accepted temporal boundaries and source
limitations. Affirmative title/non-title evidence, complete participant histories,
verified debuts and April's identity relationship remain unavailable/unresolved.
Access-challenge failures remain a separate acquisition blocker.

The smallest separately scoped next step is an **offline queue-freshness
validation design and guard**, using existing queue/source clocks and temporary
fixtures, with explicit rules for stale or insufficiently evidenced queues.
Keep any queue regeneration, fallback change, source acquisition or retry under
separate authorization. A freshness guard would still not certify discovery or
source-history completeness. Immutable raw versioning, warehouse reconciliation
and source/evidence qualification require their own scopes.

**Prospective evidence remains BLOCKED. Historical comparison remains
STILL_BLOCKED.** This scheduling repair supplies no new evidence and authorizes
no forecasting, evaluation, promotion or production use.

## Git publication handoff

Follow `next-phase-git-handoff.md`: review whitespace, scope, secrets, sizes and
preservation; commit implementation/tests/comparison together, then this detailed
report; verify committed bytes and worktree; push normally to verified
`origin/main`. No force-push, history rewrite, deployment or data operation is
authorized. Actual commit hashes, remote/ref, push result and final worktree
status belong in the final response, avoiding a self-referential report commit.
