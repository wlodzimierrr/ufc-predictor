# Phase 5C.6 immutable raw-response capture

**Implementation COMPLETE; offline verification PASSED. Prospective evidence
remains BLOCKED. Historical comparison remains STILL_BLOCKED.**

Future normal scraper jobs select a dedicated v2 capture writer. Each observed
response gets an exclusive, immutable receipt; exact response-body bytes are
stored by SHA-256 and verified before receipt publication. Identical bodies can
share an object without sharing observation identity. Changed bodies retain
separate objects. Completed and upcoming listings retain their exact URLs in
separate receipts, including observations made on the same date.

No real acquisition, network request to a source, crawling, source refresh,
migration, warehouse operation, ingestion/loading, fitting, model execution,
forecasting, evaluation or production operation occurred. No evidence-policy
change, provider qualification, access-control bypass or acquisition-policy
change occurred. Git transport for the explicitly requested commit/push handoff
is the only network activity used. All new captures were synthetic and confined
to temporary directories; no v2 runtime capture was written under repository
`data/`.

## Accepted handoff and scope

The complete Phase 5C.5 aggregate-selection report, Phase 5C.3 source-health
report, Phase 5C.1 shadow-evidence contract v1 and `next-phase-git-handoff.md`
were read before implementation. Searches found no applicable `AGENTS.md` in
the repository or its ancestors. Existing capture middleware, settings, tests,
relevant raw/manifest readers and frozen code pins were inspected.

Starting worktree was clean on `main`, HEAD
`4f43bcab3ea9d5eb0192888fe7c9b508866437b3`. The configured upstream is
`origin/main` (`origin`, `refs/heads/main`); fetch/push remote is
`https://github.com/wlodzimierrr/ufc-predictor.git`. A Git-only remote-ref check
also returned that starting commit. There were no unrelated edits to preserve.

The settings file and legacy middleware are absent from all nine frozen
`code_versions.json` inventories. Neither the accepted shadow contract's code
pins nor the activation code pins include settings. Existing strict-outcome and
aggregate-selection implementations, their tests and reports remain unchanged.
No frozen code copy or checksum was replaced, repinned or weakened.

The only existing-file change is the middleware entry and its explanatory
comment in `settings.py`. The legacy `RawCaptureMiddleware` implementation is
byte-identical, including its overwrite/listing-collision diagnostic behavior
and its historical failure handling. The phase neither invokes that writer on
real data nor retroactively repairs its records.

## Files and configuration

Paths below are relative to `scraper/UFC-Web-Scraping-main/ufc_scraper/` unless
otherwise stated.

| File | Change |
|---|---|
| `ufc_scraper/raw_capture_v2.py` | New `CaptureStoreV2`, read-only resolver, explicit error types and `ImmutableRawCaptureMiddlewareV2`. |
| `ufc_scraper/settings.py` | Replace the legacy capture class at priority 200 with `ufc_scraper.raw_capture_v2.ImmutableRawCaptureMiddlewareV2`. |
| `tests/test_raw_capture_v2.py` | 72 synthetic offline cases, including real process concurrency and process exit. |
| This report | Schema, guarantees, verification, compatibility and bounded handoff. |

The default v2 root is **`data/raw/ufcstats_v2/`**, derived from the module's
repository location, independently of working directory. No new configuration
knob, dual writer, legacy manifest append, entity alias or latest-body link was
added. Factory construction and the spider-open logging hook do not create
capture files. The filesystem namespace is created only when an observation
is recorded.

Browser-session/header middleware stays at 100. Retry settings, robots policy,
concurrency, throttling, timeouts, caches, redirects, pipelines, exports,
incremental selection, queues, parsers and warehouse loaders are unchanged.

## Storage and receipt schema

Schema identifier is **`ufcstats_raw_capture_receipt_v2`**; observation boundary
identifier is **`scrapy_downloader_priority_200_v1`**. Paths are relative to the
dedicated v2 root:

```text
.writer.lock
objects/sha256/<first two hash characters>/<64-character SHA-256>.body
observations/<canonical job UUID>/<canonical observation UUID>/pending.json
observations/<canonical job UUID>/<canonical observation UUID>/receipt.json
```

Objects contain the exact `response.body` bytes available at this middleware
boundary, without decoding, normalization, HTML invention or reserialization.
They are not promised to be original compressed HTTP wire bytes: higher-priority
Scrapy decompression has already run. Objects have no entity/date-derived name.

A job UUID is generated when the middleware's store is constructed. A fresh
observation UUID is generated for every response or exception recorded. The
identity of a reservation is the pair `(job_run_id, observation_id)`. The store
API also accepts explicit canonical IDs for controlled callers/tests, but
refuses every reuse of an existing reservation within that job, even if bytes
are identical or the earlier attempt is incomplete. Repeated URLs, hashes,
dates and Scrapy request objects do not collapse observations.

The completed receipt is canonical UTF-8 JSON with sorted keys, compact
separators and a terminal newline. It has an exact, closed field set:

| Field | Meaning |
|---|---|
| `schema`, `boundary` | Exact version identifiers above. |
| `publication_state` | `COMPLETE` for a published receipt. This is independent of source disposition. |
| `job_run_id`, `observation_id` | Canonical UUID identities, bound to the receipt path. |
| `job_started_at` | Actual UTC store/middleware construction clock. |
| `request_started_at` | Actual UTC `process_request` boundary clock, refreshed for each attempt; null if that callback was not observed. It is not a wire-send clock. |
| `observed_at` | Actual UTC entry into capture recording, before waiting for the writer lock. |
| `receipt_prepared_at` | Actual UTC clock after body durability/verification and before receipt serialization/publication. It is deliberately not called a publication-completion or upstream-observation clock. |
| `request_url`, `source_url` | Exact public request URL and response URL; both retained when they differ. For exceptions both identify the request. |
| `entity_type`, `listing_kind` | `event`, `fight`, `fighter`, `event_listing` or `unknown`; listings additionally identify `completed` or `upcoming`, otherwise null. |
| `http_status` | Actual response status at the boundary, or null when no response exists. |
| `response_present` | True even for an empty response body; false for a request exception without a response. |
| `body` | Exact object reference `{path, sha256, bytes}`, or null when no response body exists. |
| `disposition`, `failure_reasons` | `SUCCEEDED` or `FAILED`, with fixed reason codes; independent of stored-byte availability. |
| `access_challenge` | Result of the preserved legacy challenge detector. |
| `exception_category` | Null for responses; allowlisted coarse `timeout`, `dns`, `connection` or `other` for exceptions. |
| `from_cache` | Whether Scrapy marked the response `cached`; false for exceptions. |

Clocks use timezone-aware UTC ISO timestamps with microsecond precision. Clock
ordering is checked explicitly; backward clocks cause refusal rather than
invented replacement timestamps. Test-only clock simulations remain temporary.
`observed_at` is a middleware observation clock, not proof of source freshness.

`pending.json` contains the same preliminary fields, with
`publication_state=INCOMPLETE` and no `receipt_prepared_at`. Its body reference
describes the expected received bytes, **not a claim that storage succeeded**.
It remains immutable after success or failure. A directory reservation without
a pending file also records an interrupted beginning. Only a separately
published, verified completed receipt can resolve successfully.

## Publication guarantees and failure states

The implementation targets a local POSIX filesystem supporting directory file
descriptors, `O_NOFOLLOW`, advisory `flock`, hard links and file/directory fsync.
It provides coordination for cooperating processes using the same namespace;
Windows, object storage and unverified distributed-filesystem lock/durability
semantics are not supported by this phase.

1. Validate public URLs, canonical identities, metadata and UTC ordering. Root
   traversal refuses `..` and opens every ancestor through directory descriptors
   without resolving symlinks away. Namespace directories use mode 0700.
2. Open a regular `.writer.lock` without following symlinks, acquire an exclusive
   process lock, and reserve the observation directory using exclusive mkdir.
   This coordinates different jobs and concurrent processes, as well as
   separately opened lock descriptors in one process. Existing reservations are
   never resumed or overwritten.
3. Publish immutable pending metadata first. Each publication creates an
   exclusive `.partial` staging file, flushes its exact bytes, sets mode 0444,
   fsyncs it, and hard-links it to the final name without replacement. The final
   directory is fsynced before staging retirement. New directory entries are
   likewise fsynced through their parents.
4. For a body, either publish the new content-addressed object or read the
   existing one without following symlinks. Existing bytes must exactly match
   the received body and its SHA-256. Fsync the object and its directory,
   then reread/verify before preparing a completed receipt. Inconsistent existing
   bytes raise `CaptureIntegrityError`; the writer never replaces them.
5. Publish the completed receipt only after those checks. It has its own
   exclusive, fsynced publication. No shared append-only CSV is required for
   observation identity or body resolution. The process lock is released when
   the descriptor closes, including process exit.

Publication failure leaves reservations/pending metadata and any unretired
staging files visible. Body objects already published remain preserved; an
orphan object is not a usable receipt. A final receipt link can be visible if
failure occurs during its directory fsync or staging retirement, but the
remaining `.partial` marker makes the resolver reject that observation.

Staging retirement is deliberately the last fallible publication action. Its
deletion is not separately fsynced: the final link and its data are already
durable. After power loss, a retired marker may reappear, conservatively leaving
publication unconfirmed. Readers then refuse it pending separately scoped
inspection. No recovery, deletion, garbage collection, same-ID retry or automatic
repair is provided. Synthetic failure/process-exit tests exercise the protocol;
they do not certify physical hardware or every filesystem's power-loss behavior.

Modes 0444/0700 are useful protection, not proof against an authorized writer.
File hash checks remain necessary on every reuse and every resolution. Advisory
locks do not constrain a writer deliberately ignoring them or replacing the
lock/namespace. Independent receipt pins are necessary to anchor receipt
integrity outside such an authorized mutable filesystem.

## Offline resolver

`CaptureStoreV2.resolve_receipt(relative_path, expected_receipt_sha256=None,
require_success=True)` performs read-only offline verification and returns a
`ResolvedCapture(receipt, body)`. Constructing a store for resolution makes no
filesystem writes. The resolver:

- Accepts only the canonical observation/receipt path shape and refuses unsafe
  roots, ancestor/directory/file symlinks and non-regular referenced files.
- Rejects missing receipts, unfinished observation staging, malformed JSON,
  duplicate JSON keys, unknown schemas/fields and inconsistent identities,
  classification, UTC clocks, response presence or disposition.
- Checks completed metadata against the immutable pending reservation.
- Checks the canonical object path, exact byte count and SHA-256, then recomputes
  challenge disposition against the resolved bytes.
- Checks the exact receipt SHA-256 when an independently trusted pin is supplied.
- Rejects `FAILED` observations by default. `require_success=False` is an explicit
  forensic inspection option; it does not relabel failure or qualify evidence.

The optional pin supports basic local resolution without a separate trust
anchor. Matching a receipt to its pending file alone cannot defeat an authorized
writer changing both. Body hashes and receipt pins establish byte binding, not
source authority, observation authenticity, completeness, semantic reliability
or forecasting eligibility.

## Observation boundary and failed evidence

The configured capture priority remains **200**. Scrapy calls response and
exception middleware in descending priority order. RetryMiddleware at 550 can
return a new Request before capture is reached; transient retried responses and
exceptions are therefore **not all captured**. Only the responses/exceptions
that reach this existing boundary are recorded. Final exhausted HTTP/network
failures and non-retried errors can reach it. Redirect middleware at 600 and
decompression at 590 also precede capture; intermediate redirect responses
normally do not reach it. Higher-priority middleware can short-circuit a request.

The fighter spider's existing HTTP cache remains enabled. Cache hits are
explicitly marked `from_cache=true`; a new local receipt for cached bytes does
not claim a new upstream fetch or an authentic contemporary source observation.
The writer retains detail and completed/upcoming event-listing response coverage.
A-Z fighter-discovery responses continue to pass through without capture.
Request exceptions at this boundary can carry classification `unknown`.

Responses with HTTP status >=400 preserve their exact bodies and are `FAILED`
with `http_error`. Other non-2xx responses are `FAILED` with
`non_success_http_status`. A detected challenge, even with HTTP 200, preserves
its exact body with `FAILED`, `access_challenge=true` and `access_challenge` in
its reasons. Multiple reasons coexist when appropriate. The original HTTP
status stays in the receipt. To retain established downstream behavior, an
HTTP<400 challenge is returned as a synthetic 503 after RetryMiddleware has
already run. This adds no retry and does not reenter the retry chain.

The existing detector recognizes the conjunction of `Checking your browser`
and `/__c`, excluding bodies with the legacy detail-page markers. Its limits
are unchanged; `SUCCEEDED` means a captured 2xx response without that detected
challenge, not parser success, authenticity, coverage or evidence qualification.
Empty HTTP bodies remain real zero-byte objects with SHA-256 of the empty byte
string. An empty HTTP-error body remains failed.

A network/request exception has no response: `response_present=false`,
`http_status=null`, `body=null`, `FAILED`, and `request_exception`. No HTML,
zero-byte surrogate or invented body hash is emitted. The implementation never
calls exception str/repr or exports exception arguments, traceback or arbitrary
type names. Only fixed category tokens are recorded. Cookies, authorization
headers, other request/response headers and credentials are not exported in
capture metadata or v2 logging. Exact server-returned body bytes are preserved
as received, rather than scrubbed or rewritten.

The capture URL validator permits exact public HTTP/HTTPS UFCStats URLs with
the existing `page`/`char` query forms. It refuses userinfo, arbitrary query
tokens, fragments, unsafe/control characters and foreign hosts before exporting
metadata, using secret-safe errors. Refusal does not redact a URL into a false
identity. No browser-session/header behavior, access-control behavior or
acquisition policy is changed by this recording validation.

## Reader compatibility and deferred integration

| Reader/consumer | Compatibility after this phase |
|---|---|
| New `CaptureStoreV2.resolve_receipt` | Resolves and verifies v2; failure bodies require forensic opt-in. |
| Legacy `RawCaptureMiddleware` and its tests | Unchanged legacy writer/diagnostics; no v2 interpretation. It is no longer the normal configured capture writer. |
| `spiders/incremental.py`; event/fight/fighter subclasses | Continue reading historical `fetch_manifest.csv` and existing parsed CSVs. No v2 manifest skip source was added. |
| Repaired aggregate spider and round spider | Continue using their own parsed-output completion/selection behavior; no v2 integration or selection change. |
| `build_fighter_queue.py`, `tools/recover_cached_upcoming_fights.py` | Legacy fixed raw paths only. New v2 fight/event bytes are not discovered automatically. Neither utility was executed. |
| `event_coverage_report.py`, `stats_coverage_report.py`, `smoke_check.py` | Legacy manifest/raw checks only; future v2 successes/failures do not update these audit counts. |
| `tools/diagnose_phase5c3_sources.py`, `tools/audit_training_lineage.py` | Continue examining legacy manifest/body references and historical data. No historical diagnostic was regenerated in place. |
| `modeling/scoring_inputs.py`, `modeling/phase5_history_identity.py` | Legacy fixed entity filenames/manifest lineage only; no latest aliases or automatic v2 discovery. |
| Phase 5C.1/5C.2 source/evidence readers | Continue requiring their distinct independently pinned table/evidence package contracts. They do not accept a raw v2 observation receipt as such a package. |
| Parsers/exporters/warehouse loaders | Continue receiving Scrapy responses or parsed CSVs through existing interfaces. Warehouse loaders do not resolve raw v2 receipts. |

Future normal crawls therefore stop adding legacy capture-manifest rows and
stop refreshing fixed raw entity files. Legacy audit counts and raw-based queue
discovery remain tied to preserved legacy files. Existing selection code still
uses those historical manifest entries and parsed output; this phase neither
expands its skip rules nor makes failed v2 observations supply completion.

A later integration must explicitly choose observation identities/selection
rules, resolve and hash-check receipts, preserve failure/incomplete/cached states,
retain exact URL/version provenance, and define each consumer's completion
semantics. Evidence intake also requires independently qualified source authority
and the unchanged v1 claims/contracts. Such adapters, queue integration, parser
replay and warehouse publication are separate scopes. No latest aliases or
silent compatibility projections were created here.

## Offline verification

Existing installed dependencies were used; nothing was installed. Synthetic
responses/CSVs/files live in temporary directories. A temporary process-wide
runner blocks socket connections, sends-to/DNS and real `warehouse.db` imports
before test collection. V2 tests additionally block socket sends and real
warehouse/psycopg imports; forked writers inherit those guards and communicate
through local OS pipes. Loader regressions retain their synthetic database
module/mocks. Dotenv loading, plugin autoload, bytecode and pytest cache writes
were disabled. No crawler engine, real downloader, live integration test,
broad unrelated suite or model-fitting test ran.

Coverage includes identical and changed bodies, distinct jobs, same-day listing
URLs, 404/503/challenge failures, non-2xx responses, empty versus absent bodies,
secret-safe/unprintable exceptions, credential URL refusal, eight concurrent
writers sharing one object, six writers racing on one identity, duplicate-ID
refusal, interrupted pending/body/receipt links, body and receipt-directory fsync
failure, interrupted staging retirement, actual process exit, hash/length
tampering, trusted receipt-pin mismatch, metadata/identity/clock tampering,
missing objects, unsafe paths and root/ancestor/directory/file/lock symlinks.
Configuration/factory tests select v2 without filesystem writes. Synthetic
middleware-manager tests demonstrate transient retry-response exclusion, final
503 preservation and no new retry of the challenge's synthetic 503. Temporary
legacy bodies/manifests remain unchanged across v2 observations.

| Relevant safe checks | Result |
|---|---:|
| New v2 capture cases | 72 passed |
| Existing legacy capture/browser-header cases | 3 passed |
| Accepted aggregate-selection cases | 68 passed |
| Existing event selection, aggregate/round parser, append-safe export cases | 5 passed |
| Legacy transform regressions | 19 passed |
| Strict v2 outcome adapter regressions | 285 passed |
| Mock-only strict loader regressions | 10 passed |
| Preserved Phase 5C.3 diagnostic regressions | 10 passed |
| **Distinct relevant checks** | **472 passed** |

The combined scraper run passed 148 checks in 3.63 seconds. The final focused
v2 run after the last safe-category adjustment passed 72 in 3.55 seconds. The
outcome group passed 314 in 0.22 seconds; diagnostics passed ten in 0.030 seconds.
Four existing Scrapy `start_requests()` deprecation warnings remain unchanged.
Initial symlink fixtures incorrectly expected a damaged prior receipt to block
a new observation that never traverses it; those expectations were corrected.
The prior receipt still fails resolution and its symlink target is untouched.
No unresolved test failure remains.

Successful commands (the first two run from the scraper package directory):

```text
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  ../.venv/bin/python /tmp/phase5c6-immutable-capture/offline.py pytest -q -p no:cacheprovider \
  tests/test_raw_capture_v2.py tests/test_raw_capture_middleware.py \
  tests/spider_tests/fight_stats_spider_test.py tests/spider_tests/events_spider_test.py \
  tests/parser_tests/fight_stats_parser_test.py tests/parser_tests/fight_stats_by_round_parser_test.py \
  tests/test_exporters.py
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  ../.venv/bin/python /tmp/phase5c6-immutable-capture/offline.py pytest -q -p no:cacheprovider \
  tests/test_raw_capture_v2.py
# Remaining commands run from repository root.
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  python3 /tmp/phase5c6-immutable-capture/offline.py pytest -q -p no:cacheprovider \
  warehouse/tests/test_transform.py warehouse/tests/test_strict_fight_outcomes_v2.py \
  warehouse/tests/test_load_fights_strict_outcomes_v2.py
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 \
  scraper/UFC-Web-Scraping-main/.venv/bin/python /tmp/phase5c6-immutable-capture/offline.py diagnostics-tests
PYTHONDONTWRITEBYTECODE=1 python3 /tmp/phase5c6-immutable-capture/offline.py diagnose \
  > /tmp/phase5c6-immutable-capture/phase5c3-diagnostic-replay.json
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 \
  scraper/UFC-Web-Scraping-main/.venv/bin/python /tmp/phase5c6-immutable-capture/offline.py compare \
  > /tmp/phase5c6-immutable-capture/selection-comparison.json
PYTHONDONTWRITEBYTECODE=1 python3 /tmp/phase5c6-immutable-capture/preservation.py baseline
PYTHONDONTWRITEBYTECODE=1 python3 /tmp/phase5c6-immutable-capture/preservation.py verify
```

The baseline ran before repository edits. Scratch runners, inventories and replay
output remain outside Git in `/tmp/phase5c6-immutable-capture/`.

## Preservation and accepted pins

The pre-edit hash-only baseline covers **7,948 tracked files and 49,703 local
files**, excluding Git, dependencies/caches, bytecode and root `.env`. Protected
holdout/model components were hashed only. Final comparison found no missing
pre-existing file, no unrelated modification, exactly one authorized existing
file change and only the three intended additions. Every existing source body,
CSV, manifest row, frozen artifact, historical diagnostic and accepted contract
remains byte-identical. No runtime capture namespace was added under `data/`.

Exact inventory/member hashes and independently accepted checksum-root pins
were verified without amendment:

| Accepted artifact inventory | Members excluding its `checksums.json` | Checksum-root SHA-256 |
|---|---:|---|
| Phase 5B.3 role-aware preparation | 88 | `7d36dc2bdb2a99eb81a44424fe603a6beab74d05435e41c8a1489cea11f0bd4f` |
| Phase 5B.4 fixed current challenger | 49 | `59ffd4995946cc9ec7512f2f14caade09fd3144d2cfb3ee2d3b9b0951f5ca300` |
| Phase 5C.1 accepted shadow contract | 3 | `83cf581f3f90b7c0f85fcc8063d87330843bec657b03da7f5135ddde450d7514` |
| Phase 5C.2 activation | 9 | `21672682c5a7096fdc94ac8344a90755186c4cfadd92f9cb91ee2d5e4d98adfd` |
| Phase 5C.2 capture observation | 8 | `e2791af3b24ea6785e816d3c19e2ccf34020ac25acad84692874050bbcefa6a8` |
| Phase 5C.2 BLOCKED journal run | 15 | `33d8e983476c408ee7a67f785c27856d58b3a0c9a0a3ea3fc2373a5bbe61d81c` |

The Phase 5C.2 capture-receipt pin remains
`c09d3adf1065edda3582eabd9df6126d842bfa19e84ef7e222bddc384c246a18`.
All 13 embedded preparation files equal their accepted originals. Live code
pins pass for Phase 5B.3 (37), Phase 5B.4 (8), embedded preparation (37), accepted
Phase 5C.1 contract (12) and Phase 5C.2 activation (5).

Additional unchanged implementation/evidence hashes:

| File/component | SHA-256 |
|---|---|
| Legacy `ufc_scraper/middlewares.py` | `20abf2c9a430def355662ca59637c7489d93db6fc4f1961d19ce87b9fe98ec2f` |
| Accepted aggregate spider | `40e9b93962e7d26646a36ee29be68bdc65d6a4d553bda081a41402cc6c623528` |
| `warehouse/strict_fight_outcomes_v2.py` | `aec01516c479b3b7366f1b65f3980b1f1d9d34940826cdf00101c780be8eb019` |
| `warehouse/load_fights.py` | `84e9446d6dc87d78c8e7315b1b3066585449846a841b6d833871fccd744adec5` |
| Legacy `warehouse/transform.py` | `616ad63cf8402718ed8f912e7a3d75ba778109bac26745304256ef08eaef5a44` |
| Phase 5C.3 source-health report | `675273823725413bcce8ec439c12500454534357f371c25a52f9617ddd4dd86d` |
| Phase 5C.5 selection report | `207c7e780b2c3dc7ddc84669e2b1f41d9d4dd54c4b84904ddcdc4183d7d5729e` |
| Phase 5C.1 evidence contract v1 | `87df5edde953665c86358ae2d2bcbfe0ff0c058874228f00ea912308d4ee49ae` |

The historical Phase 5C.3 diagnostic replay, written only in `/tmp`, is
byte-identical to the saved JSON: 10,320 bytes, SHA-256
`b2fa5a75617096ee25739fc47d868d347be356b75f390430f1fa5313530febc4`.
The Phase 5C.5 comparison output is also byte-identical to its accepted scratch
output: 192 aggregate candidates, 8,693 completion-based skips, all 188 former
missing-aggregate suppressions still repaired, and 72 round candidates. These
remain local selection observations; no missing statistics were recovered.

Settings SHA-256 changed from
`3b885f599172a7e0371cd59b74ace642e6dd1b96787345d18cc3b044eb9103a7` to
`1ea6a1e62c70187d981777f1de5c55ab52dc1e068ca120fe45e97a3cd0447813`.
No historical preservation baseline or accepted pin was rewritten to accommodate
that authorized configuration change.

Scoped whitespace, content/secret, syntax and size reviews passed. Only the
four intended files enter Git; no generated captures, credentials, dependency
directories or ignored runtime artifacts are included.

## Limits, blockers and smallest separate next step

This repair preserves future observations reaching the existing boundary. It
cannot reconstruct versions already overwritten by the legacy writer. A
manifest hash alone cannot recover absent bytes; particular Git/frozen copies
may preserve some older versions but no recovery or migration was attempted.
The accepted 100,335 historical mismatching successful references remain
historical diagnostics, not repaired observations.

No completeness certificate, source authority, new title/non-title assertion,
complete participant history, verified debut, identity transition or new timing
window is supplied. Cached observations and incomplete/failed captures retain
their limitations. Queue freshness, stale upserts, existing false-NC records,
aliases, April's unresolved relationship and missing histories remain separate
blockers. No provider was qualified and no evidence-contract amendment applied.
No previous acquisition budget or cutoff was reused or backdated. Capture
integrity alone cannot unblock forecasting or historical comparison.

The smallest separately scoped next step is an **offline, read-only v2
observation inventory/audit adapter** using this resolver and temporary fixtures.
It should list every observation and incomplete reservation, preserve explicit
failed/cached states, and require explicit receipt selection and trusted pins
without choosing latest versions. Keep incremental scheduling, queue regeneration,
parser replay, legacy migration, source requests, warehouse reconciliation and
evidence qualification outside that adapter. Any subsequent consumer integration
needs its own bounded authorization and completion semantics.

**Prospective evidence remains BLOCKED. Historical comparison remains
STILL_BLOCKED.** The capture repair does not certify source completeness or
authorize fitting, forecasting, evaluation or production use.

## Git publication handoff

Follow `next-phase-git-handoff.md`: finish this report; review intended changes
for scope, whitespace, secrets and size; commit implementation/settings/tests
together and this report separately; verify committed bytes and accepted
inventories; push normally to verified `origin/main`. Never force-push, rewrite
history, add ignored runtime/dependency files or perform deployment/data work.
Actual commit hashes, push result and final worktree status belong in the final
response, avoiding a self-referential report-update commit loop.
