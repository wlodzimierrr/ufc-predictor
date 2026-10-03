# Phase 5C.7 read-only v2 observation inventory and audit

**Implementation COMPLETE; offline verification PASSED. Prospective evidence
remains BLOCKED. Historical comparison remains STILL_BLOCKED.**

The new offline adapter inventories an explicitly supplied immutable-v2 root,
including incomplete reservations and refused entries, and provides exact,
caller-pinned receipt inspection. It delegates receipt, reservation, source URL,
clock, disposition and exact body-binding verification to the accepted
`CaptureStoreV2` resolver. The accepted writer/resolver is unchanged.

The actual repository root `data/raw/ufcstats_v2/` is **absent**. Its read-only
inventory reports **NO_OBSERVATIONS**, zero observations in every state and no
staging markers. The namespace was not created. Synthetic captures used for
verification exist only in temporary test directories, not repository `data/`.
Absence supplies no source evidence or evidence-readiness claim.

No acquisition, downloader/crawler execution, queue regeneration, parser replay
operation, migration, warehouse operation, fitting, model execution, forecasting,
evaluation or production operation occurred. Existing safe regression tests
used their established local/synthetic fixtures and database mocks; no parser
export or source artifact was regenerated. No evidence or acquisition policy
was changed. Network use was confined to Git transport for this authorized
handoff.

## Accepted handoff and starting state

Read completely before implementation:

- `phase5c6-immutable-raw-capture-report.md`.
- `phase5c5-aggregate-consumer-selection-report.md`.
- `docs/phase5c1-shadow-evidence-contract-v1.md`.
- `next-phase-git-handoff.md`.

Searches, including hidden repository paths and repository ancestors, found no
applicable `AGENTS.md`. The accepted `CaptureStoreV2` implementation, its tests,
legacy capture behavior, relevant regression tests and all nine frozen
`code_versions.json` inventories were inspected. Accepted inventories and live
code pins were verified before repository edits.

Starting worktree was clean on `main`, HEAD
`b948f56292abcb30c321152d019bf252fab1fab6`. The branch upstream is `origin/main`
(`origin`, `refs/heads/main`); fetch/push remote is
`https://github.com/wlodzimierrr/ufc-predictor.git`. A Git-only remote-ref check
returned the same commit. There were no unrelated edits to preserve.

Only four files are added:

| File | Purpose |
|---|---|
| `scraper/UFC-Web-Scraping-main/ufc_scraper/ufc_scraper/capture_inventory_v2.py` | Compact read-only inventory, shared-read consistency guard and pinned selection adapter. |
| `tools/audit_raw_capture_v2.py` | Explicit-root CLI with compact stdout JSON. |
| `scraper/UFC-Web-Scraping-main/ufc_scraper/tests/test_capture_inventory_v2.py` | Temporary-fixture offline and concurrency tests. |
| This report | Interfaces, semantics, guarantees, verification and bounded handoff. |

No existing repository file, consumer, frozen artifact or accepted repair is
modified. Incremental scheduling, queue builders, fixed-path readers, parsers,
warehouse loaders and scoring contracts retain their accepted bytes.

## Inventory and selection interfaces

Use the existing scraper environment; no dependency installation is required.
Both CLI commands require an explicit root; neither chooses a default namespace.
Commands below run from the repository root:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 \
  scraper/UFC-Web-Scraping-main/.venv/bin/python tools/audit_raw_capture_v2.py \
  inventory --root /absolute/path/to/v2-root

PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 \
  scraper/UFC-Web-Scraping-main/.venv/bin/python tools/audit_raw_capture_v2.py \
  inspect --root /absolute/path/to/v2-root \
  --receipt observations/JOB_UUID/OBSERVATION_UUID/receipt.json \
  --receipt-sha256 INDEPENDENTLY_TRUSTED_SHA256

# Explicit forensic inspection retains failed/cached labels:
# add --forensic to the exact pinned inspect command above.
```

The literal placeholders must be replaced with exact canonical UUIDs and a
lowercase 64-character SHA-256. The full relative receipt path supplies the
canonical `(job_run_id, observation_id)` identity. URL-only, latest, timestamp,
filename preference and preferred-outcome selection are unsupported.

| Python API | Result and refusal behavior |
|---|---|
| `inventory(root)` | JSON-compatible complete enumeration or explicitly unstable/refused result; every accessible reservation remains a separate row. |
| `select_capture(root, relative_path, expected_receipt_sha256, forensic=False)` | Accepted `ResolvedCapture` with exact verified bytes; fixed-code `InventoryError` on refusal. The expected pin is mandatory. |
| `inspect_receipt(root, relative_path, expected_receipt_sha256, forensic=False)` | Compact JSON-compatible selection result, with metadata and body reference but no body contents. |

Output version is `ufcstats_v2_observation_inventory_v1`. CLI JSON uses sorted
keys, compact separators, escaped strings and one terminal newline. Observation
rows are sorted by exact relative identity path, never by clocks. Issue paths
and staging-marker paths are sorted. There is no generated inventory clock,
random identifier or root-construction clock in output, so unchanged namespace
bytes produce deterministic output. Matching URLs/hashes do not merge rows.

CLI exit 0 means enumeration completed (`INVENTORIED`), observed absence
(`NO_OBSERVATIONS`), or successful inspection (`VERIFIED_CAPTURE_BYTES`). It
does **not** mean every inventoried row is usable. Exit 2 covers unstable/refused
results, namespace issues and argument errors. Output goes only to stdout;
argument help/errors use argparse's normal streams. There is no report-file
option or adapter output-file writer. Any externally requested persistence must
be separately arranged outside the capture namespace with exclusive,
non-overwriting publication. The CLI disables imported bytecode writes.

## Observation states and metadata semantics

Every enumerated `observations/<job>/<observation>` entry is represented even if
it is malformed or not a directory. Unsafe/unreadable parents are reported in
`namespace_issues`; the adapter never traverses them to invent child identities.
Unrecognized observation entries, invalid empty job directories, unexpected
top-level entries, special files and symlinks are explicit issues.

| Row state | Meaning |
|---|---|
| `VERIFIED_SUCCEEDED` | Completed receipt/reservation and exact referenced bytes resolve; source disposition is `SUCCEEDED`, and `from_cache=false`. |
| `VERIFIED_FAILED` | Completed receipt resolves with original `FAILED` disposition; body can be present or absent for an exception. |
| `VERIFIED_CACHED` | Completed receipt resolves with `from_cache=true`; original success/failure disposition remains in metadata. |
| `INCOMPLETE` | Reservation lacks a receipt, or observation publication has a `.partial` marker, including a marker alongside a visible final receipt. |
| `INVALID_RECEIPT` | Receipt JSON/schema/identity/reservation/clock/disposition or other resolver validation refuses completion. Includes missing/invalid pending metadata. |
| `MISSING_OBJECT` | Otherwise resolver-validated completed metadata references missing body storage. |
| `CORRUPT_OBJECT` | Referenced regular body fails the accepted exact length/hash verification. |
| `UNSAFE_PATH` | Observation or referenced file/directory cannot be safely read, including links and special files. |
| `INVALID_IDENTITY` | Noncanonical reservation identity; no UUID is invented. |

Every state appears in `counts`, including zero counts. Cache state and source
disposition are independent: a cached HTTP failure is `VERIFIED_CACHED` with
`metadata.disposition=FAILED`. `COMPLETE_VERIFIED` describes receipt publication
and byte binding, not source success. Neither source success nor completed
publication proves parser success, authenticity, coverage or source completeness.

Rows include identity/path, receipt and pending SHA-256 measurements where
safely readable, publication/verification state, body availability, validated
metadata and a verified body reference where available. Metadata contains the
accepted exact public request/source URLs, entity/listing role, boundary,
job/request/observation/receipt-preparation clocks, response presence, HTTP
status, disposition/reasons, challenge flag, coarse exception category and cache
flag. Clocks retain their existing middleware-boundary meanings; none is
relabeled as an upstream fetch or freshness certificate.

`metadata` and `verified_body` remain null for incomplete/refused rows. Pending
JSON is measured, not projected into completed metadata. Its expected body
reference is never exported as verified availability, even if an object happens
to exist. A zero-byte response is `VERIFIED_BYTES` with the empty-byte SHA-256;
an exception with no response is `ABSENT_EXCEPTION_BODY` with no invented object.

All `.partial` paths seen during the first scan are reported separately. The
accepted resolver's observation-staging rule is preserved. Object staging is
visible but does not independently invalidate another completed receipt that
already resolves the exact shared object. Unreferenced/orphan object content
is not audited for semantic validity, repaired or garbage-collected.

Receipt hashes produced by inventory have `receipt_integrity=MEASURED_ONLY` and
`receipt_pin_authority=NOT_ESTABLISHED_BY_INVENTORY`. They bind the inventory
measurement to the resolver read but cannot become an independently trusted
authority decision merely by being copied into a selection call.

## Pinned selection and forensic boundaries

Selection passes the exact caller-supplied receipt pin to
`CaptureStoreV2.resolve_receipt`; all its closed-schema, pending-reservation,
path, clock, URL, challenge-disposition and body length/hash checks remain in
force. The audit subclass only translates the accepted body resolver's errors
into fixed categories; it delegates verification unchanged. No schema/body
validation was copied, weakened or replaced, and no arbitrary exception text is
serialized.

Default selection invokes the resolver with `require_success=True` and additionally
refuses cached observations. Incomplete, invalid, unsafe, pin-mismatched and
uncoordinated views refuse in both modes. Failed/cached observations require
`forensic=True`/`--forensic`; original disposition and cache labels are retained.
Forensic mode does not change bytes, clocks or evidence policy.

A matched pin is labelled `CALLER_SUPPLIED_PIN_MATCHED`. The caller remains
responsible for its trust provenance. Successful selection means **verified
capture bytes only**. All results retain `prospective_evidence=BLOCKED` and
`historical_comparison=STILL_BLOCKED`. No source-authority attestation,
forecasting-evidence adapter, automatic latest alias or legacy compatibility
projection is introduced.

## Read-only and consistency guarantees

The reader uses the accepted descriptor-anchored root traversal with
`create=False` and refuses ancestor/root/namespace/file symlinks. Snapshot
traversal uses no-follow directory opens and `stat(..., follow_symlinks=False)`;
regular-file reads retain the accepted nonblocking no-follow checks. Symlink
targets and FIFOs are never read as capture content.

The adapter never creates a root/directory/file, modifies permissions, writes a
lock, fsyncs storage, updates a receipt, repairs/replaces an object, resumes a
reservation, deletes a stage or performs garbage collection. The existing
`.writer.lock` is opened **O_RDONLY**, without `O_CREAT`, and receives only a
nonblocking **shared** advisory read lock. Closing its descriptor releases the
kernel lock without writing lock-file bytes. Read locks can briefly delay a
cooperating writer; a busy writer never makes the reader wait.

Consistency strategy:

1. Open the explicit root safely and attempt the existing shared read lock.
2. Snapshot namespace names and device/inode/mode/size/mtime/ctime/link-count
   metadata; enumerate observations and resolve their exact bytes.
3. Repeat the namespace scan while holding the same shared lock, compare both
   scans and safely reopen the root path to check the anchored root identity.
4. Any detected change, unreadable scan, excessive unexpected directory depth
   or active writer reports `UNSTABLE_INVENTORY`. A populated observation/issue
   inventory without the existing lock is likewise uncoordinated/unstable.
   Selection requires the existing shared lock and unchanged scans.

`INVENTORIED`/`INVENTORIED_WITH_ISSUES` describe an unchanged view coordinated
with cooperating local writers. Stable namespace issues remain visible and
have exit 2; valid rows are not silently omitted because another row refuses.
`UNSTABLE_INVENTORY` rows/counts are best-effort observations, not a consistent
complete snapshot or selection source. Writes completing between scans can
make newly added observations absent from those tentative rows; callers must
retry later under their own authority, without reader repair/resume actions.

Missing roots receive two safe absence probes. Empty roots receive two scans.
`ROOT_ABSENT_TWO_PROBES` and an empty `NO_WRITER_LOCK` view describe observed
absence, not a locked completeness certificate against future creation.
Root creation between absence probes is explicitly unstable. A root/lock that
cannot safely be opened produces `REFUSED`, not `NO_OBSERVATIONS`.

The guarantee targets the writer's accepted local POSIX advisory-lock semantics.
It does not certify distributed-filesystem behavior or prevent an authorized,
noncooperating writer from making undetectable changes and restoring metadata
between probes. Namespace fingerprints are change detectors, not external
trust anchors. Readers obey normal filesystem access-time policy; kernel atime
updates from reads are not suppressed or restored. Preservation compares bytes,
permissions, identities and write-related metadata; absolute atime preservation
would require an operator-supplied filesystem policy outside this phase.

## Offline verification and preservation

All dependencies were already installed. Process-wide scratch guards reject
socket connections/DNS and real `warehouse.db` imports before test collection.
New tests additionally reject socket sends and warehouse/psycopg imports.
Forked synthetic writers inherit those guards and coordinate through local OS
pipes. Tests guard audit operations against write/create/truncate flags,
exclusive locks, chmod/fsync/link/unlink/rename/truncate/write and related
filesystem mutations. Exact temporary bytes and write metadata are unchanged
across inventory, selection and CLI calls. Symlink target access times are also
checked to establish that the targets were not followed.

Tests cover multiple observations sharing one object; changed versions and
distinct listing URLs/jobs; success, HTTP/challenge/non-2xx failure and cached
success/failure; empty versus absent exception bodies; directory-only/pending-only
reservations; observation/object staging; missing/corrupt bodies; malformed,
duplicate-key, identity/clock/reservation/URL/body-binding/disposition tampering;
explicit pin mismatch; unsafe/null-byte/traversal/symlink/FIFO paths; missing/empty
roots; deterministic JSON and exact selection; real concurrent publication;
noncooperating reservation/receipt/lock/root changes; creation between absence
probes; unreadable scans; and explicit network/database guard refusal.

| Relevant safe checks | Result |
|---|---:|
| New inventory/selection cases | 76 passed |
| Accepted immutable-v2 capture cases | 72 passed |
| Legacy capture/browser-header cases | 3 passed |
| Accepted aggregate-selection cases | 68 passed |
| Existing event selection, aggregate/round parser, export regressions | 5 passed |
| Legacy transform regressions | 19 passed |
| Accepted strict-v2 outcome adapter regressions | 285 passed |
| Mock-only strict loader regressions | 10 passed |
| Preserved Phase 5C.3 diagnostic regressions | 10 passed |
| **Distinct relevant checks** | **548 passed** |

The combined initial scraper run passed 217 cases (69 new plus 148 accepted)
in 7.28 seconds. After additional unsafe-file/namespace tests and conservative
refusal handling, the final inventory suite passed 76 in 4.23 seconds. Outcome
regressions passed 314 in 0.23 seconds; diagnostics passed ten in 0.029 seconds.
Four established Scrapy `start_requests()` deprecation warnings remain unchanged.
An initial root-replacement test collided with its own previous detached fixture
directory; unique detached names corrected the fixture. The CLI's deliberate
repository-local import received a narrow E402 annotation. No unresolved test
or lint failure remains.

Successful verification commands (first two from the scraper package directory):

```text
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  ../.venv/bin/python /tmp/phase5c7-v2-inventory/offline.py pytest -q -p no:cacheprovider \
  tests/test_capture_inventory_v2.py tests/test_raw_capture_v2.py tests/test_raw_capture_middleware.py \
  tests/spider_tests/fight_stats_spider_test.py tests/spider_tests/events_spider_test.py \
  tests/parser_tests/fight_stats_parser_test.py tests/parser_tests/fight_stats_by_round_parser_test.py \
  tests/test_exporters.py
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  ../.venv/bin/python /tmp/phase5c7-v2-inventory/offline.py pytest -q -p no:cacheprovider \
  tests/test_capture_inventory_v2.py
# Remaining commands run from repository root.
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  python3 /tmp/phase5c7-v2-inventory/offline.py pytest -q -p no:cacheprovider \
  warehouse/tests/test_transform.py warehouse/tests/test_strict_fight_outcomes_v2.py \
  warehouse/tests/test_load_fights_strict_outcomes_v2.py
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 \
  scraper/UFC-Web-Scraping-main/.venv/bin/python /tmp/phase5c7-v2-inventory/offline.py diagnostics-tests
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 \
  scraper/UFC-Web-Scraping-main/.venv/bin/python /tmp/phase5c7-v2-inventory/offline.py \
  audit inventory --root /home/wlodzimierrr/ufc-data/data/raw/ufcstats_v2
PYTHONDONTWRITEBYTECODE=1 python3 /tmp/phase5c7-v2-inventory/preservation.py baseline
PYTHONDONTWRITEBYTECODE=1 python3 /tmp/phase5c7-v2-inventory/preservation.py verify
```

Bytecode, dotenv loading, plugin autoload and pytest cache writes were disabled.
No live integration test or broad unrelated/model suite ran. Historical
diagnostic JSON, comparisons, preservation baselines and code pins were not
regenerated or amended. Scratch guards/baselines remain outside Git under
`/tmp/phase5c7-v2-inventory/`.

The pre-edit hash-only baseline covers **7,951 tracked files and 49,706 local
files**, excluding Git, dependencies/caches, bytecode and root `.env`. Protected
holdout/model components were hashed only. Final preservation comparison found
no missing pre-existing file, no changed existing file, no unrelated change and
only the four intended additions. All existing raw/CSV/manifest bytes, frozen
artifacts, historical diagnostics, repairs and contracts remain unchanged.

| Accepted inventory | Members excluding own checksum file | Unchanged checksum-root SHA-256 |
|---|---:|---|
| Phase 5B.3 role-aware preparation | 88 | `7d36dc2bdb2a99eb81a44424fe603a6beab74d05435e41c8a1489cea11f0bd4f` |
| Phase 5B.4 fixed challenger | 49 | `59ffd4995946cc9ec7512f2f14caade09fd3144d2cfb3ee2d3b9b0951f5ca300` |
| Phase 5C.1 shadow contract | 3 | `83cf581f3f90b7c0f85fcc8063d87330843bec657b03da7f5135ddde450d7514` |
| Phase 5C.2 activation | 9 | `21672682c5a7096fdc94ac8344a90755186c4cfadd92f9cb91ee2d5e4d98adfd` |
| Phase 5C.2 capture observation | 8 | `e2791af3b24ea6785e816d3c19e2ccf34020ac25acad84692874050bbcefa6a8` |
| Phase 5C.2 BLOCKED journal run | 15 | `33d8e983476c408ee7a67f785c27856d58b3a0c9a0a3ea3fc2373a5bbe61d81c` |

Every inventory member verifies. All 13 embedded preparation files equal their
accepted originals. Live code pins pass for Phase 5B.3 (37), Phase 5B.4 (8),
embedded preparation (37), the accepted shadow contract (12) and activation (5).
The Phase 5C.2 receipt pin remains
`c09d3adf1065edda3582eabd9df6126d842bfa19e84ef7e222bddc384c246a18`.

Additional unchanged implementation pins:

| File | SHA-256 |
|---|---|
| Accepted `raw_capture_v2.py` | `ac5713be936e69734f629ab0118e3740800b4507146d8b24417ac7898bfb28d3` |
| Accepted `settings.py` | `1ea6a1e62c70187d981777f1de5c55ab52dc1e068ca120fe45e97a3cd0447813` |
| Legacy `middlewares.py` | `20abf2c9a430def355662ca59637c7489d93db6fc4f1961d19ce87b9fe98ec2f` |
| Accepted aggregate spider | `40e9b93962e7d26646a36ee29be68bdc65d6a4d553bda081a41402cc6c623528` |
| Accepted strict outcome adapter | `aec01516c479b3b7366f1b65f3980b1f1d9d34940826cdf00101c780be8eb019` |
| Accepted strict loader | `84e9446d6dc87d78c8e7315b1b3066585449846a841b6d833871fccd744adec5` |

Whitespace, scoped syntax/lint, secret/content and artifact-size review passed.
Only compact source/tests/documentation enter Git; capture bodies are referenced,
not copied into inventory output or handoff artifacts.

## Actual inventory, limitations and next integration boundary

Actual command result:

| Observation/namespace measure | Count/status |
|---|---:|
| Root `data/raw/ufcstats_v2/` | Absent; `ROOT_ABSENT_TWO_PROBES` |
| Inventory status | `NO_OBSERVATIONS` |
| Every row state in the status table | 0 |
| Total observations | 0 |
| Namespace issues / staging markers | 0 / 0 |
| Runtime files or namespace created | 0 |

This reader inventories the accepted middleware boundary only. It cannot
recover overwritten legacy versions, prove missing reservations never existed,
certify source-wide discovery/history completeness or expose observations that
never reached capture priority 200. Retry/redirect short-circuiting,
decompression and cache semantics retain their accepted limitations. It reads
and hashes referenced body bytes per resolution; there is no body-verification
cache. Namespace scans are proportional to existing entries and materialize
their metadata; extremely large/unexpected namespaces may require a separately
scoped scalable reader. There is no long-lived snapshot after selection returns.
Later use must preserve the returned bytes or re-resolve the same trusted pin.

The smallest next step is a **separately authorized offline audit consumer**
accepting an operator-supplied list of exact receipt paths and independently
trusted pins, calling this selector and recording its own role/completion
decisions. Before any queue integration, the consumer/operator must supply:

- Explicit observation identities and predeclared selection rules retaining
  versions and failures, without a latest or preferred-outcome alias.
- Independent receipt-pin provenance and separately reviewed source authority,
  authenticity, boundary/cache/freshness semantics and acquisition authority.
- The required source role, exact listing/detail URL semantics, observation
  clocks and coverage limits for that consumer.
- Its own parsed-output/completion rules; capture success cannot justify an
  aggregate/round/parser scheduling skip or claim missing discovery is complete.
- Separate authorization for queue regeneration, acquisition, parser replay,
  migration or warehouse work, and independently qualified evidence satisfying
  the unchanged Phase 5C.1 contract before any forecasting use.

Queue freshness, incomplete round coverage, stale upserts/false-NC records,
aliases, April's unresolved relationship, access challenges, title/non-title
assertions, complete participant histories and verified debuts remain unresolved.
This inventory supplies no new real observations or source qualification.
**Prospective evidence remains BLOCKED. Historical comparison remains
STILL_BLOCKED.**

## Git publication handoff

Follow `next-phase-git-handoff.md`: commit implementation/CLI/tests together,
then this report; verify committed bytes, accepted inventories and worktree;
push normally to verified `origin/main`. No force-push, history rewrite,
deployment or data operation is authorized. Actual commit hashes, push result
and final worktree status belong in the final response, avoiding a
self-referential report-update commit loop.
