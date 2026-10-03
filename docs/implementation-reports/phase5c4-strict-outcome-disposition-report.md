# Phase 5C.4 strict outcome disposition

**Implementation COMPLETE; offline verification PASSED. Prospective evidence
remains BLOCKED. Historical comparison remains STILL_BLOCKED.**

Future normal fight ingestion now rejects cancelled, empty resolved, partial,
contradictory and unsupported dispositions explicitly before any batch upsert.
The accepted legacy transform and all frozen artifacts remain byte-identical.
This repair prevents malformed future loads; it does not establish forecasting
readiness or correct previously loaded records.

No warehouse correction or source refresh occurred. No fitting, forecasts,
evaluation or production operation occurred. No source acquisition, warehouse
writes, data correction, model execution or evidence-contract changes occurred.
Git transport for the requested publication is the only network activity used.

## Authorization, inspection and preservation boundary

The accepted [Phase 5C.3 report](phase5c3-source-health-and-evidence-feasibility-report.md),
[diagnostic summary](phase5c3-source-diagnostics.json),
[normalization rules](../normalization-rules.md) and
[Git handoff](next-phase-git-handoff.md) were read completely before editing.
Ancestor/repository inspection found no applicable `AGENTS.md`.

Starting worktree was clean on `main`, HEAD
`dcf0042dcd56216cb95aecb86615448cd6039dc9`, tracking `origin/main` at the same
commit. Fetch and push remote were verified as
`https://github.com/wlodzimierrr/ufc-predictor.git`; upstream branch configuration
is `origin`, `refs/heads/main`. There were no unrelated edits to preserve.

Before editing, the accepted Phase 5B.3 preparation, Phase 5B.4 challenger and
embedded preparation inventories/code pins were inspected and verified.
`warehouse/load_fights.py` is absent from all nine frozen `code_versions.json`
files in `data/experiments/`. Its old hash appears in historical preservation
baselines, which record prior repository state rather than requiring the live
loader bytes for artifact reconstruction. Those baseline files remain unchanged.
No accepted code-version pin requires changing this loader's old implementation.

The only existing file modified is
[`warehouse/load_fights.py`](../../warehouse/load_fights.py), through a one-line
import replacement. Added files are the versioned adapter, two focused test
modules and this report. No manifest, checksum check, schema, evidence contract,
registry, source file, capture or model component was edited or repinned.

## Exact admission decision table

Status and outcome tokens use the legacy-compatible whitespace/null treatment:
`None`, absent keys, empty strings and whitespace-only strings are empty;
other values are converted to strings and stripped. Matching is case-sensitive.
The established local source vocabulary, inspected read-only, is statuses
`upcoming`, `completed`, `canceled` and outcomes `W`, `L`, `D`, `NC`.
There are no new status/outcome aliases: for example, `scheduled`, `finished`,
`cancelled`, `WIN`, lowercase outcome letters and `N/C` are unsupported tokens.
The established cancellation spelling `canceled` is always rejected.

Here, **missing** status includes absent, null and blank status values. **Empty**
outcomes include absent, null and blank outcome values. These are admission
rules for the new adapter, leaving historical normalization evidence intact.

| Status | Fighter 1 outcome | Fighter 2 outcome | Disposition | Winner |
|---|---|---|---|---|
| Explicit `upcoming` | Empty | Empty | `upcoming` | None |
| `completed` or missing | `W` | `L` | `win` | `fighter_1_id` |
| `completed` or missing | `L` | `W` | `win` | `fighter_2_id` |
| `completed` or missing | `D` | `D` | `draw` | None |
| `completed` or missing | `NC` | `NC` | `nc` | None |
| `canceled` | Any | Any | Reject: `canceled_status` | No row produced |
| Any other nonempty status | Any | Any | Reject: `unsupported_event_status` | No row produced |
| Otherwise known/missing status | Any nonempty unsupported outcome token on either side | Any | Reject: `unsupported_outcome_token` | No row produced |
| `upcoming` | At least one nonempty known token, including resolved pairs | Any | Reject: `upcoming_with_outcomes` | No row produced |
| `completed` | Empty | Empty | Reject: `completed_without_outcomes` | No row produced |
| Missing | Empty | Empty | Reject: `missing_status_without_outcomes` | No row produced |
| `completed` or missing | Exactly one outcome empty | Other side a known nonempty token | Reject: `partial_outcome_pair` | No row produced |
| `completed` or missing | Both nonempty known tokens forming any other pair | Any | Reject: `contradictory_outcome_pair` | No row produced |

The rejection rows are evaluated in the order shown after the valid mappings:
cancellation, unsupported status, unsupported outcome token, upcoming/outcome
contradiction, empty outcomes, partial pair, contradictory pair. Thus `upcoming`
with an unsupported outcome reports `unsupported_outcome_token`; `upcoming`
with `W/L`, `D/D`, `NC/NC`, a partial known pair or `W/W` reports
`upcoming_with_outcomes`. Cancellation takes precedence even if outcomes claim
a result. Every disposition outside the five admitted mappings is rejected.

Missing status plus an explicit resolved pair remains compatible with historical
CSV rows. Missing status plus empty outcomes is neither upcoming nor NC.
Finish methods, scheduled rounds and other fields never supply an outcome.
There is no cancellation result enum or fallback to NC.

## Versioned adapter and loader wiring

[`warehouse/strict_fight_outcomes_v2.py`](../../warehouse/strict_fight_outcomes_v2.py)
defines `ADAPTER_VERSION = "strict_fight_outcomes_v2"` and a pure
`transform_fight(row)` entry point. It validates disposition first, then passes
a shallow dictionary copy to the preserved legacy `transform_fight`. The
legacy function remains responsible for every admitted row's non-outcome
transformation, including division/title flags, finish-method normalization,
rounds, time, referee, source URL and timestamp. Neither adapter nor legacy
transform mutates the input or nested unused fixture fields.

Rejections raise `FightOutcomeValidationError`, a `ValueError` subclass. Caller
fields `reason` and `fight_id` provide stable rejection reason and fight identity;
`event_status` and `outcomes` expose the normalized diagnostic values. The
exception message includes the fight identity, reason, status and pair. No
rejected row returns `None`, becomes NC or disappears through outcome filtering.
Invalid dispositions fail before legacy parsing of unrelated fields.

The loader now explicitly imports `transform_fight` from the versioned adapter.
Its loop, foreign-key handling, batch list, `upsert`, transaction context and
`finally: conn.close()` are unchanged. It finishes transforming the complete
batch before entering `with conn:` or calling `upsert`. A late disposition
exception therefore prevents every batch write and the write transaction's
commit, and propagates to the caller after connection cleanup.

The existing reference boundary is preserved: unknown-event rows are skipped
before transformation; unknown-fighter handling remains after transformation.
The guarantee concerns disposition validation on the existing ingestion path;
foreign-key skips have not become a new quarantine or validation system.

The pinned legacy implementation in `warehouse/transform.py` must remain
available for accepted historical reconstruction and Phase 5C.3 diagnostics.
Its SHA-256 is unchanged:
`616ad63cf8402718ed8f912e7a3d75ba778109bac26745304256ef08eaef5a44`.
Editing it would break the 37-entry accepted Phase 5B.3 code pin set and the
identical code pins embedded in Phase 5B.4. The new admission boundary removes
the future-loader hazard without altering that reproducibility contract.

## Offline tests and diagnostic replay

All checks used existing installed dependencies. No live loader/database test,
mutating integration pipeline, broad unrelated suite, feature construction,
model fit, real prediction or outcome evaluation was run.

| Check | Result |
|---|---|
| Existing `warehouse/tests/test_transform.py` | 19 passed; unchanged |
| New `warehouse/tests/test_strict_fight_outcomes_v2.py` | 285 passed |
| New `warehouse/tests/test_load_fights_strict_outcomes_v2.py` | 10 passed |
| Existing `tools/tests/test_phase5c3_source_diagnostics.py` | 10 passed; unchanged |
| Read-only `tools/diagnose_phase5c3_sources.py` replay | Byte-identical to accepted diagnostic JSON |

The focused pytest invocation passed **314 tests** in 0.25 seconds. Together
with the preserved diagnostic suite, **324 tests passed**.

Adapter tests cover all valid mappings and winner orientations; null/whitespace
handling; absent/null/blank historical status; every empty, partial and
contradictory pair over the established vocabulary; unsupported status/token
rejection; status/result contradictions; non-outcome field equivalence with the
legacy transform; input immutability; and rejection before unrelated field
parsing. Delegation is checked to receive a copy only after valid admission.

Synthetic cancellation fixtures reproduce the empty outcome/finish/round shapes,
identities and latest diagnostic source clocks for:

| Fight identity | Synthetic source clock | Strict result |
|---|---|---|
| `3c34cdee-2aa5-5cef-b467-3f5ed89b0b0f` | `2026-08-16 19:52:20 UTC` | `canceled_status` |
| `0ed0563e-a80c-5aa5-a2c4-8c0e818a7273` | `2026-08-16 19:52:28 UTC` | `canceled_status` |

The synthetic tests also show the unchanged legacy mapping still returns NC for
these shapes. They never read or rewrite frozen records or claim replacement
results. The Phase 5C.3 tests retain their deliberate legacy-defect expectations.

Mocked loader tests replace `warehouse.db` before executing the loader module,
so the real helper, dotenv credential loading and psycopg2 connection code are
never imported. Connection, references, input iterator and upsert are mocks;
unconfigured cursor/input access raises. Socket construction, connection and
DNS lookup are forbidden and the guards are tested explicitly.

An entirely valid five-row batch, including both winner orientations, draw, NC,
upcoming and a missing-status resolved historical row, reaches exactly one
upsert after complete consumption. Each of the eight stable rejection reasons
is then tested at row 502, following 501 valid rows (beyond the upsert helper's
500-row chunk size). Every failure asserts zero upserts, zero transaction-context
entries, zero commits, propagated typed error with identity/reason, unchanged
input, and exactly one connection close.

Execution also used a temporary process-wide offline harness, which blocks
socket connections/sends and DNS resolution and rejects a real `warehouse.db`
import before test collection. Pytest plugin auto-loading, dotenv loading,
bytecode and pytest cache writes were disabled. Scratch helpers, receipts and
diagnostic output are isolated in `/tmp/phase5c4-strict-outcomes/` and are not
new repository artifacts. Actual commands:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  python3 /tmp/phase5c4-strict-outcomes/offline.py pytest -q -p no:cacheprovider \
  warehouse/tests/test_transform.py \
  warehouse/tests/test_strict_fight_outcomes_v2.py \
  warehouse/tests/test_load_fights_strict_outcomes_v2.py
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 \
  scraper/UFC-Web-Scraping-main/.venv/bin/python \
  /tmp/phase5c4-strict-outcomes/offline.py diagnostics-tests
PYTHONDONTWRITEBYTECODE=1 python3 \
  /tmp/phase5c4-strict-outcomes/offline.py diagnose \
  > /tmp/phase5c4-strict-outcomes/diagnostic-replay.json
PYTHONDONTWRITEBYTECODE=1 python3 \
  /tmp/phase5c4-strict-outcomes/preservation.py baseline
PYTHONDONTWRITEBYTECODE=1 python3 \
  /tmp/phase5c4-strict-outcomes/preservation.py verify
```

The preservation baseline command ran before any repository edit. The replay
is exactly 10,320 bytes, SHA-256
`b2fa5a75617096ee25739fc47d868d347be356b75f390430f1fa5313530febc4`, equal
byte-for-byte to `phase5c3-source-diagnostics.json`. Its accepted pins, frozen
counts, false-NC reproductions and blocker context are unchanged. This remains
descriptive diagnosis, with no new evidence-authority assertion.

## Artifact and code preservation results

Hash-only baselines cover **7,941 tracked files and 49,696 local files**.
The local inventory excludes Git/dependency/cache directories, bytecode and
root `.env`: `.git`, `.venv`, `venv`, `node_modules`, `__pycache__`,
`.pytest_cache`, `.mypy_cache`, `.ruff_cache`, `.cache`, `.pyc` and `.pyo`.
Protected holdout outcomes and model components were only hashed.
Comparison found no missing pre-existing files, no unrelated modifications,
only the authorized loader change, and only the four intended new files.

The following exact inventories and every listed member hash passed before
editing and after verification. Counts exclude each inventory's own
`checksums.json` file.

| Accepted inventory | Members | External checksum-root SHA-256 |
|---|---:|---|
| Phase 5B.3 `20261002_phase5b3_role_aware_v2_validated` | 88 | `7d36dc2bdb2a99eb81a44424fe603a6beab74d05435e41c8a1489cea11f0bd4f` |
| Phase 5B.4 `20261002T183215Z_phase5b4_current_challenger_v1_fixed` | 49 | `59ffd4995946cc9ec7512f2f14caade09fd3144d2cfb3ee2d3b9b0951f5ca300` |
| Phase 5C.2 `activation` | 9 | `21672682c5a7096fdc94ac8344a90755186c4cfadd92f9cb91ee2d5e4d98adfd` |
| Phase 5C.2 `capture_attempt/observation` | 8 | Covered by unchanged accepted receipt and whole-directory inventory |
| Phase 5C.2 `journal/runs/real_intake_v1` | 15 | `33d8e983476c408ee7a67f785c27856d58b3a0c9a0a3ea3fc2373a5bbe61d81c` |

The Phase 5C.2 capture receipt remains pinned to
`c09d3adf1065edda3582eabd9df6126d842bfa19e84ef7e222bddc384c246a18`.
The observation checksum file also remains equal to the accepted diagnostic
input hash `e2791af3b24ea6785e816d3c19e2ccf34020ac25acad84692874050bbcefa6a8`.
All 13 embedded Phase 5B.4 preparation files match the original preparation
byte-for-byte. All 37 Phase 5B.3 live code pins, eight Phase 5B.4 live code pins
and 37 embedded preparation live code pins pass without amendment.

Loader SHA-256 changed solely for the import wiring, from
`f5d0743c8e606e922779c6b668ea0173b0bd719226b7df2d238264f602b7f81a` to
`84e9446d6dc87d78c8e7315b1b3066585449846a841b6d833871fccd744adec5`.
Original manifests, accepted checksum inventories, baseline records, hash-check
implementations and `warehouse/transform.py` remain unchanged.

## Remaining defects and separate remedies

The two existing September 12 false-NC warehouse records and their cancelled
source versions remain unchanged. A future normal load encountering those
versions with known events now fails explicitly before all batch writes.
The current local CSV contains six cancelled versions across those two IDs;
this phase did not authorize running or correcting that load. Correcting live
warehouse history requires separately authorized source/result and identity
review, a bounded correction/publication plan and subsequent new observations.
Frozen captures retain their original rows and lineage.

Other accepted Phase 5C.3 defects remain: aggregate-consumer deduplication skips
188 missing aggregates; queues lack freshness guarantees; raw-body overwrites
and listing collisions prevent recovery of many historical versions; cross-load
stale inputs can overwrite values; expired announcements and unresolved aliases
need occurrence review. Binary outcomes stop August 29 and aggregate event
coverage stops May 30 in the accepted capture. Complete participant histories,
verified debuts and affirmative title/non-title claims are still unavailable.
April's relationship remains unresolved. None of these were repaired here.

Aggregate selection/queue repair, immutable raw versioning, stale-upsert
handling and historical data reconciliation each require their own authorized
scope and verification. Authentic source acquisition/refresh and a new bounded
observation require separate authority. Source qualification and any proposed
evidence-contract amendment require independent review; the current evidence
contract remains in force and no source was certified authoritative.

**Prospective evidence remains BLOCKED. Historical comparison remains
STILL_BLOCKED.** Preventing new invented NC results neither establishes complete
history nor unblocks forecasting, evaluation, model promotion or production.

## Git publication handoff

Publication follows `next-phase-git-handoff.md`: review the scoped code/tests
and report for whitespace, secrets, size and preservation; commit implementation
and verification together, then the detailed report; verify committed bytes
and final worktree; push normally to verified `origin/main`. No force-push or
history rewrite is permitted. Actual commit hashes, push result and final
worktree status are recorded in the final response, avoiding a self-referential
report-update commit loop.
