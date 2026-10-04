# Phase 5C.12: approved current-data fight correction

Date: 2026-10-04. **APPLIED AND INDEPENDENTLY VERIFIED.**
Exactly one current CSV record and one warehouse fight row were corrected.
Models, frozen datasets and prior evidence remain unchanged. Prospective
forecasting remains BLOCKED; historical comparison remains STILL_BLOCKED.

## Approval and scope

The user answered “approved, lets continue” to the explicit question authorizing
the manual pages, with unknown capture metadata, for this fight's correction in
the current CSV and warehouse. `user_authorization.json` records that case-only
authority. Its original message issuance UTC is unknown; the separate recording
clock is not a source capture clock. V1 remains enforced and the broader v2
proposal remains PROPOSED_NOT_APPROVED.

A fresh phase agent implemented and tested the bounded helper. Root verified
the implementation, committed it before execution, applied the correction,
independently reread the warehouse and owns this report and Git handoff. No
applicable AGENTS.md was found. Starting clean HEAD was
`4fc0795c2389c967766c801fc1ff23b4b0e9b0e2`, main/origin/main, verified origin
`https://github.com/wlodzimierrr/ufc-predictor.git`.

The immutable [Phase 5C.11 proposal](phase5c11-offline-manual-fight-correction-proposal-report.md)
and its three received texts remain byte-identical. Its checksum-index pin is
`e65b01dddfae1b4af42a63a1a2d4fe541b5b683bbc8e8cea6e6411819b9b40b2`.
Its proposal-only/unapproved labels are historical lineage; this phase's separate
user authorization admits only the current correction, not the entire provider
or historical availability.

## Exact changes

Fight `53b9cada-68ba-5c11-8da4-28833cd6b5fe` stays under event
`68a758a6-bd6a-5ec7-933e-72251614d52f`. Cannonier remains fighter 1 and Duncan
fighter 2. The detail's L/W pair maps to Duncan's existing ID
`364ad7d6-2d46-5e3c-a5f9-5696d76b73a9`; the winner-first card does not swap IDs.
This was a status/outcome discrepancy, not an occurrence or event-ID transition.

| Corrected CSV values | Normalized warehouse values |
|---|---|
| fighter_1_outcome=L; fighter_2_outcome=W | winner_fighter_id=Duncan's existing ID |
| event_status=completed | result_type=win |
| finish_method=Decision - Unanimous; primary_finish_method=decision | finish_method=decision |
| secondary_finish_method=unanimous | finish_detail=unanimous |
| finish_round=3 | finish_round=3 |
| finish_time_minute=5; finish_time_second=0 | finish_time_seconds=300 |

That is nine CSV fields and six warehouse columns. All other fight columns,
identities, URLs, participant order, scheduled rounds, referee, judges and scrape
timestamps remain unchanged. Parent CSV status was and remains `upcoming`;
parent warehouse status was already `completed` and remains so. Neither parent
was repaired or certified. Both participant profiles and six existing round-stat
rows remain unchanged; the aggregate-stat row remains absent. No statistics were
acquired, synthesized or derived. Other identities and false-NC cases are out of scope.

## Application safeguards and provenance

The standard-library helper `tools/apply_phase5c12_manual_fight_correction.py`
has no CSV writer, connection discovery, batch loader, model call or automatic
rollback. Root supplies the configured warehouse connection factory without
publishing credentials. The database target is bound by a hash, not a published
database name/address/port. The pinned proposal is verified before its frozen
review tool is executed.

Before changes, exact CSV lexical records and full-file hashes, complete bounded
warehouse before/expected-after snapshots, authorization and recovery records
were durably written. Read-only captures used repeatable-read transactions,
rolled back and closed. Root then applied exactly one CSV row using apply_patch,
restored the original newline pattern mechanically, verified the full-file hash
and unchanged prefix/suffix bytes, and fsynced the file and directory.

The file contains 9,115 CRLF lines and four LF-only tail lines. An initial
all-CRLF assertion failed before any CSV write. A later check anticipated either
all-LF or fully restored patch output; the intermediate patch output matched
neither assumption. The bounded formatter restored the actual mixed pattern,
and the final bytes matched the exact approved candidate before any warehouse
update. Both precheck corrections are disclosed in the verification evidence;
no unrelated newline or data change remained.

One short serializable write transaction acquired a normal fight-table lock
before checking schema/rules/triggers, locked the target row, checked the complete
before snapshot and CSV hash, and used NULL-safe conditions on all 17 fight
columns. UPDATE assigned only the six approved columns and required rowcount 1
and exact full-row RETURNING parity. Parent/profiles/statistics parity and the
CSV hash were checked before the single commit. No custom triggers/rules or RLS
were present; no migration or batch ingestion was run.

The commit attempt clock was `2026-10-04T21:31:42.047730Z`; independent confirmation
was `2026-10-04T21:31:42.162637Z`. Result: `VERIFIED_CORRECTION_COMMITTED`, one commit
attempt. A further root read-only connection matched the entire expected after
snapshot. No compensation or recovery execution was needed. Ambiguous commit or
post-commit failures require independent reread before any compensation; the
helper never blindly restores files or reports success from an unconfirmed commit.

**Knowledge-time limitation:** retained `scraped_at=2026-08-08 23:38:48 UTC` is
base lineage only, not availability of these corrected claims. Their current
operational clocks and provenance are recorded separately. Original capture UTC,
HTTP status, final browser URLs and provider authenticity remain unverified.
There is no signed timestamp or historical-availability certificate. Generic
readers must not treat the old scrape timestamp as knowledge of these facts;
as-of use requires consuming the correction provenance. No existing model/source
admission rule was changed. Ordinary “Middleweight Bout” still does not certify
non-title status, complete history or debut under v1.

## Frozen audit and integrity pins

Run: `data/audits/phase5c12_manual_fight_correction/20261004T211152Z_duncan_cannonier_v1/`.
Its 21 component files plus checksum index are read-only. They contain approval,
CSV before/after records and plans, DB snapshots, execution/code pins, a durable
four-state transaction journal, application/root verification, recovery evidence,
format diagnostics, tests and preservation results. Full original CSV recovery
is bound to `4fc0795c2389c967766c801fc1ff23b4b0e9b0e2:data/fights.csv`; compensation
must check the complete current file/row first rather than overwrite concurrent changes.

| Component | SHA-256 |
|---|---|
| New audit checksum index | `39e62692a6a531718698696c8ef26630560e7f497254a9e03f642857dd20f0e8` |
| Original fights.csv | `616f7be79c7c61ddec2aaddc4aa7d36ec0a2478106e6d10697df078406012b8c` |
| Corrected fights.csv | `76e6a94443a6fa3e2e6035f27a53fcdd0c41d2a082716fd3621d0b49530722d9` |
| Warehouse before snapshot | `1af57ba0c272a65c89e360052eb49d36dc8693bac3367628d611240b9b8217be` |
| Warehouse after snapshot | `f022a2d8ff2e55aa8d078a62a177e82dc788e0a26502477305bd5951575f8728` |
| Helper | `7fa979a36b81139b42d68524156c61336ae22cd905d22294d1e44d6f5812e616` |
| New synthetic tests | `7e2e84afbc30c4e3c37e0191e56ea50bfc0cdd2143e2f701dd76d347185505ec` |

The checksum index is externally pinned; exact membership, each member hash and
byte length were read back. Git preserves these bytes, not read-only permissions.
Checksums/modes do not prevent an authorized writer from changing both records
and indexes; trusted Git revision/external-pin review remains necessary.

## Verification and preservation

39 new synthetic tests passed. Root's combined safe suite passed 416 tests in
0.84s; the preserved diagnostic suite separately passed 10 tests and 3 subtests.
Total unique pytest cases: 426. Compilation and scoped code whitespace checks
passed. Tests guard real DB/network/model access and cover exact projections,
CSV byte/newline/quote preservation, pins, duplicate/conflicting/drifting inputs,
full CAS/RETURNING, rollback/close, already-correct no-op, ambiguous commit and
post-commit close errors, file guards and durable-journal failures. Actual checks
were bounded to this approved case. No broad mutating integration suite, source
acquisition, model execution, holdout outcome parsing or predictive evaluation ran.

All 7,987 other starting tracked files remain byte-identical; the only modified
pre-existing file is the one-line `data/fights.csv` correction. Ordered aggregate
path/hash checks matched before/after:

| Overlapping preservation group | Paths | Aggregate SHA-256 |
|---|---:|---|
| Starting tracked files except fights.csv | 7,987 | `659e05d6c21643f74562d93124ef741ce7cd01fe7ead51ef0c7728416d4f9e3b` |
| Models/holdouts/experiments | 7,612 | `34dfbea459fe3b8e48d7bc99e8001ce0dc3984a673ab24c61732cca8cc13c43e` |
| Existing raw/audits except new phase namespace | 14,320 | `50384d1331c3977302fae3d5d963f5ad6ce17f13369d850d6ac1503c21af8d9c` |

Groups overlap and must not be summed; generated caches are excluded from the
latter two inventories. Original production pointer SHA-256 remains
`ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa`.
Challenger/reference/control artifacts, both holdouts, source policy/proposals,
all earlier reports and accepted repairs are unchanged. CSV whitespace was not
globally reformatted; its original newline bytes are intentionally preserved.

## Handoff and remaining operator gate

Implementation/tests were committed as
`b804684abaeee7ea16bb9ed7263088b3ff967dfd` before live execution. Corrected CSV,
frozen receipts and this report form the separate data/audit commit. Root reports
final commit/push/clean-worktree verification in the user handoff, avoiding a
self-referential report update. The actual live warehouse change is complete;
Git publication does not perform or roll back that transaction.

This resolves Duncan's current July result discrepancy only. It does not supply
prospective title, full-history/debut, revision/frontier or observation evidence,
resolve April mappings/other false-NC rows, approve broader provider semantics,
or make the historical comparison available. Old v2 passages about a necessary
July identity transition are superseded by the linked identity audit and this
case correction, not silently edited or accepted. The next broader gate is an
explicit prospective evidence-policy/source qualification decision; do not infer
blanket approval from this one-fight authorization or run forecasts from these receipts.
