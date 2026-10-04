# Phase 5C.11: offline manual fight correction proposal

Date: 2026-10-04. **Proposal preparation COMPLETE; correction NOT APPLIED.**
Source admission remains `SOURCE_ADMISSION_UNAPPROVED`. Forecasting is BLOCKED;
historical comparison is STILL_BLOCKED. V1 remains enforced, v2 remains
PROPOSED_NOT_APPROVED, and production is unchanged.

## Authority and scope

The user answered “yes, lest continue phases” to the specific question approving
an isolated offline proposal using the manually supplied pages, keeping unknown
capture metadata explicit, with no live warehouse writes or frozen-data changes.
The question and response are preserved in `proposal.json`; their original
issuance UTC is unknown. This is not permission to apply a correction or approve
the broader source-evidence contract. A fresh implementation agent produced the
tool, tests and frozen bundle; root independently reviewed and verified them and
owns this report and Git handoff. No applicable AGENTS.md was found.

The supported discrepancy is stale status and missing outcomes/finish fields,
**not an event-ID mismatch**. The earlier diagnosis was corrected in the
[manual intake report](20261004-manual-source-corroboration-and-identity-correction-report.md).
All earlier reports and frozen artifacts remain intact.

## Exact proposal

Fight `53b9cada-68ba-5c11-8da4-28833cd6b5fe`, native URL suffix
`fight-details/4eff5a845db17572`, stays under event
`68a758a6-bd6a-5ec7-933e-72251614d52f`, `event-details/f354c50b8d63d9b3`.
Stored participant order remains Cannonier first, Duncan second. The card's
winner-first order is joined by exact profile URLs, yielding permutation `[2,1]`.
UUID5 checks reproduce both event/fight IDs and both participant IDs.

| Fight CSV field | Current value | Proposed value |
|---|---|---|
| fighter_1_outcome | empty | L |
| fighter_2_outcome | empty | W |
| event_status | upcoming | completed |
| finish_method | empty | Decision - Unanimous |
| primary_finish_method | empty | decision |
| secondary_finish_method | empty | unanimous |
| finish_round | empty | 3 |
| finish_time_minute | empty | 5 |
| finish_time_second | empty | 0 |

Lowercase normalized method tokens match the existing CSV parser. No IDs, URLs,
participant order, scrape timestamps, bout/weight/round designation, referee,
judges or statistics are proposed for modification. The parent event remains
unchanged: one result does not certify an entire card. The parent roster's manual
Bashi–Delgado reference versus supplied native `3bd159c1bed14700` remains an
explicit diagnostic, not a silently resolved alias, cancellation or deletion.

The original fight `scraped_at=2026-08-08 23:38:48 UTC` is retained as **base-row
lineage only**. It does not date the newly proposed claims or establish their
historical availability. “Middleweight Bout” remains UNKNOWN for affirmative
non-title status under v1. No history-completeness or debut claim is established.

## Frozen package and provenance

Run: `data/audits/phase5c11_manual_fight_correction/20261004T203255672479Z_duncan_cannonier_v1_proposal/`.
Computation began `2026-10-04T20:32:55.593919Z`; publication clock is
`2026-10-04T20:32:55.672479Z`. These are local audit clocks, not source captures.

| Received text | Bytes | SHA-256 |
|---|---:|---|
| Listing | 29,854 | `89bf7782105d187da970026e10d5f596306753095372403ade54730333f043df` |
| July card | 43,781 | `bdfcaf45105fa1a028b06e588198c9146a662f2642472f277e2d742c0d719395` |
| Fight detail | 44,995 | `05da35470b65c2bbd35d04b0904eea98c19ea482845f0619753a576d7aa95f29` |

The three exact received texts are preserved, without executing their scripts.
Capture UTC, final browser URL, HTTP status and request/cache provenance remain
null. Hashes bind received text, not original HTTP wire bytes or provider
authenticity. The detail has no self fight URL/date; its assignment is corroborated
by the card link, profiles, result, finish and event backlink. No fresh acquisition
or browser-cookie replay occurred. No credentials or challenge tokens were added.

Ten members include those texts, exact selected event/fight snapshots, full input
CSV hash references, source facts with line/locator references, the nine-field
diff and proposed row, code/reference pins, the frozen review tool, validation and
publication receipts, and the checksum index. Only selected CSV rows are archived;
full-file hashes are lineage references, not a self-contained full CSV archive.

Checksum-index SHA-256:
`e65b01dddfae1b4af42a63a1a2d4fe541b5b683bbc8e8cea6e6411819b9b40b2`.
Tool SHA-256: `8c489087a4f2c7db326ea248da82eed26071f3065934a61890fea5a22abf9af4`.
Test SHA-256: `5785539d44c7f2d64805f66d8b95ec53c902d41dccebc1ed39594dffaf534200`.

## Implementation and verification

`tools/prepare_phase5c11_manual_fight_correction.py` exposes only `prepare` and
`validate`, uses the standard library, and has no apply, network, DB, model,
training or scoring entry point. Exact received-text and base-row pins, unique
CSV identities, linked source consistency and the change allowlist fail closed.
Publication is exclusive even for empty existing destinations; partial failures
are retained as incomplete and refused. Files are read-only. Exact member hashes,
membership, pinned code, strict JSON and deterministic reconstruction are checked.

Executed verification:

- 63 new synthetic tests passed. Cases include reversed card orientation,
  malformed/challenge/changed bytes, contradictions, duplicates, missing targets,
  changed/already-resolved base rows, null provenance, allowlist enforcement,
  deterministic payloads, tampering/extra members, writable files, overwrite and
  mid-write refusal, strict JSON and absence of an apply option. Network,
  subprocess, database and model access is guarded in this suite.
- Root combined rerun: 377 passed in 0.65s, comprising those 63 and 314 existing
  strict-outcome/loader/transform regressions. Separately, the scraper environment
  passed 10 preserved diagnostic tests and 3 subtests. An initial combined
  system-Python collection stopped because Scrapy was absent; no dependency was
  installed and no substantive tests ran in that failed collection.
- Two real payload builds were byte-identical. Frozen-only validation rebuilt all
  seven deterministic payloads and checked all ten members without original CSVs
  or attachments. The implementation agent also ran guarded frozen-only replay.
  Root independently checked the exact nine changes, identities, null provenance,
  unchanged snapshots, all member hashes, checksum pin and staged-byte parity.
- Both Python files compile; scoped new-code/JSON whitespace checks passed. Exact
  received HTML is preserved, not reformatted. No broad live/integration suite,
  frozen outcome parsing or predictive evaluation was run.

Test and reproduction commands (`prepare` publishes a new run; it is not needed
to validate the existing frozen package):

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -B -m pytest -q -p no:cacheprovider tools/tests/test_phase5c11_manual_fight_correction.py warehouse/tests/test_strict_fight_outcomes_v2.py warehouse/tests/test_load_fights_strict_outcomes_v2.py warehouse/tests/test_transform.py
scraper/UFC-Web-Scraping-main/.venv/bin/python -m pytest -q tools/tests/test_phase5c3_source_diagnostics.py
python3 -B tools/prepare_phase5c11_manual_fight_correction.py prepare
python3 -B tools/prepare_phase5c11_manual_fight_correction.py validate data/audits/phase5c11_manual_fight_correction/20261004T203255672479Z_duncan_cannonier_v1_proposal
```

## Preservation and Git portability

Root recorded and repeated these ordered path/hash aggregate checks before work
and after publication. All match; these groups overlap and must not be summed.

| Scope | Paths | Aggregate SHA-256 |
|---|---:|---|
| Files tracked at starting HEAD | 7,975 | `bdf7af3eac772790597b71aed828fe9be1de874dcd577efc6fff114c94d61f26` |
| Models, holdouts, experiments | 7,612 | `34dfbea459fe3b8e48d7bc99e8001ce0dc3984a673ab24c61732cca8cc13c43e` |
| Existing raw/audits, excluding this new namespace | 14,310 | `ca3ac8e33ea24a3ffceeced6cb9006ca3820a5ea2fd1a06e36d10077d55c37b4` |

Cache directories are excluded from the latter two groups. Original production
pointer SHA-256 remains `ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa`.
Only new phase files were added; no existing repository file was changed.

Git preserves bytes but not read-only permissions. Root materialized the staged
Git tree in `/tmp/phase5c11-git-materialization.AQnylP`; validation correctly refused
its initially writable members, then passed after restoring read-only permission
on exactly the ten bundle files. A fresh checkout needs the same restoration:

```bash
proposal_run=data/audits/phase5c11_manual_fight_correction/20261004T203255672479Z_duncan_cannonier_v1_proposal
chmod a-w "$proposal_run"/{base_snapshot.json,checksums.json,code_pins.json,proposal.json,publication.json,review_tool.py,validation_receipt.json} "$proposal_run"/sources/{listing.html,card.html,detail.html}
python3 -B "$proposal_run/review_tool.py" validate "$proposal_run"
```

Permissions and checksums do not prevent an authorized writer from changing
evidence and indexes together. Review the known external checksum pin and trusted
Git revision before running a frozen tool. This bundle is not a loader reference
or a source-availability certificate.

## Handoff and next operator decision

Starting clean HEAD was `31a144d34e405162fb3cc35ad24123836efe5f09`, branch `main`,
upstream `origin/main`, verified origin `https://github.com/wlodzimierrr/ufc-predictor.git`.
Root follows the existing Git handoff: implementation/tests first, frozen evidence
and report second; final commit hashes and actual push outcome are reported in the
user handoff after committed-byte checks, without a self-referential report loop.

**Required decision:** accept these manually supplied pages, with their unknown
capture/authenticity metadata, for this one current-data correction, and separately
authorize its application to the current CSV and warehouse. Do not infer blanket
v2 approval, historical availability, event-status repair or model admission.
Any application requires fresh target-state checks, explicit provenance and a
recoverable transactional plan. No correction, source refresh, warehouse access,
training, forecast, holdout scoring, evaluation or production operation occurred.
