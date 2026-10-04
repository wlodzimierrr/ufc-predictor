# Manual source corroboration and identity correction — 2026-10-04

The three supplied HTML texts corroborate Duncan's displayed July 18 result
against Cannonier. Their linked July event and fight resolve to the **same IDs
already stored locally**. The supported discrepancy is stale status/outcome
content, not a different event identity. This is a documentation-only manual
intake audit; no data correction or authoritative evidence acceptance occurred.

**V1 remains enforced; v2 remains PROPOSED_NOT_APPROVED. Prospective forecasting
remains BLOCKED; historical comparison remains STILL_BLOCKED.** No warehouse,
model, fitting, feature, contract, or production execution was performed.

## Scope and two distinct evidence sources

The previously authorized private diagnostic GET consumed its one-request
authorization. Its local receipt is
`/tmp/ufcstats-single-read.0hetE27H/result.json`: requested
`http://ufcstats.com/statistics/events/completed`, HTTP 200, 2,998 body bytes,
SHA-256 `0309755cbf357001cdd3fef5c92fb1d6b783f71e3236581c1667d60df092ee7e`.
The receipt classifies it as `BROWSER_CHALLENGE_HTML`, with no usable event-list
evidence, `STOP_ACCESS_CHALLENGE`, one connection, zero followed redirects, and
zero additional requests. No challenge JavaScript was executed.

Its recorded request window is `2026-10-04T14:22:09.923998702Z` through
`2026-10-04T14:22:10.463331977Z`. Those clocks describe that diagnostic only;
they do not date the user-provided captures. No additional UFCStats source
requests were made for this report; authorized Git verification/publication is
separate. The larger Phase 5C.10 robots-policy/64-request pilot was
**not authorized or run**. This report does not restart that pilot.

The three supplied attachments are separate manual inputs:

| Supplied text | Lines | Bytes | SHA-256 |
|---|---:|---:|---|
| Event listing | 742 | 29,854 | `89bf7782105d187da970026e10d5f596306753095372403ade54730333f043df` |
| July event card | 1,801 | 43,781 | `bdfcaf45105fa1a028b06e588198c9146a662f2642472f277e2d742c0d719395` |
| Cannonier–Duncan detail | 1,557 | 44,995 | `05da35470b65c2bbd35d04b0904eea98c19ea482845f0619753a576d7aa95f29` |

Exact attachment paths, respectively:

- `/home/wlodzimierrr/.codex/attachments/8db73d66-d51d-4938-b54b-759585cea750/Pasted text.txt`
- `/home/wlodzimierrr/.codex/attachments/eb7700d0-13d2-49b9-a95a-64d14e9a997e/Pasted text.txt`
- `/home/wlodzimierrr/.codex/attachments/ee0df532-4d32-423a-a651-814813ebc175/Pasted text.txt`

These hashes certify the **exact received text**, not original HTTP wire bytes
or provider authenticity. Attachment capture UTC, final browser URL, HTTP
status, and request/cache provenance remain unknown. Neither file mtime nor
this audit's date supplies those missing facts.

## Content chain and participant order

Listing lines 324–333 link event `f354c50b8d63d9b3` to “UFC Fight Night:
Du Plessis vs. Usman,” July 18, 2026, Oklahoma City, Oklahoma, USA. The July
event text repeats that title/date/location at lines 74–92. Its row at
281–405 links fight `4eff5a845db17572`, displays Christian Leroy Duncan first
with `win`, then Jared Cannonier, and states Middleweight, U-DEC, round 3, 5:00.

Detail lines 67–138 backlink to the same event URL/name, display Cannonier
**L** first and Duncan **W** second, and state “Middleweight Bout,”
“Decision - Unanimous,” round 3, 5:00, with `3 Rnd (5-5-5)` format. Exact
profile hrefs match between card and detail. Card order Duncan/Cannonier maps
to detail order Cannonier/Duncan by permutation `(2,1)`; membership and winner
agree. No stored participant swap is indicated by this display reversal.

After joining by profile href, card/detail aggregate parity is:

| Field | Duncan card / detail | Cannonier card / detail |
|---|---:|---:|
| KD | 0 / 0 | 0 / 0 |
| Card Str / detail significant strikes landed | 68 / 68 | 15 / 15 |
| Takedowns landed | 1 / 1 | 5 / 5 |
| Submission attempts | 0 / 0 | 0 / 0 |

Card `Str` matches significant landed counts, not detail total landed counts
94/36. Its abbreviated header alone does not independently define that field.
The detail contains no self fight URL/ID, event date, or capture/transport
metadata. The card link, exact profiles, result, finish, aggregates, and event
backlink corroborate its assignment; a final browser URL is not established.

## Reproduced identities and current local projection

For the URLs checked here, remove only the optional `www.` host prefix from
`ufcstats.com`, retain `http` and the remaining URL unchanged, then compute
`uuid5(NAMESPACE_URL, normalized_url)`. This is an independently reproduced
identity check, not a newly qualified parser or a diagnosis of pipeline code.

| Object | UFCStats path suffix | Reproduced UUID |
|---|---|---|
| July event | `event-details/f354c50b8d63d9b3` | `68a758a6-bd6a-5ec7-933e-72251614d52f` |
| Fight | `fight-details/4eff5a845db17572` | `53b9cada-68ba-5c11-8da4-28833cd6b5fe` |
| Cannonier | `fighter-details/13a0275fa13c4d26` | `c00bb616-ac26-58fe-af2e-8d953a63322d` |
| Duncan | `fighter-details/a93f94c923c3a9cb` | `364ad7d6-2d46-5e3c-a5f9-5696d76b73a9` |

Current `data/events.csv` physical line 789 stores that same event UUID, its
`http://www.ufcstats.com/event-details/f354c50b8d63d9b3` URL, the matching
Du Plessis–Usman name, date `2026-07-18`, and `event_status=upcoming`.
Current `data/fights.csv` physical line 8835 (data record 8834) stores that
same fight and event UUID, the matching `www` fight URL, Cannonier as fighter 1
and Duncan as fighter 2, blank participant outcomes and finish fields, and
`event_status=upcoming`. These statuses/outcomes conflict with the supplied
completed-result content; their occurrence time and update cause are unproved.

**Corrigendum:** Phase 5C.8 lines 215–220 and Phase 5C.9 lines 209–214 described
an event-binding discrepancy and required an event/occurrence transition.
The purportedly different event UUID is exactly the UUID of the linked July
event. Those passages therefore do not establish a wrong event binding or a
need to change the event ID. The unresolved issue is status/outcome revision
and evidence qualification. Old reports and their checksum pins are preserved
byte-for-byte; this report records the correction without rewriting them.

Those reports also describe C2 `sources/fights.json` pointer `/2923` as
upcoming. That is **documented C2 context**, not a fresh database or C2 snapshot
audit performed here. No source revision, parser/cache/upsert cause, or source
version preference was established.

## Remaining boundary and concrete next review

The content cross-check resolves supplied-text consistency and the claimed
identity mismatch. It does not supply signed source authority, historical
availability at a cutoff, revision coverage, exhaustive history/completeness,
verified debut, or an affirmative non-title statement under v1. Ordinary
“Middleweight Bout” remains UNKNOWN for that v1 title requirement. No tenth
occurrence was admitted. No frozen holdout outcomes or joined identity/outcome
evidence was parsed. No preferred historical source version was selected.
Future historical source availability is
not certified by these captures or by the diagnostic request.

The next proposed step is one tightly scoped **offline correction review** for
this existing fight and event: produce a reviewable status/outcome discrepancy
proposal using the received-text pins, keep the identity bindings and detail
participant order, and identify required source authority/revision evidence.
Any proposed event-status update requires its own justified rule; one bout's
result does not certify the entire card. This report neither applies that patch
nor accepts the captures as authoritative. Additional HTML is unnecessary to
repeat this content/identity cross-check; a broader acquisition or correction
phase requires its own scope and authority.

## Reproducible offline checks and handoff

The identity and target-row checks can be repeated without network access:

```bash
python3 - <<'PY'
import uuid
for suffix in (
    "event-details/f354c50b8d63d9b3",
    "fight-details/4eff5a845db17572",
    "fighter-details/13a0275fa13c4d26",
    "fighter-details/a93f94c923c3a9cb",
):
    url = "http://www.ufcstats.com/" + suffix
    normalized = url.replace("http://www.ufcstats.com/", "http://ufcstats.com/", 1)
    print(normalized, uuid.uuid5(uuid.NAMESPACE_URL, normalized))
PY
rg -n '68a758a6-bd6a-5ec7-933e-72251614d52f' data/events.csv
rg -n '53b9cada-68ba-5c11-8da4-28833cd6b5fe' data/fights.csv
jq '{attempt_count,http_status,body_bytes,body_sha256,stop_disposition,additional_requests_performed}' /tmp/ufcstats-single-read.0hetE27H/result.json
```

Attachment sizes/hashes were checked with `wc -l -c` and `sha256sum` on the
three exact paths above. Only the supplied texts, safe diagnostic metadata,
target CSV rows, and disclosed report context support these findings. Existing
reports, attachments, pins, data, and contracts were not edited; no raw cookies
or challenge tokens are included. No model/database tests were run.

The attachment files and `/tmp` diagnostic are external references; they were
not copied into Git. This report is not a self-contained frozen source bundle.
Independent root checks confirmed ten selected file pins unchanged: the
production pointer, four data CSVs, C9 collector/test files, legacy transformer,
strict adapter, and `load_fights`. This targeted check does not establish a full
repository byte baseline or predictive/model test coverage.

Starting HEAD: `2090440b23d01fb1d693258c700244c4092f6fd2`; worktree was clean,
branch `main`, upstream `origin/main`, origin
`https://github.com/wlodzimierrr/ufc-predictor.git`. No applicable ancestor or
documentation-directory AGENTS.md was present. The next-phase Git handoff was
read; root review is required before this report is committed or pushed.
