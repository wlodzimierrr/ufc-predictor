# Phase 5C.9 Allen–Duncan evidence pilot

**The one authorized attempt is complete and acquisition STOPPED at its first
request: HTTP 404 on the required robots read. No contemporary bout or profile
evidence was obtained. This is a preserved source-error attempt, not successful
source qualification.** V1 remains enforced; v2 remains **PROPOSED_NOT_APPROVED**.
Prospective forecasting remains **BLOCKED**; historical comparison remains
**STILL_BLOCKED**. Production is unchanged.

The operator authorized evidence collection only, explicitly superseding the
earlier report's requirement to wait for source-policy decisions before this
research pilot. That authorization does not approve those decisions. Its full
text is preserved in the isolated bundle's `authorization.txt`. No warehouse
connection/change, contract activation, fitting, model loading/execution,
forecast, feature construction, outcome ingestion or outcome evaluation occurred.
Historical result wording below describes the existing disclosed discrepancy
only; it supplies no correction or evaluation.

## Handoff and preservation boundary

Read all seven named handoff documents, including the complete 998,315-byte
index through a full JSON parse and subsequent independent reference checks.
Hidden repository and ancestor searches found no applicable `AGENTS.md`.
The accepted capture implementation and local installed downloader behavior
were inspected. No global scraper settings or frozen implementation changed.

Starting worktree was clean on `main`, HEAD
`abedef5bcbef2a3902ead0757fe4315a7a0975f4`, upstream `origin/main`.
Fetch and push remote were verified as
`https://github.com/wlodzimierrr/ufc-predictor.git`; Git's remote-ref check returned
the same HEAD. Git publication transport is separate from the source budget.

All six accepted checksum inventories and exact membership passed: B3 (88),
fixed B4 (49), shadow contract (3), C2 activation (9), C2 observation (8),
C2 BLOCKED run (15). Their unchanged checksum-root pins are:

```text
7d36dc2bdb2a99eb81a44424fe603a6beab74d05435e41c8a1489cea11f0bd4f
59ffd4995946cc9ec7512f2f14caade09fd3144d2cfb3ee2d3b9b0951f5ca300
83cf581f3f90b7c0f85fcc8063d87330843bec657b03da7f5135ddde450d7514
21672682c5a7096fdc94ac8344a90755186c4cfadd92f9cb91ee2d5e4d98adfd
e2791af3b24ea6785e816d3c19e2ccf34020ac25acad84692874050bbcefa6a8
33d8e983476c408ee7a67f785c27856d58b3a0c9a0a3ea3fc2373a5bbe61d81c
```

C2's receipt remains
`c09d3adf1065edda3582eabd9df6126d842bfa19e84ef7e222bddc384c246a18`.
All nine frozen code inventories were inspected; the five live code-pin groups
passed (37/8/37/12/5), and all 13 embedded preparation files match their originals.
All raw bodies, CSVs, manifests, learned components, registries, source/evidence
contracts, accepted repairs and earlier reports were preserved as opaque bytes
for hashing where semantic reading was unnecessary. The pre-edit baseline
covers **7,958 tracked and 49,713 local files**, excluding Git, dependencies,
caches, bytecode and root `.env`. Final comparison found **zero changed or
missing pre-existing files**. Only the new collector, its mock tests, this report
and this isolated compact bundle were added.

## Target, authorization and actual clocks

The unchanged target is Allen–Duncan, announced **October 10, 2026**:

- Fight `6fe1d59a-6ae9-5436-bc78-767da12a4707`.
- Event `e213540d-2ca7-54ee-96af-4159bd741028`.
- Ordered participants `5d26bddf-51d3-5417-aeab-7329263c2e27`,
  `364ad7d6-2d46-5e3c-a5f9-5696d76b73a9`.

The actual UTC clock tool returned `2026-10-04 11:36:49 UTC` during handoff and
`2026-10-04 11:49:58 UTC` immediately before launch. The original window closing
**October 9, 2026 at 00:00 UTC** was open. No target substitution or backdating
occurred. The authorization's pasted “12:24 PM” is retained as message text;
it is not used as an observation clock. No independently measured user-message
issuance timestamp is claimed.

| Boundary | Actual UTC |
|---|---|
| Exclusive destination preparation | `2026-10-04T11:49:24.963160+00:00` |
| Authorization/code/settings/plan freeze | `2026-10-04T11:49:24.979161+00:00` |
| Exclusive launch; authorization consumed | `2026-10-04T11:49:59.130541+00:00` |
| Acquisition start | `2026-10-04T11:49:59.135115+00:00` |
| Request ledger reservation | `2026-10-04T11:49:59.147154+00:00` |
| Actual priority-200 request callback | `2026-10-04T11:49:59.150643+00:00` |
| Supporting response callback observation | `2026-10-04T11:49:59.388721+00:00` |
| Response ledger publication preparation | `2026-10-04T11:49:59.394543+00:00` |
| Acquisition completion | `2026-10-04T11:49:59.398061+00:00` |

The attempt consumed this session's acquisition authorization. No second
destination, retry, resume, scheme substitution or further source read occurred.
Clocks are actual local UTC callback/preparation clocks, not wire-send times,
provider database-update times or original Phase 5C.2 clocks. C2's cutoff remains
`2026-10-03T05:23:34.369563+00:00` and was not applied to this packet.

## Frozen scope, settings and request order

The exclusive bundle is
[`data/audits/phase5c9_allen_duncan/20261004T114924Z/`](../../data/audits/phase5c9_allen_duncan/20261004T114924Z/).
Its immutable `freeze.json` pins the collector, tests, accepted writer/resolver,
legacy detector, utility, all named handoff documents, authorization, settings,
plan and pre-acquisition mock-test receipt. Installed dependency versions are
Python 3.11.2, Scrapy 2.13.3, Twisted 25.5.0 and parsel 1.11.0; exact measured
versions in `freeze.json` are the replay reference.

[`tools/collect_phase5c9_allen_duncan.py`](../../tools/collect_phase5c9_allen_duncan.py)
is a single-use collector for this frozen plan. It uses actual Scrapy downloader
callbacks at priority 200 and the accepted immutable writer API. It imports no
project settings, pipeline, warehouse or model API. Pilot-only settings remove
all default downloader middleware, including browser-session headers, proxies,
cookies, HTTP caching, automatic redirects and retry middleware. The HTTP
handler also disables persistent connection reuse and Twisted automatic retries.
Requests identify this pilot with a plain agent string, request identity
encoding and no-cache/no-store, and carry no browser cookies or copied headers.
The TLS context factory performs standard certificate/hostname verification;
its class name does not imply browser impersonation. No global setting changed.

Limits were frozen at **64 total HTTP attempts**, **600 acquisition seconds**,
**15 seconds per request** and **at least two seconds between requests**. Reads
are serial, with the spacing applied after the previous response/failure.
Supporting robots reads and each manually permitted redirect count separately.
A 2 MiB per-response ceiling is an additional conservative limit.

All initial content URLs come from accepted index `/targets/66`, its participant
body references, `/example_occurrence_consistency/20` and exact indexed listing
URLs. The July detail uses its recorded profile-linked URL; the index's second
host spelling remains distinct. Source identities were not silently normalized.

| Frozen content order | Exact URL | Actual disposition |
|---:|---|---|
| 1 | `http://www.ufcstats.com/event-details/7f98d9d5a10fa25c` | Not requested |
| 2 | `http://www.ufcstats.com/fight-details/7db1a3dac7e343e7` | Not requested |
| 3 | `http://ufcstats.com/fighter-details/2f181c0467965b98` | Not requested |
| 4 | `http://ufcstats.com/fighter-details/a93f94c923c3a9cb` | Not requested |
| 5 | `http://ufcstats.com/fight-details/4eff5a845db17572` | Not requested |
| 6 | `http://ufcstats.com/event-details/f354c50b8d63d9b3` | Not requested |
| 7 | `http://www.ufcstats.com/statistics/events/upcoming` | Not requested |
| 8 | `http://www.ufcstats.com/statistics/events/completed?page=all` | Not requested |

The deterministic supporting URL rule is the exact content URL's scheme/host
origin plus `/robots.txt`, before content at each new origin. That standard
permission endpoint is derived openly, not represented as an indexed target.
The explicit gate counts and preserves its reads; default robots middleware is
disabled to prevent unaccounted requests. The frozen rule fails closed on a
missing/error/unparseable robots response or denial, including HTTP 404.
This is a conservative pilot stopping rule, not a claim that robots universally
requires stopping on 404.

Only bounded same-path/query redirects within the two approved hosts, without
HTTPS downgrade, credential/query changes or loops, may be followed under the
frozen rules; their exact chains are retained. A new content origin requires its
own robots gate. No arbitrary page links, assets, profile pagination, full-history
detail expansion or unrelated card participants are scheduled. Whole profile
bodies would preserve every observed row without domain filtering; any additional
enumeration/pagination need would remain a limitation. All declared stop
conditions, including challenges, denial, rate limit, network/source errors,
changed target, budget exhaustion, caching and unsafe redirects, are in
`plan.json`. No redirect behavior was exercised in this real attempt.

## Actual request and preserved failure

**Exactly one HTTP attempt** was made:

| Sequence | Role and exact request/source URL | Response | Redirects | Stop |
|---:|---|---|---:|---|
| 1 | robots; `http://www.ufcstats.com/robots.txt` | HTTP **404**, 18 bytes | **0** | `ESSENTIAL_HTTP_ERROR` |

The response's exact bytes are `<h1>Not Found</h1>` (no terminal newline).
No CAPTCHA, access-denial wording or rate limit was observed. This must not be
reported as a proven access challenge. The permission check failed and the
frozen essential-error rule ended collection immediately. No second source
request occurred; the two-second spacing requirement is vacuously satisfied.
Total acquisition duration was **0.2629506529774517 seconds**.

The allowlisted response header states `Date: Sun, 04 Oct 2026 11:51:24 GMT`,
about 85 seconds ahead of the local callback clock. This is preserved provider
header text, not substituted for local acquisition time. No upstream cache-origin
or update clock was established. Request/response cookies, sensitive headers and
raw exception strings were not exported.

The accepted writer's public URL grammar excludes the dot in `/robots.txt`.
Mock testing found this before freeze. Its frozen schema was not expanded.
This response was observed by the pilot's priority-200 wrapper and preserved as
`support/001.body`, with its hash/length and actual clocks bound in
`ledger/001.result.json`. **Its accepted receipt is null.** No v2 receipt,
reservation or object was invented for it. The planned v2 root remains absent;
the accepted offline inventory reports `NO_OBSERVATIONS` and zero in every state.
Had eligible content reached this boundary, it would have entered the unchanged
writer/resolver. No such content response exists in this attempt.

The ledger reserves the attempt before download and records the observed result.
It accounts for all attempts, including those outside the accepted schema.
There are zero missing response results and zero incomplete v2 reservations.
Old bodies were neither copied into this bundle nor relabelled as fresh captures.

## Statement support and remaining discrepancies

| Question | Fresh packet directly supports | Continuing limitation |
|---|---|---|
| Date, roster, orientation | No new bout statement; only the robots error was obtained. | The indexed October 10 date and ordered links remain historical references. No current target revision check succeeded. |
| Title | No new designation observation. | Old `Middleweight Bout` remains **UNKNOWN under v1**. No affirmative non-title taxonomy or whole structured claim was supplied. |
| Profiles and linked occurrences | No current profile or occurrence rows. | No exhaustive history, revision watermark, page/count/terminal evidence or verified debut. Empty admitted history would remain unverified. |
| Duncan July result/event relationship | No new relationship evidence. | The Phase 5C.8 discrepancy remains unresolved and uncorrected. |
| Source domain | No new domain/authority attestation. | Mixed DWCS/Road to UFC rows must be preserved and reviewed under an approved domain policy; no filtering/classification occurred. |

The existing July reference, at its **old** observation clocks, says Duncan's
profile row `[9301,12290)` records a win over Jared Cannonier on July 18, 2026,
linking `4eff5a845db17572`, computational fight
`53b9cada-68ba-5c11-8da4-28833cd6b5fe`. The pinned detail body is
`3c5f19b3612765165abd8e6a9811e0cf335b14fee7a978447778b830c0d2b089`,
matching manifest data record 106083 at `2026-08-09T11:49:41Z`; it displays
ordered Cannonier/Duncan statuses L/W and links event `f354c50b8d63d9b3`.
C2 `sources/fights.json` `/2923` and local CSV record 8834 instead retain that
same fight identity as upcoming under event
`68a758a6-bd6a-5ec7-933e-72251614d52f`. This pilot did not reconcile the result,
replace the event binding, admit a tenth occurrence or select a preferred version.
An explicit supported occurrence/event transition and separately authorized
source revision remain necessary.

Likewise Allen's old profile's 20 linked resolved rows versus 19 admitted rows
include the preserved DWCS 3.4 row; Duncan's old ten versus nine include the July
discrepancy. Those are old observed-set comparisons, not complete UFC histories.
Original request/cache provenance and contemporary correction coverage for
historical anchors remain limitations. No historical body was given a new clock.

Provider statements in this new bundle are only HTTP status, public response
bytes and the allowlisted Date header. The target IDs, accepted-index references,
hashes and UUID5/NAMESPACE_URL convention (remove only `www.`, retain scheme)
are computational bindings. None grants provider authority. No
`authoritative=true`, `complete=true`, signed-review or verified-debut assertion
was generated. This is not a v1 table/evidence package or a scoring-ready package.

Operator decisions still required are named source authority, UFC-domain
classification, affirmative title vocabulary, enumeration/completion and revision
capability, identity/display-order semantics, and observation authenticity/cache/
freshness policy. Missing source evidence includes all current content pages and
explicit July relationship support. Successful future page access alone would
not approve these policies or complete the proof obligations.

**One concrete next step:** the operator supplies a contemporary manual public
capture or authentic provider export for this same target, with exact original
bytes, source/request/observation provenance, current event/detail, both full
profile enumerations including mixed-domain rows, and explicit July event/result
relationship evidence. It must disclose capability/coverage gaps and retain its
real clocks. No more automated acquisition is authorized in this session. Any
future acquisition, signed qualification, source correction or v2 implementation
requires its own scope and evidence; this failed attempt cannot be resumed.

All **163 considered rows and 35 original registrations** remain unchanged.
This research target is not a reduced trial cohort. Earlier aliases, April's
unresolved relationship and false-NC discrepancies also remain unchanged.

## Integrity, tests and artifact pins

Mock tests ran before the freeze with socket/DNS and real warehouse imports
blocked, bytecode/dotenv/plugin-autoload/pytest caches disabled, temporary-only
fixtures and an actual Scrapy engine using a mocked HTTP handler. The mock suite
checks first-response stops, supporting bytes outside the writer's schema,
accepted failure receipt resolution, redirect identity/unsafe-location refusal,
robots denial, exception-without-response semantics, budgets/timing, target
invalidation and exclusive no-resume publication.

- **22 collector cases passed**; final focused run after conservative date
  extraction adjustment: 22 passed in 0.83 seconds.
- **72 accepted writer and 76 accepted inventory/resolver cases passed**;
  combined run: **170 distinct tests passed** in 8.08 seconds.
- **5,704 independent index/reference/span checks passed**, including all 163
  considered rows, 35 original pins, historical body bindings and source URLs.
- Offline packet verification passed frozen hashes, exact support bytes,
  complete one-request ledger, null receipt truthfulness, clock order, open
  window and absence of v2 observations. **Actual receipt resolver bindings
  are not applicable: there are zero real accepted receipts.** The accepted
  resolver was exercised on mocks without changing it.
- Full preservation checks passed all accepted inventories/live code groups
  and zero changes/missing files against the pre-edit byte baseline.
- Whitespace, syntax, scope, secret/content, ignored-file and size review passed.
  No live integration test, dependency installation or model suite ran.

Successful commands from the repository root:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  scraper/UFC-Web-Scraping-main/.venv/bin/python /tmp/phase5c7-v2-inventory/offline.py \
  pytest -q -p no:cacheprovider tools/tests/test_phase5c9_evidence_pilot.py \
  scraper/UFC-Web-Scraping-main/ufc_scraper/tests/test_raw_capture_v2.py \
  scraper/UFC-Web-Scraping-main/ufc_scraper/tests/test_capture_inventory_v2.py
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  scraper/UFC-Web-Scraping-main/.venv/bin/python /tmp/phase5c7-v2-inventory/offline.py \
  pytest -q -p no:cacheprovider tools/tests/test_phase5c9_evidence_pilot.py
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 PYTHONWARNINGS=ignore \
  scraper/UFC-Web-Scraping-main/.venv/bin/python tools/collect_phase5c9_allen_duncan.py \
  /home/wlodzimierrr/ufc-data/data/audits/phase5c9_allen_duncan/20261004T114924Z
PYTHONDONTWRITEBYTECODE=1 PYTHON_DOTENV_DISABLED=1 \
  scraper/UFC-Web-Scraping-main/.venv/bin/python /tmp/phase5c7-v2-inventory/offline.py \
  audit inventory --root /home/wlodzimierrr/ufc-data/data/audits/phase5c9_allen_duncan/20261004T114924Z/v2
PYTHONDONTWRITEBYTECODE=1 python3 /tmp/phase5c9-evidence-pilot/preservation.py baseline
PYTHONDONTWRITEBYTECODE=1 python3 /tmp/phase5c9-evidence-pilot/preservation.py verify
```

The acquisition command above is recorded for audit only; **do not rerun it**.
Scratch guards, baseline and independent-check outputs remain outside Git in
`/tmp/phase5c9-evidence-pilot/`. The bundle's `offline-verification.json` and
`preservation-check.json` retain compact results; `checksums.json` lists every
bundle member's exact SHA-256 without duplicating the corpus. Hashes supply
integrity with session-local measured pins, not independent provider authenticity.

Artifact paths below are relative to the bundle, except the two repository
source/test paths. The complete final member inventory is in `checksums.json`.
The bundle is **22,105 bytes** across 13 files, including its manifest; it contains
one 18-byte response and no corpus/model copy.

| Artifact | SHA-256 |
|---|---|
| `tools/collect_phase5c9_allen_duncan.py` | `4098a3d2a1c0cb2ae2647090c763ef9250956ea0c70c4203fc70b578918ee068` |
| `tools/tests/test_phase5c9_evidence_pilot.py` | `4a1f51547b0c3f82402f5ad5a59aff81204ec90f8c5926b9ea2547fbd3b8552a` |
| `authorization.txt` | `abdae4efc328821e3017a8ac4653603611294294725d310af4e7167b65675e4f` |
| `freeze.json` | `d6b789a6165761dd3e88830535565d6aec2f9d1adc9c1d68748f44834c4fd243` |
| `plan.json` | `151942d46a956d8f85a2bb481181ff7423d2b9f5b6ab32a427293b380d88e133` |
| `settings.json` | `cb1964f4048e903ae87b0a512d9d139eed3776f6ce2838a0091d23d704b4a22a` |
| `mock-test-receipt.json` | `1052adb73103b42573ec724368766fe40ae8a34544aa602a8918db08da2a3237` |
| `attempt-start.json` | `f3efc2fd990b2cdd7412de83ea355fc1def0c86ecf4cac708d2053a028e91827` |
| `ledger/001.request.json` | `27f3d387001c2712b87ad7341e15a43b9de8fcaf314a96e80e1af0821dae1af7` |
| `ledger/001.result.json` | `a501f959b35a5a55fc233e228e32250c3deb9cf26341945f4241a549dd2312e9` |
| `support/001.body` | `67a84dd28e5b6288ef934643ad2f0d8af1145b6da9707d430fa1506a778459c0` |
| `acquisition-result.json` | `b20751b59b2780fa6a357f749c90688aa94c887db0ecc8a9f1162d2e41dd2820` |
| `offline-verification.json` | `701a4713e8f52490e51f621cf1b510e3ea09fb3101538e570fcdb5574ae69e8d` |
| `preservation-check.json` | `3cd4d50072e993a27d0e7cd17c5b298862afa0d99f3d4462c2274bb4478f4fcc` |
| `checksums.json` | `9b920b119ad3631ad69421b895a1bc8e1876ffd5f024ff49ac99ecfd8c5e9bd7` |

## Git publication

Followed the accepted Git handoff: collector/tests in one logical commit;
preserved failed-attempt bundle and report in another. Only explicitly scoped,
nonignored files enter Git; no force-add, force-push or history rewrite.
Committed artifacts, bundle hashes, frozen code pins, accepted inventories and
final worktree are verified before the normal push to verified `origin/main`.
Actual commit hashes and push outcome belong in the final response, avoiding
a self-referential report-update commit loop.
