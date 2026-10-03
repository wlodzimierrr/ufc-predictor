# Phase 5 source evidence contract v2 proposal

**PROPOSED_NOT_APPROVED. No source is qualified by this document.**

This is the concrete amendment proposed in Phase 5C.3, grounded in the
[Phase 5C.8 local qualification report](implementation-reports/phase5c8-local-source-qualification-report.md)
and its [byte-reference index](implementation-reports/phase5c8-local-source-index.json).
The enforced contract remains `phase5c1_evidence_package_v1` and
[the accepted v1 document](phase5c1-shadow-evidence-contract-v1.md). No runtime
gate, frozen contract, receipt, registration or source file changes here.

The proposed future identifier is `phase5_source_evidence_v2`. It would permit
derived evidence under separately qualified provider semantics, while keeping
provider statements, operator attestations and computational bindings distinct.
Approval permits an extraction rule; it cannot supply missing observations,
make a history complete, or make an unsupported fact true.

## Decisions required before implementation

An accountable operator must record approval or refusal of each item below,
with the approving person's identity, authority, rationale, exact supporting
documents and independent pins. This proposal contains no such approval.

| Decision | Proposed scope | Present evidence limit |
|---|---|---|
| Source authority | Qualify the public `ufcstats.com` / `www.ufcstats.com` event, fight and fighter pages for UFC occurrence and bout-designation statements. Identify the responsible provider and why it is trusted for each role. | Domain names and HTTP-200 manifest entries establish neither provider authority nor authenticity. No provider authority/coverage attestation is present. |
| UFC domain | Approve a versioned event-ID classification ledger for UFC-promoted bouts in the accepted feature-history domain, beginning no later than 1993-11-12. Include numbered UFC, Fight Night, UFC on broadcaster and UFC/TUF Finale cards where supported; classify DWCS, Road to UFC, TUF exhibitions and other promotions explicitly. | Profiles mix domains: Allen and Chovancek list DWCS; You lists Road to UFC. Classification by a `UFC` substring is unsafe. Proposed exclusions require authority and a compatibility review; they are not applied to current rows. |
| Title vocabulary | Obtain affirmative provider/reviewer support that the exact ordinary bout designations below mean non-title, and that qualifying designations are exhaustive within their declared schema/version. | The packet contains ordinary labels but no assertion that omission of `Title` means false. |
| Enumeration capability | Establish whether the full profile table is an exhaustive, single-page occurrence enumeration, or require an authenticated export with all pages and a completion receipt. Establish revision/correction coverage through the boundary. | HTML footer, row count and absence of next-page controls do not establish this capability. |
| Identity/display semantics | Approve source-native URL identities, detail order as the target's orientation, and independent handling of card display order, with explicit revision evidence for changed identities. | Twenty-one card rows reverse detail order. April's two URLs lack a transition; Duncan's July row conflicts with the frozen source. |
| Observation/freshness policy | Approve the bounded contemporary policy below and identify how source clocks and cache origin are authenticated. | Legacy manifests have only `fetched_at`; profiles may have been cached. No request clocks or cache receipts establish contemporary upstream observations. |

If UFCStats cannot support an item, use an authentic UFC-domain export or
primary-source packet from a separately named authority for that role. Do not
accept an operator assertion of provider capability without supporting evidence.
Choosing v1 instead remains possible, but requires truthful native structured
claims or externally trusted signed review, including complete-domain evidence.

## Permitted statements and extraction rules

Every extraction uses exact pinned original bytes. UTF-8 spans are half-open
`[start,end)` byte offsets; DOM serialization is never the preserved body.
Publish extractor source/version/hash, dependencies, selectors, whitespace/date
rules, input pins, output hash and the entire derivation chain. Ambiguous or
duplicate nodes refuse extraction. This document does not implement an extractor.

| Role | Proposed rule | Binding and refusal |
|---|---|---|
| Announced date | Read the unique `li.b-list__box-list-item` whose `i.b-list__box-item-title` text is `Date:` on the exact event detail page. Preserve literal text; parse an English full month/day/year as a Gregorian date. | Bind the native event URL to the target event ID and the detail's `h2 a[href*="event-details/"]`. Use the existing conservative date-at-00:00-UTC timing rule, not a claimed local start time. Missing/ambiguous dates or contradictory versions remain UNKNOWN. |
| Participants/orientation | Read the two ordered `.b-fight-details__person-link` hrefs in the fight header. Bind both profile identities; preserve order and names as displayed. | Validate the event card's unique `tr[data-link]` occurrence and its participant hrefs. An approved card-display rule may compare the unordered membership while recording the original card order. Without that rule, disagreement blocks. Never infer the primary orientation from winner-first card order, names or UUID sorting. |
| Positive title | Exact normalized label `UFC Flyweight Title Bout` or `UFC Women's Flyweight Title Bout` in the unique `.b-fight-details__fight-title` is a candidate for a derived true predicate under approved designation semantics. | This packet demonstrates these two strings only. Bind target/date/orientation; reject contradictory card/detail designations. New or interim/BMF/other labels require an approved vocabulary extension or separate explicit evidence. |
| Non-title | Only an affirmative designation matching the finite ordinary-label vocabulary below may map to false, and only after the operator qualifies that vocabulary with provider support. | Missing title node, empty label, generic `Bout`, absence of a title icon, or an unknown string remains UNKNOWN. Schedule, finish round and stored false never decide title status. |
| Profile availability | Preserve the exact profile body, unique displayed name and source ID, and structurally valid corresponding captured profile row. Physical fields may be unknown. | Bind DOB/height/reach/stance to their labelled fields in `.b-list__info-box_style_small-width li`; preserve `--`/absent values as missing. A manual name-only stub or external athlete URL without preserved bytes does not prove provider profile availability. |
| History rows | Enumerate every `table.js-fight-table tr[data-link]`; retain the raw row span, detail href, two participant hrefs, event href/name/date and `.b-flag__text` relative to the profile participant. | Profile `win`/`loss` are participant-relative, not the repository's bout-level result type. Resolve both participants and the corresponding detail statuses. Do not copy lifetime `Record:` totals into UFC experience. |

Proposed ordinary-label vocabulary, observed in the packet: `Middleweight Bout`,
`Lightweight Bout`, `Bantamweight Bout`, `Featherweight Bout`, `Heavyweight Bout`,
`Light Heavyweight Bout`, `Welterweight Bout`, `Flyweight Bout`,
`Women's Flyweight Bout`, `Women's Strawweight Bout`, `Women's Bantamweight Bout`,
and `Catch Weight Bout`. This list describes observed strings, not an established
provider taxonomy. Their non-title entailment is currently **unsupported**.

For example, Allen–Duncan's detail SHA-256
`4d321d27f06d122ccb908aa99ad0a6cc98dc926d49c77bc664ee5378fc261ee5`
contains `Middleweight Bout` in bytes `[3466,3552)`. The event SHA-256
`66b2e2cec114cdb95b4cd2328939983547cebba59938f6a8b4fcc447e103b427`
contains `October 10, 2026` within `[2566,2699)`. Date and participant statements
are preserved; the boolean and repository identity bindings require the proposed
semantics and derivation. The old observations do not meet the contemporary rule.

## Identity and revision handling

Retain each exact request/source URL, source-native detail identifier and source
version. The existing computational UUID binding is UUID5/NAMESPACE_URL after
removing only `www.` from the URL. HTTP and HTTPS remain different computational
identities. This algorithm is lineage, not evidence that two URLs represent one
occurrence. Public host/path equivalence may corroborate links; it must not
silently coalesce repository records.

Create a separate mapping receipt for every native-to-repository identity and
record an explicit permutation when approved display semantics explain ordering.
For manual identities require the preserved manual construction plus the
affirmative unique source occurrence and compatible whole-row/statistic bindings.
The twelve accepted mappings and exact 1997 rematch retain their existing
bounded policy and contemporary revalidation requirements; this proposal does
not grant a general distinct-occurrence exception.

Changed event/date/opponent/detail IDs require a revision relationship supported
by a provider redirect/alias record, explicit announcement revision or a named
authority's evidence-backed transition. Preserve all competing observations,
original registration IDs and hashes, cancellations and unresolved page rows.
Do not choose latest, completed, preferred result or most convenient orientation.
April still needs an explicit `a842a365f408bb4a` ↔ `552f7cdaf93e1055` relationship.
Duncan's July occurrence needs the same kind of source/history reconciliation;
the observed profile/detail result does not silently replace an upcoming row.
Date/roster revisions append records under the existing protocol and invalidate
original primary membership where required. No backdated registration is allowed.

## Enumeration and completeness

Completeness is a proof obligation for each participant and exclusive boundary
`B = min(announced target date, observation cutoff's UTC date)`. It is not a
boolean copied from a source label. A proposed history is qualified only if all
of the following hold:

1. A named authority establishes the UFC domain, coverage from no later than
   1993-11-12, full-history enumeration semantics and revision/correction policy.
   For a single-page profile, supply evidence that all relevant rows are served;
   otherwise preserve every page/cursor, cursor transition and terminal marker.
2. Preserve enumeration roots, scope/filters, page identities, ordering, row and
   page counts, advertised totals if supplied, terminal response and a complete
   request/disposition ledger. Detect repeated/missing cursors, duplicate rows,
   truncation, access challenges and partial jobs. A total must be independently
   justified; a parsed number is not a coverage certificate.
3. Classify every linked event within the approved domain ledger. Preserve
   out-of-domain rows as disclosed source observations. Unknown promotion/domain
   classification blocks; do not silently discard DWCS/Road to UFC links or
   assume that every profile link is an admitted UFC occurrence.
4. Cross-check the participant enumeration against independently preserved
   completed-event listings and relevant event-card/detail occurrences through
   B, or an equivalent authoritative export with explicit completion and revision
   coverage. A failed completed listing cannot be replaced by a surviving
   upcoming listing that occupied the same legacy filename. Account for both
   participants, draws, genuine NC, overturned results and revisions.
5. Map every in-domain occurrence, resolve result/disposition differences and
   compare the complete mapped set to the admitted source projection exactly.
   Retain a discrepancy ledger and rejected/unresolved rows. Cancellations and
   empty/unknown outcomes are not resolved NC. The two known false-NC rows
   cannot be ignored to make sets match; a later separately authorized source
   correction and new projection must retain their original lineage.
6. Reject unresolved same-event occurrences and unapproved alias changes under
   existing identity guards. A profile/table with no resolved rows supports
   debut only after the entire empty in-domain enumeration is qualified. An
   explicit trusted UFC-debut statement can support a separate direct predicate,
   but cannot replace the admitted-history consistency check.

This packet lacks steps 1–2's authority/completion records and contemporary
step 4 coverage. Thus no complete-history/debut rule can execute truthfully on
it now. Source-domain completeness must not be described as complete lifetime
combat-sports knowledge.

## Freshness, clocks and capture dispositions

Proposed contemporary policy: roster, designation, profile availability and
enumeration frontier observations must be genuine non-cached source observations
within a declared interval of at most 24 hours ending at the common cutoff,
after all relevant orchestration/component freezes. All required bodies must
be observed by that cutoff; forecast completion still obeys the inclusive
14-day/24-hour protocol window. The 24-hour limit is an operator policy proposed
here, not a measured UFCStats update guarantee.

Older resolved occurrence bodies may be referenced as historical anchors only
with independently authenticated original provenance and contemporary authority
covering additions, removals and corrections through B. Require a provider
coverage watermark/export boundary or an approved exhaustive current enumeration
with current detail cross-checks and revision coverage. Recent page access alone
does not certify that the provider's database is current. Absent frontier or
revision capability retains BLOCKED even if every page fetched successfully.

| Capture state | Permitted treatment |
|---|---|
| Legacy hash-matching body | Forensic/source-statement reference at the exact original manifest clock. Missing request/cache provenance requires independent contemporaneous records or attestation with supporting records. A present-day review/copy cannot convert it into a new observation. |
| Legacy overwritten hash | Unavailable unless an exact hash-matching Git/frozen/operator-preserved body and authentic original observation are independently bound. Hashes alone cannot reconstruct it. Keep every reference visible. |
| V2 completed non-cached success | Verify exact independently trusted receipt pin, reservation and body through the accepted resolver. Success supplies byte integrity, not authority, parser success or completeness. |
| Cached | Preserve cache/origin clocks; never reset freshness to the local receipt clock. Ineligible for the proposed contemporary frontier. Historical use needs original provenance and revision coverage. |
| Failed/challenge/non-2xx | Preserve original status, reasons and exact bytes where available; forensic only. HTTP 200 challenge does not count as successful coverage. Stop acquisition on access blocking/rate limiting. |
| Incomplete/invalid/missing/corrupt | Keep reservations and reasons; do not infer an absent body, repair, resume, or fill coverage. Reject dependent claims. |

V2 clocks retain `scrapy_downloader_priority_200_v1` meanings: request boundary,
observation entry and receipt preparation are not wire-send, upstream-update
or publication-completion clocks. Retries/redirects may bypass this boundary;
A–Z discovery is not captured. Enumeration therefore requires its own complete
scope/disposition records. V2 does not certify acquisition-wide completeness.
No v2 observation exists in the present repository; legacy files are not v2.

## Proposed evidence structure and UNKNOWN conditions

Use three distinct, hash-bound record classes in a future closed schema:

| Record class | Required contents |
|---|---|
| Provider observation | Independently pinned original bytes/receipt, provider/native URL/identity, original clocks, boundary, status/cache/failure labels and source-stated values with selectors/spans. No locally invented provider boolean. |
| Operator qualification/attestation | Named accountable reviewer and externally verified authority/signature; approved roles/domain/taxonomy/enumeration/freshness policies; evidence and limitations; approval date and exact policy pin. Assertions of capability must be evidenced. Review clock remains separate from observation. |
| Computational derivation/binding | Input references, extractor/semantics pins, typed literal-to-predicate rules, native/repository identity maps, orientation, exclusive boundary, page/set checks, discrepancy dispositions and resulting predicate with reasons. Hashes bind computations; they do not grant authority. |

A predicate may be `SUPPORTED_DIRECT`, `SUPPORTED_DERIVED` or `UNKNOWN`; its
runtime use additionally requires qualified authority, observation eligibility
and whole-package validation. `UNKNOWN` always produces data-blocked, never
false, zero experience or NO PICK. Missing/ambiguous label, missing profile,
unqualified domain, incomplete enumeration, uncertain cache/origin clock,
stale frontier, failed page, unsupported identity, unresolved revision,
result mismatch, false-NC input, missing prior occurrence, conflicting source
statements or receipt/hash failure retain UNKNOWN/BLOCKED. Do not certify one
claim because another claim is supported. Integrity failures refuse the package.

No `authoritative`, `complete`, signed-review or verified-debut assertion is
generated by this proposal or its index. A future complete predicate must carry
the proof obligations above and match the admitted projection; a schema field
cannot stand in for missing evidence.

## Compatibility and activation boundary

The existing v1 validator accepts whole claims from exact JSON pointers or
trusted structured signed review. HTML extraction and this proposed three-record
derivation are deliberately incompatible with that validator. A raw v2 capture
receipt is also not a v1 table/evidence package. Do not implement this amendment
by setting `authoritative=true` on computed JSON or weakening v1 checks.

Approval would authorize a separately scoped versioned evidence adapter and new
contract freeze, plus independently authorized genuine observation packages.
Retain the four table/source schemas, exact source admission lineage and all
identity/revision checks. Domain classification or correction that changes the
admitted histories requires explicit source/scoring compatibility review; never
reuse a training manifest or rewrite its pinned rows to make it agree.

Keep registration coverage, outcome isolation, component pins, distinct feature
recipes, same-date exclusion, UNKNOWN schedules, first complete paired capture,
forecast lead times and evaluation definitions unchanged. No real feature
construction, prediction, fitting, outcome evaluation or promotion follows from
proposal approval. C2's October 3 observation cannot receive a later attestation
retroactively. Prospective forecasting remains **BLOCKED** and historical
comparison **STILL_BLOCKED** until separately authorized gates pass.
