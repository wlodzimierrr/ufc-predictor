# Phase 5C.13: qualified listed-history shadow policy proposal

Date: 2026-10-25. Status: **PROPOSED_NOT_APPROVED**.
Document-only proposal ID: `phase5c13_ufcstats_listed_history_shadow_proposal_v1`.
No executable contract, observation package, approved source or trial is created.

The proposed experiment asks whether the frozen models are useful when supplied
with a reproducibly enumerated, explicitly qualified source view. It does **not**
ask whether that source view is a certified complete UFC career. These are
different evidence standards and different experiments. The proposed narrower
claim is: "all admitted rows in this captured, declared UFCStats source scope
were enumerated and reconciled under the pinned rules," not "every real-world
UFC occurrence is known."

The enforced [v1 contract](phase5c1-shadow-evidence-contract-v1.md) and
[original prospective protocol](phase5-prospective-shadow-protocol-v1.md) remain
unchanged. The [earlier v2 proposal](phase5-source-evidence-contract-v2-proposal.md)
remains PROPOSED_NOT_APPROVED; this document neither approves nor implements it.
Historical comparison remains **STILL_BLOCKED**. Actual forecasting remains
**BLOCKED**, and production is unchanged. Approval to develop this proposal is
not adoption of its rules or authority to acquire sources or forecast.

## 1. The decision being offered

An operator can retain v1 and its existing proof requirements, or separately
adopt a *new, shadow-only* observable-source experiment with the limitations
below. The recommended option is to review this narrower proposal, audit
compatibility offline, and only then consider a new preregistration. It is not
to make v1 pass by setting `complete=true` on extracted HTML.

Three independent kinds of assurance must remain distinct:

| Assurance | What a successful check would establish | What it would not establish |
|---|---|---|
| Byte integrity and reproducibility | An externally pinned receipt resolves to the exact captured bytes; the same parser and policies reproduce the same records. | Authentic provider authorship, original publication time, completeness or truth. |
| Operator acceptance of source roles | An identified operator knowingly accepts UFCStats event, fight and profile statements for this limited shadow experiment, with a stated trust rationale and transport limitations. | A provider endorsement, externally verified signature, independently trusted timestamp or universal source authority. |
| Observable enumeration consistency | Every row/page actually within the captured and declared enumeration graph has an explicit disposition, with cross-source discrepancies resolved or blocked. | Absence of omitted real-world events, exhaustive careers, complete upstream corrections or a provider-update guarantee. |

The approval record must distinguish a risk decision from an evidenced factual
claim. An operator may accept unknown provider omissions for a descriptive
shadow trial; neither operator nor code may turn that choice into evidence that
omissions do not exist. No reviewer identity, authority or signature is supplied
by this proposal. Every approval below is presently **PENDING**.

## 2. Operator decisions, separately recorded

Each adopted item needs an accountable operator identity, role, actual decision
UTC, approve/refuse disposition, rationale, exact proposal/Git pin, evidence
references and explicit accepted limitations. A durable local decision receipt
is not described as a cryptographic signature or independently authenticated
source attestation. One blanket `approved=true` is insufficient.

| ID | Proposed decision for the new experiment | Refusal or missing decision means |
|---|---|---|
| D1: source-role risk | Accept public UFCStats event/detail/profile observations as the named listed-source view, not as certified complete UFC records. Disclose HTTP/TLS authentication and provider-authority limits; attach the actual trust rationale. | No real source qualification or ingestion into the proposed track. |
| D2: competition scope | Adopt the event-ID classification policy in section 3, including explicit DWCS/Road to UFC/TUF dispositions. | Domain UNKNOWN; dependent histories blocked. |
| D3: experience semantics | Accept captured listed-set consistency, not v1 complete-domain/debut evidence; initially require at least one admitted prior occurrence for each target participant. | Do not derive source-count experience for model input. |
| D4: title semantics | Adopt the exact finite label rules in section 4 as pragmatic shadow semantics, including the acknowledged non-title inference risk. | Ordinary labels stay UNKNOWN; v1 is unaffected. |
| D5: identity/revisions | Adopt source-native joins, preserved registered orientation and explicit permutation rules; retain unresolved aliases and append-only revisions. | No name-only joins, silent remaps or preferred-version selection. |
| D6: operational freshness | Accept the bounded recorder-observation/cache policy in section 6, explicitly not an upstream freshness certificate. | No eligibility based on local access time alone. |
| D7: model compatibility | Require and review a feature/domain/prior compatibility certificate for each frozen model, with no fitting or altered preprocessing. Acceptance cannot waive a demonstrated mismatch. | That model, and any required pair containing it, remain blocked. |
| D8: new trial scope | Require a separate future-only namespace, population and preregistration, retaining all original coverage, locked predictions and outcome isolation. | No real forecasting, retrospective substitution or evaluation in this track. |

These decisions authorize only the specifically adopted semantics. Separate
implementation, bounded acquisition and real-forecast scopes are still required.
Declining an item does not permit silently shrinking an already registered
population until it passes.

For a short operator response, the recommended named decision bundle is
**QLS1: qualified listed-source shadow semantics**: adopt D1–D6 and D8 as
written, require D7 rather than declaring it satisfied, and authorize a next
documentation-only, no-model offline compatibility audit. "Approve QLS1" would
record that limited choice after the operator has reviewed these rules; it
would not approve an adapter, capture, forecasting, source authenticity claim
or production change. "Retain v1 only" refuses the new track. Individual item
changes may instead be specified and must be recorded explicitly. No answer is
recorded in this document, and the actual user's identity/time must not be
invented by tooling. This offers a feasible risk-policy decision, not a demand
for a nonexistent provider completeness certificate.

## 3. What "listed history" would mean

The proposed domain is **UFCStats-listed, resolved occurrences on explicitly
classified UFC competition cards**. It is not all rows on a fighter profile,
all professional MMA, lifetime combat-sports history or complete actual UFC
participation. The historical starting-date claim required by v1 is not made.

Use a pinned event-ID ledger, with source body/span references and a reason for
every classification. The proposed taxonomy is:

| Class | Proposed treatment |
|---|---|
| Supported numbered UFC, UFC Fight Night/on-broadcaster, or UFC/TUF Finale competition card | In scope, only after positive event-ID classification supported by the captured event/card context. |
| DWCS, Road to UFC, TUF exhibition/house bout, other promotion | Explicitly outside the proposed UFC competition domain; preserve and report every source row and its classification. A TUF Finale is not a TUF exhibition. |
| Ambiguous promotion, unknown event identity, conflicting card classification | UNKNOWN; block dependent histories rather than drop the occurrence. |

No `UFC` substring classifier, lifetime `Record:` count or subtraction chosen to
match warehouse counts is acceptable. If saved training rows use a different
competition domain, do not pretend that this proposed domain is compatible.
Section 7 must resolve that question before model execution. A different domain
requires a separately versioned reviewed proposal, not an undocumented exception.

For each capture, declare an enumeration graph before extraction: target/event
roster roots; full participant profile tables; all advertised pages/cursors;
linked in-scope historical event/detail bodies; and every further occurrence,
opponent/statistic or global Elo dependency required by each feature recipe.
Record every requested, failed, incomplete, unknown and out-of-domain member.
An opponent or Elo index cannot quietly read a broader current warehouse after
the target participants' profiles have been checked.

"Enumeration complete within captured scope" requires all of the following:

1. Preserve exact root bodies, links, row/page counts, advertised totals,
   filters and request/disposition ledger. Follow every advertised continuation
   within a preregistered finite budget. Missing pages, repeated cursors,
   truncation, challenges, unmatched totals or exhausted budgets block.
2. Extract every occurrence row with byte spans and native event/fight/profile
   identities. Preserve contradictory/duplicate rows before reconciliation.
   No visible continuation plus a successful full-table parse may be recorded
   as `no_advertised_continuation_observed`, never proof of provider capability
   or hidden-page absence.
3. Cross-check the declared participant tables against captured event/card/detail
   links and an independently requested completed-event listing or a separately
   accepted equivalent export within the same capture. Record the exact covered
   event/date frontier. The second page route is an internal source-consistency
   check, not independent confirmation of real-world completeness. Inaccessible
   or unsupported listing coverage blocks; do not substitute an upcoming list.
4. Classify and account for every enumerated row. Compare sets by native
   occurrence identity, not aggregate counts. All mapped in-domain eligible
   occurrences must match the admitted projection, including draws and genuine
   NC. A missing, extra, conflicting or unsupported occurrence blocks the
   dependent target; the evidence packet retains every such discrepancy.
5. Apply strict outcome disposition before history admission. Empty/cancelled
   rows are not NC; L/W, draw and NC must resolve consistently across participants
   and detail. Unresolved announcements, target results and same-day/future
   results do not enter history. Existing false-NC defects need independent
   dispositions; this proposal repairs none of them.

Even a packet passing all five checks remains vulnerable to consistent provider
omissions and undisclosed corrections. Its permitted claim is scoped observable
set reconciliation. Store `career_completeness=UNKNOWN` separately from that
result. Do not emit v1 `complete`, `verified_history` or `verified_debut` claims.

Zero listed rows means **zero listed rows in this source view**, never verified
debut or known zero actual experience. Initial eligibility under D3 requires
positive admitted prior history for both participants. Empty-history bouts stay
registered as `listed_history_empty_experience_unsupported`, not deleted and
not NO PICK. Allowing them later would require separately supported feature
semantics and a new pre-outcome protocol, not a default-zero shortcut.
An offline audit may establish that the frozen software already uses an
*observed-zero-history proxy*, rather than verified career debut, and that the
same proxy/domain can be supplied unchanged from the new source view. That is
a possible compatibility finding without complete-career certification or
retraining. It would need an explicit additional operator decision and
pre-outcome eligibility amendment before zero-history targets could enter the
new track. Public labels would say `observed_zero_listed_history`, never
`verified_debut`; the unchanged internal column name is retained as technical
lineage, not promoted to a factual claim.

## 4. Target designation, profiles and extraction

All extraction is from pinned exact original bodies, with extractor/version/
dependency pins, UTF-8 half-open byte spans, selectors, literal values and typed
derivations. Do not preserve a DOM serialization as if it were the response.
Missing, ambiguous or duplicate required nodes block. Structural profiles must
exist; optional measurement `--`/missing values remain missing, with repository
unit conventions preserved. A name-only manual stub does not prove a profile.

Proposed title labels are matched after one pinned whitespace-collapse rule,
with no case folding, substring test or inference from schedule/result/icon:

| Exact designation | Proposed qualified-track value |
|---|---|
| `UFC Flyweight Title Bout`; `UFC Women's Flyweight Title Bout` | `true`, labelled `EXPLICIT_TITLE_DESIGNATION`. These are the two specific positive strings evidenced by the earlier packet, not a complete UFC title taxonomy. |
| Each exact ordinary designation listed below | `false`, labelled `OPERATOR_ADOPTED_ORDINARY_DESIGNATION`, only if D4 is explicitly adopted. This is a pragmatic inference, not a provider-certified negative. |
| Generic `Bout`, absent/empty label, unsupported weight/title/interim/BMF/other label, card/detail contradiction | UNKNOWN and data-blocked. No fallback to a stored false or absence of `Title`. |

Proposed finite ordinary set: `Middleweight Bout`, `Lightweight Bout`,
`Bantamweight Bout`, `Featherweight Bout`, `Heavyweight Bout`,
`Light Heavyweight Bout`, `Welterweight Bout`, `Flyweight Bout`,
`Women's Flyweight Bout`, `Women's Strawweight Bout`,
`Women's Bantamweight Bout`, `Catch Weight Bout`.

This explicitly changes the evidential standard only for a new approved
qualified track. It does not establish that ordinary labels are exhaustive or
correct in all provider pages. Unknown/new labels require a reviewed finite
vocabulary extension frozen before use, never outcome-driven exceptions.
Preserve title-derived risk labels beside forecasts so qualified negatives are
not presented as direct source booleans. Weight-class mapping is independently
pinned and contradictory/missing classes block; it is not inferred from results.

## 5. Identities, orientation and known discrepancies

Bind target/native fight/event/profile URLs through exact preserved links.
Preserve request and final URLs separately. Repository UUID derivation is a
computational binding: remove only the optional `www.` prefix before the
existing UUID5/NAMESPACE_URL convention; do not collapse HTTP/HTTPS or invent
general native-ID aliases. Different derived IDs need an explicit mapping
receipt, not automatic equivalence because the names match.

The registered detail-oriented participant order remains fixed. Event cards
may display winner-first order: compare unique native-profile membership and
record the explicit permutation. Never orient by winner, alphabet, UUID sort
or a model probability. Resolve outcome/statistic sides before conversion to
the registered orientation. Changes in registered orientation, opponent or date
are revisions, not harmless formatting corrections.

Keep all competing observations and append-only dispositions. A later capture
does not inherit old duplicate/alias decisions without whole-row/statistic and
identity revalidation. Same-event rematches require explicit occurrence
evidence; no generic exception is granted by the known 1997 case. Historical
fitting exclusions do not resolve source identity conflicts.

April `a842a365f408bb4a` versus `552f7cdaf93e1055` remains UNRESOLVED. Any new
target whose required fighter/opponent/Elo history depends on that unresolved
relationship is blocked. Do not choose the resolved/preferred outcome or hide
the stale announcement to make an enumeration agree.

The July Cannonier–Duncan issue is **not an unresolved identity transition**.
The [identity corrigendum](implementation-reports/20261004-manual-source-corroboration-and-identity-correction-report.md)
establishes the same native/repository occurrence and event identities. The
[Phase 5C.12 report](implementation-reports/phase5c12-approved-manual-fight-correction-report.md)
records the approved current CSV/warehouse result correction, independently
verified complete. Older proposal/report passages requiring a July event-ID
transition are superseded on that point, not silently rewritten. The correction
does not certify title, full histories, current observations or historical
availability. Its old `scraped_at` is base lineage, not the correction's
knowledge time; future as-of consumption must retain the correction receipt.

## 6. Achievable clocks, freshness and immutable observation

Let C be one shared capture-completion UTC instant. Proposed D6 requires every
required root, profile, enumeration, detail and statistic observation in the
packet to come from the current bounded capture interval `[C-24h, C]`, after
the new implementation/component/protocol freeze. Initial implementation should
not reuse older body observations as historical anchors. Unchanged body content
observed through a genuine new request is allowed; copying it locally is not a
new observation. The 24-hour limit is an operational policy, not a measured
provider update SLA.

Capture exact observed bytes, source/request/final URLs, request/response local
recorder boundaries, status, redirects, relevant cache headers, access-block
classification, parser disposition and SHA-256 in immutable objects and distinct
observation receipts. Preserve the boundary semantics of the existing v2 raw
capture implementation; do not relabel callback entry as wire-send, publication
or upstream-update time. Pin actual clock provenance and known uncertainty.
Missing required local clocks, impossible order/future instants or clock error
crossing a timing boundary block. Local clocks are not independently trusted
timestamps and must not be presented as such.

Proposed cache handling deliberately does not claim provable non-cached origin:

| Observation | Proposed treatment |
|---|---|
| Actual new permitted response with exact body and recorder metadata, no local replay | Eligible for *operational observation age*. Upstream freshness and absent cache metadata remain UNKNOWN with explicit risk labels if D6 accepts them. |
| Positive cache evidence | Preserve `Age`, `Date`, `Via` and other available metadata without treating them as trusted upstream clocks. Reject explicit stale/warning states or a declared cache age exceeding 24 hours. A within-limit age does not establish the underlying record update time. |
| Local/304/offline body replay without a new complete body observation under this initial policy | Ineligible for a new frontier; retain as forensic evidence. A future revalidation-cache protocol needs separate review. |
| Manually supplied text with unknown capture UTC/status/final URL/cache provenance | Forensic/current-case review only, as in Phase 5C.11–12; cannot satisfy post-freeze observation/timing. A genuine contemporaneous manual capture needs separately evidenced recorder metadata and explicit acceptance, not a retroactive attestation. |
| Challenge, access denial, rate limit, failed/incomplete response, corrupt hash | Preserve failure bytes/receipts; stop that acquisition at the boundary; no retries, cookie replay or access-control bypass. Never treat HTTP 200 alone as success. |

Transport security must be truthful: ordinary HTTP does not authenticate origin;
TLS verification must not be disabled to obtain a success. D1/D6 must explicitly
accept the observed transport and provenance limits or reject the package.
Source-server timestamps do not override the recorder cutoff. Source database
freshness, hidden omissions and comprehensive revision coverage remain UNKNOWN
even after all responses succeed. New observations can record changed source
statements, but cannot establish the statements' original publication time.

## 7. Hard model-input compatibility gate

A new evidential name does not change the saved models' meaning. Before any
loading that might execute estimators, conduct a separately scoped offline
semantic audit of manifests, source-domain admission, feature code and saved
prior contracts for challenger, reference and optional March control. Audit
must not fit, score, tune, open frozen outcomes or rewrite pinned components.

Produce a feature-by-feature compatibility certificate covering at least:

- The training occurrence domain versus the proposed event-ID ledger, including
  DWCS/Road to UFC/TUF and draws/NC; exactly which source counts each experience,
  career, record, activity, Elo and opponent feature encodes.
- Nonempty but partial source histories: omitted older occurrences can alter
  rates, activity, opponent quality and global Elo even when both fighters have
  observed prior bouts. A positive count neither resolves those differences
  nor licenses an unbounded warehouse/global-history index. Enumerate the full
  required dependency graph and document whether each recipe's original
  semantics permit that qualified source scope unchanged.
- Missing-data semantics: sparse admitted statistics and legitimate null optional
  physical fields versus absent occurrences/profiles. No new zero filling,
  aggregate-from-round synthesis or omitted missing history to make inputs pass.
- `both_debuting` and the three saved debut-prior columns. Source emptiness cannot
  become verified debut. Determine whether these are already computational
  observed-zero-history proxies under the frozen training code and declared
  source domain; actual complete-career proof is not required to establish
  equivalent proxy semantics. Use only unchanged saved priors under those
  established original computational preconditions and explicit operator risk
  acceptance. Positive prior histories do not by themselves prove every other
  experience feature is compatible. Zero-history cases remain blocked until
  that finding and the additional eligibility decision above, even if a numeric
  matrix can be produced.
- Recipe-specific clock, schedule, orientation and feature-column requirements,
  including separate legacy reference defaults and the March training-reference
  interface. A contemporary source certificate is never relabelled training data.

The challenger manifest already describes retrospective reconstruction with
incomplete histories; that limitation is not automatic approval of arbitrary
new domains. Its fixed schema and saved prior guards still apply. There are
three possible audit dispositions: `SUPPORTED_UNCHANGED_WITH_QUALIFICATIONS`,
`UNSUPPORTED` or `UNKNOWN`. Only the first permits later model use after the
remaining gates and explicit scope authorization. An observed semantic mismatch
cannot be waived by operator optimism, a new field name or shape/hash parity.
Require a separate model/preparation experiment if compatibility would require
new features, modified priors, recalibration or refitting. This proposal does
not authorize that experiment or automatic substitution of a different model.

## 8. New prospective population, scoring and outcomes

If adopted and implemented, create a fresh qualified-track namespace, registry
and preregistration before any real capture, feature construction or probability
for that track. Freeze exact discovery roots, bounded acquisition policy,
competition/eligibility rules, required model pair, first-capture selection,
timing, coverage denominators, feature recipes, metrics and stopping/monitoring
rules. Those choices must be fixed without trial outcomes or performance-driven
selection. A new namespace identifier and new start instant must be explicit;
the proposal identifier above is not a runnable trial version.

Do not transplant the old trial's primary membership/timing or retrospectively
backdate Allen–Duncan/another expired target as a new forecast. Preserve all
**35 original registrations and 163 considered rows** in their original roots,
including expired, unresolved and blocked records. Reference them for lineage
without copying them into a new primary cohort. New consideration is all rows
in the preregistered discovery scope from genuine post-freeze observations,
irrespective of data readiness or confidence. Unknown provisional identities,
failed pages, domain blocks and revisions remain in coverage.

Propose carrying forward the original conservative timing rule only as a
separately frozen choice for the new cohort: inclusive target-date-00:00-UTC
minus 14 days through minus 24 hours, for both C and forecast completion.
Registration precedes probabilities. Eligibility histories are strictly before
`B = min(registered target event date, UTC(C).date())`, excluding the entire
capture day, target day and target identity. Feature-reference dates and each
model's pinned recipe remain explicit; no intervening result, target outcome,
future/same-day opponent statistic or unrestricted Elo dependency may leak in.

Select and reserve the **first eligible complete paired capture** before any
probability is seen. Both required models use the same C and immutable source
packet, not necessarily the same feature algorithm. Preserve all earlier
incomplete attempts. A failed/crashed reservation stays visible and blocks
automatic favorable retry. Revisions invalidate primary membership under the
new frozen rule while retaining every original registration and forecast.
Optional March failure is explicit and cannot silently change the required pair.

Load only saved preprocessing, priors, learners and calibrators after all gates.
No fit-at-inference, raw fallback, changed component or source substitution.
Hash-lock raw/final probabilities, inputs, processed matrices, policy/receipt/
model pins, timing and decisions before any outcome loader is authorized.
Outcome-free capture utilities may read independently observed prior results
needed for histories; they must not join target outcomes or trial result tables.
Cancellation, unresolved and revised targets retain separate dispositions.

Keep inclusive NO PICK `0.40 <= p <= 0.60`, high confidence `p <= 0.30` or
`p >= 0.70`, and latent fighter-1 selection `p >= 0.5`. Data-blocked never means
NO PICK. The new preregistration must explicitly pin log loss/Brier/calibration,
paired event-cluster uncertainty and all coverage definitions; do not silently
inherit incompatible historical Phase 4 versus prospective Phase 5 bin/seed
definitions. Qualified-source results must not be pooled with v1 as if the
evidence standards were identical. No threshold, source rule, domain, model or
calibration change may be selected from outcomes. Evaluation and promotion each
require later separate authority; this trial never automatically promotes.

## 9. Decisive acceptance tests for any future implementation

These are required future synthetic/offline tests, not a claim they ran here:

| Test family | Required passing/refusal behavior |
|---|---|
| Contract isolation | Reject v1/v2 namespace impersonation, missing item approvals, mismatched decision/policy pins and approval receipts with fabricated source guarantees. Existing v1 tests and artifacts remain unchanged. |
| Byte/provenance integrity | Refuse body/receipt/member/selector tampering, duplicate JSON keys, missing/escaping paths, fake clocks, stale declared cache or access checks. Reproduce original spans, enumeration and ledgers from preserved sources offline. |
| Full declared scope | Preserve every discovered row, mixed-domain item, provisional ID and failed/partial request; refuse truncated pages, unknown classifications, missing continuation and unresolved set differences. Matching counts alone never pass. |
| Experience/debut | Zero listed rows remain unsupported; no v1 complete/debut claim is generated. Require approved positive-history eligibility and model-specific semantic certificates, not just 50 numeric columns. |
| Title | Exact approved positive/ordinary labels derive typed risk-marked values. Generic `Bout`, missing/unknown/interim labels, duplicate nodes and contradictory card/detail descriptions block. V1 still rejects ordinary labels. |
| Identity/revision | Reversed winner-first card order maps by profile IDs without changing registered orientation. Names, same-event pairings, new aliases and April conflicts cannot choose a canonical result. July uses the existing corrected identity plus correction provenance. |
| Temporal invariance | Appending future, same-day, target-outcome or post-C data cannot change eligible matrices; all opponent/statistic/Elo accesses obey declared dependencies and caps. Preserve each model's distinct date/recipe semantics. |
| Pair selection/publication | Same C, pre-probability first-complete reservation, concurrent/replayed capture refusal, retained failures, no overwrite and deterministic full-source rebuild. Missing counterpart never becomes a loss. |
| Isolation/preservation | Synthetic tests cannot access network, warehouse, frozen target outcomes, real estimators or fitting. Existing registrations, 163-row coverage, component/source policies and production pointers are byte-identical. |

## 10. Narrow change boundary and next step

This phase adds this proposal and a separate handoff report only. It changes no
runtime validator, adapter, feature function, schema, scraper, raw-capture store,
warehouse, current data, source contract, model bundle, production pointer,
registry, holdout, original probability or earlier report. No source is newly
qualified, no historical gap repaired, and no existing blocked run completed.

The smallest next authorized step is **operator review of D1–D8** with explicit
accept/refuse decisions and limitations. A separately authorized follow-on may
perform the offline domain/feature/prior audit first if D7 lacks enough evidence;
that audit does not need real forecasting or new website access. Only after
adoption and a compatible model pair should a fresh phase implement the closed
schema/extractor with synthetic tests and freeze the new preregistration.
Bounded genuine acquisition and real scoring remain separately scoped later
phases. If compatibility is unsupported, stop with that concrete finding rather
than building a permissive adapter to force a READY status.

References reviewed: [v1 evidence contract](phase5c1-shadow-evidence-contract-v1.md),
[old v2 proposal](phase5-source-evidence-contract-v2-proposal.md),
[original prospective protocol](phase5-prospective-shadow-protocol-v1.md),
[Phase 5C.8 qualification](implementation-reports/phase5c8-local-source-qualification-report.md),
[Phase 5C.9 pilot](implementation-reports/phase5c9-allen-duncan-evidence-pilot-report.md),
[identity corrigendum](implementation-reports/20261004-manual-source-corroboration-and-identity-correction-report.md),
[Phase 5C.11 proposal](implementation-reports/phase5c11-offline-manual-fight-correction-proposal-report.md),
and [Phase 5C.12 correction](implementation-reports/phase5c12-approved-manual-fight-correction-report.md).
Code reviewed read-only: `modeling/phase5c1_contract.py`,
`modeling/phase5c1_evidence.py`, and the relevant history/debut compatibility
definitions in `features/forecast_replay.py`, `modeling/phase5c1_adapters.py`
and `modeling/phase5b4_contract_v1.py`. No estimators or source observations
were executed during proposal preparation.
