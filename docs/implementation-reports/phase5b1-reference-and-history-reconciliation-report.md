# Phase 5B.1 reference freeze and current-history reconciliation handoff

**Phase implementation: COMPLETE within the bounded authorization. Frozen reference
bundle: READY. Challenger preparation/fitting: BLOCKED. Matched prospective
capture/scoring: BLOCKED. Historical comparison: STILL_BLOCKED.**

The production-derived prospective reference now contains the unchanged base
learner, saved legacy debut preprocessing and one persisted Platt estimator.
It is **not an exact recovery of the original deployed probability stream**.
The fourteen current-history groups have twelve evidence-backed duplicate
dispositions, one proven distinct same-event rematch disposition, and one
unresolved April occurrence group. That unresolved group blocks the entire
challenger preparation. No replacement training preparation or challenger fit
was published. The existing diagnostic 8,558 rows and 341 proposed OOF IDs
remain blocked diagnostics.

Work took place offline in `/home/wlodzimierrr/ufc-data` on October 2, 2026.
Initial worktree was clean on `main`, HEAD
`5ac88648c3cda5523e8bd80f906557d37edd43b8`, upstream `origin/main`, with fetch/push
remote `https://github.com/wlodzimierrr/ufc-predictor.git`. Local upstream tracking
and HEAD matched. Ancestor and repository searches found no applicable
`AGENTS.md`. The complete Phase 5A report, v1 protocol, preregistered config and
Git handoff were read. No source refresh, warehouse connection, historical
Phase 4 recovery search, frozen holdout outcome parsing or joined outcome
evidence parsing occurred. Git publication is the explicitly authorized
network operation at handoff; no data network fetch occurred.

## Starting trust anchors and preservation

The accepted parent is
`data/experiments/phase5a_prospective_shadow/20261002_phase5a_current_preparation_v2_blocked/`.
Its checksum root `eda0256815b59a749d215fe00ec533826150bd2a8f2240b90d2baa6f8370bf56`
and training manifest
`e23f7635c096e54090ffd229ab547ceb9e5f26fa7b71b2eac876be70f14ebe79`
were verified before use, including all 69 files, source-manifest sizes/hashes,
training source hashes, package versions and recorded code hashes. Reference
partitions and manifest were independently regenerated from the exact stored
rows and matched byte-for-byte before component fitting. The production pointer
remained `ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa`;
the recorded base learner and metadata hashes also matched current protected
bytes. Parent training/folds were never supplied to an estimator.

A hash-only baseline covers **42,346 existing files**, including frozen
holdouts, Phase 1–4 evidence/reports, Phase 5A runs and all 35 registry records,
accepted preparations, the March research control, models/pointer/predictions,
root CSVs, raw storage and unrelated files. It excludes Git internals,
environments/caches and the credential `.env`. Credential contents were not
inspected, printed or serialized into phase evidence.
Initial unrelated tracked changes: none. No credential or dependency directory
is included in phase artifacts.

Baseline SHA-256:
`03c4d262e28aba28ab5539559c01372812039c8e071cae35bfb01bc2a5d1a0a9`.
Final post-publication preservation at `2026-10-02T14:45:11.177402+00:00` verified all
42,346 files: **zero changed, zero missing, zero new files in protected roots**.
The complete baseline and receipts are exclusively published at `data/experiments/phase5b1_reference_and_history/20261002_phase5b1_handoff_evidence_v1/`,
checksum root `834cd649daaec1ad73b6251cd4b7297767391208375e02bc56001f4dc2dfd1fc`.
The final preservation, tests and authoritative verification receipts are at
`data/experiments/phase5b1_reference_and_history/20261002_phase5b1_handoff_evidence_v2_final/`, root
`c3551f23dc74a54f6ab00af821d89ee1969020b5031c87b64e9b2a2856640c22`.
Its verified dependency points to the complete unchanged initial baseline.
Final preservation-check hash: `dfd196f7f82adb6e5f93e8827b2be06376800402a196ee7a924d54806dac9b8b`.
Hashing protected outcome bytes for preservation did not parse their contents.
The initial receipt’s wording that credential content was never opened was too
broad: inherited `warehouse.db` imports invoke `load_dotenv` at import time.
Those imports can read `.env` into the process environment; no connection was
made, no credential was displayed/published, and the file was not written.
The frozen initial receipt is preserved with this explicit correction.

## Independently frozen reference

Artifact: `data/experiments/phase5b1_reference_and_history/20261002_phase5b1_frozen_reference_v3_identity_guarded/`.
Checksum root: `12c17761fe76998e49fb5e79ea8a455861e22eaf57b37071f692bab17c1ba381`.
Manifest pin: `20a9695de6cd77d74ff67f5cb0b62da2f127fbcdc181fa9d9fc2b3f8f62ad48a`.
There are ten checksum-covered components plus the root; publication refuses
existing names and an incomplete artifact is unloadable. This is outside
`models/` and copies the verified base bytes rather than depending on a mutable
production pointer at load time.

| Partition | Exact rows | Exclusive/inclusive calendar contract | Sorted membership SHA-256 |
|---|---:|---|---|
| Legacy preprocessing | 6,391 | before 2022-03-12 | `2013992e43c41c7c1825b93d3d71a84b14a3ef4e68760e4093a79f7bcf69fbf3` |
| Legacy calibration | 1,009 | [2022-03-12, 2024-03-09) | `de0ed1872e81ab8e4a5ae77a550a94a555e178ba8e53bcf42118e2f70503d5db` |

All 7,400 bootstrap rows pass unique identity, exclusion, source event/fighter
join, captured orientation, binary label and finite-or-missing numeric checks.
The partitions are disjoint and calibration has both classes. All 166 exclusions
are enforced. Exact input and source component hashes are recorded individually
in the reference manifest's `input_hashes`, not inferred from row counts.
Original production metadata recorded 6,376/1,007: this freeze retains the
**+15/+2 membership difference**. The existing learner's original fitting
population cannot be repaired by today's exclusion/preprocessing checks.

Legacy preprocessing uses the unchanged `compute_debut_priors` recipe on the
exact 6,391 rows. It preserves neutral 0.5 debut probability and training-derived
physical standardization/fallbacks. Saved priors are applied to the exact
calibration rows in the recorded 50-column order. The existing learner obtains
raw probabilities; one `LogisticRegression(C=1e10, solver="lbfgs", max_iter=1000)`
fits clipped log odds with epsilon 1e-8 and positive class 1. It is persisted,
not recomputed during loading. Fitted slope/intercept are
`1.1047836936721098` / `-0.18915824612170556`, convergence in five iterations.
These are component parameters, not predictive comparison metrics.
No base learner retraining, hyperparameter search, membership substitution,
corrected challenger feature substitution or calibration assessment occurred.

Final adapter audit required identity rejection before *all* indexes, including
statistic lookups. Two exclusive successors carried forward the already saved
components without another preprocessing or Platt fit. v2 added repeated-pair
rejection; v3 moved that rejection ahead of shared source-validation index
construction and pinned the exact rematch participant set. v3 is the
authoritative bundle for the next session. All eight non-manifest/code
components, including learner, preprocessing, calibrator and parity, are
byte-identical across all three runs. Prior freeze times for those components
are retained honestly; the adapter/bundle clocks advance.

| Preserved predecessor | checksums.json SHA-256 | manifest SHA-256 |
|---|---|---|
| `data/experiments/phase5b1_reference_and_history/20261002_phase5b1_frozen_reference_v1/` | `a1075709950149e25ca97c3b772366ae913b433a6ce759ffe41cf86e75b81982` | `6edce65a0dc5994026074bff2380998e549afb66c1c81c0e400d8bd73c19be9d` |
| `data/experiments/phase5b1_reference_and_history/20261002_phase5b1_frozen_reference_v2_identity_guarded/` | `cf13e2f9196b9ca83ffead6364386f6d6bbe586f4a75e8d02b3c8966d27aa9dd` | `c7f2ab872b735de72c6faea872a04f4aff812496dd77bc5d75a71a9e6bac1036` |

These predecessors remain immutable evidence of the freeze/audit sequence.
Their older code provenance is superseded; the current fail-closed loader does
not accept them as the authoritative reference. This implementation hardening
changes no numeric preprocessing/calibration/legacy feature recipe and fits
nothing again. No frozen file was rewritten to conceal the audit.

| Reference component | SHA-256 |
|---|---|
| `base_model.joblib` | `0585077675968c2a3537ffd3fddb87a9eb610c98d36200b64dd5fe148599389a` |
| `production_metadata.json` | `d10a0e6fd17996edac5d6aa90b4436ade1a5d2424b7dbe2f9ed379c469c4e02a` |
| `preprocessing.json` | `82ac7082a22a8f7dc46d429c0ee5c39e9e3fe3b8262c3b13b6f5f124018cbb74` |
| `calibrator.joblib` | `c31d67bbf3d859bdea97705ce4d8261294bd0d1500e49f4ee01f966e72f24f27` |
| `feature_order.json` | `7053fdc209884967e19635555c5cf9ca5b2fc23a4fc34500941429e316ce86cc` |
| `bootstrap_parity.json` | `322bab3c5714cc67e5dd1bef16ff9d0b9e1db56e19c4a726e974ed20507d51fe` |
| `code_versions.json` | `2ef025e31ac2247c7b76958092934c80d6de3efb2604343e4ba6d8326526dff3` |
| `legacy_source_provenance.json` | `e54aa725b9f0f8ef71c33d9f18a964a4ea5b40605b36ab10d939bcfeee731a44` |

Actual component freeze clocks are UTC and follow the original source capture;
they were never backdated to Phase 5A:

| Component clock | Actual UTC |
|---|---|
| adapter_pinned_at | `2026-10-02T14:42:15.887155+00:00` |
| base_bytes_verified_at | `2026-10-02T14:24:32.549708+00:00` |
| bundle_frozen_at | `2026-10-02T14:42:15.887537+00:00` |
| calibrator_frozen_at | `2026-10-02T14:24:32.818738+00:00` |
| preprocessing_frozen_at | `2026-10-02T14:24:32.563287+00:00` |

The source capture completed at `2026-10-02T13:28:09.905938+00:00`.
The learner/preprocessing/calibrator were frozen at approximately 14:24 UTC;
the final guarded adapter and authoritative bundle froze at 14:42:15.887537 UTC.
**A later paired
forecast requires a new eligible matched capture after the components are
frozen. The original capture cannot be retroactively turned into a forecast.**

`load_reference` requires externally supplied root and manifest pins, the exact
component list, trusted parent/source/membership provenance, unchanged learner
and metadata bytes, package/code compatibility, ordered features, recipe and
valid actual freeze clocks. All checks precede deserialization. It rejects
missing/tampered components and incompatible provenance and has no fitting,
live query, writer or raw-probability fallback. Bootstrap verification reuses
saved components; it does not refit them.

Save/load parity is **exact** on both authorized bootstrap populations: prepared
matrix, raw probability and calibrated probability byte hashes match the
in-memory freeze and subsequent loads. `bootstrap_parity.json` stores hashes
and counts, not predictive scores or prospective forecasts. The verification
CLI independently repeated the check. Historical bootstrap probabilities were
used only for the authorized Platt fit and persistence parity.

## Isolated legacy reference feature adapter

`modeling/phase5_reference_adapter.py`, version
`phase5_legacy_reference_adapter_v1`, consumes an in-memory frozen capture,
explicit UTC observation cutoff, outcome-free target IDs and saved preprocessing.
It performs no live data loading, production writing, component fitting or
probability calculation. Real contemporary target feature/scoring calls were
not made; future behavior was exercised only with synthetic fixtures.

Pure feature modules are pinned under `modeling/reference_legacy_v1/` from
preserved pre-correction Git commit
`6c000dfc19efc43a3ba24091baf9c8fcd760d226`. Original source paths and byte hashes
are in `provenance.json`. Changes are mechanical import namespace relocation
and verbatim extraction of the two pure bout-mapping functions and weight ranks
without pipeline writers. The adapter wrapper explicitly checks source/target
contracts and uses the caller's UTC cutoff instead of machine `today`.
The selected legacy recipe is disclosed; exact original deployed code or
probability-stream recovery is not asserted.

The legacy snapshot date remains `min(observation UTC date, event date)` for
age, activity and decay. Legacy Elo processes the supplied entire capture in
stable date order, with sequential within-date updates and source-order
sensitivity. Synthetic adversarial tests explicitly preserve this difference
from the date-frozen challenger: intervening capture results can affect Elo at
a future target date even when histories/physical snapshots use an earlier
observation date. Later primary captures still require all source bytes to be
known at the matched observation cutoff. The adapter requires an identity-normalized common capture before any statistic,
fighter-history, opponent or Elo indexing. It rejects repeated pairs except the
exact evidenced rematch event/bout/participant/link/date contract. This input
validity gate does not substitute corrected challenger numeric features or
schedule semantics into the reference. The unresolved current capture cannot
be indexed by the future reference adapter either; reference component loading
and bootstrap freezing remain independent of that future-input blocker.

Stored schedule values and legacy duration/five-round-experience behavior,
boolean/stance defaults, sparse/statistic default zeroes, mutable profiles,
rounded stored bootstrap numerics and uncertified history coverage remain.
The 7,400 stored bootstrap rows retain finish-round/schedule defects and were
computed in August 2026 after their historical events. Unknown history can still
look like debut internally; future registry eligibility separately requires
attested experience/debut and title evidence and cannot infer them from defaults.
Optional profile values remain missing; entirely absent profiles are rejected.
There is no claim that the isolated legacy adapter equals the corrected
challenger or that its fresh in-memory values recover earlier deployed values.

Packages remain Python 3.11.2, numpy 2.4.3, pandas 3.0.1, scikit-learn 1.8.0,
xgboost 3.2.0, joblib 1.5.3, psycopg2-binary 2.9.11 and pytest 9.0.2.
All runtime feature dependencies and freeze/loader command bytes are pinned.

## Fourteen bounded identity dispositions

The policy was written before derived-view publication at
`docs/phase5-history-identity-policy-v1.md`, version `phase5_history_identity_v1`,
SHA-256 `8b829d4a65641c4963f18931453e183b43eced8153e3e024010ebc72f05a16a9`.
Shared Phase 3B/current-v1 guards and all frozen exclusions remain unchanged.
The investigation considered these fourteen groups and their directly relevant
four preserved event pages, two April detail pages, manual construction code,
and pinned contemporary rows/statistics/identity exclusions. No historical
Phase 4 recovery campaign or new page/warehouse reads occurred.

Each table row is backed by `identity_ledger.json`: original IDs and untouched
full source rows, individual row/statistic hashes, occurrence links, captured
fighter orientations, manual creation orientation where relevant, evidence
hashes, chosen whole-row lineage and conflicts. All fourteen captured source
pairs have matching orientation across their two records. The twelve retained
canonical rows keep the captured official orientation. Manual creation order,
which can differ from subsequent captured orientation, is recorded separately;
no labels/statistics are flipped to choose a convenient winner.

| Group | Date / participants | Original IDs | Disposition | Chosen whole source row(s) | Evidence |
|---|---|---|---|---|---|
| 1 | 1997-12-21 / Kazushi Sakuraba ↔ Marcus Silveira | `2c4d505e-c625-5e27-89f8-e36c2b8224b4`<br>`383d786b-c425-517e-a2af-d4cb19081135` | PROVEN DISTINCT — retain both | Both original rows | E1: two card/detail occurrences, separate captured finish/statistic records |
| 2 | 2026-04-18 / John Castaneda ↔ Mark Vologdin | `498de4bd-d781-52af-a383-802158196d2d`<br>`4b08f65d-db68-5091-9568-4748c4cf7318` | UNRESOLVED — retain both, global blocker | None; both preserved | E2/E3/E4: old/new URLs lack an explicit occurrence transition |
| 3 | 2026-07-18 / Jean-Paul Lebosnoyani ↔ Seokhyeon Ko | `4e4eed91-2b39-5797-83a7-81cce50c17d2`<br>`6168dff5-3df1-5ddd-8a5f-17bff9a3b0df` | PROVEN DUPLICATE — coalesce in derived view | `6168dff5-3df1-5ddd-8a5f-17bff9a3b0df` | E5 + M: unique linked card occurrence, unique manual ACTIVE_BOUTS entry, exact manual UUID/URL reconstruction |
| 4 | 2026-07-18 / Fatima Kline ↔ Tabatha Ricci | `7189f7d3-d325-5abd-8e57-da16f3032017`<br>`76e979fe-4b60-5d8a-ba49-811cf53505c3` | PROVEN DUPLICATE — coalesce in derived view | `7189f7d3-d325-5abd-8e57-da16f3032017` | E5 + M: unique linked card occurrence, unique manual ACTIVE_BOUTS entry, exact manual UUID/URL reconstruction |
| 5 | 2026-07-18 / Stewart Nicoll ↔ Alden Coria | `3ea09870-977d-53fc-a70e-d49be991ebbd`<br>`a01b704f-c26b-54b0-bc6c-68af89c315eb` | PROVEN DUPLICATE — coalesce in derived view | `3ea09870-977d-53fc-a70e-d49be991ebbd` | E5 + M: unique linked card occurrence, unique manual ACTIVE_BOUTS entry, exact manual UUID/URL reconstruction |
| 6 | 2026-07-18 / Tommy McMillen ↔ Alberto Montes | `d16ce11f-f1eb-5eee-aefa-304e9ce176d3`<br>`f2783f53-9556-5653-9815-c15e8614412c` | PROVEN DUPLICATE — coalesce in derived view | `f2783f53-9556-5653-9815-c15e8614412c` | E5 + M: unique linked card occurrence, unique manual ACTIVE_BOUTS entry, exact manual UUID/URL reconstruction |
| 7 | 2026-07-18 / Dricus Du Plessis ↔ Kamaru Usman | `3119317d-2397-5e20-838f-d4508e3eea42`<br>`e439fb69-cd79-5705-842a-4f98ddbf492a` | PROVEN DUPLICATE — coalesce in derived view | `e439fb69-cd79-5705-842a-4f98ddbf492a` | E5 + M: unique linked card occurrence, unique manual ACTIVE_BOUTS entry, exact manual UUID/URL reconstruction |
| 8 | 2026-07-18 / Felipe Franco ↔ Levi Rodrigues Jr. | `03bec0e0-8efe-55db-9971-bd36132b3c1e`<br>`36c6e7dd-6acd-5f34-9f91-66afd004ffd5` | PROVEN DUPLICATE — coalesce in derived view | `03bec0e0-8efe-55db-9971-bd36132b3c1e` | E5 + M: unique linked card occurrence, unique manual ACTIVE_BOUTS entry, exact manual UUID/URL reconstruction |
| 9 | 2026-08-01 / Navajo Stirling ↔ Jan Blachowicz | `89be14b9-3037-55de-8fb8-d9c514388569`<br>`dc3dbfcd-1cd2-5c01-ba42-5826aa1a49a6` | PROVEN DUPLICATE — coalesce in derived view | `89be14b9-3037-55de-8fb8-d9c514388569` | E6 + M: same affirmative construction and unique occurrence checks |
| 10 | 2026-08-01 / Michael Oliveira ↔ Oban Elliott | `108f5aca-513a-5fd9-af6f-c7c8f78e0e03`<br>`37015e6b-0070-5ffb-a692-eb3b72bf463a` | PROVEN DUPLICATE — coalesce in derived view | `37015e6b-0070-5ffb-a692-eb3b72bf463a` | E6 + M: same affirmative construction and unique occurrence checks |
| 11 | 2026-08-01 / Bogdan Grad ↔ Dennis Buzukja | `81a625a4-672a-5644-8bf9-26053f0a823f`<br>`ad8d2846-75b9-5a2b-a4e4-b4b593e386e4` | PROVEN DUPLICATE — coalesce in derived view | `81a625a4-672a-5644-8bf9-26053f0a823f` | E6 + M: same affirmative construction and unique occurrence checks |
| 12 | 2026-08-01 / Kyle Prepolec ↔ Mateusz Rebecki | `0a078174-ba92-5058-be64-d62257966f3d`<br>`7684f614-543d-53ab-b738-d9c77916713a` | PROVEN DUPLICATE — coalesce in derived view | `0a078174-ba92-5058-be64-d62257966f3d` | E6 + M: same affirmative construction and unique occurrence checks |
| 13 | 2026-08-01 / Alexander Poppeck ↔ Jovan Leka | `7ae9189e-11fb-5bb8-93f6-ccf755ce7435`<br>`c51bc854-f7b8-59cf-80e5-3c332126916f` | PROVEN DUPLICATE — coalesce in derived view | `c51bc854-f7b8-59cf-80e5-3c332126916f` | E6 + M: same affirmative construction and unique occurrence checks |
| 14 | 2026-08-01 / Ludovit Klein ↔ Tofiq Musayev | `3a8bcd8d-f048-57ed-9a79-12f5745643f6`<br>`5077ad8f-9ef1-5fc3-a595-f3a79dcf2915` | PROVEN DUPLICATE — coalesce in derived view | `3a8bcd8d-f048-57ed-9a79-12f5745643f6` | E6 + M: same affirmative construction and unique occurrence checks |

Evidence references are repository paths with exact baseline-verified bytes:

| Reference | Supporting evidence path | SHA-256 |
|---|---|---|
| E5 | `data/raw/ufcstats/events/68a758a6-bd6a-5ec7-933e-72251614d52f.html` | `7499107ad4ded97f494fc2a07207edae08c9a1978ec4f29b39148f61736afc47` |
| E6 | `data/raw/ufcstats/events/6952ee17-a74d-5432-832f-b9c6d22f8e65.html` | `8e7a01d293e3f1c332283eacbc072a54c991d024f4d91ab4d409bba0609085fb` |
| E2 | `data/raw/ufcstats/events/f9fdb60d-f963-535d-a85d-6d782b6e17f4.html` | `260c1d3ac79d4629bfa960036961547015171aadeaf5dea0af3fc6f1eea5d44c` |
| E1 | `data/raw/ufcstats/events/fe881b94-92d3-5dbe-97e7-28014b17202d.html` | `83370b7b25e07df9bd82e502b398b195ec6cb279cdc9f2b9ab684fecd07c4f98` |
| E3 | `data/raw/ufcstats/fights/498de4bd-d781-52af-a383-802158196d2d.html` | `f13f8bf8f3e283abd50fcc7791f1c394367ca03529ccc36e610ae785b23e7b7a` |
| E4 | `data/raw/ufcstats/fights/4b08f65d-db68-5091-9568-4748c4cf7318.html` | `096f1f16ac0a582dae9eeb3a8cfdaae4ff18edec3ee48b44a0d7ebb3b3994894` |
| Policy | `docs/phase5-history-identity-policy-v1.md` | `8b829d4a65641c4963f18931453e183b43eced8153e3e024010ebc72f05a16a9` |
| M | `tools/apply_manual_catchup_cards.py` | `467c2d1a2b17142f686242e51c45680a131952230662daa56d6e1a66cb3b127e` |

For group 1, occurrence links end in `ec1bda9a4c2aab42` and
`2750ac5854e8b28b`. The captured records differ in finish/statistic signatures:
one submission at 3:44, one NC/overturned at 1:51, with separately keyed
participant statistics. The source card lists both occurrences. Both original
bout IDs and the real event ID remain; neither card order nor UUID sorting
claims within-date chronology. The narrow Phase 5 occurrence guard accepts
only this evidence-backed event/ID set. Same-date history is excluded and
challenger Elo stays date-frozen; both occurrences can enter later history once.

For groups 3–14, the duplicate decision uses affirmative manual creation
lineage, not the old exclusion file's `verified_same_bout` terminology or a
participant match alone. The manual script's unique active announcement
reproduces the exact manual UUID and URL from the captured IDs/names. A single
corresponding official card occurrence is independently keyed by fighter
profile hrefs and its detail href. Entire non-provenance row semantics agree;
all twenty-four records are unresolved upcoming rows with no aggregate
statistics. The official row supplies the corroborated occurrence link and is
kept whole. Source times/URLs are provenance differences, not a statistical
priority rule. No outcomes or new statistics were taken from supplementary HTML;
automated event evidence extraction projects href identities only.

For group 2, occurrence links end in `552f7cdaf93e1055` and
`a842a365f408bb4a`. The frozen source has a draw/catch-weight occurrence with
statistics and a stale upcoming/bantamweight announcement without statistics.
The preserved pages/card corroborate event/participants but contain no explicit
old-to-new occurrence mapping. A cancellation/replacement or distinct
occurrence cannot be ruled out from this evidence. Both rows remain, no source
lineage is chosen, and the entire preparation stays blocked. Neither the newer
scrape nor the completed row is privileged to resolve this ambiguity.

## Published derived view and challenger readiness

Artifact: `data/experiments/phase5b1_reference_and_history/20261002_phase5b1_history_reconciliation_v1_blocked/`.
Checksum root: `8c28425abf011bcce88cff2fa8d3f4d866156396a84a59f9a609050aa16344cb`.
Manifest pin: `af4c1b3a57b55b63065eb116feec508fd892a0ab6ee5e7e0d0310ecd77c1130e`.
Eleven deterministic payloads rebuilt byte-for-byte on independent calls before
publication and again after publication. The independent verification CLI
repeated rebuilding and integrity checks. The guarded loader's actual refusal
is `Reconciliation remains blocked by unresolved identity evidence`.

| Quantity | Exact count / interpretation |
|---|---|
| Original source fights | 8,992, all preserved in parent and full lineage |
| Retained derived fight rows | 8,980, after twelve proven alias coalescences |
| Proven alias mappings | 12 |
| Source/derived events | 798 / 798, unchanged IDs and bytes |
| Source/derived fighters | 4,522 / 4,522, unchanged bytes |
| Source/derived aggregate statistics | 17,386 / 17,386, unchanged bytes |
| Fitting exclusions | 166, propagated across proven alias classes; both sides already excluded |
| Unresolved identity groups | 1, globally blocking |
| Newly published training preparation | None |
| New fitting/preprocessing/OOF/calibration rows returned for challenger | 0 |
| New challenger feature reconstruction or fold memberships | None |
| Existing blocked diagnostic counts | 8,558 rows / 341 proposed OOF IDs, unchanged and unloadable |

| Derived/evidence component | SHA-256 |
|---|---|
| `derived/events.json` | `e36c7250b9965ec8294fe9bac196f45ec96ad51624ef704e5e188b83eb868a78` |
| `derived/fight_stats_aggregate.json` | `ec19a9e5e0b13766744e187f1d099e92c65103940ac02fefe804b4421414bcb4` |
| `derived/fighters.json` | `1cf4a351788b7de8496e1e05c8591f5cf462739a4843f44831fa7168d20a4caf` |
| `derived/fights.json` | `ecb85479feb335f9877cbddb9f2749a483d8098356125d2649e23ade56d02486` |
| `identity_ledger.json` | `59ccd368c30ced4faaf4397f6a1179c8adeb707d472fe3c33173f1b962fbed95` |
| `manifest.json` | `af4c1b3a57b55b63065eb116feec508fd892a0ab6ee5e7e0d0310ecd77c1130e` |
| `source_identity_projections.json` | `c3cfbf3ea6f41c46b9acc2aa2e025a471fbe0eb1bb6cb68260b8ed57b8f752c7` |
| `source_to_derived_lineage.json` | `ee0b6fb43fd9ad26a1e5d51811e9fe584cf5f34fb0a63786a19cf01333c4843f` |

`source_to_derived_lineage.json` maps every original fight to the retained source
ID, both orientations, original/chosen row hashes and fitting-exclusion status.
Every statistic retains its original row hash, fighter identity and chosen
statistic source ID/hash. No statistic is blended or selected across conflicts.
Unchanged event/fighter tables are separately hashed. Source originals are never
deleted or overwritten; retained rows contain no invented fight/event IDs.

Normalization precedes all history/Elo/opponent/statistic indexing in
`history_source`; unresolved groups are rejected *before* any index is built.
Proven duplicates cannot double-count either fighter's history. Conflicting
whole rows/statistics fail rather than selecting an arbitrary preferred value.
The returned expanded exclusion set covers every fitting, preprocessing, OOF
and calibration use. Independently captured earlier excluded results may enter
later histories once only after structural readiness. No real normalized
histories or diagnostic features were indexed/rebuilt in this session because
the April group remains unresolved.

The exclusive event cutoff stays **2026-10-02**. No offline computation advances
the source capture or cutoff. No new learner inputs were manufactured, and the
prior counts were not forced onto a replacement. If the remaining group later
resolves, an exclusive new preparation must recompute features/eligibility and
fixed calendar folds under the preregistered 50-feature recipe, date-frozen Elo,
strictly earlier fighter/opponent histories, unknown schedules and deferred
debut columns. It must independently rebuild deterministically, preserve full
source lineage and pass a Phase 5 occurrence-aware guarded loader. This report
does not claim that future preparation work has already been performed.

## Snapshot, registry and authorization boundaries

The original snapshot's binary resolved-result coverage stops August 29;
statistics were last refreshed August 9. It contains **128 past-dated unresolved
upcoming source rows** and **35 prospective registry records**, all unchanged.
The derived view coalesces twelve of those unresolved representations; this does
not refresh their results or prove complete histories. Neither view is complete
through October 2. Contemporary stored title flags/statistic defaults and source
history are not independent title/experience attestations.

All 35 original records and registry tail bytes are preserved, with zero real
forecasts and zero complete pairs. No title/experience assertion, relaxed
eligibility, selected scored subset, reused production counterpart or
retroactive forecast was created. The new reference is ready as a frozen
component artifact; prospective forecasting still requires the challenger,
attested metadata and a new eligible common capture after component freeze.
No trial outcome was ingested, no historical holdout was evaluated, and
historical comparison remains **STILL_BLOCKED**.

This session performed exactly one real Platt fit and legacy preprocessing for
the authorized reference. It performed **no challenger fitting, prospective
probability scoring, comparative predictive evaluation, historical holdout
scoring, source network fetch, warehouse query/write/repair, production
pointer/model/prediction change or promotion**. Historical bootstrap predictions
were limited to reference calibration and persistence parity.

## Commands, tests and deviations

Executed with `python3` (the shell has no `python` executable):

```text
python3 tools/freeze_phase5b1_reference_and_history.py reference --run-name 20261002_phase5b1_frozen_reference_v1
python3 tools/freeze_phase5b1_reference_and_history.py reconcile --run-name 20261002_phase5b1_history_reconciliation_v1_blocked
python3 tools/freeze_phase5b1_reference_and_history.py verify-reference --run data/experiments/phase5b1_reference_and_history/20261002_phase5b1_frozen_reference_v3_identity_guarded --checksums-sha256 12c17761fe76998e49fb5e79ea8a455861e22eaf57b37071f692bab17c1ba381 --manifest-sha256 20a9695de6cd77d74ff67f5cb0b62da2f127fbcdc181fa9d9fc2b3f8f62ad48a
python3 tools/freeze_phase5b1_reference_and_history.py verify-reconciliation --run data/experiments/phase5b1_reference_and_history/20261002_phase5b1_history_reconciliation_v1_blocked --checksums-sha256 8c28425abf011bcce88cff2fa8d3f4d866156396a84a59f9a609050aa16344cb
python3 tools/freeze_phase5b1_reference_and_history.py preserve --baseline /tmp/phase5b1-preservation-baseline.json
python3 -m pytest -q modeling/tests/test_phase5b1_reference_history.py modeling/tests/test_phase5a_prospective.py features/tests/test_replay.py features/tests/test_debut_prior.py features/tests/test_elo.py features/tests/test_history.py
```

The two successor publications were separate offline Python invocations of
`freeze_adapter_successor` from `modeling.phase5_frozen_reference`: v1 → v2
with parent root/manifest `a1075709950149e25ca97c3b772366ae913b433a6ce759ffe41cf86e75b81982` /
`6edce65a0dc5994026074bff2380998e549afb66c1c81c0e400d8bd73c19be9d`,
then v2 → v3 with `cf13e2f9196b9ca83ffead6364386f6d6bbe586f4a75e8d02b3c8966d27aa9dd` /
`c7f2ab872b735de72c6faea872a04f4aff812496dd77bc5d75a71a9e6bac1036`.
Both returned `saved_components_unchanged=true`, `new_fits=0`, and exact parity.
Independent checks verified all predecessor roots and the eight unchanged
component hashes; final CLI verification used v3's pins shown above.

Final explicit suite: **139 passed, zero failed/skipped**, including **47 new
Phase 5B.1 cases**; 15.57 seconds. Final test receipt hash:
`0483c11b74b69c9832963ec0f5a6c59ac6ce8c114cfc5f524404bca669e95a9b`. Compilation of the new modules/legacy namespace,
command and test file and `git diff --check` passed. No broad test-directory
discovery, known mutating integration test or live warehouse integrity test ran.

Coverage includes exact bootstrap partitions/exclusions/orientation/numerics;
persisted preprocessing and estimator with exact matrix/raw/calibrated parity;
tampered/missing component rejection before deserialization; repinned but
incompatible package/code/source/recipe/membership/freeze provenance; no
load-time fitting, network/warehouse calls or writers; whole-row/statistic
conflict rejection; reversed alias orientation; one-count fighter/opponent
history and Elo; legitimate occurrence contracts versus unsupported duplication
and other-event rematches; alias exclusion propagation in both directions;
synthetic same-date/future challenger invariance after normalization; synthetic
legacy debut and reference-date/Elo limitations; deterministic exclusive
publication/overwrite refusal; real blocked-loader refusal; original registry,
freshness and artifact preservation.

Pre-freeze test development had one fixture failure because captured raw date
strings needed schema decoding at the normalized history entry point; that was
fixed before the reference/code freeze. The following pre-freeze run passed
25 checks and intentionally skipped 15 persisted-reference checks until the
one real freeze existed. The first complete suite passed 136 and failed one
freshness assertion that incorrectly extended the binary-result August boundary
to all nonbinary source dispositions. The assertion was corrected to the
recorded binary population, without changing source data or scope. That suite passed all 137. After the adapter audit, the final suite
passed all 139, including two additional identity-input/occurrence guard cases. There was no live-test selection deviation this session.
Direct exploratory review displayed directly relevant preserved raw card
summaries; it did not parse frozen holdout outcomes/joined evidence or feed
supplementary results into any feature, model or evaluation operation.

Frozen runs contain integrity-checked immutable bytes and no incomplete markers.
The new full derived view and full lineage are intentional reviewable evidence
(approximately 38 MB); the copied learner is approximately 328 KB and the full
preservation baseline approximately 7.8 MB. They are outside ignored model/raw
roots and require no force-add. No large dependency directories, credentials,
external-service headers or unrelated legacy repairs belong to this phase.

## Exact remaining evidence and next session

1. Resolve group 2 with a preserved primary-source ID transition, redirect or
   announcement revision explicitly mapping `a842a365f408bb4a` to
   `552f7cdaf93e1055`, or explicit evidence of distinct/cancelled occurrences.
   Participants/event, current single-card membership, old exclusion status and
   scrape freshness are insufficient. A cancellation or a result cannot be
   inferred simply to remove the blocker. Keep both originals meanwhile.
2. If that evidence resolves the final group, authorize a narrowly scoped
   offline Phase 5 normalized preparation: new exclusive run, unchanged original
   capture/cutoff, occurrence-aware validation, recomputed features/eligibility/
   memberships, independent deterministic rebuilds and guarded loading. Do not
   fit a challenger in that preparation session unless separately authorized.
3. After a ready challenger is separately fit/frozen, prospective work needs a
   new eligible common contemporary capture **after** all component freezes,
   sourced title and complete-history/explicit-debut evidence, conservative
   timing eligibility and all considered registry coverage. Old predictions or
   this pre-freeze capture cannot supply the reference counterpart. Source
   refresh and trial outcome ingestion need their own bounded authorization.

Recommended next session: obtain/inspect only the missing April occurrence
mapping and, if justified, publish the independently validated normalized
preparation. Keep fitting, forecasts, outcome ingestion/evaluation, warehouse
repairs and production changes outside that session's scope. The reference
bundle can be integrity-loaded independently while challenger evidence remains
blocked; historical comparison remains STILL_BLOCKED.

Git handoff follows `docs/implementation-reports/next-phase-git-handoff.md`:
logical implementation, frozen artifacts/evidence, and report commits followed
by a normal push to verified `origin/main`. Exact commit hashes, push outcome
and remaining worktree changes belong in the final session response, avoiding
a self-referential report-update commit loop.
