**Phase 4A scoring-input readiness report — 2026-10-02 (Europe/Warsaw)**

**Status: BLOCKED.** The preparation workflow and Phase 4B comparison protocol are implemented, executed and frozen. Exactly 108 original targets are preserved. Essential inputs remain unresolved for 98 forecasts, so no real feature vectors, `features.csv`, reduced scoring cohort or candidate probabilities were published. There was no training, real candidate scoring or outcome evaluation.

Strict source-availability certification is **not established**. Exact selected Git bytes are evidenced before every scoring instant by repository timestamps. Those timestamps and raw capture logs are not independently trusted timestamps; complete historical website availability and archive completeness remain unverified. This is the authorized qualified archived-as-of mode. Missing independently trusted timestamps are a qualification, not the reason for BLOCKED.

The isolated, non-overwriting run is:

`data/experiments/phase4a_pre_event_2026_scoring_inputs/20261002_phase4a_archived_asof_v1_blocked/`

Run preparation timestamps: `2026-10-02T08:38:21.046282+00:00` through `2026-10-02T08:38:28.771632+00:00`. These UTC times record computation/publication, not historical availability. The complete request is preserved as `user_authorization.txt`, SHA-256 `2a4f9196ea1773100206ff8d512b3b43a61a1cc79e436b70bb6dfb3b26f06553`.

**Implementation and preservation.** No applicable ancestor or repository `AGENTS.md` was found. The initial worktree contained extensive tracked modifications and untracked Phase 1–3 work. All were preserved. New repository files are:

- `modeling/scoring_inputs.py`: pinned target projection, deterministic coherent-tree selection, exact raw-body supplementation, source diagnostics, gated reconstruction, compatibility checks, immutable resolver, frozen-evidence rebuild, comparison protocol and exclusive publication.
- `tools/prepare_phase4a_scoring_inputs.py`: preparation CLI; two independent builds, preservation checks, authorization receipt, checksums and read-only artifact freeze.
- `modeling/tests/test_scoring_inputs.py`: 30 focused tests, including synthetic complete-108 reconstruction and adversarial chronology checks.
- `docs/implementation-reports/phase4a-scoring-input-readiness-report.md`: this report.

The pre-existing `docs/phase4a-source-policy.md` and `features/forecast_replay.py` were adopted without changing their bytes. Phase 3A preparation, candidate bundle/metadata/checksums and the existing loader were unchanged. New data is confined to the run above. No commit or push occurred.

**Fixed source policy.** Before constructing real forecast features, require every target's essential inputs to resolve. Search all reachable commits affecting exactly `data/events.csv`, `data/fights.csv`, `data/fighters.csv` and `data/fight_stats.csv`, bounded at 256 trees. Require a single schema-compatible coherent tree with unique identifiers/statistic pairs and valid event, profile and participant references. Orphan statistics are preserved in the exact source blobs, reported and excluded from joins.

Availability is the later of author and committer UTC instants. Select the maximum `(availability_utc, full_commit_SHA)` among eligible coherent trees no later than the original `scored_at`. Reject conflicting duplicate trees rather than choose revisions or mix their components. Selection uses only allowlisted identities, dates/times and matchup metadata; baseline probabilities, decisions, winners, confidence tiers and manifest performance summaries do not participate.

The bounded search found six trees. Counts below are raw events/fights/fighters/statistics, including duplicate rows where present. Author and committer instants agree for these six commits.

| Exact commit | Repository availability UTC | Raw counts | Classification |
|---|---|---|---|
| `0a13162ea60e0a2ede49d8d8d10b30a81683b717` | 2026-03-12 16:48:33 | 761 / 8511 / 4452 / 16662 | Coherent; superseded by March 18 |
| `1f477d3ddc87b123d0099025b669e728e7881a34` | 2026-03-18 12:43:50 | 764 / 8550 / 4452 / 17102 | Selected for all 108 |
| `8bb0552fc2fa4f91e1e769c41dc999ac61b2f14b` | 2026-04-17 10:40:11 | 762 / 8713 / 4452 / 17182 | Rejected: 186 missing event references and 8 missing participant profiles |
| `e23dd7cf3572c41776197c1990a09f17c9d96cc9` | 2026-05-15 14:25:46 | 789 / 8826 / 4464 / 17182 | Rejected: 8 duplicate event identities and 71 duplicate fight identities |
| `4af6c2f63ebcf63e13fbdbbdcf3bce59243334e4` | 2026-08-09 12:36:40 | 790 / 8891 / 4492 / 17386 | Rejected: 12 duplicate fight identities |
| `6e5c0cd6afcb360c58036eaa1c489e71f1d4bc81` | 2026-08-19 19:27:06 | 803 / 9031 / 4496 / 17386 | After every scoring instant; also contains duplicate identities |

The later duplicates contain differing revisions, not merely identical repeated rows. No revision-resolution or archive-repair rule was introduced. The March selection follows full coherence validation; later snapshots were inspected rather than assumed usable or ignored.

Every catalogue component has its exact Git path, immutable blob ID and SHA-256 in `source-manifest.json`. All 24 commit/component references resolved successfully with the tested resolver, which checks both `commit:path -> blob` and `blob bytes -> SHA-256`. Exact selected source hashes are:

| Component | SHA-256 |
|---|---|
| events | `df2d02bbadbe5b293bcb33c08f0ae4095b4ba4398cd3f8884032da61be5ad81f` |
| fights | `3c89061dc315a640f29ddfd260b2b23afc1e48faed5106de0cbe461558e90ff1` |
| fighters | `ecce91a1aa2fb024d0f9c8eb489e9d9b5ddb0663f2f745ce089c8bad5461ce12` |
| fight_stats | `b20c0e07aefff66f749476a3acd8abc57c0ef6cdc09d545c05fe7fe068d93324` |

**Staleness and coverage.** Repository observation is March 18. The latest resolved event in the selected archive is March 7. Event scrape dates span February 19–March 13; fights February 19–March 13; statistics February 21–March 13; profiles were scraped February 19. The source has 40 orphan statistic rows and 19 resolved bouts missing at least one participant's statistics. At original scoring, archive age ranges from 12.976 to 151.321 days and the gap from the last resolved event to the exclusive history cutoff ranges from 24 to 162 days.

These are archive diagnostics, not completeness certification. The previously documented missing March 14/21/28 histories remain unavailable in this selected coherent archive; no broad warehouse audit was repeated. Per-target prior IDs/counts, own/opponent statistic missingness, profile-field missingness, archive ages and date lags are recorded in `source-selection.json`. No target histories were reconstructed from frozen outcomes or current warehouse features/profiles.

**Bounded supplemental evidence and exact blockers.** Supplementation is limited to missing target profiles and explicit target matchup flags; it cannot change result/statistic histories. Require a successful HTTP 200 capture no later than scoring, matching URL-derived identity, and SHA-256 equality with the surviving body. Ties use `(fetched_at_UTC, content_hash, storage_path, job_run_id)`. An old fetch record does not establish that an overwritten body survives.

The scan covered 330 original target entity paths and 1,725 relevant records within the 108,259-row fetch manifest. The scanned manifest SHA-256 is `40587f0b8b3c736fec99a7fc4459c9ddc24577a49399b8ce969d4d6b7c5b36e3`. At the per-forecast/entity level, 95 fight bodies, 108 event bodies and 168 fighter bodies lack a surviving hash-matched eligible version. Thirteen fight bodies and 48 fighter-body assignments do match eligible records.

Thirteen August 22 matchup bodies establish explicit title/non-title flags; four absent August fighter profiles are recovered from exact bodies fetched August 16. Their physical parsing preserves repository measurement conventions, including integer-centimetre reach. Only needed supplements were used: 13 fight bodies and four fighter bodies are preserved under `sources/raw/`, each linked to its exact capture record. The captured relevant records and observed body hashes are preserved in `source-fetch-evidence.json`.

The essential gaps are:

- **95 forecasts:** unknown target title status. Their selected Git matchup is absent and eligible exact fight/event metadata bodies do not survive. Unknown was not converted to false.
- **One forecast:** Gable Steveson (`6c80aabe-5671-5caf-a5e4-bd0f0dbf1448`) lacks an eligible surviving profile. Target fight: `949f5ffc-911a-5802-8474-79be9265aa60`, scored July 1 for July 11. Its profile manifest records point to overwritten content. This forecast also has unknown title status.
- **Four fighters across three August 22 forecasts:** Ryan Kuse (`816d5d3f-ef76-591b-9979-63735a26b250`), Stan Dorsainvil (`7a343523-cc85-5054-b924-3c49120e4861`), Terrance Chatman (`8110cdb9-d1b1-5cb3-9f1c-8cf601304496`) and Anthony Wint (`41d605df-9392-5afd-8366-a8187b29444c`) have recovered profiles but no eligible history in the selected snapshot. Incomplete archival coverage does not establish zero experience. Affected targets are `3a247061-eef3-5698-aeb4-82d62b5164c1`, `cc88d51e-6818-5219-89a8-adcca1eccf4d` and `e1a1f140-7576-5843-955e-f3e4e044c8f2`.

There are 98 distinct affected forecasts and ten with essential inputs resolved under this qualified policy. The ten were not reconstructed or published as a reduced cohort. Missing individual measurements and sparse statistics remain separate from essential profile/title/experience gaps. Real feature missingness is not available because the all-108 gate prevented construction.

**Exact target verification.** `targets.csv` retains frozen row order, original fight/event IDs, fighter IDs/orientation, known event date, names, weight class and allowlisted original forecast provenance/model/file metadata. It preserves the original timestamp string as `scored_at_original` and normalizes `scored_at` to UTC without changing the instant. All 108 identities match the outcome-free identity evidence, SHA-256 `a9119e05aa62907b49a3bfae52c602551de840e3f541d7d8f0fbca3ed2956567`. There are 13 events, dated April 4–August 22. No identity substitutions, later predictions or row deletions occurred.

| Original scoring instant, normalized UTC | Forecasts |
|---|---:|
| `2026-03-31T12:09:04.077607+00:00` | 11 |
| `2026-04-06T17:11:01.455546+00:00` | 54 |
| `2026-05-31T20:14:28.201894+00:00` | 9 |
| `2026-07-01T10:06:40.765321+00:00` | 10 |
| `2026-08-09T11:17:50.025384+00:00` | 11 |
| `2026-08-16T20:25:39.927324+00:00` | 13 |

The forecast manifest and predictions pins passed unchanged: respectively `f28c9b052dbe883d05c00d3b46fe711997dfa39c7fca09d8a722c6fc2fb36f6a` and `81e133bcb88422d3e0ddae12ac79de4ce6b9f60ab4d009545d7bfb41ff1a522b`. Neither frozen `outcomes.csv` nor joined identity/outcome evidence was parsed. Preservation checks hash their bytes only.

**Date semantics and compatibility.** The algorithm remains `v2_date_frozen_elo_schedule_unknown_v1`, feature version 2, exact candidate 50-column order. The existing `reconstruct_at_scored_at` passes the history cutoff to snapshot functions, thereby also changing age/activity/rolling/decay reference dates. Training reconstruction instead calls those unchanged functions at the target event date. The preserved forecast wrapper therefore separates:

- Eligible histories: strictly before `min(original_event_date, UTC(scored_at).date())`; exclude the entire scoring day and target day, and remove the target identity even if a corrupted source date would otherwise admit it.
- Feature reference: original known event date for age/activity/rolling/decay calculations, without admitting intervening results.
- Elo: date-frozen updates, deterministic `(event_date, fight_id)` ordering and a label-free probe at the knowledge cutoff. Opponent indexes obey the same cap; each opponent's prior history also precedes the historical bout date.
- Target matchup: only allowlisted identity/date/weight/title inputs reach construction. Target winner/result/duration/finish/label fields are removed. All target and historical schedules are unknown. The three debut columns remain deferred; final priors are neither applied nor refit during preparation.

Previously resolved fitting-excluded identities may inform independent, eligible later inference histories. The 166-ID fitting exclusion contract is unchanged; no frozen outcomes supply those histories.

All 16 training preparation code hashes matched the pinned training manifest `42e8e1661409b2ca758a4e18775f9816865439448fbce0d88af68dda22325671`. All 29 candidate components passed integrity checks under checksum-manifest pin `41f8c8afb8fd091fa0b04823ad9175cedcfdbf85ccf3f62d77c71e0d9219279f`. The unchanged saved loader successfully loaded the candidate with fitting/prediction/database calls guarded; it did not score forecasts.

`compatibility.json` separates actual scoring-source provenance from the training contract being matched. Its status is `BLOCKED_NO_COMPLETE_SCORING_INPUT`; no real vectors are certified and no loader reference is supplied. The narrow `validated_loader_reference` adapter requires a completed run, integrity checks and full frozen-source reconstruction parity before translating to the loader's reference contract. It rejects this run. The training-manifest hash identifies a reference contract, never the source of newly reconstructed forecasts.

Packages are frozen in `package_versions.json`: Python 3.11.2, NumPy 2.4.3, pandas 3.0.1, scikit-learn 1.8.0, XGBoost 3.2.0, joblib 1.5.3, psycopg2-binary 2.9.11 and pytest 9.0.2. Required candidate package checks passed.

**Published artifacts and checksum lock.** The run contains 33 checksummed component files plus `checksums.json`; generated files are read-only. The checksum manifest SHA-256 is **`ac762f369aa132fc3e83694071056cda217f436f6e4315b665c454963d4a59d0`**. It covers every artifact and all 17 preserved raw bodies.

| Artifact | SHA-256 |
|---|---|
| targets.csv | `386edd85e248be571bf97796db0b1ea7dd0d6aaca01587140dfd003ba60c152e` |
| source-selection.json | `e0173084d985aedc9e142e2ad98e1608033c8e06bea774f3ccdf6ebe4777cfbf` |
| source-manifest.json | `c87a2d5e32806275297b894452664676f20701b3fe638b96af1ba92dc747cb5e` |
| source-fetch-evidence.json | `53290e3b88fbee740ccdcf70e6758825cbe69ff01745110d300b005f48c33fa4` |
| feature-lineage.json | `cd293f6b44251cbc0d03c828859736a2d5efe06b4222d1a8005724f03a8e3c18` |
| compatibility.json | `1a3c0709b99e2ee29f53db0e3a562b3fe99cccbdbf374a9efa6027395994c382` |
| comparison-protocol.json | `6e6c127973c9ccc15fa4bfa933d8b65b6438db1ea7a16da725171a4ebdaa0f6b` |
| package_versions.json | `bb841c0a84f659cff99b65f760be9a05bc530c00ebd1d569ab74165f0b810b51` |
| validation_results.json | `84bd20fce0db979001fa3c8badd034303959b10a31d668f3ab5e26cc53ca3b95` |
| run_receipt.json | `c5b68da7dbcafc3bdce147260120d370c18a1d8ce976518ba82ce6229a436bc0` |
| INCOMPLETE.json | `d99a593f3daab80afeff76f37c75c9b84b3f88b9b724b73dd4ffa232f26b646f` |
| regressions.log | `99608b91f1543396028309751015bd8f1950fc7b979436a2e3c70df4cdce74c1` |
| preservation_check.json | `c6f2cfc78f691be360a184176c7e4566440cc176f6a2d90c73f1d3ace412d28d` |
| preservation_baseline.json | `c287345f26ec2f1e41785fa1e2d53649c3379877dbdf04fd2a24ccb7114efa7d` |
| commands.json | `f6ad0454dc69c4d2e3a112c904c49383275885b65a4e640f5ef3345b64ea7091` |

`targets.csv` contains no labels, correctness, target results or model probabilities. No feature file exists. Raw/Git source archives may contain independently available historical results; those are source evidence, distinct from an outcome-free target/scoring input.

**Frozen Phase 4B protocol.** No outcome metrics were calculated. Primary comparison is saved final calibrated candidate probabilities versus original frozen calibrated baseline probabilities on these exact 108 fights. Preserve baseline values and historical rounding; never recompute them. Primary paired metrics use all fights, including NO PICK: natural-log loss with probabilities clipped to `[1e-8, 1-1e-8]`, and Brier score on original stored probabilities without clipping. Differences are candidate minus baseline. Candidate raw proper scores are predeclared secondary diagnostics only.

Report overall latent accuracy (`p >= 0.5` selects fighter 1), each model's own actionable and high-confidence groups, both models on baseline-selected actionable/high-confidence groups, and both models on the jointly actionable intersection. Include selected/resolved/correct counts, accuracy and coverage against 108; empty accuracy is null. Inclusive NO PICK is `0.40 <= p <= 0.60`; high confidence is `p <= 0.30 or p >= 0.70`. Strongest-57 ranking and historical 146-fight figures are separate and are not transferred.

Calibration bins are `[0,.1), [.1,.2), …, [.9,1]`; report count, mean probability and observed fighter-1 win rate, with nulls for empty bins and count-weighted ECE. Primary paired-difference intervals use 10,000 event-cluster bootstrap replicates, NumPy `Generator(PCG64(20261002))`, lexicographically sorted event IDs and 13 sampled event indices with replacement per replicate. The same event multiplicities apply to both models; retain every sampled event's fights and compute fight-weighted mean differences. Use percentile 95% intervals with `numpy.quantile([.025,.975], method='linear')`. Thirteen clusters limit stability and generalization.

Require complete original-orientation binary labels for the primary comparison; unresolved labels block it rather than define a replacement cohort. Candidate raw/calibrated predictions and provenance must be saved and hash-locked **before any Phase 4B outcome read**. After outcomes are opened, prohibit threshold tuning, feature/source changes, recalibration and model reselection. Neither this protocol nor qualified retrospective results authorize promotion.

**Verification and executed commands.** Final scoped regression result: **268 passed, zero failures**, in 18.43 seconds; 30 tests are new Phase 4A tests. Coverage includes exact targets, UTC instants, availability/tie rules, rejected/stale snapshots, raw version matching, target/future/same-scoring-day invariance, capped Elo/opponents, distinct clocks, unknown schedules, deferred priors, numeric/order/missingness guards, honest provenance, complete synthetic-108 reconstruction, deterministic rebuilds, checksum tampering, blocked compatibility and refusal to overwrite.

Relevant existing replay/history/Elo/opponent/career/rolling/decay/physical/refit-preflight, synthetic holdout-recovery, outcome-free holdout and mocked candidate-contract regressions passed. Existing holdout fixtures that parse real frozen outcomes and bundle fixtures that fit real estimators were excluded. The mutating live integration test, live warehouse tests and known unrelated legacy failures were not run. Runtime guards covered forbidden frozen-outcome access, DB connections, model/prior fitting or application, real prediction and real feature construction for the blocked cohort.

Two independent preparations produced **27 byte-identical deterministic payload files**. Publication verified every written hash. A third rebuild used only the frozen source catalogue, immutable Git resolver, captured relevant fetch records and preserved needed raw bodies; every deterministic published artifact matched. Both synthetic and actual-run overwrite attempts were refused. The blocked adapter was independently rejected after publication.

`commands.json` records the full regression and preparation argument vectors plus bounded inspection/check commands. Operational commands were:

```bash
python3 -m pytest -q modeling/tests/test_scoring_inputs.py
python3 -m pytest -q <the exact 23 file/node arguments preserved in commands.json>
python3 -m py_compile modeling/scoring_inputs.py tools/prepare_phase4a_scoring_inputs.py modeling/tests/test_scoring_inputs.py
python3 tools/prepare_phase4a_scoring_inputs.py \
  --output data/experiments/phase4a_pre_event_2026_scoring_inputs/20261002_phase4a_archived_asof_v1_blocked \
  --authorization '/home/wlodzimierrr/.codex/attachments/ff3c16b0-7db1-434c-9625-3c770cfe83d4/Pasted text.txt' \
  --preservation-baseline /tmp/phase4a-preservation-baseline.json \
  --tests-log /tmp/phase4a-regressions.log \
  --commands-file /tmp/phase4a-commands.json
```

Inspection used `rg`, `cat`/`sed`, `git status`, scoped `git log`, `git show`/`rev-parse`/`cat-file`, and offline Python diagnostics. Initial standalone Phase 4A tests passed before the final broader suite. One later synthetic assertion incorrectly rejected the required deferred debut-prior column by name; it was corrected and all tests passed. An unscoped `git diff --check` exited 2 on pre-existing dirty CSV whitespace; the new files passed their scoped whitespace check. No unrelated whitespace or data repairs were made.

Hash-only checks before preparation, before/after publication and after independent read-back verified **all 422 initially recorded files unchanged**, with zero missing files and zero new protected files. This covers `models/`, both accepted holdouts, Phase 1–3 evidence/preparation, the entire Phase 3B candidate and unrelated user changes. Production pointer SHA-256 remains `ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa`. No production promotion, live migration, warehouse write/repair, dashboard work, network fetch, calibration fit, commit or push occurred.

**Phase 4B handoff and remaining constraints.** Inputs for planning are this report; the frozen run and checksum pin above; `comparison-protocol.json`; the unchanged candidate at `data/experiments/phase3b_xgb_pre_april_2026/20261001_phase3b_retrospective_v1/`; and the unchanged original forecasts at `data/holdouts/pre_event_2026_apr_aug/` with their pinned manifest/predictions.

Phase 4B scoring/evaluation must remain blocked until all 108 essential inputs resolve. Obtain defensible exact pre-scoring target title metadata for 95 forecasts, an eligible surviving Gable Steveson profile, and adequate experience evidence for the four supplemented profiles. Any proposed handling of conflicting later archive revisions needs a separately reviewed deterministic source policy before reconstruction; it cannot silently repair this frozen run or use post-scoring versions. Today's website/warehouse and frozen outcomes cannot fill these gaps.

After resolution, create a new run, require complete 108-row features, original identities/orientation/instants, source compatibility and deterministic parity, then use the validated adapter with the unchanged bundle. Phase 4B must load saved final priors and the saved calibrator, save/hash-lock candidate predictions, and only then open outcomes under the frozen comparison protocol. The incomplete package is source/target/protocol evidence, not permission to score a subset or promote a model.
