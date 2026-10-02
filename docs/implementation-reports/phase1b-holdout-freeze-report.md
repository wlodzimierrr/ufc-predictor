# Phase 1b: corrected contracts and holdout freeze

Repository: `/home/wlodzimierrr/ufc-data`. Audit date: 2026-10-01.
Frozen artifact creation time: `2026-10-01T12:32:52.434367+00:00`.

## 1. Dataset status

| Dataset | Status | Population and evaluation qualification |
|---|---|---|
| `data/holdouts/historical_2026_apr_aug/` | **FROZEN** | All 146 original records, across 16 events; mixed pre-event/catch-up historical evidence. Eight identity mappings remain unresolved, so the entire population is **not certified for clean model evaluation**. No rows were dropped. |
| `data/holdouts/pre_event_2026_apr_aug/` | **FROZEN** | Exact 108-row parent subset, across 13 events; primary population for later comparison of original pre-event forecasts. All 108 identities and labels are corroborated by the available evidence. Historical feature availability still requires audit. |

Both datasets span April 4–August 22, 2026. FROZEN denotes acceptance and preservation of the historical snapshot; it does not establish an unseen test set or complete historical feature availability. The old `prospective_2026_apr_aug/` directory contains an explanatory README only, with no misleading manifest.

## 2. Metric correction

The previous prompt conflated two different groups. Inclusive threshold high confidence is `p <= 0.30 or p >= 0.70`, giving **44 fights / 30 correct**. Strongest 57 ranks by `abs(p - 0.5)` descending, with `fight_id` ascending as the deterministic tie-breaker, giving **57 / 39**. Here `p` always means the original calibrated probability for fighter 1.

The strongest-57 boundary has favorite probability **0.6798**; the next row has **0.6794**, so there is no boundary tie. Ranking reads no outcomes. Probability thresholds, original probabilities, confidence tiers, and uncertainty flags were preserved.

## 3. Adopted source and exact selection

The user's corrected request explicitly authorized adopting Git blob `e7a6e34bb35e015d91975f182572613ad4d5b48e` as the historical comparison source.

Preserved source:
`docs/implementation-reports/phase1-holdout-evidence/recovered-report-e7a6e34bb35e015d91975f182572613ad4d5b48e.csv`

SHA-256: `8b66ee10d706061a739604aaa74a6b71fcd3b02731d534c4a134eb9243c5d5ae`.
Recovered source tree: `54783dabfba46427ace3bb64e74c2402b8fe4a7b`. Its original analysis session, creation timestamp, and source commit remain unknown.

Historical selection reads that exact source, keeps `2026-04-01 <= event_date < 2026-09-01`, and keeps only rows whose original `resolved` string is `True`. This selects 146 unique fights from 169 window rows; the 23 originally unresolved rows remain excluded. Correctness and desired confidence counts never enter selection.

The pre-event population selects from the frozen 146 using both original `pre_event_evidence == "database_scored_at_before_event"` and UTC scoring date strictly earlier than event date. It preserves exactly 108 prediction records and their corresponding original outcomes.

Schema version is 2. Rows are stably ordered by `(event_date, fight_id)`. CSVs use UTF-8 and LF; JSON uses sorted keys, two-space indentation, and a trailing LF. Original scalar strings, probability precision, scoring timestamps, and outcome strings such as `1.0` remain unchanged. Fighter IDs are added from corroborated original prediction history, without remapping prediction fight IDs. `predictions.csv` contains no outcome columns; `outcomes.csv` joins one-to-one by original `fight_id`.

## 4. Reconciliation

All expected counts match independently recomputed counts. Latent correctness compares the original outcome with the prediction `p >= 0.5`; probability bands are inclusive where specified.

| Group | Historical fights / correct | Accuracy | Pre-event fights / correct | Accuracy |
|---|---:|---:|---:|---:|
| All | 146 / 81 | 55.48% | 108 / 57 | 52.78% |
| Uncertain: `0.40 <= p <= 0.60` | 55 / 22 | 40.00% | 46 / 17 | 36.96% |
| Actionable: outside uncertain band | 91 / 59 | 64.84% | 62 / 40 | 64.52% |
| Threshold high confidence: `p <= 0.30 or p >= 0.70` | 44 / 30 | 68.18% | 23 / 16 | 69.57% |
| Strongest 57: probability-margin ranking | 57 / 39 | 68.42% | Not part of this subset contract | — |

## 5. Provenance and timing audit

| Original provenance / UTC timing | Historical | Pre-event |
|---|---:|---:|
| `database_scored_at_before_event`; before event day | 108 | 108 |
| `catchup_scored_before_result_load`; after event day | 38 | 0 |
| Scored on event day | 0 | 0 |

The catch-up records comprise:

| Event date | Rows | Original scoring timestamp, UTC |
|---|---:|---|
| 2026-07-18 | 12 | `2026-08-08 18:05:36.577481+00:00` |
| 2026-08-01 | 14 | `2026-08-08 18:05:36.577481+00:00` |
| 2026-08-08 | 12 | `2026-08-09 11:44:27.480069+00:00` |

Both populations' earliest original scoring instant is `2026-03-31T12:09:04.077607+00:00`; the latest is `2026-08-16T20:25:39.927324+00:00`. Catch-up provenance is a historical review assertion and does not prove that post-event information was unavailable. The 146-fight population is not wholly prospective.

The targeted warehouse capture used a read-only repeatable-read transaction beginning `2026-10-01 12:32:52.461696+00:00`, with database timezone UTC. It captured 166 fight records, 305 prediction-history records, 51 reviewed records, and 287 relevant fighter records. SQL and query identifiers are preserved in `phase1b-identity-outcome-audit.json`. All 146 original prediction records match warehouse history at their original scoring instant. The previous forensic search was not repeated.

## 6. Bout identity, orientation, and unresolved mappings

All 20 reviewed mapping targets refer to the same event/date. Twelve also have the exact same unordered fighter-ID pair, establishing alternate identifiers for the same bout. All twelve retain the original fighter orientation. Eight targets have different fighter IDs; their original IDs include manual stubs without an independent identity bridge to the scraped IDs. Matching names was not accepted as proof.

In the following table, V means verified same event and unordered fighter-ID pair; U means unresolved identity, with orientation unverified. V mappings have `orientation_reversed = false`.

| Event | Original matchup | Original prediction fight ID | Reviewed target ID | Audit |
|---|---|---|---|---|
| Jul 18 | Dricus Du Plessis–Kamaru Usman | `3119317d-2397-5e20-838f-d4508e3eea42` | `e439fb69-cd79-5705-842a-4f98ddbf492a` | V |
| Jul 18 | Levi Rodrigues Jr.–Felipe Franco | `36c6e7dd-6acd-5f34-9f91-66afd004ffd5` | `03bec0e0-8efe-55db-9971-bd36132b3c1e` | V |
| Jul 18 | Ezra Elliott–Damien Anderson | `425d4ef7-f6dc-53ff-a2a7-fe18a7851933` | `ff2f8419-ef0c-51d0-92a5-e2ec152290b3` | U |
| Jul 18 | Jean-Paul Lebosnoyani–Seokhyeon Ko | `4e4eed91-2b39-5797-83a7-81cce50c17d2` | `6168dff5-3df1-5ddd-8a5f-17bff9a3b0df` | V |
| Jul 18 | Tabatha Ricci–Fatima Kline | `76e979fe-4b60-5d8a-ba49-811cf53505c3` | `7189f7d3-d325-5abd-8e57-da16f3032017` | V |
| Jul 18 | Alden Coria–Stewart Nicoll | `a01b704f-c26b-54b0-bc6c-68af89c315eb` | `3ea09870-977d-53fc-a70e-d49be991ebbd` | V |
| Jul 18 | RJ Harris–Alvin Hines | `b3707b19-f5db-5498-af6e-d5942fbff04b` | `ce8015a7-1253-5de4-89fd-9e445ec35c02` | U |
| Jul 18 | Anna Melisano–Dione Barbosa | `bafc9895-d371-58b5-b294-a53219377532` | `d2315b90-4f6f-54f4-86d1-bf30ff0c37c8` | U |
| Jul 18 | Tommy McMillen–Alberto Montes | `d16ce11f-f1eb-5eee-aefa-304e9ce176d3` | `f2783f53-9556-5653-9815-c15e8614412c` | V |
| Jul 18 | Austin Bashi–Jose Miguel Delgado | `f78ba6f7-05b1-5f19-a20c-721df9a9da88` | `19d4f02e-cb5b-5a82-8aac-9598812eaefd` | U |
| Aug 1 | Oban Elliott–Michael Oliveira | `108f5aca-513a-5fd9-af6f-c7c8f78e0e03` | `37015e6b-0070-5ffb-a692-eb3b72bf463a` | V |
| Aug 1 | Nina Milosevic–Hailey Cowan | `2f17110e-a377-5ca9-b251-976567e85b79` | `8167b140-2879-58b7-b8b7-f95a7f6981e1` | U |
| Aug 1 | Ludovit Klein–Tofiq Musayev | `5077ad8f-9ef1-5fc3-a595-f3a79dcf2915` | `3a8bcd8d-f048-57ed-9a79-12f5745643f6` | V |
| Aug 1 | Mateusz Rebecki–Kyle Prepolec | `7684f614-543d-53ab-b738-d9c77916713a` | `0a078174-ba92-5058-be64-d62257966f3d` | V |
| Aug 1 | Jovan Leka–Alexander Poppeck | `7ae9189e-11fb-5bb8-93f6-ccf755ce7435` | `c51bc854-f7b8-59cf-80e5-3c332126916f` | V |
| Aug 1 | Dennis Buzukja–Bogdan Grad | `ad8d2846-75b9-5a2b-a4e4-b4b593e386e4` | `81a625a4-672a-5644-8bf9-26053f0a823f` | V |
| Aug 1 | Milos Janicic–Noah Gugnon | `d7d7481d-4092-5aec-a683-0a55f52291e6` | `ef2d6f88-48f2-5ade-b725-e9a3197f3a7a` | U |
| Aug 1 | Jan Blachowicz–Navajo Stirling | `dc3dbfcd-1cd2-5c01-ba42-5826aa1a49a6` | `89be14b9-3037-55de-8fb8-d9c514388569` | V |
| Aug 1 | Marina Spasic–Stephanie Luciano | `e5b0ebf7-a224-53a5-a451-0953ea57f96d` | `9d2083b0-2853-5ca6-b289-e65e4e4fa47c` | U |
| Aug 1 | Borislav Nikolic–Mark Vologdin | `fece261a-3217-5d28-bdfe-b7aee132dbdc` | `6ede7ecf-41c0-50de-b18d-44e1ba6edc3a` | U |

The eight U rows remain frozen as historical evidence, explicitly uncertified for clean evaluation. All eight are catch-up records; none belongs to the 108 pre-event subset. Different IDs alone do not establish an actual opponent replacement, so these are unresolved identity changes rather than certified replacements. No opponent replacement was established among the selected records. The original audit's eight replacement annotations concern originally unresolved, excluded source rows and remain separate evidence.

Two unchanged-ID bouts have current fight orientation reversed relative to prediction orientation: Steve Erceg–Tim Elliott (`c6e4fe50-736e-5a81-a7f7-0dda592e972d`) and Robert Whittaker–Nikita Krylov (`6ccd1d31-8d51-5f94-a9e9-253c8b97e8fc`). Both original labels are `1.0` and agree with the current winner's fighter ID in original prediction orientation. No positional current label was substituted.

Available evidence corroborates original labels for all 146 rows: 95 through current winner IDs and 51 through reviewed records retaining original fighter IDs/orientation. This does not resolve the eight uncertain mapped identities. The manifests record 138 corroborated identity/label rows in the historical population and 108 in the pre-event subset, without creating a silently reduced evaluation cohort.

## 7. Historical versus current outcomes

There are 38 discrepancies between historically resolved outcomes and current original-fight rows marked `upcoming`: 12 on July 18, 14 on August 1, and 12 on August 22. The first 26 are catch-up originals; their review targets and current states are preserved separately. Nineteen of the 20 alternate targets are also currently upcoming. The Austin Bashi target has a current win but a different opponent fighter ID, so that result does not certify the unresolved mapping.

All 12 August 22 reset rows retain reviewed evidence dated `2026-08-23 15:52:37.886945+00:00`, with matching event, original fight ID, scoring instant, fighter IDs/orientation, and historical label. Current original rows are upcoming with no winner. The preserved source and these reviewed rows agree:

| Original fight ID | Original fighter 1–fighter 2 | Historical label | Historical winner |
|---|---|---:|---|
| `1ce74697-5531-568f-b769-051f65fb459f` | Carli Judice–Jeisla Chaves | 1.0 | Carli Judice |
| `32d7a6c3-bb16-573d-acdd-562fb93af46b` | Kennedy Nzechukwu–Shamil Gaziev | 0.0 | Shamil Gaziev |
| `384559ef-d5df-5af4-9c7f-a58c7c359359` | MarQuel Mederos–Mason Jones | 1.0 | MarQuel Mederos |
| `3a247061-eef3-5698-aeb4-82d62b5164c1` | Marcio Barbosa–Ryan Kuse | 1.0 | Marcio Barbosa |
| `58dbd5ff-f2df-5814-854c-a8c4fb45486f` | Reinier de Ridder–Roman Dolidze | 1.0 | Reinier de Ridder |
| `8972e4f7-7df7-59cb-92be-bb0bf07dfd27` | Chris Padilla–Nasrat Haqparast | 1.0 | Chris Padilla |
| `a8ba8843-7caa-56c1-8991-937bbfa4ac76` | Wes Schultz–Jackson McVey | 0.0 | Jackson McVey |
| `cc88d51e-6818-5219-89a8-adcca1eccf4d` | Gauge Young–Stan Dorsainvil | 0.0 | Stan Dorsainvil |
| `e00625f4-ad7b-50e8-baac-7af1bc43157b` | Serghei Spivac–Vitor Petrino | 0.0 | Vitor Petrino |
| `e871e0b6-3a81-561d-bad4-bee64c4f87d4` | Jamall Emmers–Lerryan Douglas | 1.0 | Jamall Emmers |
| `f467a342-40c3-5abd-902c-cdda666a883c` | Anthony Hernandez–Gregory Rodrigues | 0.0 | Gregory Rodrigues |
| `fa2eb031-73c9-5807-9b6e-51c8f55cfba6` | Shanelle Dyer–Elise Reed | 1.0 | Shanelle Dyer |

The thirteenth selected August 22 bout, Terrance Chatman–Anthony Wint, remains currently resolved and agrees with its historical label. Reviewed records corroborate the preserved historical snapshot; this session did not independently verify official results or repair any warehouse state. No current outcome replaced a historical outcome.

## 8. Training cutoff and exclusion policy

The proposed later-training filter is `event_date < "2026-03-31"`, supported by the earliest original scoring instant `2026-03-31T12:09:04.077607+00:00`. March 30 is the latest permitted event date. Event dates alone do not establish result/statistic availability; a historical knowledge-timestamp audit is required before training begins.

Both datasets contain the same label-free, checksummed `identity-exclusions.json`: **146 original IDs**, including all 38 catch-up records, plus **12 verified alternate bout IDs** and **8 precautionary unverified review targets**, for **166 excluded IDs**. The eight precautionary exclusions do not assert verified identity. Full identifier pairs and evidence are preserved in the file.

`modeling.holdout.load_holdout_fight_ids()` and `assert_no_holdout_fights()` default to the historical population. Passing the pre-event subset still excludes all 166 IDs. The helper opens only its accepted manifest, predictions, and label-free identity/exclusion file; it opens neither outcomes nor joined historical evidence. It detects original-ID and alternate-ID overlap without mutating the training DataFrame. It has not been wired into training pipelines in this session.

## 9. Frozen paths and SHA-256 hashes

In this table, H is `data/holdouts/historical_2026_apr_aug/`; P is `data/holdouts/pre_event_2026_apr_aug/`.

| Frozen file | SHA-256 |
|---|---|
| H `predictions.csv` | `8bbeaa886e5b5002fb8676a75b1c588463594a5e62b8770fee372bec6bbcacf8` |
| H `outcomes.csv` | `d884948188eff812bb73e5c9333df5499710ea5210fe6708a46f5c94587862a0` |
| H `manifest.json` | `51b27abaa13718240f245a125f06e8b6c865fdcdd8062d876b1ad46726721b30` |
| H `identity-exclusions.json` | `a9119e05aa62907b49a3bfae52c602551de840e3f541d7d8f0fbca3ed2956567` |
| H `README.md` | `0fe410be7eb8b933c4980aacdff4e7a7e789fb807cd06ea490b073ce1c5d0b56` |
| P `predictions.csv` | `81e133bcb88422d3e0ddae12ac79de4ce6b9f60ab4d009545d7bfb41ff1a522b` |
| P `outcomes.csv` | `04eeff282639b6f6a888e296bcc75a45321c95f82454d05e585010eadac7285f` |
| P `manifest.json` | `f28c9b052dbe883d05c00d3b46fe711997dfa39c7fca09d8a722c6fc2fb36f6a` |
| P `identity-exclusions.json` | `a9119e05aa62907b49a3bfae52c602551de840e3f541d7d8f0fbca3ed2956567` |
| P `README.md` | `d5b85fa5ef0d0ef8f1200124e116ab833f37ce9733621f00d9e23a76e115d872` |

Evidence files below reside in `docs/implementation-reports/phase1-holdout-evidence/`:

| Evidence file | SHA-256 |
|---|---|
| Preserved source CSV identified in section 3 | `8b66ee10d706061a739604aaa74a6b71fcd3b02731d534c4a134eb9243c5d5ae` |
| `recovery-audit.json` (unchanged original audit) | `3511778afb79ebff2c788598408bd430724d5e9418eb38b7284a88800dad1451` |
| `candidate-provenance-audit.json` (unchanged original audit) | `bef6d8aa41f98ac267186800e7c7291ea14a971b5daea715b4bf45bfab18d31d` |
| `phase1b-identity-outcome-audit.json` | `5b5f636ca646c97f89d59b524f6dee5ccf6cbf235d3d5965dc8c293a096d0e18` |
| `phase1b-historical-validation.json` | `f6743ba9a2112327a10089162f32f71381ec9818ba3d0da37c0206edd4345557` |
| `phase1b-pre-event-validation.json` | `6794f55f37e2730c429c96d530b55129b788998eb5487aba3a69be8c7d79f7b8` |
| `phase1b-verification.json` | `c8c4a56fe7eb8dfecbfbf137be59322ef388002c24c7d0b3f40e495b9113fe03` |

New frozen files and machine-readable evidence were created exclusively and made read-only. Existing accepted artifacts were not overwritten. The subset manifest pins its parent's manifest, prediction, and outcome hashes. Staged artifacts passed validation before dataset publication. Rerunning the publisher refuses existing directories before querying the warehouse.

## 10. Files changed

Modified authorized Phase 1 tooling/documentation:

- `modeling/holdout.py`: explicit two-population contracts and outcome-free full-parent/alias exclusions.
- `tools/validate_prospective_holdout.py`: corrected metrics, strongest-57 ranking, independent snapshot/identity audit, and exact parent/subset validation.
- `tools/audit_holdout_recovery.py`: corrected diagnostics and targeted read-only adopted-source identity/outcome audit.
- `modeling/tests/test_holdout.py` and `modeling/tests/test_holdout_recovery.py`: retained checks and added population, provenance, alias, precision, orientation, and exclusive-publication coverage.
- `data/holdouts/prospective_2026_apr_aug/README.md`: superseded-directory explanation.

Created `tools/freeze_phase1_holdouts.py`, this report, all ten frozen files listed above, and the four new Phase 1b evidence JSON files. The original Phase 1 report and its three evidence files remain unchanged audit records. Unrelated user changes were preserved; no commit or push was performed.

## 11. Tests and repeated offline validation

Focused verification:

```sh
python3 -m pytest modeling/tests/test_holdout.py modeling/tests/test_holdout_recovery.py modeling/tests/test_data.py -q
```

**90 passed, 0 failed.** Tests cover both approved contracts, invalid/nonfinite probabilities, missing fields, duplicate IDs, one-to-one joins, outcome contamination, unaccepted manifests, unsafe paths/symlinks, latent correctness, inclusive bands, strongest ranking/ties/outcome independence, original scalar preservation, identity/orientation/exclusion consistency, parent linkage, subset row retention, and exclusive creation.

The following commands were each run twice against the real frozen datasets:

```sh
python3 tools/validate_prospective_holdout.py --holdout-dir data/holdouts/historical_2026_apr_aug
python3 tools/validate_prospective_holdout.py --holdout-dir data/holdouts/pre_event_2026_apr_aug
```

All four runs returned **exit 0**, with byte-identical results for each population and unchanged hashes for every frozen file. Saved validation JSON hashes are the two validation hashes in section 9. Tests instrument file opening and confirm that exclusion reads only the three permitted files, for both populations, and rejects overlap through an original ID and a verified alternate ID. They also confirm that rehashed identity inconsistencies fail closed.

The preservation baseline covered 42,026 existing files: only the six authorized pre-existing Phase 1 files changed; the other **42,020 remained byte-identical**, with no missing or unrelated changed files. All **55 existing files under `models/`**, including model artifacts, saved predictions, and production pointers, remained unchanged.

`models/production_model.json` SHA-256 remains `ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa`. The sorted model path/hash inventory has SHA-256 `22f2761342d8866a21bfe27cb0667e36f0492d8c3d97233fecc45c2f86efc98b` (compact sorted-key JSON plus LF). Changed-code whitespace checks passed.

## 12. Remaining limitations

- Outcomes were already inspected during recovery and this reconciliation. Neither population is an unseen, blind test set.
- Eight catch-up identity mappings remain unresolved and must not be certified, silently remapped, or quietly removed to manufacture a clean historical cohort.
- Pre-event timestamps do not prove historical availability of every feature, result, statistic, or refreshed input. Training remains contingent on that later audit.
- Catch-up records carry post-event knowledge-leakage risk; scored-before-result-load assertions do not eliminate it.
- The recovered report's original analysis session and creation timestamp remain unknown. Historical reviewed labels are corroborating warehouse evidence, not independent official-result certification.
- Cancellation counts cannot be certified from the preserved report; pending/replacement annotations were not converted into invented cancellation labels.
- Checksums and read-only permissions detect/protect against routine changes but do not prevent an authorized writer from changing data and manifests together. Accepted artifacts should receive version-control review in a later authorized session, without overwriting this freeze.

## 13. Scope confirmation

No model training, candidate-model scoring, confidence-policy changes, NO PICK implementation, production-pointer/artifact modification, dashboard work, warehouse-result repair, market stacking, production promotion, commit, or push occurred. Warehouse access was read-only and rolled back. Work was confined to the corrected Phase 1 contracts, identity/outcome audit, frozen evaluation datasets, exclusion helper, validation, tests, and report.

## 14. Recommended inputs for the next implementation session

Use this report, both accepted manifests and frozen prediction files, identical full-parent identity/exclusion files, the unchanged original recovery evidence, the new identity/outcome audit, saved repeated-validation results, and existing XGBoost artifact metadata. Keep individual outcomes and joined evidence within evaluation/audit utilities.

Treat `pre_event_2026_apr_aug` as the primary original-forecast comparison population. Treat the historical 146 as mixed historical comparison evidence with eight uncertified identities, not as a wholly prospective or entirely clean cohort. Resolve the eight mappings using evidence beyond names if clean historical evaluation is later required; do not silently drop or remap rows. Any future uncertainty affecting the 108 must block that population rather than reduce it quietly.

Before training, audit historical result/statistic/feature knowledge timestamps, enforce `event_date < "2026-03-31"`, and integrate the reusable exclusion helper so all 146 originals and verified/precautionary alternate IDs stay out of training. Establish development splits outside the frozen populations. Any training, scoring, policy/product work, or production change requires the next session's separate scope; existing frozen artifacts must remain intact.
