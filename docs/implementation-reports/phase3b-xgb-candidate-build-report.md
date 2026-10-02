# Phase 3B: isolated pre-April-2026 XGBoost candidate build

**Status: COMPLETED.** Repository: `/home/wlodzimierrr/ufc-data`. Report date: 2026-10-01 (Europe/Warsaw); recorded run timestamps are UTC. Real stages A–D, persistence, independent offline loading and preservation verification completed. No blockers remain within the authorized build scope.

## Authorization, source and limitations

Accepted mode: `retrospective_git_pre_cutoff`, explicitly authorized by this session’s user request. Authorization is recorded separately in `run_receipt.json` and the byte-preserved `user_authorization.txt`, request SHA-256 `ef17526a8461a4a6c662db8c35b754303681e16675f78dbd12164941da03f226`. The TOML and accepted snapshot retain `phase3b_mode_approval_required = true`; the TOML remains `draft_preparation_only`. Authorization was recorded at 2026-10-01T20:42:26.041293+00:00, not attributed to historical preparation.

This is chronological retrospective reconstruction, not certified historical replay. Exact source versions predate the global knowledge cutoff; availability before each historical bout remains unverified. Scheduled rounds remain unknown and were not derived from finish rounds, results or title status. The eligible population is exactly 8,400 archived binary-result bouts through March 7, 2026; missing March/other bouts were not sourced from today’s warehouse. Frozen forecast outcomes were previously inspected, so the 108/146 populations are not blind or unseen tests.

Only the accepted metadata-safe snapshot was loaded through `load_prepared_snapshot`, with the pinned manifest hash and explicit source mode. The preliminary snapshot remains rejected. Source commit: `1f477d3ddc87b123d0099025b669e728e7881a34`. Exclusive cutoff: `event_date < 2026-03-31`. Knowledge cutoff: `2026-03-31T12:09:04.077607+00:00`. Feature version 2, algorithm `v2_date_frozen_elo_schedule_unknown_v1`.

Preparation source schemas, archived label/orientation consistency, Git blobs, file hashes, all 16 preparation code hashes, configuration hash, package versions, 50-feature order, row/date/label/missingness summaries, 166 exclusions and every complete fold membership passed preflight. No discrepancies or accepted-input rewrites occurred. Eligible dates: 1994-03-11–2026-03-07; 764 events, 759 dates; labels: 5,401 fighter-1 wins and 2,999 fighter-2 wins. All 530 both-debuting bouts and legitimate other NaNs were preserved.

## Files and implementation

New repository files:

- `modeling/xgb_candidate_contract.py` — pinned contracts, exact partition guards, fold/final priors, weighted selection and probability transforms.
- `modeling/train_xgb_candidate.py` — offline A–D training with frozen selection and OOF, persisted calibration and final refit.
- `modeling/xgb_candidate_bundle.py` — checked offline loader/inference, publication and eligible-row verification CLI.
- `tools/verify_candidate_preservation.py` — separate hash-only preservation check.
- `modeling/tests/test_xgb_candidate.py` and `modeling/tests/test_xgb_candidate_bundle.py` — focused guard and learned-bundle tests.
- `docs/implementation-reports/phase3b-xgb-candidate-build-report.md` — this report.

New data is confined to the isolated candidate run listed below. No existing repository file was modified. No applicable ancestor or repository `AGENTS.md` was present. The initial dirty worktree and every existing user change were preserved.

The workflow repeatedly checks exact memberships, chronology, identities/orientation, labels and base features before preprocessing and fitting. Early-stopping and calibration labels also pass exclusion guards. Entire dates/events stay together. Training imports no legacy trainer/scorer entrypoint and consumes no current warehouse features/results/profiles. Frozen outcomes and joined evidence are not opened by training/preprocessing; preservation checks separately hash their bytes without parsing them.

## Input and code provenance

| Input | SHA-256 |
|---|---|
| accepted manifest / bundled source_manifest.json | `42e8e1661409b2ca758a4e18775f9816865439448fbce0d88af68dda22325671` |
| accepted training.csv | `e53cd2f253985290ca2e03185fd4b67ee67c209473f595d5fda3cecbb34e764c` |
| accepted folds.json | `c425c98ff96f7e4dba4505e1215ace017587fef3d6a41f8996ad5ce726b615d1` |
| TOML / effective configuration | `b7e518293c07c0201f9a003087faf86a6a110546c861e48cbf587612f8ab4fdc` |
| archived sources/events.csv | `df2d02bbadbe5b293bcb33c08f0ae4095b4ba4398cd3f8884032da61be5ad81f` |
| archived sources/fight_stats.csv | `b20c0e07aefff66f749476a3acd8abc57c0ef6cdc09d545c05fe7fe068d93324` |
| archived sources/fighters.csv | `ecce91a1aa2fb024d0f9c8eb489e9d9b5ddb0663f2f745ce089c8bad5461ce12` |
| archived sources/fights.csv | `3c89061dc315a640f29ddfd260b2b23afc1e48faed5106de0cbe461558e90ff1` |

Preparation provenance remains in the unchanged copied `source_manifest.json`; all 16 recorded code hashes were rechecked. Canonical preparation-code-map SHA-256: `02a3ac50feea6ae671c8c9dfde1e106c72f8223e515bb4e59797dc59adab0c53`. New lifecycle hashes are recorded separately:

| Code | SHA-256 |
|---|---|
| modeling/decisions.py | `f47d76a804d25a5ae3711091cd5dfb2c208b292eea08ee8b381a5097fc3dbc87` |
| modeling/train_xgb_candidate.py | `cf4c415206e39c632686a3a4fa154a3e19b9d47fa2a56da6a887b97102541893` |
| modeling/xgb_candidate_bundle.py | `b28a69d773a5d3ffbc31309fd90819eac0eee0aa8cefb14aff8483596de36097` |
| modeling/xgb_candidate_contract.py | `6b74813c9b887a3507d4e292f7e1d553bbc4add9d23a4d458f48fa24c569b057` |
| tools/verify_candidate_preservation.py | `756b19c86f9c5a530b699f3b04c1be693859e2592022ee102bf248bec1eb7ae3` |
| modeling/tests/test_xgb_candidate.py | `83d0dbb6867077e19b2168800cacea98dad5054f081c049af885cd6310b88d86` |
| modeling/tests/test_xgb_candidate_bundle.py | `8176c7c4654c7a225be26694bbe884bba7a6cd2b3b611dd5fd733dad85505183` |

`modeling/decisions.py` is an unchanged dependency. Post-publication checks confirmed the lifecycle source bytes still match execution provenance.

| Package | Version |
|---|---|
| joblib | 1.5.3 |
| numpy | 2.4.3 |
| pandas | 3.0.1 |
| psycopg2-binary | 2.9.11 |
| pytest | 9.0.2 |
| python | 3.11.2 |
| scikit-learn | 1.8.0 |
| xgboost | 3.2.0 |

The five packages pinned by preparation match exactly; Python, joblib and pytest were additionally recorded.

## Stage A: development selection

Executed every one of the 27 grid configurations on all three fixed development folds: **81 fits**. Train/validation counts: dev_2022 6,414/502; dev_2023 6,916/519; dev_2024 7,435/506. Windows: [2022-03-31,2023-03-31), [2023-03-31,2024-03-31), [2024-03-31,2025-03-31).

Fixed parameters: `objective=binary:logistic`, `eval_metric=logloss`, learning rate 0.02, subsample 0.8, colsample_bytree 0.8, random_state 42, n_jobs 1, tree_method hist, verbosity 0. Each fit allowed 500 rounds and patience 50 on its own validation partition. Each fold’s priors were computed from its training rows and saved with membership lineage. Validation probabilities explicitly used `iteration_range=(0, best_iteration+1)` with installed XGBoost 3.2.0.

All development results follow. Each fold cell is **log loss / Brier / best rounds / fitted rounds**. Best rounds = zero-based best_iteration + 1. Displayed metrics use nine decimals; `development_results.json` contains full precision, settings, best scores, memberships and prior digests for every fit. Weighted loss uses all 1,527 validation rows. These are development-selection diagnostics.

| Depth | Min child | Lambda | dev_2022 | dev_2023 | dev_2024 | Weighted loss |
|---:|---:|---:|---|---|---|---:|
| 3 | 20 | 0.1 | 0.646714782 / 0.227999746 / 195 / 245 | 0.663206730 / 0.235823676 / 191 / 241 | 0.636953688 / 0.223670792 / 490 / 500 | 0.649085579 |
| 3 | 20 | 1.0 | 0.646050474 / 0.227777761 / 255 / 305 | 0.662635597 / 0.235523100 / 255 / 305 | 0.636797766 / 0.223638294 / 496 / 500 | 0.648621403 |
| 3 | 20 | 5.0 | 0.645767411 / 0.227667111 / 273 / 323 | 0.663401037 / 0.235871122 / 186 / 236 | 0.636811323 / 0.223555521 / 498 / 500 | 0.648792998 |
| 3 | 50 | 0.1 | 0.645374156 / 0.227555531 / 219 / 269 | 0.660893375 / 0.234677483 / 328 / 378 | 0.635443524 / 0.222958581 / 499 / 500 | 0.647358161 |
| 3 | 50 | 1.0 | 0.645882407 / 0.227805750 / 273 / 323 | 0.661530007 / 0.234985130 / 328 / 378 | 0.634616718 / 0.222528950 / 499 / 500 | 0.647467650 |
| 3 | 50 | 5.0 | 0.645738364 / 0.227721501 / 303 / 353 | 0.661878587 / 0.235127751 / 346 / 396 | 0.637112230 / 0.223626078 / 481 / 500 | 0.648365706 |
| 3 | 100 | 0.1 | 0.644833190 / 0.227269211 / 397 / 447 | 0.662443847 / 0.235327524 / 240 / 290 | 0.633911126 / 0.222050609 / 498 / 500 | 0.647199508 |
| 3 | 100 | 1.0 | 0.644353773 / 0.227041201 / 375 / 425 | 0.663568362 / 0.235841978 / 235 / 285 | 0.633581020 / 0.221853326 / 498 / 500 | 0.647314715 |
| 3 | 100 | 5.0 | 0.645016505 / 0.227359862 / 390 / 440 | 0.663855653 / 0.235986648 / 240 / 290 | 0.633927935 / 0.222045968 / 498 / 500 | 0.647745190 |
| 4 | 20 | 0.1 | 0.648958748 / 0.229118187 / 178 / 228 | 0.661525596 / 0.235042403 / 179 / 229 | 0.635726303 / 0.222998635 / 339 / 389 | 0.648845177 |
| 4 | 20 | 1.0 | 0.647041237 / 0.228194756 / 178 / 228 | 0.661317577 / 0.234933290 / 180 / 230 | 0.635174124 / 0.222769022 / 337 / 387 | 0.647961120 |
| 4 | 20 | 5.0 | 0.647733176 / 0.228521570 / 193 / 243 | 0.661629592 / 0.235058365 / 182 / 232 | 0.635890231 / 0.223016905 / 344 / 394 | 0.648531938 |
| 4 | 50 | 0.1 | 0.647235813 / 0.228327583 / 192 / 242 | 0.661705834 / 0.234988447 / 171 / 221 | 0.635413973 / 0.222892044 / 481 / 500 | 0.648236527 |
| 4 | 50 | 1.0 | 0.647943638 / 0.228698838 / 180 / 230 | 0.661000316 / 0.234601695 / 273 / 323 | 0.636550016 / 0.223345307 / 393 / 443 | 0.648605880 |
| 4 | 50 | 5.0 | 0.647478417 / 0.228441491 / 192 / 242 | 0.661703543 / 0.235000392 / 179 / 229 | 0.635688765 / 0.222912924 / 473 / 500 | 0.648406562 |
| 4 | 100 | 0.1 | 0.642617753 / 0.226170304 / 303 / 353 | 0.663154427 / 0.235670327 / 172 / 222 | 0.633830366 / 0.221904073 / 498 / 500 | 0.646685936 |
| 4 | 100 | 1.0 | 0.643197197 / 0.226455191 / 279 / 329 | 0.662810753 / 0.235507133 / 154 / 204 | 0.634295423 / 0.222140997 / 498 / 500 | 0.646913725 |
| 4 | 100 | 5.0 | 0.644045077 / 0.226925073 / 332 / 382 | 0.663375952 / 0.235754602 / 194 / 244 | 0.634621341 / 0.222301331 / 498 / 500 | 0.647492565 |
| 6 | 20 | 0.1 | 0.645599378 / 0.227446780 / 170 / 220 | 0.660493895 / 0.234371479 / 133 / 183 | 0.634570604 / 0.222615377 / 297 / 347 | 0.647007168 |
| 6 | 20 | 1.0 | 0.645002284 / 0.227101957 / 177 / 227 | 0.659046612 / 0.233756234 / 133 / 183 | 0.635307898 / 0.222984187 / 338 / 388 | 0.646563284 |
| 6 | 20 | 5.0 | 0.647186960 / 0.228144111 / 179 / 229 | 0.660161425 / 0.234177003 / 133 / 183 | 0.633782125 / 0.222055194 / 338 / 388 | 0.647154806 |
| 6 | 50 | 0.1 | 0.646862620 / 0.228128027 / 132 / 182 | 0.662567993 / 0.235259512 / 183 / 233 | 0.633706242 / 0.222008451 / 466 / 500 | 0.647840984 |
| 6 | 50 | 1.0 | 0.647671414 / 0.228518731 / 129 / 179 | 0.662975900 / 0.235449990 / 151 / 201 | 0.636008079 / 0.222926951 / 324 / 374 | 0.649008271 |
| 6 | 50 | 5.0 | 0.645382217 / 0.227513836 / 178 / 228 | 0.663635395 / 0.235602864 / 221 / 271 | 0.631636833 / 0.220892229 / 391 / 441 | 0.647031356 |
| 6 | 100 | 0.1 | 0.642036988 / 0.225934096 / 273 / 323 | 0.663167363 / 0.235559373 / 165 / 215 | 0.633124295 / 0.221590682 / 357 / 407 | 0.646265437 |
| 6 | 100 | 1.0 | 0.642593144 / 0.226265065 / 310 / 360 | 0.663410863 / 0.235628807 / 165 / 215 | 0.631681337 / 0.220839769 / 500 / 500 | 0.646052883 |
| 6 | 100 | 5.0 | 0.643899337 / 0.226802435 / 273 / 323 | 0.662526315 / 0.235291974 / 166 / 216 | 0.632317254 / 0.221108249 / 498 / 500 | 0.646392374 |

Winner: **max_depth=6, min_child_weight=100, reg_lambda=1.0**; row-weighted development log loss **0.6460528831081821**. Winning best iterations: 309, 164, 499; best round counts: 310, 165, 500. Selected rounds: `floor(median(310,165,500)+0.5) = 310`. The winning third fit reached the fixed 500-round cap; the procedure and cap were retained. Exact ties would use lexicographic grid-parameter order.

Selection was written and made read-only before OOF generation. Settings/round digest: `06e33ada191190f33b7792939f91743a619960fb2068687ce270a5138f7a7359`. No legacy artifact best iteration or OOF metrics influenced selection.

## Stage B: frozen-settings OOF

| Fold | Prediction window | Train | Predictions | Raw log loss | Raw Brier |
|---|---|---:|---:|---:|---:|
| oof_1 | [2025-03-31,2025-06-30) | 7941 | 129 | 0.599755683342 | 0.205796259766 |
| oof_2 | [2025-06-30,2025-09-30) | 8070 | 126 | 0.647776536242 | 0.228363071487 |
| oof_3 | [2025-09-30,2025-12-31) | 8196 | 128 | 0.626238183583 | 0.219887083501 |
| oof_4 | [2025-12-31,2026-03-31) | 8324 | 76 | 0.591093946031 | 0.202946265855 |

Every OOF model used the frozen winner and exactly **310 rounds**, with no eval set, early stopping or OOF-driven tuning. Each expanding fold saved independently fitted training-only priors. `oof.csv` has exactly **459** full-precision raw probabilities, one per eligible final-12-month row; it preserves fight/event/date identities, fighter orientation, labels, fold, settings digest, training membership and prior digest. Native XGBoost probability precision is retained without decimal rounding; Platt outputs are float64.

`validate_oof_inputs` plus additional settings/prior/event provenance checks passed before calibration. The raw CSV and OOF manifest were frozen read-only, reloaded using `float_precision="round_trip"`, and checked again. No procedure changes followed OOF metrics. Overall raw OOF log loss: **0.6188887889205785**; Brier: **0.21544864359558352**.

Exact fitting and prediction membership digests (complete ID lists remain in `folds.json`):

| Fold | Training IDs SHA-256 | Prediction IDs SHA-256 |
|---|---|---|
| dev_2022 | `d262d66b43432fa8014fc07386eb94ea408dad59100a77197d694caf972a03ef` | `9c9fecee50d3df5630edb4a9c819019d520d9df8f1ee261bb8c5aecdbc3b9aee` |
| dev_2023 | `ce4f10fbe2cf75a39fc1e5edfe24c5d6e27e0668c5283cb256a982386f1aeaf7` | `da992e75706fc0a266494d2452570b2051d599b6d5c6186a0057129916967e58` |
| dev_2024 | `72821f86867fe92c2a3065494ea185e89988a406dd6c703d79da0753c06e5ab2` | `019027b8fd3288a499c301e71e5ec93b0a40e85314fea03dd74bfceb95b42ed1` |
| oof_1 | `0e6c64a1be161db739331026acead085a59091ec2e126a798ce00f66429ce0b4` | `f8826d45de2d8e824ee7b2f7f04c8c22b1c6eeae5e45d9dbc1a0caa31ddc50f3` |
| oof_2 | `d3ea21e89105997043c81fafa56ab71a2e4959702d3f0479a96f26ed5f75b839` | `160435953e6a0ec91cacf5a08ce09a0e4f1de0caa132fd27ef801c9194c533ec` |
| oof_3 | `259ddfec107bba197737dd972e39c0e1530e47ae6a5300a8a16c30ca9f72aee3` | `fabe89bdec6ed15c0c5e6f3077a3b65ce17d11a198e0947fe3613e55c0ff7edb` |
| oof_4 | `45ae08735bfeecfcb6731c01263c896f3e750422ed0b0c8e04772f9e46fa962d` | `cb589f1d821df51441dac2a23404e642a45c66be66bd6d27035f4f83207eb76a` |

Prior artifact digests are in the complete component checksum table below; each saved prior also records its training count, membership digest and endpoint.

## Stage C: persisted Platt calibration

Fit exactly one `LogisticRegression(C=1e10, solver="lbfgs", max_iter=1000)` on validated OOF log odds after clipping raw probabilities to [1e-8,1−1e-8]. Saved the fitted estimator as `calibrator.joblib`; metadata records clipping, log-odds input, classes [0,1], and positive class 1 = fighter 1 wins. No estimator-discarding helper was used.

Coverage: **459 rows**, 252 label-1 and 207 label-0; both classes required. Coefficient: **1.4029650944569299**; intercept: **-0.3637652858897927**; converged: **true**, **5 iterations**, **no warnings**. Calibration-input SHA-256: `0fc468908c691a796d2b4d0da1619bdbffefe67645644b49ac8a4f087ab2138b` (ordered fight IDs, labels, full-precision raw probabilities and transformed log odds). Raw CSV SHA-256 is listed below.

**Calibration-fit diagnostics on those same 459 rows:** log loss **0.6081959030065491**, Brier **0.2108109523654254**. These are not independent validation. Settings were not changed to improve diagnostics or suppress warnings.

## Stage D: final refit

Final XGBoost fit used **all 8,400 eligible rows**, the frozen selected parameters and **310 rounds**. There was no early stopping, eval set, or retained legacy two-year validation/test reservation. Actual training endpoint: **March 7, 2026**; this is a pre-April historical candidate, not a model trained through today. Final membership SHA-256: `b4c6a122b45d663e1ea6b10dffb910d6f38c2295e71df6be74e99e17f27701a6`.

Final priors were fitted independently on all eligible rows and persisted as `final_debut_priors.json`. Neutral base prior 0.5; global height standard deviation 6.4729358277841715; global reach standard deviation 8.315230583738112. Training both-debut win rate 0.7339622641509433 is informational and does not replace the neutral prior. Weight-class statistics and final membership lineage are saved in the prior artifact. Other legitimate NaNs were retained; no global imputation or bout dropping occurred.

## Bundle publication and offline verification

Published bundle:

`data/experiments/phase3b_xgb_pre_april_2026/20261001_phase3b_retrospective_v1/`

A–D ran from 2026-10-01T20:42:26.041276+00:00 to 2026-10-01T20:43:31.249475+00:00. Publication: 2026-10-01T20:44:52.122233+00:00. The run first remained under `.incomplete-20261001_phase3b_retrospective_v1` with an incomplete marker. Save/load validation and a separate preservation check passed before exclusive publication. There are **30 files**, all read-only; no incomplete run remains. Existing run names are refused.

The offline loader checks required components and every checksum before estimator loading, pinned source/fold/configuration contracts, exact feature order/version/algorithm, package compatibility, model objective/classes/rounds, final-prior lineage and fitted-calibrator settings/coefficients. It checks persisted OOF coverage, fold/prior/settings lineage and the exact calibration-input hash. Inference requires explicit compatible feature provenance including the accepted preparation identity and algorithm; matching legacy column names alone is rejected. Missing features/components, incompatible schedules/debut slots and malformed probabilities fail. Saved final priors and calibration are loaded; neither is refitted from any database.

Label 1 and p mean fighter 1 wins. Raw and calibrated complementary probabilities are retained at full available precision. The unchanged `probability_band_v1` applies inclusive 0.40–0.60 NO PICK; immediate floating-point neighbors are tested.

In-memory versus loaded final-pipeline probabilities matched **exactly on all 8,400 eligible non-holdout rows**: raw and calibrated maximum absolute difference both **0.0**. Independent publication reload and post-publication CLI verification reproduced these digests:

- Raw: `73fa57197a86d462b73495b98337da7ab34909f0e63a9f0e59e4bc5f3646acc8`.
- Calibrated: `499e4e98385e91e5a05a277a0465e09bfd949941238dbe8eb89515b43d0241ac`.

This is serialization/inference verification, not evidence of final-model predictive performance. No final-training accuracy or proper-score performance claim was produced.

Component SHA-256 values (`checksums.json` covers the other 29 files and excludes itself):

| Component | SHA-256 |
|---|---|
| base_learner.json | `58e056dc2637606393a18288f408c821ac3602257e69e749cbb6c0b631a7cdfb` |
| calibrator.joblib | `d6b29b6a32f45feb3ccbe6b5ddca34f0f40f108946c072a12c96aee4ab990a63` |
| development_results.json | `626053b4912d31a1500c1070cc62613b388f3b515f4fd4e3a7584793eaf864c1` |
| effective_configuration.toml | `b7e518293c07c0201f9a003087faf86a6a110546c861e48cbf587612f8ab4fdc` |
| final_debut_priors.json | `c79705d942f70e79aaef1ff3f31d77b823093213cc3e6befd873efd2346e5e95` |
| fold_priors/dev_2022.json | `3f8a851a81b7414cc4019f6be5958e81973d283ed142df7602e56a135dd58064` |
| fold_priors/dev_2023.json | `8f69af4a4c782ea07a28770e3761030b38657956e453ffe4342770f3b0ab804c` |
| fold_priors/dev_2024.json | `df2f817124c5a71a0ffd4e8708a06277172ce1abf8d200b07f6135b803d550da` |
| fold_priors/oof_1.json | `51cdb46dc180d9de210f86313415016b078ef69225abce037d830f054c885eeb` |
| fold_priors/oof_2.json | `723a6ed014d5e10b7efc97f3e3f14f01878c749784f69570ca202dbac5bcd751` |
| fold_priors/oof_3.json | `801ea4da4c7d3bdc3228ab528ca497d213cdc9909fb960434107e4c595426883` |
| fold_priors/oof_4.json | `47f32c3534c96194916416bd52fbc414c6d849c709076422b85aaca49d82c131` |
| folds.json | `c425c98ff96f7e4dba4505e1215ace017587fef3d6a41f8996ad5ce726b615d1` |
| metadata.json | `278b4fb3f9022365e3d345e27d2cc778666446646b0a74a9a2b22e02dd3f95fb` |
| oof.csv | `7d771e1117071da3af50a14e335d0e8b5315d0c9ee1e03e8064027e0a4b9c4f7` |
| oof_manifest.json | `ee6e3c15d7c7e158248206832fa33b57de842c76c9caa30ebc25a478b6051f9e` |
| package_versions.json | `bb841c0a84f659cff99b65f760be9a05bc530c00ebd1d569ab74165f0b810b51` |
| preflight.json | `8e49c5ae5dcc88dde44ad6bb18bb7233d97ff9b6ccb37c01be071833c85ad792` |
| preservation_baseline.json | `f61ed9acb4f9ffde92cb940385f2587720843e86ffa2c5e0e146bcce820c580d` |
| preservation_check.json | `c3e52601f9244584a840efacd3b063cf63c632baf873b36115362bbeba2f777d` |
| regressions.log | `0f999e78a876e90d452d0cb059bcb3ca548b34be4413239636370a2f476eb873` |
| run_receipt.json | `897061a08db28e4a01b352e6a77dd8191a3187076a60bfe125d11257a9eda8b5` |
| selection.json | `489d99b1e7ffe5c4b483bbd340bb704593272229d63b0e798696d4d8d9a52d81` |
| source_manifest.json | `42e8e1661409b2ca758a4e18775f9816865439448fbce0d88af68dda22325671` |
| training.log | `1111ce2e0b47b238b563b7063d4b553480004e438789b3704dcd2f96fad1feda` |
| training_manifest.json | `f78e91525fa8a7017d0623a8b1192650a5b2d0a5603129e8b92c827973e0b2c6` |
| user_authorization.txt | `ef17526a8461a4a6c662db8c35b754303681e16675f78dbd12164941da03f226` |
| validation_results.json | `eaa6ee025d74c8711a9628352a1322ad0db63e7bb79a4e17c38245d59f3e75ac` |
| verification.json | `700ce81d8337eb66d25c86bf79ff73790be4ff327cb70742057c7b90d9d65877` |

External SHA-256 of `checksums.json`: `41f8c8afb8fd091fa0b04823ad9175cedcfdbf85ccf3f62d77c71e0d9219279f`; it is recorded here, not inside a self-referential manifest.

## Tests and exact execution commands

Final affected suites: **446 distinct tests passed, 0 failed, 0 skipped**: 57 candidate tests and 389 regressions. The SQL filtering regression used a disposable PostgreSQL cluster; no live warehouse writes occurred. Focused coverage includes exclusion/temporal/membership checks before fitting, isolated priors, weighted selection/ties/rounds, explicit best-iteration predictions, fixed OOF/final fits, exact OOF provenance/orientation/labels, calibrator clipping/classes/persistence/convergence reporting, real bundle parity, missing/tampered/incompatible artifacts, full-precision NO PICK boundaries, and blocked warehouse/frozen-outcome access.

An initial candidate-only run produced 28 passed/1 failed because the inherited synthetic fixture had scheduled_rounds=3 while the test asserted unknown schedules; the new fixture was corrected to NaN. The subsequent candidate-only run passed 29, and the combined candidate suite passed 57. No real training failure or procedure change occurred.

Intentionally not run: the live integration test that writes warehouse records and overwrites `models/upcoming/upcoming_features.csv`; unrelated legacy v1 snapshot tests with 19 documented existing failures; the live leakage suite with 34 documented persisted label discrepancies. Those failures/discrepancies were not repaired. They are known previous-phase findings, not failures of this run. Scoped whitespace checks and module compilation passed.

All commands used repository cwd `/home/wlodzimierrr/ufc-data`. Read-only inspection used `git status --short`, applicable `AGENTS.md` searches and the documents/modules named in the user request. The baseline was created before edits by hashing all `git ls-files -c -o --exclude-standard` files plus existing files under `models`, `data/holdouts`, `data/audits`, `data/experiments`, and `docs/implementation-reports`; its complete 383-path catalog and original git status are bundled. Preflight was also executed directly through the same `preflight(...)` API before training and returned VALID with no discrepancies. Actual fit and publication commands:

```bash
set -o pipefail
python3 -m modeling.train_xgb_candidate \
  --source-mode retrospective_git_pre_cutoff \
  --manifest-sha256 42e8e1661409b2ca758a4e18775f9816865439448fbce0d88af68dda22325671 \
  --authorization-request "/home/wlodzimierrr/.codex/attachments/0342d801-fe7a-49fb-a0e5-2f254821cb75/Pasted text.txt" \
  --preservation-baseline /tmp/ufc-phase3b-preservation-baseline.json \
  --run-name 20261001_phase3b_retrospective_v1 | tee /tmp/ufc-phase3b-training.log

python3 tools/verify_candidate_preservation.py \
  --baseline /tmp/ufc-phase3b-preservation-baseline.json \
  --run data/experiments/phase3b_xgb_pre_april_2026/.incomplete-20261001_phase3b_retrospective_v1

python3 -m modeling.xgb_candidate_bundle \
  --bundle data/experiments/phase3b_xgb_pre_april_2026/.incomplete-20261001_phase3b_retrospective_v1 \
  --manifest-sha256 42e8e1661409b2ca758a4e18775f9816865439448fbce0d88af68dda22325671 \
  --source-mode retrospective_git_pre_cutoff --publish

python3 -m modeling.xgb_candidate_bundle \
  --bundle data/experiments/phase3b_xgb_pre_april_2026/20261001_phase3b_retrospective_v1 \
  --manifest-sha256 42e8e1661409b2ca758a4e18775f9816865439448fbce0d88af68dda22325671 \
  --source-mode retrospective_git_pre_cutoff
```

Training/regression logs and validation commands/results were copied into staging before checksummed publication. Test/check commands, exactly as executed (candidate-only command ran twice):

```bash
python3 -m pytest modeling/tests/test_xgb_candidate.py -q
python3 -m pytest modeling/tests/test_xgb_candidate.py modeling/tests/test_xgb_candidate_bundle.py -q

set -o pipefail
python3 -m pytest modeling/tests/test_refit_preflight.py modeling/tests/test_refit_sql.py modeling/tests/test_holdout.py modeling/tests/test_holdout_recovery.py modeling/tests/test_data.py modeling/tests/test_calibrate.py modeling/tests/test_decisions.py modeling/tests/test_evaluate.py features/tests/test_replay.py features/tests/test_debut_prior.py features/tests/test_history.py features/tests/test_elo.py features/tests/test_opponent.py features/tests/test_career.py features/tests/test_rolling.py features/tests/test_decay.py features/tests/test_physical.py betting/tests/test_prediction_decision_compatibility.py -q | tee /tmp/ufc-phase3b-regressions.log

python3 -m compileall -q modeling/train_xgb_candidate.py modeling/xgb_candidate_contract.py modeling/xgb_candidate_bundle.py tools/verify_candidate_preservation.py
git diff --check -- modeling/train_xgb_candidate.py modeling/xgb_candidate_contract.py modeling/xgb_candidate_bundle.py modeling/tests/test_xgb_candidate.py modeling/tests/test_xgb_candidate_bundle.py tools/verify_candidate_preservation.py
```

Post-publication read-only verification recomputed the 383 existing-file hashes, all component hashes and current lifecycle-code hashes, checked read-only bundle files and absence of incomplete runs, and inspected `git status --short`. All checks passed. This does not invoke training again. Re-running the fit command with this existing run name will deliberately fail instead of overwriting it.

## Preservation, scope and deviations

All **383 existing files** matched the before-work baseline, including `models/`, saved predictions and production pointer, both accepted holdout directories, Phase 1/2/3A evidence/reports/audits/preparation snapshots, and all existing tracked/untracked user changes. There were no new files under protected models/holdouts/audits/Phase 3A snapshot roots. The independent pre-publication preservation receipt and a post-publication recheck both passed.

Unchanged production-pointer SHA-256:

`ff5272e0d2f11f9f6cbc6597f91a2cf9961ae317f8dd47f90bfb1f6e44dd5caa`.

No frozen 108/146 forecast scoring or comparison, production promotion/pointer change, live migration, warehouse write, dashboard work, confidence modifier, market stacking, commit or push occurred. No new authorized-scope blocker or procedural deviation exists. The only lifecycle detail beyond the TOML is separate staging/preservation/publication to ensure completed artifacts are distinguishable from incomplete work. The initial synthetic test-fixture correction is disclosed above.

## Next-session handoff

Use this report, the complete candidate bundle and checksums, its separate authorization receipt/effective configuration, the accepted Phase 3A report/snapshot and pinned manifests, `docs/xgb-production-refit-plan.md`, the unchanged 166-ID guard, both accepted holdout manifests, Phase 1b/2 evidence, and `docs/no-pick-contract.md`. Candidate scoring or comparison on the original 108 forecasts requires a separately authorized next session.

For each future comparison vector, retain the original forecast’s `scored_at`, event/fighter identities and orientation. Establish exact source versions available by that scoring instant for earlier outcomes/statistics, mutable profiles and matchup metadata. The March archive cannot supply later resolved histories. With date-only evidence, cap histories and date-frozen Elo at `min(event_date, UTC(scored_at).date())`, excluding the scoring day and target day; `reconstruct_at_scored_at` enforces chronology but labels availability unverified and cannot certify source versions. Unknown schedules must remain unknown. Independently evidenced earlier results, including fitting-excluded identities, may inform later histories when available by that later scoring time; their labels remain excluded from fitting, selection, early stopping and calibration. Frozen outcome files must not supply feature-development histories.

A reviewed scoring-input provenance contract must establish compatibility with this algorithm and its explicit loader provenance requirement; do not relabel today’s warehouse vectors as compatible merely because columns match. Load this bundle’s final priors and fitted calibrator without refitting. Preserve original frozen baseline probabilities instead of recreating the old model/calibrator. Keep the 146 historical population qualified as mixed evidence and the already-inspected 108 outcomes visible as a retrospective comparison limitation.

**Completed:** 81 development fits, frozen selection of (6,100,1.0)/310 rounds, 459-row OOF, one saved Platt estimator, final 8,400-row refit, complete immutable isolated bundle, exact probability parity and 446 passing tests. These diagnostics do not establish promotion readiness or unbiased final-model performance.
