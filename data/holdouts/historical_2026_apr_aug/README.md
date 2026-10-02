# historical_2026_apr_aug

Status: FROZEN (historical snapshot acceptance). Classification: `mixed_pre_event_catchup_historical`.

Read the exact adopted preserved source; keep 2026-04-01 <= event_date < 2026-09-01 and original resolved == True; no correctness/probability selection.

146 unique fights across 16 events, 2026-04-04 through 2026-08-22.
UTC timing: 108 before event day, 0 on event day, 38 after event day.
The pre-event subset is the primary population for later original-forecast comparison. Timestamp evidence alone does not establish complete historical feature availability.

`predictions.csv` preserves original prediction strings/precision/timestamps and contains no outcomes. `outcomes.csv` preserves the original resolved outcome snapshot in prediction orientation. Current warehouse outcomes are never substituted.
The manifest distinguishes inclusive threshold high confidence (p <= .30 or p >= .70) from strongest 57 (historical probability-margin ranking, fight_id tie-break).

Identity/label checks corroborate 138 rows; 8 remain uncertified for clean evaluation. A FROZEN historical snapshot is not blanket certification for clean model evaluation. Do not silently drop uncertain rows or treat the historical 146 as wholly prospective.
`identity-exclusions.json` contains IDs and identity evidence only: all 146 originals, 12 verified alternate bout IDs, and 8 precautionary unverified review targets. The subset carries the same full exclusions. Training uses `modeling.holdout` without opening outcomes or joined evidence.

Proposed later-training filter: `event_date < "2026-03-31"`, supported by `2026-03-31T12:09:04.077607+00:00`; audit historical result/statistic availability first.
Original report creation time and analysis session remain unknown. Outcomes were already inspected during recovery; these datasets are not unseen test sets.

Files were created exclusively and made read-only. Do not overwrite accepted artifacts. Validate offline with `python3 tools/validate_prospective_holdout.py --holdout-dir data/holdouts/historical_2026_apr_aug`.
See `docs/implementation-reports/phase1b-holdout-freeze-report.md` for the full audit and hashes.
