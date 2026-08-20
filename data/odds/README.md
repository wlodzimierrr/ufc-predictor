# Odds Data Artifacts

This folder separates raw external odds inputs from normalized, validated odds
artifacts used by the betting loader.

## Paths

| Path | Owner | Purpose |
|---|---|---|
| `raw/` | Manual input | Raw downloads from external sources such as Kaggle. These files are not loaded directly. |
| `sources/` | Generated | Source-specific normalized files, for example `kaggle_fight_odds.csv`. |
| `fight_odds.csv` | Generated or reviewed manual output | Canonical V1 loader input for `warehouse/load_fight_odds.py`. |
| `unmatched_odds.csv` | Generated | Review file for source odds rows that could not be mapped safely to warehouse IDs. |
| `../reports/odds_matching_qa.csv` | Generated | One-row-per-source-row QA report with match status, candidate IDs/names, and summary counts. |

Raw source files must be converted into the canonical contract before loading.
The loader only treats `fight_odds.csv` as validated input after it has passed
warehouse ID, enum, odds, and two-fighter checks.

## Current Raw Sources

| Source | Raw file | Metadata |
|---|---|---|
| Kaggle UFC Betting Odds Daily Dataset | `raw/UFC_betting_odds.csv` | `raw/UFC_betting_odds.metadata.md` |
| BestFightOdds reviewed snapshots | `raw/bestfightodds/*.html` | Adjacent `*.metadata.json`; adapted offline to `sources/bestfightodds_fight_odds.csv` only after review. |
| BestFightOdds line-history payloads | `raw/bestfightodds/payloads/*` | Adjacent `*.metadata.json`; decoded offline to `sources/bestfightodds_line_history_fight_odds.csv` for source-specific review. |

## Adapters

Convert the Kaggle raw file into the canonical loader contract:

```bash
python3 warehouse/adapt_kaggle_odds.py
```

The adapter writes:

- `sources/kaggle_fight_odds.csv`
- `fight_odds.csv`
- `unmatched_odds.csv`
- `../reports/odds_matching_qa.csv`

Kaggle rows are matched by UFCStats fight and fighter URLs. Rows with missing
moneyline odds, missing source/timestamp values, unknown URLs, ambiguous URLs, or
fighter URLs that do not match the warehouse fight are written to
`unmatched_odds.csv` instead of being guessed.

The QA report is regenerated from the same source rows on each adapter run. It
classifies each source row as `matched`, `unmatched`, `duplicate`, `ambiguous`,
or `rejected`, includes source fighter/event fields and candidate warehouse
IDs/names, and repeats the status counts for quick spreadsheet review.

BestFightOdds line-history adapter:

```bash
python3 warehouse/adapt_bestfightodds_line_history_payloads.py
```

The adapter reads stored BFO fighter-history snapshots plus stored `/api/ggd`
payloads only. It decodes timestamped `BestFightOdds Mean` moneyline points and
writes source-specific review outputs:

- `sources/bestfightodds_line_history_fight_odds.csv`
- `sources/bestfightodds_line_history_unmatched_odds.csv`

These rows are not merged into `fight_odds.csv` automatically.

Reviewed source rows can be promoted into the canonical loader input with:

```bash
python3 warehouse/promote_odds_source_to_canonical.py --source-csv data/odds/sources/bestfightodds_line_history_fight_odds.csv
```

The promotion helper validates source rows against local warehouse identity CSVs,
strips source-specific audit columns, skips duplicate stable odds keys, and
writes only the canonical `fight_odds.csv` columns. The current promoted
BestFightOdds rows are `BestFightOdds Mean` market-benchmark rows, not
named-bookmaker executable prices. Current canonical BFO Mean coverage is 5682
rows: 5550 completed-fight rows for UFC 330 and 132 upcoming rows for
Hernandez/Rodrigues.

Bounded multi-target BFO captures must go through an explicit manifest:

```bash
python3 warehouse/capture_bestfightodds_manifest.py \
  --manifest data/odds/raw/bestfightodds/manifests/bfo_mean_targets.seed.csv \
  --output data/odds/raw/bestfightodds/manifests/bfo_mean_capture_results.dry_run.csv \
  --dry-run \
  --request-cap 6
```

Live capture should use the same manifest after review and approval. The
manifest runner does not discover events, crawl the site, schedule refreshes, or
load canonical rows.

Current/upcoming market-benchmark comparisons can be generated with:

```bash
python3 betting/bfo_mean_upcoming_snapshot.py --as-of 2026-08-19T21:40:00+00:00
```

The output is
`../reports/bfo_mean_market_benchmark/bfo_mean_upcoming_market_snapshot.csv`.
It is a `BestFightOdds Mean` market benchmark only, not sportsbook-executable
odds or P/L. The report uses `risk.max_odds_age_hours_current` to flag stale
benchmark rows and preserves placeholder rows with reason codes when BFO rows
are missing, future-only, one-sided, or otherwise unusable.

BFO pipeline runbook:

1. Build a completed-fight review manifest:

```bash
python3 warehouse/build_bestfightodds_historical_manifest.py \
  --max-fights 10 \
  --as-of-date 2026-08-20
```

2. Manually review BFO event/fighter pages and create an explicit capture
   manifest with exact event/fight IDs and `/api/ggd?m=...&p=...` payload URLs.
   Do not guess matchup IDs.

3. Dry-run the explicit manifest:

```bash
python3 warehouse/capture_bestfightodds_manifest.py \
  --manifest data/odds/raw/bestfightodds/manifests/bfo_mean_historical_payload_targets.csv \
  --output data/odds/raw/bestfightodds/manifests/bfo_mean_historical_payload_results.dry_run.csv \
  --dry-run \
  --request-cap 20
```

4. After review/approval, run the same bounded manifest live with the same cap:

```bash
python3 warehouse/capture_bestfightodds_manifest.py \
  --manifest data/odds/raw/bestfightodds/manifests/bfo_mean_historical_payload_targets.csv \
  --output data/odds/raw/bestfightodds/manifests/bfo_mean_historical_payload_results.live.csv \
  --request-cap 20
```

5. Decode stored payloads offline:

```bash
python3 warehouse/adapt_bestfightodds_line_history_payloads.py
```

6. Promote reviewed canonical-compatible source rows:

```bash
python3 warehouse/promote_odds_source_to_canonical.py \
  --source-csv data/odds/sources/bestfightodds_line_history_fight_odds.csv
```

7. Regenerate QA and benchmark reports:

```bash
python3 warehouse/report_bestfightodds_canonical_coverage.py
python3 betting/bfo_mean_market_backtest.py --end-date 2026-08-15
python3 betting/bfo_mean_upcoming_snapshot.py --as-of 2026-08-20T10:30:00+00:00
```

8. Generate current-card recommendations with BFO benchmark fields:

```bash
python3 betting/recommend.py --next --as-of 2026-08-20T10:30:00+00:00
```

Use `--no-bfo-benchmark` to suppress report-only BFO enrichment. Raw BFO
captures, source CSVs, canonical odds CSVs, and generated benchmark reports are
local/generated artifacts and are ignored by git. Source code, tests, docs, and
small fixture HTML are source-controlled.
