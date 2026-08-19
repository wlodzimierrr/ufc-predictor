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
