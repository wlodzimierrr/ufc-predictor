# UFC Betting Odds Daily Kaggle Source Metadata

## Source

| Field | Value |
|---|---|
| Dataset URL | https://www.kaggle.com/datasets/jerzyszocik/ufc-betting-odds-daily-dataset |
| Source file name | `UFC_betting_odds.csv` |
| Local raw path | `data/odds/raw/UFC_betting_odds.csv` |
| Download date | 2026-08-19 |
| Download method | Public Kaggle dataset download endpoint |
| License | CC0: Public Domain |
| Archive SHA-256 | `9f358f83a4e5fcfe7f01a2496c28610f36f085466253a2917e5270e418619eb2` |
| CSV SHA-256 | `9dc5ad30e485b2a146205d141455ec8167f5be916f9f3504e925beb9c370af81` |
| CSV rows including header | `199393` |

## Raw Columns

```text
fight_url
fighter_1_url
fighter_2_url
fighter_1
fighter_2
odds_1
odds_2
f1_ko_odds
f2_ko_odds
f1_sub_odds
f2_sub_odds
f1_dec_odds
f2_dec_odds
event_date
adding_date
source
region
```

## Adapter Field Check

| Needed by adapter | Raw source status |
|---|---|
| Event date/name | `event_date` exists. No explicit event name column. |
| Fight identity | `fight_url` exists and should map to `fights.url`. |
| Fighter names | `fighter_1` and `fighter_2` exist. |
| Fighter identity | `fighter_1_url` and `fighter_2_url` exist and should map to `fighters.url`. |
| Bookmaker/source | `source` exists, with some missing rows. |
| Moneyline/head-to-head odds | `odds_1` and `odds_2` exist as decimal odds, with some missing rows. |
| Odds collection timestamp | `adding_date` exists, with some missing rows. |
| Region | `region` exists, with some missing rows. |

## Known Gaps For Adapter Ticket

- `event_name` is not present. Prefer mapping by `fight_url` to warehouse
  `fights.url`; use `event_date` only as an audit field.
- `adding_date` is missing on 684 raw rows. Those rows should not be used for
  leakage-safe backtests unless a future source-specific rule supplies a
  trustworthy timestamp.
- `source` is missing on 684 raw rows. Those rows should be rejected or written
  to `data/odds/unmatched_odds.csv`.
- `region` is missing on 31149 raw rows. Region should be optional metadata, not
  a blocker for V1 moneyline imports.
- At least one side of `odds_1`/`odds_2` is missing on 238 raw rows. Those rows
  should be rejected or written to `data/odds/unmatched_odds.csv`.

Raw Kaggle rows must not be loaded directly into `fight_odds`. They must first
be converted into the canonical `data/odds/fight_odds.csv` contract and pass the
existing odds loader validation.
