# Betting Value and Risk Management

Phase 8 adds betting-value analysis beside the existing UFC prediction workflow. The production model still answers "who is more likely to win?" Betting reports answer "is the market price worth taking, and how much risk is allowed?"

Betting data and reports must stay separate from model training/scoring outputs. Positive model edge is not guaranteed profit, and historical betting profit/loss is only meaningful when both predictions and odds existed before the event.

Recommendations are analytical outputs for review and research. They are not
automated betting instructions, do not place bets, and should not be treated as
financial advice or a guarantee of profit.

---

## Methodology

Prediction probability and betting profitability answer different questions.
`model_probability` estimates a fighter's chance to win. Profitability also
depends on the offered price, the market baseline, and risk limits. A fighter
can be more likely to win but still be a bad bet if the odds are too short.

Example:

```text
Model says Fighter A wins 60% of the time.
Sportsbook offers decimal odds 1.50.
EV per unit = 0.60 * (1.50 - 1) - (1 - 0.60)
EV per unit = 0.30 - 0.40 = -0.10
```

That is a negative-value price even though the fighter is favored by the model.

### Odds And Implied Probability

American odds and decimal odds are two ways to express the same price.

```text
+150 American odds -> 2.50 decimal odds
-200 American odds -> 1.50 decimal odds
```

Decimal odds convert to raw implied probability:

```text
implied_probability = 1 / decimal_odds
2.50 decimal odds -> 1 / 2.50 = 0.40
```

Sportsbooks usually price both sides with margin included. The two raw implied
probabilities often sum to more than `1.0`; that excess is the overround. The
no-vig probability removes that margin by normalizing the two sides.

Example:

```text
Fighter A raw implied probability = 0.55
Fighter B raw implied probability = 0.50
Overround = 1.05

Fighter A no-vig probability = 0.55 / 1.05 = 0.5238
Fighter B no-vig probability = 0.50 / 1.05 = 0.4762
```

The no-vig probabilities sum to `1.0` and are used as the market baseline for
edge.

### Edge And EV

Edge compares the calibrated model probability to the no-vig market
probability:

```text
edge = model_probability - no_vig_market_probability
```

Expected value uses the actual offered decimal odds:

```text
net_decimal = decimal_odds - 1
ev_per_unit = model_probability * net_decimal - (1 - model_probability)
```

Example:

```text
Model probability = 0.57
No-vig market probability = 0.52
Decimal odds = 2.05

edge = 0.57 - 0.52 = 0.05
ev_per_unit = 0.57 * 1.05 - 0.43 = 0.1685
```

With default thresholds, a bet candidate must have both `edge >= 0.03` and
`ev_per_unit >= 0.01`.

### Kelly Sizing And Caps

Kelly sizing estimates a bankroll fraction from model probability and offered
odds. Full Kelly can be volatile, especially when model probabilities or market
prices are noisy, so this subsystem defaults to fractional Kelly and then caps
the result.

Default risk settings are conservative:

| Setting | Default |
|---|---:|
| `kelly_fraction` | `0.25` |
| `max_single_bet_fraction` | `0.02` |
| `max_event_fraction` | `0.06` |
| `medium_tier_cap` | `0.01` |
| `high_tier_cap` | `0.02` |
| `toss_up_tier_cap` | `0.00` |

Example:

```text
Full Kelly fraction = 0.08
Fractional Kelly = 0.08 * 0.25 = 0.02
High tier cap = 0.02
Final stake fraction before event cap = 0.02
```

For a medium-tier candidate, the same fractional Kelly value would be capped at
`0.01`. A toss-up candidate is capped at `0.00` and passes even when edge and EV
look positive.

### Confidence-Tier Rules

The confidence tier is a risk gate, not a claim of certainty.

| Tier | Behavior |
|---|---|
| `toss-up` | Always pass; default cap is zero |
| `medium` | Can bet only when odds are valid and edge/EV/Kelly rules pass; capped below high tier |
| `high` | Can bet only when odds are valid and edge/EV/Kelly rules pass; capped by high tier and single-bet/event exposure |

The event cap is cumulative. Qualifying bets are allocated deterministically by
highest EV, then highest edge, then timestamp/fight identifiers. Once event
exposure is exhausted, later qualifying rows are reduced to zero or passed with
`event_exposure_cap_reached`.

---

## V1 Policy Defaults

These defaults resolve the Phase 8 V1 policy questions. They are intentionally
conservative and can be overridden by CLI flags or config files where supported.

| Policy question | V1 decision |
|---|---|
| Historical odds source | `data/odds/fight_odds.csv` is the canonical V1 source. Rows must include trustworthy `odds_timestamp` values. |
| Current recommendation bookmaker handling | Evaluate all imported bookmakers independently by default. Use `--bookmaker` to review one preferred source. |
| Backtest odds policy | Default to `latest-before-prediction`, which requires odds before both the event date and the saved prediction timestamp. |
| Bankroll unit | Current recommendations report stake fractions unless `--bankroll` is supplied. Backtests use a paper starting bankroll of `1000` unless `--initial-bankroll` is supplied. |
| Default confidence caps | Keep medium/high caps at `0.01`/`0.02`, toss-up at `0.00`, single-bet cap at `0.02`, and event cap at `0.06`. |

For a first live paper-trial, use tighter caps without changing the committed
defaults:

```bash
python3 betting/recommend.py --bankroll 1000 --medium-tier-cap 0.005 --high-tier-cap 0.01 --max-single-bet-fraction 0.01 --max-event-fraction 0.03
```

The live-trial caps are a review posture, not a new default. They make early
recommendation output easier to inspect while the odds import source and report
workflow are still being validated.

---

## Odds Input CSV

Canonical V1 input path:

```text
data/odds/fight_odds.csv
```

Odds artifact layout:

| Path | Owner | Purpose |
|---|---|---|
| `data/odds/raw/` | Manual input | Raw external downloads such as Kaggle exports. These are never loaded directly. |
| `data/odds/sources/` | Generated | Source-specific normalized outputs before final review/merge. |
| `data/odds/fight_odds.csv` | Generated or reviewed manual output | Canonical V1 input for the warehouse loader. |
| `data/odds/unmatched_odds.csv` | Generated | Rows that could not be safely mapped to warehouse fight/fighter IDs. |

Raw odds files are staging inputs only. They must be converted into the canonical
`fight_odds.csv` contract and pass loader validation before they are considered
usable betting odds.

Kaggle adapter:

```bash
python3 warehouse/adapt_kaggle_odds.py
```

The Kaggle adapter reads `data/odds/raw/UFC_betting_odds.csv`, matches UFCStats
fight/fighter URLs to local warehouse IDs, writes matched moneyline rows to
`data/odds/sources/kaggle_fight_odds.csv` and `data/odds/fight_odds.csv`, and
writes uncertain rows to `data/odds/unmatched_odds.csv`.

It also writes `data/reports/odds_matching_qa.csv`, a deterministic
one-row-per-source-row QA report. The report labels rows as `matched`,
`unmatched`, `duplicate`, `ambiguous`, or `rejected`, includes source
fighter/event names, candidate warehouse IDs/names, match and rejection reasons,
and summary counts for manual alias review.

Some Kaggle rows contain historical fight dates but odds collection timestamps
from much later. These rows can still be stored for audit, but leakage-safe
backtests will not use them because `odds_timestamp` is after `event_date`.

### Future Odds Scrapers

BestFightOdds, or another reputable public sportsbook-odds archive with MMA
coverage, may be useful later for deeper historical moneyline coverage. V1 does
not run or schedule any odds website scraper; it stays focused on reviewed
normalized CSV imports.

Any future scraped source must keep the same boundary as the Kaggle adapter:

| Artifact | Requirement |
|---|---|
| Raw snapshots | Store untouched fetches under `data/odds/raw/<source>/` for auditability. |
| Source output | Normalize parsed rows to `data/odds/sources/<source>_fight_odds.csv`. |
| Canonical loader input | Only reviewed rows may be merged into `data/odds/fight_odds.csv`. |
| Unmatched rows | Write ambiguous or unmatched rows to a review CSV instead of guessing. |
| Attribution | Preserve source URL, source name, bookmaker, observed timestamp, and import timestamp. |

Before scraping any odds website, review its terms of use, robots policy, and any
available official API/export option. A future scraper must use conservative rate
limits, backoff, request timeouts, resumable fetch manifests, and raw snapshot
storage. Continuous or scheduled scraping needs its own approved ticket.

Odds-source data remains separate from production model training and scoring
unless a future ticket explicitly approves market-derived features. It may feed
recommendation reports, leakage-safe betting backtests, and clearly labeled
research-only analyses.

### Odds Source Decision Matrix

Research snapshot date: 2026-08-19.

| Source | Type | MMA/UFC fit | Historical fit | Implementation / terms risk | Current recommendation |
|---|---|---|---|---|---|
| BestFightOdds | Website archive | Strong MMA-specific archive | Strong depth signal: archive advertises thousands of matchups dating back to 2007 | Medium/high: website extraction, terms/permission, brittle parsing, unclear timestamps | Best first scraping candidate. Start with manual feasibility and a tiny raw snapshot probe. |
| OddsPortal | Website archive | Broad sports archive, not MMA-specific-first | Broad historical odds archive | High: terms restrict automated requests, scraping, aggregation, database extraction, and non-personal/commercial use without consent | Do not scrape without explicit permission or licensed data path. |
| The Odds API | Paid documented JSON API | Strong: supports `mma_mixed_martial_arts`, h2h/moneyline, multiple bookmaker regions | Promising but paid historical access is required | Low technical risk, but out of scope because paid APIs are not being used | Do not pursue under current no-paid-API constraint. |
| BALLDONTLIE | API | Useful for recent/current MMA odds with existing key | Limited for our historical backfill based on current sample size | Low implementation risk, but API route is not the next path | Keep existing importer only; do not make it the historical plan. |
| SportsDataIO | Commercial API/feed | Potentially strong with paid MMA access | Potentially useful with paid/commercial access, but free/trial data is scrambled | Medium: sales/plan access and scrambled trial values make validation harder | Do not pursue under current no-paid-API constraint. |

The current decision is scraping-only because paid API routes are out of scope.
BestFightOdds is the first candidate to investigate because it is MMA-specific
and has a deep archive signal. The first step is manual feasibility plus a tiny
respectful raw snapshot probe, not a broad crawl. OddsPortal should be avoided
unless explicit permission or a licensed path is obtained.

Loader:

```bash
python3 warehouse/load_fight_odds.py
python3 warehouse/load_fight_odds.py --csv data/odds/fight_odds.csv
```

The loader validates IDs against the warehouse, validates enum values, confirms the fighter/opponent pair matches the fight, normalizes odds for audit checks, and upserts raw observations into `fight_odds`.

### Required Columns

| Column | Required value | Description |
|---|---|---|
| `fight_id` | yes | UUID from `fights.fight_id` |
| `event_id` | yes | UUID from `events.event_id`; must match the fight's event |
| `event_date` | yes | Event date in `YYYY-MM-DD` format |
| `fighter_id` | yes | UUID from `fighters.fighter_id` for the priced fighter |
| `fighter_name` | yes | Display name at import time, used for review/debugging |
| `opponent_fighter_id` | yes | UUID for the opposite fighter in the same fight |
| `bookmaker` | yes | Sportsbook or odds source name |
| `market` | yes | V1 supports `moneyline` only |
| `line_type` | yes | `opening`, `current`, `closing`, or `unknown` |
| `odds_timestamp` | yes | Timestamp when the price was observed |
| `american_odds` | conditionally | American odds; may be blank if `decimal_odds` is supplied |
| `decimal_odds` | conditionally | Decimal odds; may be blank if `american_odds` is supplied |
| `source` | yes | Manual/import/API source label |
| `source_url` | no | Optional URL or source reference |
| `imported_at` | yes | Timestamp when the row entered this project |

At least one of `american_odds` or `decimal_odds` must be supplied. American odds cannot be `0`; decimal odds must be greater than `1.0`.

### Example

```csv
fight_id,event_id,event_date,fighter_id,fighter_name,opponent_fighter_id,bookmaker,market,line_type,odds_timestamp,american_odds,decimal_odds,source,source_url,imported_at
22222222-2222-2222-2222-222222222222,11111111-1111-1111-1111-111111111111,2026-08-01,33333333-3333-3333-3333-333333333333,Fighter One,44444444-4444-4444-4444-444444444444,ExampleBook,moneyline,current,2026-07-31T12:00:00+00:00,+150,,manual,,2026-07-31T12:05:00+00:00
22222222-2222-2222-2222-222222222222,11111111-1111-1111-1111-111111111111,2026-08-01,44444444-4444-4444-4444-444444444444,Fighter Two,33333333-3333-3333-3333-333333333333,ExampleBook,moneyline,current,2026-07-31T12:00:00+00:00,-170,,manual,,2026-07-31T12:05:00+00:00
```

### Leakage Rule

`odds_timestamp` is required for leakage-safe betting backtests. Backtests must only use odds observed before the event or before the configured betting decision cutoff. Closing lines, current lines, or manually entered lines with unknown observation timing must not be treated as historical pre-event betting opportunities unless their timestamps prove they were available at the time.

Rows with missing, stale, ambiguous, duplicated, or mismatched odds should produce pass decisions rather than guessed bets.

---

## Warehouse Objects

### `fight_odds`

Raw odds observations keyed by:

```text
(fight_id, fighter_id, bookmaker, market, line_type, odds_timestamp)
```

Stored columns:

- `fight_id`
- `event_id`
- `fighter_id`
- `opponent_fighter_id`
- `bookmaker`
- `market`
- `line_type`
- `odds_timestamp`
- `american_odds`
- `decimal_odds`
- `source`
- `source_url`
- `imported_at`

### `fight_odds_normalized`

View that derives:

- `normalized_decimal_odds`
- `implied_probability`

These values are calculated from raw odds and are not hand-entered.

### `latest_fight_odds`

View with the latest `current` line per fight, fighter, bookmaker, and market.

### `fight_odds_no_vig`

View that emits no-vig probabilities only when a market group has exactly two reciprocal fighter sides for the same fight, bookmaker, market, line type, and odds timestamp.

Derived columns:

- `overround`
- `no_vig_implied_probability`

---

## Formulas

American to decimal:

```text
positive American: decimal_odds = 1 + american_odds / 100
negative American: decimal_odds = 1 + 100 / abs(american_odds)
```

Raw implied probability:

```text
implied_probability = 1 / decimal_odds
```

Two-sided no-vig probability:

```text
overround = implied_probability_f1 + implied_probability_f2
no_vig_probability_f1 = implied_probability_f1 / overround
no_vig_probability_f2 = implied_probability_f2 / overround
```

Expected value per unit:

```text
net_decimal = decimal_odds - 1
ev_per_unit = model_probability * net_decimal - (1 - model_probability)
```

Model edge:

```text
edge = model_probability - no_vig_market_probability
```

---

## Current Recommendation Reports

Current recommendation reports:

- `data/reports/betting_recommendations.csv`
- `data/reports/betting_event_summary.csv`

Historical backtest reports:

- `data/reports/betting_backtest_fights.csv`
- `data/reports/betting_backtest_events.csv`
- `data/reports/betting_backtest_summary.csv`

Positive model edge is not guaranteed profit. It means the model probability is
higher than the two-sided no-vig market probability by at least the configured
threshold. A recommended bet still loses whenever the fighter loses, and repeated
positive-edge recommendations can have drawdowns.

### `betting_recommendations.csv`

One row per evaluated fighter side or pass issue.

| Column | Description |
|---|---|
| `event_id` | Event identifier |
| `event_name` | Event display name |
| `event_date` | Event date |
| `fight_id` | Fight identifier |
| `fighter_id` | Evaluated fighter identifier |
| `fighter_name` | Evaluated fighter display name |
| `opponent_fighter_id` | Opposing fighter identifier |
| `opponent_fighter_name` | Opposing fighter display name |
| `bookmaker` | Sportsbook or odds source |
| `market` | V1 value: `moneyline` |
| `line_type` | Odds line type: `current`, `opening`, or `closing` for recommendation CLI filters |
| `odds_timestamp` | Odds observation timestamp |
| `model_probability` | Calibrated model probability for the evaluated fighter |
| `market_implied_probability` | Raw implied probability from offered decimal odds |
| `no_vig_market_probability` | Two-sided no-vig market probability |
| `edge` | `model_probability - no_vig_market_probability` |
| `ev_per_unit` | Expected value per unit staked |
| `ev_percent` | Same value as `ev_per_unit`; format as a percentage only in presentation layers |
| `full_kelly_fraction` | Full Kelly stake fraction before caps |
| `fractional_kelly_fraction` | Kelly stake after configured Kelly multiplier |
| `final_stake_fraction` | Final bankroll fraction after tier, single-bet, event, and optional drawdown caps |
| `stake_amount` | Currency amount when bankroll is supplied |
| `decision` | `bet` or `pass` |
| `recommended_fighter_id` | Fighter identifier to bet, blank for pass rows |
| `recommended_fighter_name` | Fighter display name to bet, blank for pass rows |
| `confidence_tier` | Prediction confidence tier used by the conservative policy |
| `drawdown_protection_enabled` | `true` when drawdown protection threshold is configured |
| `drawdown_protection_fired` | `true` when the provided current drawdown triggered a pass |
| `current_drawdown` | Optional caller-provided current drawdown fraction |
| `reason_codes` | Pipe-delimited audit reasons |

### `betting_event_summary.csv`

One row per event represented in the recommendation output.

| Column | Description |
|---|---|
| `event_id` | Event identifier |
| `event_name` | Event display name |
| `event_date` | Event date |
| `bets_recommended` | Count of final `bet` rows for the event |
| `total_stake_fraction` | Sum of final stake fractions for recommended bets |
| `total_stake_amount` | Sum of currency stake amounts when bankroll is supplied |
| `event_exposure_fraction` | Final event exposure fraction after caps |
| `pass_count` | Count of final `pass` rows for the event |
| `top_pass_reasons` | Most common pass reason codes with counts |
| `drawdown_protection_enabled` | `true` if any event row had drawdown protection enabled |
| `drawdown_protection_fired` | `true` if drawdown protection fired on any event row |

### Decision Semantics

`decision = bet` means the evaluated fighter side passed the conservative policy:
odds were valid and fresh, the model edge met `min_edge`, expected value met
`min_ev`, the confidence tier was not toss-up, Kelly sizing was positive, and
staking caps left a positive final stake.

`decision = pass` means no bet should be placed for that row. Pass rows keep
machine-readable `reason_codes` so reports can be audited without reading free
text.

Stale odds always pass. Current recommendation joins compare `odds_timestamp`
to the command's `--as-of` timestamp, or to runtime now when `--as-of` is not
provided. If the odds are older than `max_odds_age_hours_current`, the row gets
`stale_odds` and is excluded from bet eligibility.

Ambiguous odds always pass. Ambiguity includes incomplete two-sided markets,
duplicate fighter sides, odds fighter IDs that do not match the prediction's
fight participants, or groups that cannot be matched to exactly one reciprocal
moneyline market. The system does not guess the missing side or infer a price.

### Historical Backtest Dataset Policy

Historical betting datasets use `pre_event_prediction_fights` as the prediction
source. That view keeps the latest saved prediction where `scored_at::date <
event_date`; the Python backtest dataset builder also excludes any fixture row
that violates that rule.

Historical odds are eligible only when `odds_timestamp < event_date`. By default,
they must also satisfy `odds_timestamp <= scored_at`, so a backtest cannot use a
price that appeared after the saved prediction timestamp. The line policy is
configurable:

| Policy | Behavior |
|---|---|
| `latest_current` | Default; latest eligible `current` line per fight/bookmaker/market |
| `closing` | Latest eligible `closing` line per fight/bookmaker/market |
| `opening` | Earliest eligible `opening` line per fight/bookmaker/market |

Rows without an eligible two-sided no-vig market become `pass` rows with
`missing_odds` or `ambiguous_odds`.

Draws and no contests become non-bet rows with `non_win_outcome`. Unresolved
fights become non-bet rows with `unresolved_outcome`. Fighter replacements or
resolved winners outside the predicted fighter pair become non-bet rows with
`fighter_replacement`.

Historical backtests process events in ascending `event_date`. All bets on one
event are sized from the bankroll available before that event, then the whole
event settles before the next event is sized. Same-card wins therefore do not
increase stake capacity for later fights on that same card.

Settlement uses explicit moneyline P/L formulas:

```text
winning profit = stake_amount * (decimal_odds - 1)
losing profit = -stake_amount
```

Because V1 excludes non-W/L rows from betting eligibility, draw/no-contest and
unresolved rows normally appear as `no_bet`. If a non-W/L row ever reaches
settlement as a bet, it is treated explicitly as `push` with zero P/L.

After each event settles, the simulator records bankroll after the event, peak
bankroll, current drawdown, and max drawdown.

### Historical Backtest Reports

Backtest report generation writes three CSVs under `data/reports/` by default:

| Report | Contents |
|---|---|
| `betting_backtest_fights.csv` | Fight-level rows, either final recommendation decisions or raw evaluated dataset sides depending on detail mode |
| `betting_backtest_events.csv` | Event-level settlement rows with bets, staked amount, P/L, ROI, ending bankroll, and drawdown after event |
| `betting_backtest_summary.csv` | Overall totals plus ROI by confidence tier, edge bucket, and event |

CLI:

```bash
python3 betting/backtest.py --odds-policy latest-before-prediction --initial-bankroll 1000
python3 betting/backtest.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD --bookmaker ExampleBook --line-type current
python3 betting/backtest.py --odds-policy closing --initial-bankroll 1000 --kelly-fraction 0.25
```

Supported filters and overrides include `--start-date`, `--end-date`,
`--bookmaker`, `--line-type current|opening|closing`,
`--odds-policy latest-before-event|latest-before-prediction|opening|closing`,
`--initial-bankroll`, `--kelly-fraction`, tier caps, single-bet/event caps,
`--detail-mode decisions|evaluated`, `--config`, and `--report-dir`.

The default CLI policy is conservative: `latest-before-prediction` uses current
lines only when their `odds_timestamp` is before both the event date and the
saved prediction timestamp. `latest-before-event` still requires odds before the
event, but does not require odds before the prediction timestamp.

The summary report includes total bets, total staked, profit/loss, ROI, hit
rate, average odds, max drawdown, ROI by confidence tier, ROI by edge bucket,
and ROI by event.

Default edge buckets are:

| Bucket | Edge range |
|---|---|
| `0-3%` | `0 <= edge < 0.03` |
| `3-5%` | `0.03 <= edge < 0.05` |
| `5-10%` | `0.05 <= edge < 0.10` |
| `10%+` | `edge >= 0.10` |

Backtest fight, event, and summary reports label the odds policy, whether odds
were required before the prediction timestamp, starting/ending bankroll, and the
risk configuration used for staking.

Example `reason_codes`:

```text
positive_edge|positive_ev|fractional_kelly
edge_below_threshold|ev_below_threshold
missing_odds
stale_odds
ambiguous_odds
non_win_outcome
unresolved_outcome
fighter_replacement
```

Backtest reports also include result and bankroll fields such as
`actual_winner_fighter_id`, `bet_result`, `profit_loss_amount`,
`bankroll_before_event`, and `bankroll_after_event`.

---

## Known Limitations

V1 supports two-sided UFC moneyline markets only. Props, method-of-victory
markets, round totals, parlays, and live odds are out of scope.

Odds quality depends on the imported source data. The system requires explicit
timestamps for historical analysis, but it cannot prove that an external price
was actually available to a specific bettor at that exact moment.

The model probability is calibrated from historical data and can still be wrong
for matchup-specific reasons, late injuries, fighter replacements, weigh-in
news, market movement, or data errors. Betting reports should be reviewed
manually before any real-world action.

Backtest profitability can be sensitive to line policy, bookmaker selection,
stale odds thresholds, bankroll assumptions, and cap settings. Treat backtest
results as analytical diagnostics, not as evidence of guaranteed future returns.
