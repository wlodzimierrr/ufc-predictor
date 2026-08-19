# UFC Data Commands

Most-used commands for scraping, loading, predicting, and reviewing events.

## Setup

Run commands from the repo root unless noted:

```bash
cd ~/ufc-data
```

If UFCStats blocks requests, export your browser headers first:

```bash
export UFCSTATS_COOKIE_HEADER='paste_cookie_header_here'
export UFCSTATS_USER_AGENT='Mozilla/5.0'
```

## Scrape Data

Full fresh scrape:

```bash
make refresh_scrape
```

Target one completed event:

```bash
cd scraper/UFC-Web-Scraping-main
make update_fights ARGS="-a event_url=http://www.ufcstats.com/event-details/<event_id>"
make build_stats_queue
make update_fight_stats
make update_fight_stats_by_round
cd ~/ufc-data
```

Update fighter profiles after new fights are discovered:

```bash
cd scraper/UFC-Web-Scraping-main
make build_queue
make update_fighters
cd ~/ufc-data
```

## Load Warehouse

Load everything:

```bash
make load_all
```

Load individual tables:

```bash
make load_events
make load_fighters
make load_fights
make load_stats
make load_upcoming
```

Apply database migrations:

```bash
make migrate
```

## Build Features And Predict

Build upcoming features:

```bash
make build_upcoming_features
```

Run predictions and save them to CSV/database:

```bash
make predict
```

Run the usual upcoming prediction pipeline:

```bash
make predict_pipeline
```

Catch up one past unresolved event before results are loaded:

```bash
python3 features/build_upcoming.py --from-date YYYY-MM-DD --to-date YYYY-MM-DD --include-past
python3 predict.py --event "Event name"
python3 features/build_upcoming.py
```

## Review Results

Review one completed event:

```bash
make review_event EVENT="Event name"
```

Regenerate dashboard/report CSVs:

```bash
make pre_event_log
```

## Betting Workflows

Methodology, formulas, report schemas, and limitations are documented in
`docs/betting.md`.

Import normalized moneyline odds from `data/odds/fight_odds.csv`:

```bash
make load_odds
```

Convert the raw Kaggle odds download into the canonical odds CSV:

```bash
make adapt_kaggle_odds
```

Pass loader options with `ARGS`:

```bash
make load_odds ARGS="--csv data/odds/fight_odds.csv"
make adapt_kaggle_odds ARGS="--raw-csv data/odds/raw/UFC_betting_odds.csv"
```

The Kaggle adapter writes matched odds to `data/odds/fight_odds.csv`, source
normalized odds to `data/odds/sources/kaggle_fight_odds.csv`, unmatched rows to
`data/odds/unmatched_odds.csv`, and matching QA counts/details to
`data/reports/odds_matching_qa.csv`.

Generate current-card betting recommendations after predictions already exist:

```bash
make predict_pipeline
make betting_recommendations
```

Common recommendation filters:

```bash
make betting_recommendations ARGS="--next --bookmaker TestBook --line-type current --bankroll 1000"
make betting_recommendations ARGS="--event \"Event name\" --line-type closing"
```

The recommendation command reads existing `current_event_predictions` and odds; it does not run model scoring itself. Outputs are written to:

```text
data/reports/betting_recommendations.csv
data/reports/betting_event_summary.csv
```

Run betting tests:

```bash
make test_betting
```

Run a leakage-safe historical betting backtest:

```bash
make betting_backtest ARGS="--odds-policy latest-before-prediction --initial-bankroll 1000"
```

Common backtest filters and policy overrides:

```bash
make betting_backtest ARGS="--start-date YYYY-MM-DD --end-date YYYY-MM-DD --bookmaker TestBook --line-type current"
make betting_backtest ARGS="--odds-policy latest-before-event --initial-bankroll 1000 --kelly-fraction 0.25"
make betting_backtest ARGS="--odds-policy closing --initial-bankroll 1000"
```

## Common Workflows

After an event finishes:

```bash
make refresh_scrape
make load_all
make review_event EVENT="Event name"
make pre_event_log
make build_upcoming_features
make predict
```

Targeted post-event refresh:

```bash
cd scraper/UFC-Web-Scraping-main
make update_fights ARGS="-a event_url=http://www.ufcstats.com/event-details/<event_id>"
make build_stats_queue
make update_fight_stats
make update_fight_stats_by_round
cd ~/ufc-data
make load_all
make review_event EVENT="Event name"
make pre_event_log
```

Check warehouse health:

```bash
make warehouse_check
```
