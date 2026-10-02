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

### Refresh Current Odds For An Upcoming Event

Capture, adapt, and promote current per-bookmaker moneyline odds from the
BestFightOdds event page. Find the event identifier on
`https://www.bestfightodds.com/` first (for example `ufc-331-4302`):

```bash
make refresh_current_odds BFO_EVENT_ID=ufc-331-4302
make update_dashboard_data
```

The three steps it runs can also be run individually:

```bash
python3 warehouse/probe_bestfightodds_snapshot.py --event-id <bfo_event_id>
make adapt_bfo_live_odds
make promote_bfo_live_odds
```

The adapter reads the per-bookmaker moneyline grid on stored event snapshots and
writes `line_type=current` rows timestamped with the snapshot capture time to
`data/odds/sources/bestfightodds_live_fight_odds.csv`, with rows it could not
match to a local fight sent to
`data/odds/sources/bestfightodds_live_unmatched_odds.csv` for review. Prop and
total rows are skipped. Promotion appends to canonical `data/odds/fight_odds.csv`
and skips rows already present.

Fighter names that differ between BestFightOdds and UFCStats beyond spacing and
generational suffixes need a reviewed entry in `data/odds/bfo_name_aliases.csv`;
without one the rows stay in the unmatched review output rather than being
guessed.

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

Refresh the dashboard-backed prediction and betting tables after results or odds
change:

```bash
make update_dashboard_data
```

This reloads local event/fight/upcoming CSVs, reloads canonical odds, rebuilds
pre-event/current prediction reports, persists predictions to the database,
writes dashboard betting reports under `data/reports/betting_default/` and
`data/reports/betting_conservative_candidate/`, regenerates current betting
recommendations, and loads the dashboard betting tables in Postgres.

The `predict` step matters: `predict_pipeline` only writes prediction CSVs, while
`make predict` is what upserts rows into the `predictions` table that the
`current_event_predictions` view reads. `betting/recommend.py --next` picks the
earliest event present in that view, so if predictions were never persisted for
the upcoming card, `--next` silently produces recommendations for a later event
instead. Check that the event name printed by the recommendation step is the card
you expect.

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
