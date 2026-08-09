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
