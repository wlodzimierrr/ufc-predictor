# Phase 1 Acquisition Runbook

Entry-point reference for running, updating, and validating Phase 1 data acquisition.
All commands are run from `scraper/UFC-Web-Scraping-main/` unless stated otherwise.

_Implements T1.1.3. See `docs/acquisition-contract.md` for the full output layout and schema._

---

## Setup (one time)

```bash
cd scraper/UFC-Web-Scraping-main
python3 -m venv .venv
source .venv/bin/activate
pip install -e ufc_scraper
```

Verify the install:

```bash
cd ufc_scraper && python -m scrapy list
# expected output:
# crawl_events
# crawl_fight_stats
# crawl_fight_stats_by_round
# crawl_fighters
# crawl_fights
```

---

## Output locations

| Output type | Path | Written by |
|---|---|---|
| Parsed CSV tables | `data/*.csv` | All spiders |
| Raw HTML pages | `data/raw/ufcstats/{entity_type}/{id}.html` | T1.2.2 (not yet implemented) |
| Fetch manifest | `data/manifests/fetch_manifest.csv` | T1.2.2 (not yet implemented) |
| Entity manifests | `data/manifests/events_manifest.csv` etc. | T1.3.2, T1.4.1, T1.5.1 (not yet implemented) |
| Coverage reports | `data/reports/*_YYYYMMDD.csv` | T1.3.4, T1.4.3, T1.5.4 (not yet implemented) |

`DATA_DIR` in the Makefile resolves to `../../data` (repo root `data/`).

---

## Commands

### Full crawl (all spiders, write CSV)

Runs all five spiders in series. Overwrites existing parsed CSV files.

```bash
make crawl_csv_all
```

### Full crawl (single spider)

```bash
make crawl_csv_events
make crawl_csv_fights
make crawl_csv_fighters
make crawl_csv_fight_stats
make crawl_csv_fight_stats_by_round
```

### Incremental update (append only new rows)

Reads existing `data/<spider>.csv`, skips IDs already present, appends only new rows.
Safe to re-run; will not duplicate records.

```bash
make update_all

# or single spider:
make update_events
make update_fights
make update_fighters
make update_fight_stats
make update_fight_stats_by_round
```

### Bounded sample run

Stops after fetching `N` pages (default `N=10`). Writes to `data/<spider>.csv` (overwrite).
Use this during development to validate parsing without a full history crawl.

```bash
make sample_events N=5
make sample_fights N=5
make sample_fighters N=5
make sample_fight_stats N=5
make sample_fight_stats_by_round N=5
```

### Smoke run

Fetches 5 events pages and exits. Confirms the crawler, parser, and CSV write path all work.

```bash
make smoke
```

Expected output: `data/events.csv` exists and contains at least one data row.

### Filtered run (pass extra Scrapy args)

The `ARGS` variable is forwarded to the Scrapy command for any target.

```bash
# Pass a custom Scrapy setting
make crawl_csv_events ARGS="-s LOG_LEVEL=DEBUG"
```

Spider-level argument filtering by event URL or event ID will be added in T1.3.x once canonical
manifests exist. At that point the pattern will be:

```bash
# Future — not yet implemented
make update_fights ARGS="-a event_url=http://ufcstats.com/event-details/<id>"
```

---

## Common failure modes

| Symptom | Likely cause | Action |
|---|---|---|
| `ModuleNotFoundError: scrapy` | Virtualenv not activated | `source .venv/bin/activate` |
| Empty CSV after crawl | Spider name mismatch or 0 items parsed | Run with `ARGS="-s LOG_LEVEL=DEBUG"` and check spider logs |
| Duplicate rows in CSV | `crawl_csv_%` used instead of `update_%` on an existing file | Use `update_%` for incremental runs; `crawl_csv_%` always overwrites |
| Crawl stops early unexpectedly | `CLOSESPIDER_PAGECOUNT` set (sample/smoke targets only) | Expected for `sample_%` and `smoke`; use `crawl_csv_%` for a full run |
| `FileNotFoundError` for `data/` | Working directory wrong | Run from `scraper/UFC-Web-Scraping-main/`; Makefile creates `data/` if missing |

---

## Quick-reference table

| Goal | Command |
|---|---|
| Full crawl, all spiders → CSV | `make crawl_csv_all` |
| Full crawl, one spider → CSV | `make crawl_csv_<spider>` |
| Incremental update, all spiders | `make update_all` |
| Incremental update, one spider | `make update_<spider>` |
| Bounded sample, one spider | `make sample_<spider> N=<pages>` |
| Smoke test (5 event pages) | `make smoke` |
