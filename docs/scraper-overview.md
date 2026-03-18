# Scraper Overview

How the Phase 1 acquisition pipeline works and what data it extracts.

---

## Architecture

Built on **Scrapy** (Python async crawl framework). Five spiders run independently; a shared mixin and middleware layer handles deduplication and raw capture across all of them.

```
scraper/UFC-Web-Scraping-main/
├── ufc_scraper/ufc_scraper/
│   ├── spiders/          — 5 spiders
│   ├── parsers/          — HTML → structured data
│   ├── pipelines.py      — post-parse processing (events manifest)
│   ├── middlewares.py    — RawCaptureMiddleware
│   └── settings.py
├── build_fight_stats_queue.py
├── build_fighter_queue.py
├── event_coverage_report.py
├── fighter_review.py
├── stats_coverage_report.py
└── Makefile
```

---

## Spider flow — dependency order

```
crawl_events          ──► events.csv + events_manifest.csv
crawl_fights          ──► fights.csv
crawl_fighters        ──► fighters.csv
                                │
                          build_stats_queue  ──► fight_stats_queue.csv
                          build_queue        ──► fighter_queue.csv
                                │
crawl_fight_stats          ──► fight_stats.csv
crawl_fight_stats_by_round ──► fight_stats_by_round.csv
```

---

## What each spider extracts

### `crawl_events` → `data/events.csv`

Starts at `ufcstats.com/statistics/events/completed?page=all`, follows every event-details link.

| Field | Example |
|---|---|
| `event_id` | UUID5 of the event URL |
| `event` | "UFC 309: Jones vs. Miocic" |
| `date` | "November 16, 2024" |
| `date_formatted` | "2024-11-16" |
| `location` | "New York City, New York, USA" |
| `fights` | comma-separated fight UUIDs (card order) |
| `fight_urls` | comma-separated fight detail URLs (same order) |
| `event_status` | "completed" / "upcoming" |

Also writes `data/manifests/events_manifest.csv` (same key fields, upserted per run).

---

### `crawl_fights` → `data/fights.csv`

Follows fight-details links from each event page.

| Field | Example |
|---|---|
| `fight_id` | UUID5 of the fight URL |
| `event_id` | parent event UUID |
| `fighter_1_id`, `fighter_2_id` | fighter UUIDs |
| `winner` | fighter UUID or "draw" / "nc" |
| `finish_method` | "KO/TKO", "SUB", "U-DEC", "S-DEC", "M-DEC", "DQ", "NC" |
| `finish_round` | 1–5 |
| `finish_time` | "4:23" |
| `weight_class` | "Heavyweight", "Women's Strawweight", etc. |
| `is_title_fight` | bool |

---

### `crawl_fighters` → `data/fighters.csv`

Starts from A-Z listing pages, or from `fighter_queue.csv` if present.

| Field | Example |
|---|---|
| `fighter_id` | UUID5 |
| `first_name`, `last_name`, `full_name` | |
| `nickname` | "Bones" |
| `height_cm` | 193.04 |
| `weight_lbs` | 205.0 |
| `reach_in` | 84.5 |
| `stance` | "Orthodox" / "Southpaw" / "Switch" |
| `dob_formatted` | "1987-07-19" |
| `wins`, `losses`, `draws`, `no_contests` | career record |
| `url` | source URL |

> Nationality is not available on ufcstats.com and is not collected.

---

### `crawl_fight_stats` → `data/fight_stats.csv`

One row per fighter per fight (2 rows per fight). Seeded from `fight_stats_queue.csv`.

| Field | Example |
|---|---|
| `fight_id`, `fighter_id`, `event_id` | UUIDs |
| `knockdowns` | 0 |
| `sig_strikes_landed`, `sig_strikes_attempted` | 47, 89 |
| `sig_strike_pct` | 52% |
| `total_strikes_landed`, `total_strikes_attempted` | |
| `takedowns_landed`, `takedowns_attempted` | |
| `takedown_pct` | |
| `submission_attempts` | |
| `reversals` | |
| `control_time` | "3:22" |

Fights with no stats table (old bouts) log `NO_STATS_PAGE` and produce no rows.

---

### `crawl_fight_stats_by_round` → `data/fight_stats_by_round.csv`

Same fields as `fight_stats.csv` plus a `round` column (1–5). One row per fighter per round per fight. Seeded from the same `fight_stats_queue.csv`.

Fights with no round-level table log `NO_ROUND_TABLE` and produce no rows.

---

## Shared infrastructure

### `IncrementalCrawlMixin`

Each spider tracks already-seen IDs from its output CSV. On re-run it skips rows it already has. The `crawl_fight_stats_by_round` spider overrides the dedup method to ignore the shared fetch manifest — otherwise the aggregate spider's fetches would cause it to skip everything in incremental mode.

### `RawCaptureMiddleware`

Intercepts every HTTP response, SHA-256 hashes it, writes to `data/raw/ufcstats/{entity_type}/{id}.html`, and appends a row to `data/manifests/fetch_manifest.csv`.

| `fetch_status` | Meaning |
|---|---|
| `fetched` | New page, written for the first time |
| `unchanged` | Same content hash as last capture; file not overwritten |
| `updated` | Content changed; file overwritten |
| `failed` | HTTP error or network failure |

### Three-tier seed priority

`crawl_events`, `crawl_fighters`, `crawl_fight_stats`, and `crawl_fight_stats_by_round` all follow the same seed logic:

1. **Single-URL arg** (debug) — crawl that one page only
2. **Queue CSV** if present — seed from the canonical queue
3. **Listing page fallback** — original full-history discovery crawl

---

## Scale (Phase 1 completion run)

| Dataset | Rows |
|---|---|
| Events | ~750+ |
| Fights | 8 550 |
| Fight stats (aggregate) | ~17 000 (2 rows per fight) |
| Fight stats (round-level) | ~60 000+ |
| Fighters | 4 452 |
| Raw HTML files (fights) | ~8 531 |
