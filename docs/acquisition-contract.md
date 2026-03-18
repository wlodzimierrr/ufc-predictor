# Phase 1 Acquisition Contract

This document is the single authoritative contract for all Phase 1 acquisition job outputs.
Every spider, pipeline, and report must conform to the layout, naming, and schema defined here.

_Last updated: 2026-03-17. Implements T1.1.2._

---

## 1. Directory layout

```
data/
├── raw/
│   └── ufcstats/
│       ├── event_listing/      # full event-list page(s)
│       ├── events/             # one file per event detail page
│       ├── fighters/           # one file per fighter profile page
│       └── fights/             # one file per fight detail page
│                               # (fight stats are parsed from the same page)
├── manifests/
│   ├── fetch_manifest.csv      # one row per HTTP fetch attempt (the audit trail)
│   ├── events_manifest.csv     # canonical event registry (T1.3.2)
│   ├── fighter_queue.csv       # fighter profiles to capture (T1.4.1)
│   └── fight_stats_queue.csv   # fights eligible for stats capture (T1.5.1)
├── reports/
│   └── <report_name>_YYYYMMDD.csv
├── events.csv                  # parsed tabular output — unchanged
├── fights.csv
├── fighters.csv
├── fight_stats.csv
└── fight_stats_by_round.csv
```

`data/raw/` and `data/manifests/` are Phase 1 additions. Everything under `data/*.csv` is the
existing parsed output layer and is not affected by this contract.

---

## 2. Raw artifact naming rules

All raw artifacts are stored as plain `.html` files.
Paths are relative to the repo root.

| Entity type | Storage path |
|---|---|
| Event listing page | `data/raw/ufcstats/event_listing/event_listing_{YYYYMMDD}.html` |
| Event detail page | `data/raw/ufcstats/events/{event_id}.html` |
| Fighter profile page | `data/raw/ufcstats/fighters/{fighter_id}.html` |
| Fight detail page | `data/raw/ufcstats/fights/{fight_id}.html` |

Rules:
- IDs are the deterministic UUID5 strings already produced by `utils.get_uuid_string()`.
- Fight stats and round-level stats are parsed from the fight detail page; no separate raw file is written for them.
- If a page is re-fetched and the content hash is unchanged, the existing file is kept as-is and the manifest records status `unchanged`.
- If a page is re-fetched and the content hash changes, the existing file is overwritten and the manifest records status `updated`.

---

## 3. Fetch manifest schema (`data/manifests/fetch_manifest.csv`)

One row is written for every HTTP fetch attempt, whether successful or not.

| Column | Type | Description |
|---|---|---|
| `job_run_id` | `str` | UUID4 assigned once per Scrapy process invocation. All rows from the same `scrapy crawl` run share one value. |
| `entity_type` | `str` | One of: `event_listing`, `event`, `fighter`, `fight`. |
| `source_url` | `str` | The URL that was requested (after `format_href` normalisation). |
| `fetched_at` | `str` | ISO 8601 UTC timestamp of when the response was received, e.g. `2026-03-17T14:05:32Z`. |
| `http_status` | `int` | HTTP response status code. `0` for DNS/connection failures. |
| `content_hash` | `str` | SHA-256 hex digest of the raw response body. Empty string on failure. |
| `storage_path` | `str` | Repo-relative path where the raw file was written, e.g. `data/raw/ufcstats/events/{event_id}.html`. Empty string when the fetch failed or the file was not written. |
| `fetch_status` | `str` | One of: `fetched`, `unchanged`, `updated`, `failed`, `skipped`. |
| `error_message` | `str` | Error description for `failed` rows; empty otherwise. |

The manifest is append-only. A new batch of rows is appended at the end of every run.
Deduplication for idempotency uses `(source_url, content_hash)` — see T1.2.3.

`fetch_status` values:
- `fetched` — new page written for the first time.
- `unchanged` — page already captured with an identical content hash; raw file not overwritten.
- `updated` — content hash changed since last capture; raw file overwritten.
- `failed` — HTTP error or network failure; raw file not written.
- `skipped` — URL was in scope but intentionally bypassed (e.g. incremental mode, future fight).

---

## 4. Entity manifests (high-level)

These are stable queue/registry files read by downstream spiders. Their detailed schemas are
defined in the tickets that create them; this contract fixes their paths and key columns.

| File | Purpose | Key columns | Defined in |
|---|---|---|---|
| `data/manifests/events_manifest.csv` | Canonical registry of known events | `event_id`, `source_url`, `event_status`, `discovered_at` | T1.3.2 |
| `data/manifests/fighter_queue.csv` | Fighter profile pages to capture | `fighter_id`, `source_url`, `queue_status` | T1.4.1 |
| `data/manifests/fight_stats_queue.csv` | Fights eligible for stats capture | `fight_id`, `event_id`, `source_url`, `stats_status` | T1.5.1 |

All entity manifests are CSV, append-updated with deduplication on their primary key column.

---

## 5. Report outputs (`data/reports/`)

Reports are read-only artefacts generated after acquisition runs. They are not inputs to any
subsequent crawl.

| File pattern | Produced by | Purpose |
|---|---|---|
| `events_coverage_YYYYMMDD.csv` | T1.3.4 | Discovered vs fetched vs parsed event counts, parse failures |
| `fighters_coverage_YYYYMMDD.csv` | T1.4.3 | Fetched vs parsed, missing-field flags, identity review flags |
| `fight_stats_coverage_YYYYMMDD.csv` | T1.5.4 | Eligible fights vs aggregate stats vs round-level stats, failure reasons |

`YYYYMMDD` is the date of the run that produced the report. Reports are never overwritten; a new
file is written for each run date.

---

## 6. Format choices

| Layer | Format | Rationale |
|---|---|---|
| Raw artifacts | `.html` | Preserves source exactly; re-parseable without re-fetching. |
| Fetch manifest | CSV | Consistent with existing parsed outputs; no extra dependencies. |
| Entity manifests | CSV | Same rationale. |
| Reports | CSV | Human-readable, queryable with pandas. |

All CSV files use UTF-8 encoding with a header row. Fields containing commas or newlines are
quoted per RFC 4180.

---

## 7. What this contract does NOT change

- `data/events.csv`, `data/fights.csv`, `data/fighters.csv`, `data/fight_stats.csv`,
  `data/fight_stats_by_round.csv` — existing parsed outputs. Spiders continue writing to these.
  Their schemas are not modified by Phase 1.
- `scraper/data/` — intermediate outputs written during development runs. Not part of the
  canonical output layer.
- The Scrapy spider, parser, or entity code — none of that is redefined here.
