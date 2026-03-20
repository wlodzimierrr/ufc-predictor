# Phase 1 Acquisition Runbook

Entry-point reference for running, updating, and validating Phase 1 data acquisition.
All commands are run from `scraper/UFC-Web-Scraping-main/` unless stated otherwise.

_Implements T1.1.3–T1.6.2. See `docs/acquisition-contract.md` for the full output layout and schema._

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
| Raw HTML pages | `data/raw/ufcstats/{entity_type}/{id}.html` | `RawCaptureMiddleware` |
| Fetch manifest | `data/manifests/fetch_manifest.csv` | `RawCaptureMiddleware` |
| Events manifest | `data/manifests/events_manifest.csv` | `EventsManifestPipeline` |
| Fighter queue | `data/manifests/fighter_queue.csv` | `build_fighter_queue.py` |
| Fight stats queue | `data/manifests/fight_stats_queue.csv` | `build_fight_stats_queue.py` |
| Coverage reports | `data/reports/*.csv` | Report scripts |

`DATA_DIR` in the Makefile resolves to `../../data` (repo root `data/`).

---

## Commands

### Bounded end-to-end backfill (~50 events)

Validates the full pipeline: events → queues → incremental stats fetch → all reports.
Use this after a schema or spider change before running a full-history crawl.

```bash
make backfill_sample              # default: ~50 most recent events
make backfill_sample BACKFILL_N=26  # ~25 events
```

`BACKFILL_N` sets `CLOSESPIDER_PAGECOUNT` on the events spider.
The events listing page at `ufcstats.com/statistics/events/completed?page=all` is one response;
each event detail page is one more. So `BACKFILL_N=51` → 1 listing + 50 event pages ≈ 50 events.

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

### Single-URL debug run

Each spider accepts an optional URL argument for targeted single-page debug runs.

```bash
# Events spider — crawl one event page
make crawl_events ARGS="-a event_url=http://ufcstats.com/event-details/<id>"

# Fighters spider — crawl one fighter profile
make crawl_fighters ARGS="-a fighter_url=http://ufcstats.com/fighter-details/<id>"

# Fight stats spider — crawl one fight detail page
make crawl_fight_stats ARGS="-a fight_url=http://ufcstats.com/fight-details/<id>"
make crawl_fight_stats_by_round ARGS="-a fight_url=http://ufcstats.com/fight-details/<id>"
```

### Queue build commands

Queue files are inputs to the incremental stats and fighters spiders. Rebuild after a fresh
events/fights crawl, or whenever the source CSVs change.

```bash
make build_stats_queue   # builds data/manifests/fight_stats_queue.csv
make build_queue         # builds data/manifests/fighter_queue.csv
```

`build_stats_queue` reads `data/fights.csv` (primary), `data/manifests/events_manifest.csv`,
and `data/events.csv`. It excludes upcoming events and is idempotent (preserves existing
`queued_at` timestamps).

`build_queue` reads `data/fighters.csv` and globs `data/raw/ufcstats/fights/*.html` to extract
fighter URLs from raw fight pages. No network calls.

### Coverage and review reports

Run these after any acquisition job to validate output completeness.

```bash
make report_events                     # event coverage; exits 1 if miss rate > 5%
make report_events THRESHOLD=0.10      # override threshold

make review_fighters                   # fighter bio/identity review; always exits 0
                                       # writes data/reports/fighter_review.csv

make report_stats                      # fight stats coverage; exits 1 if miss rate > 5%
make report_stats THRESHOLD=0.10       # override threshold
```

Report outputs:

| Report | Path | Exits non-zero |
|---|---|---|
| Event coverage | `data/reports/stats_coverage.csv` (gap rows only) | Yes — if miss rate > threshold |
| Fighter review | `data/reports/fighter_review.csv` (flagged fighters only) | No — flags are informational |
| Stats coverage | `data/reports/stats_coverage.csv` (gap rows only) | Yes — if agg or round miss rate > threshold |

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

Fetches 5 events pages then runs the output validator. Confirms the crawler,
`RawCaptureMiddleware`, incremental mixin, and CSV write path all work together.

```bash
make smoke
```

`make smoke` does two things in sequence:
1. `make sample_events N=5` — bounded crawl
2. `make check` — output validator (see below)

The run passes only when **all** of the following are produced:
- `data/manifests/fetch_manifest.csv` with at least one captured row
- `data/raw/ufcstats/event_listing/` with at least one `.html` file
- `data/raw/ufcstats/events/` with at least one `.html` file
- `data/events.csv` with at least one data row

### Output validator

Run the validator independently after any acquisition job:

```bash
make check
```

Or directly:

```bash
python3 smoke_check.py
```

The validator checks three sections and prints a PASS/FAIL line per check.
Optional artifacts (fights, fighters, other CSVs) are always reported but
never counted as failures. Exit code is `0` on all mandatory passes, `1`
if any mandatory check fails.

Example output after a successful events smoke run:

```
── Manifest ────────────────────────────────────────────────
  PASS  fetch_manifest.csv exists
  PASS  fetch_manifest.csv has data rows (6 total)
  PASS  manifest has all required fields
  PASS  manifest has captured rows  (5 captured, 1 failed/other)

── Raw artifacts ───────────────────────────────────────────
  PASS  data/raw/ufcstats/event_listing/  (1 .html files)
  PASS  data/raw/ufcstats/events/  (4 .html files)
  PASS  data/raw/ufcstats/fights/  (0 .html files)  [optional]
  PASS  data/raw/ufcstats/fighters/  (0 .html files)  [optional]

── Parsed CSV outputs ──────────────────────────────────────
  PASS  data/events.csv  (12 rows)
  PASS  data/fights.csv  (8551 rows)  [optional]
  ...

════════════════════════════════════════════════════════════
  ALL CHECKS PASSED
════════════════════════════════════════════════════════════
```

To confirm source-safety controls are active during the crawl:

```bash
make smoke ARGS="-s LOG_LEVEL=DEBUG"
# Look for lines like:
#   RawCaptureMiddleware active | job_run_id=...
#   Throttle  slot=ufcstats.com ...
#   RawCaptureMiddleware summary | fetched=5 | unchanged=0 | ...
#   IncrementalCrawlMixin summary | skipped=0 | reason=finished
```

### Filtered run (pass extra Scrapy args)

The `ARGS` variable is forwarded to the Scrapy command for any target.

```bash
make crawl_csv_events ARGS="-s LOG_LEVEL=DEBUG"
```

---

## Restart and resume

All spiders support incremental mode — they skip IDs already present in the output CSV. If a
run is interrupted, re-running with `update_%` continues from where it left off without
duplicating rows.

The stats spiders also use `fight_stats_queue.csv` for seeding. If the queue was built before
the interruption, the re-run will pick up all un-processed fight URLs automatically.

```bash
# Resume an interrupted stats fetch:
make update_fight_stats
make update_fight_stats_by_round

# Resume an interrupted fighters fetch:
make update_fighters
```

If the queue file itself is missing or corrupt, rebuild it first:

```bash
make build_stats_queue
make build_queue
```

---

## Common failure modes

| Symptom | Likely cause | Action |
|---|---|---|
| `ModuleNotFoundError: scrapy` | Virtualenv not activated | `source .venv/bin/activate` |
| Empty CSV after crawl | Spider name mismatch or 0 items parsed | Run with `ARGS="-s LOG_LEVEL=DEBUG"` and check spider logs |
| Duplicate rows in CSV | `crawl_csv_%` used instead of `update_%` on existing file | Use `update_%` for incremental runs; `crawl_csv_%` always overwrites |
| Crawl stops early unexpectedly | `CLOSESPIDER_PAGECOUNT` set (sample/smoke targets only) | Expected for `sample_%` and `smoke`; use `crawl_csv_%` for a full run |
| `FileNotFoundError` for `data/` | Working directory wrong | Run from `scraper/UFC-Web-Scraping-main/`; Makefile creates `data/` if missing |
| `fight_stats_queue.csv not found` logged by spider | Queue not built yet | Run `make build_stats_queue` before the stats spiders |
| `fight_stats_by_round` produces 0 rows in incremental mode | Cross-spider dedup via shared `fetch_manifest.csv` | This is prevented by `_load_captured_uuids()` override; if it recurs check the override is intact |
| Stats coverage report exits 1 | Miss rate above threshold | Check `data/reports/stats_coverage.csv` gap rows; re-run `make update_fight_stats` to retry `not_fetched` fights |
| Report script says `fight_stats_queue.csv` absent | Queue not built | Run `make build_stats_queue` |
| `NO_STATS_PAGE` or `NO_ROUND_TABLE` in spider logs | Fight page has no stats table | Expected for old fights with no recorded stats; these appear as `missing_stats` in the coverage report |

---

## Quick-reference table

| Goal | Command |
|---|---|
| Bounded end-to-end validation (~50 events) | `make backfill_sample` |
| Full crawl, all spiders → CSV | `make crawl_csv_all` |
| Full crawl, one spider → CSV | `make crawl_csv_<spider>` |
| Incremental update, all spiders | `make update_all` |
| Incremental update, one spider | `make update_<spider>` |
| Rebuild fight stats queue | `make build_stats_queue` |
| Rebuild fighter queue | `make build_queue` |
| Event coverage report | `make report_events` |
| Fighter bio/identity review | `make review_fighters` |
| Fight stats coverage report | `make report_stats` |
| Bounded sample, one spider | `make sample_<spider> N=<pages>` |
| Smoke test (5 event pages + validate) | `make smoke` |
| Validate outputs only (no crawl) | `make check` |

---

## Phase 1 handoff checklist

Phase 2 (parsing, feature engineering, warehouse ingestion) may proceed when all items below
are satisfied.

### Required files

- [ ] `data/events.csv` — complete event registry, including `fight_urls` column
- [ ] `data/fights.csv` — complete fight results with `finish_method`
- [ ] `data/fighters.csv` — fighter profiles (bio fields may be sparse for historical fighters)
- [ ] `data/fight_stats.csv` — aggregate per-fighter stats per fight
- [ ] `data/fight_stats_by_round.csv` — per-round per-fighter stats

### Required manifests

- [ ] `data/manifests/fetch_manifest.csv` — audit trail of every HTTP fetch
- [ ] `data/manifests/events_manifest.csv` — canonical event registry with fight URLs
- [ ] `data/manifests/fight_stats_queue.csv` — canonical queue of completed fights (used to measure coverage)
- [ ] `data/manifests/fighter_queue.csv` — canonical queue of fighter profiles

### Required reports (must pass thresholds)

- [ ] `make report_events` exits 0 — event miss rate ≤ 5%
- [ ] `make report_stats` exits 0 — fight stats miss rate ≤ 5% for both aggregate and round-level
- [ ] `make review_fighters` has run — `data/reports/fighter_review.csv` reviewed; no unexpected identity collisions

### Validation baseline (Phase 1 completion run, 2026-03-18)

| Metric | Value |
|---|---|
| Events | All completed events in manifest; coverage threshold passes |
| Fights | 8 550 queued |
| Fight stats (aggregate) | 8 531 / 8 550 (99.8%) |
| Fight stats (round-level) | 8 531 / 8 550 (99.8%) |
| Not-yet-fetched fights | 19 (0.2%) — within 5% threshold; retry with `update_fight_stats` |
| Fighters in queue | 4 452 |
| Fighters flagged (informational) | 636 (14.3%) — sparse bios and duplicate names; see `fighter_review.csv` |

Phase 2 **may rely on**:

- All `data/*.csv` columns documented in `docs/acquisition-contract.md` being present and UTF-8 encoded.
- `fight_id`, `event_id`, and `fighter_id` being deterministic UUID5 values derived from ufcstats.com URLs via `utils.get_uuid_string()`.
- Raw HTML files in `data/raw/ufcstats/fights/` being available for re-parsing without network calls (SHA-256 dedup ensures idempotency).
- `fight_stats_queue.csv` correctly reflecting `finish_method` for all queued fights (sourced from `fights.csv`).

Phase 2 **must not assume**:

- Fighter bio fields (height, weight, reach, stance, DOB) are complete — ~14% of profiles are sparse.
- All fight stats are present — 0.2% of fights have no stats page on ufcstats.com (old bouts).
- Round-level stats are present for every fight that has aggregate stats — some fights with aggregate stats have no round breakdown.

---

---

---

# Phase 2 Warehouse Runbook

Entry-point reference for building and validating the UFC data warehouse.
All commands are run from the **repo root** unless stated otherwise.

_Implements T2.1.1–T2.5.1. Schema DDL lives in `warehouse/sql/`. Normalization rules in `docs/normalization-rules.md`._

---

## Setup (one time)

### Prerequisites

- Python 3.11+ with `psycopg2-binary` and `python-dotenv` available.
- A running PostgreSQL instance.
- Copy `.env.example` to `.env` and fill in connection details:

```bash
cp .env.example .env
# edit .env: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
```

Test the connection:

```bash
python3 warehouse/db.py
# Connected: PostgreSQL 17.x on ...
```

### Run migrations

Creates all five warehouse tables and indexes. Safe to re-run — already-applied migrations are skipped.

```bash
make migrate
```

---

## Full load (first time or full reload)

Loads all five tables in dependency order: events → fighters → fights → stats.
Each loader is idempotent — re-running a loader that has already run is a no-op (upsert on PK).

```bash
make load_all
```

Or run loaders individually:

```bash
make load_events       # data/events.csv + data/manifests/events_manifest.csv → events
make load_fighters     # data/fighters.csv → fighters
make load_fights       # data/fights.csv → fights
make load_stats        # data/fight_stats.csv + data/fight_stats_by_round.csv → fight_stats_*
```

**Load order matters.** `load_fights` requires fighters to exist (FK constraint). `load_stats` requires fights.
`load_all` enforces the correct order automatically.

---

## Incremental update

After re-running the Phase 1 scraper (e.g. after a new UFC event), reload the affected tables:

```bash
make load_events       # picks up new events
make load_fighters     # picks up new/updated fighter profiles
make load_fights       # picks up new fights
make load_stats        # picks up new fight stats
```

All loaders use `INSERT ... ON CONFLICT DO UPDATE`, so only changed values are written.
`scraped_at` is kept as the more recent value on conflict, so re-loading older data will not
overwrite a newer scrape.

---

## Validation

### Integrity check (FK completeness + row counts)

```bash
make validate_integrity
```

Checks: row counts within expected ranges, zero orphaned rows across all FK relationships.
Exits non-zero if any check fails.

### Consistency check (logical data quality)

```bash
make validate_consistency
```

Checks: result/winner coherence, finish round within scheduled rounds, weight class vocabulary,
aggregate ≈ round-sum for sig_strikes_landed (soft warning, does not fail).
Exits non-zero only on hard logic failures.

### Run both validators

```bash
make warehouse_check
```

---

## Full pipeline (migrate → load → validate)

```bash
make warehouse_up
```

Equivalent to `make migrate && make load_all && make warehouse_check`.
Use this after a fresh clone or a full reload.

---

## Quick-reference table

| Goal | Command |
|---|---|
| Apply pending schema migrations | `make migrate` |
| Full load (all tables, correct order) | `make load_all` |
| Load single table | `make load_events` / `load_fighters` / `load_fights` / `load_stats` |
| FK + row count validation | `make validate_integrity` |
| Logical consistency validation | `make validate_consistency` |
| Both validators | `make warehouse_check` |
| Full pipeline (migrate + load + validate) | `make warehouse_up` |

---

## Common failure modes

| Symptom | Likely cause | Action |
|---|---|---|
| `KeyError: 'POSTGRES_HOST'` | `.env` not present or python-dotenv not installed | Copy `.env.example` → `.env` and fill in credentials; `pip install python-dotenv` |
| `psycopg2.OperationalError: could not connect` | DB host unreachable | Check `POSTGRES_HOST` in `.env`; confirm Postgres is running |
| `ForeignKeyViolation` on `load_fights` | fighters not loaded yet | Run `make load_fighters` before `make load_fights`; or use `make load_all` |
| Row count check FAIL | Loader skipped rows (unknown event/fight IDs) | Check loader output for `warn  unknown ... — skipping`; ensure upstream table was loaded first |
| `validate_integrity` fails | Orphaned rows exist | Re-run loaders in order; check for FK constraint issues in migration files |
| `WARN aggregate ≠ round-sum` | Known source-data discrepancy on ufcstats.com | Informational only — does not fail the run; note count for Phase 3 feature engineering |

---

## Phase 2 handoff checklist

Phase 3 (feature engineering) may proceed when all items below are satisfied.

### Required

- [ ] `make migrate` has been run and all 6 migration files applied
- [ ] `make load_all` exits 0 — all five tables populated
- [ ] `make validate_integrity` exits 0 — zero FK violations

### Validation baseline (Phase 2 completion, 2026-03-20)

| Table | Rows loaded |
|---|---|
| events | 764 |
| fighters | 4,452 |
| fights | 8,550 |
| fight_stats_aggregate | 17,062 |
| fight_stats_by_round | 40,238 |

### Known data quality exceptions

| Exception | Count | Impact on Phase 3 |
|---|---|---|
| Fights with no stats rows | 19 (0.2%) | These fights will have no feature vectors from stats; exclude from model training or impute |
| Sparse fighter bios (height/weight/reach/stance/DOB null) | ~636 fighters (14.3%) | Physical feature columns will be NULL for these fighters; models must handle nulls |
| Duplicate full_name values | 7 pairs | Distinct fighters sharing a name; resolved by `fighter_id` UUID — do not join on name |
| Fights with NULL weight_class | 15 | Early UFC tournament bouts; weight class features will be NULL |
| Stats rows skipped at load (unknown fight_id) | 40 aggregate / 82 by-round | Fight IDs in stats CSV not present in fights table; safe to ignore — these are fights outside the scraped event window |

Phase 3 **may rely on**:
- All five warehouse tables loaded and FK-validated.
- `fight_id`, `event_id`, `fighter_id` being stable UUID5 values derived from ufcstats.com URLs.
- `result_type` and `winner_fighter_id` being logically consistent (validated by `validate_consistency`).
- `control_time_seconds` and `finish_time_seconds` being pre-computed integers (not raw minute/second columns).

Phase 3 **must not assume**:
- Fighter physical attributes (height, weight, reach, stance, DOB) are always present.
- Every fight has associated stats rows.
- `weight_class` is non-null for all fights (15 early bouts have NULL).

---

## Unresolved acquisition risks

These are open items that did not block Phase 1 sign-off but may affect Phase 2 work.

| Risk | Severity | Detail |
|---|---|---|
| 19 fights have no fetched stats page | Low | The `not_fetched` gap (0.2%) persists after the full crawl. A single `make update_fight_stats` re-run should close it; if fights remain missing they likely have no stats table on the source. |
| ~14% sparse fighter bios | Low | ufcstats.com does not expose full bio data for older or less-prominent fighters. Height, weight, reach, stance, and DOB can all be absent. Phase 2 models must handle nulls in these fields. |
| 7 duplicate-name fighter pairs | Low | Seven `full_name` values map to more than one `fighter_id`. These are likely distinct fighters sharing a name (e.g. regional vs UFC). Identity resolution requires manual review or a secondary disambiguation signal (e.g. DOB, nationality). |
| Nationality field absent | Low | ufcstats.com fighter pages do not expose nationality. If nationality is needed for features it must be sourced elsewhere (e.g. Wikipedia, Tapology). |
| `fight_stats_by_round` shares `fetch_manifest` with `fight_stats` | Medium | The by-round spider overrides `_load_captured_uuids()` to return `set()` to prevent cross-spider dedup collision. If this override is inadvertently removed in a future refactor, the by-round spider will silently produce zero output in incremental mode. The override is documented in the spider docstring. |
| No rate-limit handling beyond Scrapy throttle | Low | The scraper relies on Scrapy's `AutoThrottle` and `DOWNLOAD_DELAY`. If ufcstats.com introduces rate-limiting or CAPTCHAs, the spider will accumulate `failed` rows in the fetch manifest rather than retrying with backoff. Monitor `fetch_failed` counts in `stats_coverage_report`. |
| Full-history crawl not yet executed | Low | The Phase 1 validation run confirmed pipeline integrity at 99.8% coverage. A full-history crawl from scratch has not been timed or stress-tested. Estimate conservatively and monitor `CLOSESPIDER_ERRORCOUNT` for sustained failure. |

---

---

---

# Phase 3 Feature Engineering Runbook

Entry-point reference for building, validating, and extending the UFC feature pipeline.
All commands are run from the **repo root**.

_Implements T3.1.1–T3.6.1. Feature catalog in `docs/feature-catalog.md`. DDL in `warehouse/sql/007_feature_tables.sql`._

---

## Prerequisites

- Phase 2 warehouse fully loaded (`make warehouse_up` or `make load_all` + `make warehouse_check`).
- Python 3.11+ with `psycopg2-binary` and `python-dotenv` available.
- `.env` configured with PostgreSQL connection details.

---

## Running the feature pipeline

### Full build (compute all features from scratch)

```bash
make build_features
```

Loads all warehouse data into memory, computes Elo ratings in one chronological pass,
builds fighter snapshots and bout feature rows for every fight, and upserts into
`fighter_snapshots` and `bout_features` tables.

Output: ~17,100 snapshots (2 per fight) and ~8,550 bout rows.

### Full pipeline (build → test → validate)

```bash
make features_up
```

Equivalent to `make build_features && make test_leakage && make validate_features`.
Use this after a fresh build or after modifying feature code.

### Leakage tests only

```bash
make test_leakage
```

Runs 7 integration tests that verify no feature uses data from the target fight or
any future fight. Must pass before any modeling work.

### Feature quality report only

```bash
make validate_features
```

Prints missingness rates, distribution stats, label correlations, and completeness
metrics. Informational — always exits 0.

---

## Quick-reference table

| Goal | Command |
|---|---|
| Build all features from warehouse data | `make build_features` |
| Run leakage prevention tests | `make test_leakage` |
| Run feature quality/distribution report | `make validate_features` |
| Full pipeline (build + test + validate) | `make features_up` |

---

## How to add a new feature

1. **Choose the feature family.** Career aggregates go in `features/career.py`, rolling
   windows in `features/rolling.py`, decay metrics in `features/decay.py`, physical/demo
   in `features/physical.py`, Elo/opponent in `features/elo.py` or `features/opponent.py`.

2. **Add the computation** to the appropriate module function. Each module is a pure
   function that takes a `FightHistory` list (or similar inputs) and returns a flat dict.
   Add your new key to the returned dict.

3. **Add the DDL column** to `fighter_snapshots` (per-fighter) or `bout_features` (per-bout)
   via a new migration in `warehouse/sql/`. Run `make migrate` to apply.

4. **Map module output → DDL column** in `features/pipeline.py`:
   - For fighter-level features: add the mapping in `_snapshot_to_row()`.
   - For bout-level features: add the mapping in `_bout_to_row()`.

5. **Add unit tests** in `features/tests/test_<module>.py` for the new computation.

6. **Rebuild and validate:**
   ```bash
   make features_up
   ```

7. **Update `docs/feature-catalog.md`** with the new feature's name, type, formula,
   source, and null handling.

---

## Leakage prevention rules

These rules must hold for every feature. The leakage tests (`make test_leakage`) verify them.

| Rule | Enforcement |
|---|---|
| **Temporal exclusion:** only fights with `event_date < cutoff` are included in history | `get_history()` in `features/history.py` uses `bisect_left` with strict `<` |
| **Target fight exclusion:** the fight being predicted is never in the feature window | Same `< cutoff` filter — the target fight's own date equals the cutoff |
| **Monotonic career counts:** `career_fights` must be non-decreasing across dates | Verified by `TestMonotonicHistory` — accounts for same-date tournament fights |
| **Elo causality:** Elo ratings reflect only prior fight outcomes | `compute_all_elos()` processes fights in chronological order; debut Elo = 1500 |
| **Label isolation:** `bout_features.label` comes only from `fights.result_type` and `winner_fighter_id` | Verified by `TestLabelIsolation` |

### Same-date tournament fights (edge case)

Early UFC events (1994–1998) had single-night tournaments where one fighter could have
2–4 fights on the same date. For these fights:

- **Career features** use strict `< cutoff_date`, so all same-date fights share the same
  `career_fights` count (the prior bouts on that date are excluded).
- **Elo** updates sequentially within the same date (intentional — the prior bout on the
  same card has already happened by the time the next one starts).
- Leakage tests account for both behaviors.

---

## Known limitations

| Limitation | Impact | Mitigation |
|---|---|---|
| ~16% of snapshots have NULL career stats | Debut fighters have no prior fights to aggregate | Models must handle NULLs; `is_debut` flag available |
| ~8% missing reach data | ufcstats.com doesn't have reach for all fighters | `height_reach_missing` flag; do not impute |
| 24 DDL columns always NULL | Rolling/decay columns not yet computed by modules (e.g. `sig_strike_defense_last{N}`, `streak_last{N}`) | Can be extended in future; currently stored as NULL |
| `career_control_rate` is in seconds (not a rate) | Named "rate" but stores `control_time_per_fight` in seconds | Values range 0–854; widened to `numeric(8,4)` in migration 008 |
| `both_debuting` bouts have 0% core-diff completeness | Both fighters have no prior fights → all diff features are NULL | 537 bouts (6.3%); consider excluding from training or using debut-specific features |

---

## Common failure modes

| Symptom | Likely cause | Action |
|---|---|---|
| `NumericValueOutOfRange` during upsert | New feature exceeds `numeric(6,4)` column width | Add a widening migration (see `008_widen_feature_numerics.sql` for example) |
| `KeyError` in `_snapshot_to_row` | Module output key doesn't match mapping | Check the key name in the feature module's return dict |
| Leakage test fails after feature change | Feature is using `<=` instead of `<` for cutoff, or including the target fight | Check `get_history()` call and ensure strict `< cutoff_date` |
| `No snapshots found` in leakage tests | Feature tables are empty | Run `make build_features` first |
| `psycopg2.errors.UndefinedColumn` | DDL column missing for a new feature | Run `make migrate` to apply pending migrations |

---

## Phase 3 handoff checklist

Phase 4 (modeling) may proceed when all items below are satisfied.

### Required

- [ ] `make build_features` exits 0 — both feature tables populated
- [ ] `make test_leakage` exits 0 — all 7 leakage tests pass
- [ ] `make validate_features` reviewed — no features with |r| > 0.5

### Validation baseline (Phase 3 completion, 2026-03-20)

| Table | Rows |
|---|---|
| fighter_snapshots | 17,100 |
| bout_features | 8,550 |

| Metric | Value |
|---|---|
| Leakage tests | 7/7 passing |
| Features with \|r\| > 0.5 (suspicious) | 0 |
| Max label correlation | diff_age: r = −0.19 |
| Core-diff completeness (non-debut) | 58.5% |
| Core-diff completeness (all bouts) | 54.8% |

### Known missingness rates

| Feature group | Typical NULL % | Reason |
|---|---|---|
| Career stats (debut fighters) | ~16% | No prior fights |
| Takedown accuracy | ~24% | No takedown attempts in prior fights |
| Career finish rate | ~25% | Debut fighters (0 wins) |
| Age/age_squared | 0.8% | Missing DOB in source |
| Reach / reach_to_height | ~8% | Missing reach in source |
| 24 uncomputed rolling/decay columns | 100% | Not yet implemented in modules |

Phase 4 **may rely on**:

- `fighter_snapshots` containing one row per (fighter, fight) with pre-fight features only.
- `bout_features` containing one row per fight with difference, ratio, and matchup features.
- `bout_features.label` = 1 (fighter_1 wins), 0 (fighter_2 wins), NULL (draw/NC).
- All leakage tests passing — no feature uses data from the target fight or future fights.
- `fighter_snapshots.elo_rating` starting at 1500 for debut fighters (single first-date fight).

Phase 4 **must not assume**:

- All feature columns are non-NULL — debut fighters and sparse bios create NULLs.
- The 24 always-NULL columns contain data — these are DDL placeholders for future features.
- `career_control_rate` is a percentage — it's control time in seconds per fight (0–854).
- `both_debuting` bouts have usable difference features — all core diffs are NULL for these.
