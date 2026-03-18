# Phase 1 Execution Tickets

Phase 1 in [ufc-predictor.md](/home/wlodzimierrr/ufc-data/ufc-predictor.md) is `Data acquisition`. This file turns that scope into a real implementation backlog.

These tickets assume:

- the current implementation base is the Scrapy project in `scraper/UFC-Web-Scraping-main/`;
- current seed outputs already exist in `data/events.csv`, `data/fights.csv`, `data/fight_stats.csv`, `data/fight_stats_by_round.csv`, and `data/fighters.csv`;
- Phase 1 is about making acquisition reproducible, auditable, and safe, not about warehouse modeling yet;
- Python remains the implementation language.

Recommended execution order:

1. Foundation and standards
2. Shared crawler hardening
3. Event discovery and event detail capture
4. Fighter profile acquisition
5. Fight stats acquisition
6. Sample backfill and handoff to Phase 2

## Foundation

#### T1.1.1 Audit current scraper/data baseline
- **Description:** Inventory the existing Scrapy project, current spiders, Makefile commands, exported CSVs, and known gaps versus the Phase 1 scope in `ufc-predictor.md`. This ticket exists to stop the team from building a second scraping path when there is already one in the repo.
- **Status:** DONE — see appendix.
- **Acceptance Criteria:**
  - A short audit note is added to `docs/` or `tickets.md` appendix listing the current spiders: `crawl_events`, `crawl_fights`, `crawl_fighters`, `crawl_fight_stats`, `crawl_fight_stats_by_round`.
  - The audit identifies what is already covered, what is missing, and what must be extended for Phase 1.
  - Existing output files in `data/` are mapped to the relevant spiders and source pages.
  - The audit explicitly calls out that current outputs are parsed CSVs, not raw-page captures with fetch metadata.
- **Dependencies:** None
- **Complexity:** S
- **Risk:** Low
- **Notes:** Use `scraper/UFC-Web-Scraping-main/README.md`, `Makefile`, `ufc_scraper/ufc_scraper/spiders/`, and the existing CSVs as the baseline.

#### T1.1.2 Define the Phase 1 acquisition contract
- **Description:** Define the file layout, naming rules, and metadata contract for raw artifacts, manifests, and scrape reports. This should become the single contract all Phase 1 acquisition jobs write to.
- **Status:** DONE — see `docs/acquisition-contract.md`.
- **Acceptance Criteria:**
  - A documented output contract exists for:
    - raw pages under `data/raw/ufcstats/`;
    - manifests under `data/manifests/`;
    - reports under `data/reports/`.
  - Every raw fetch is defined to carry at least `entity_type`, `source_url`, `fetched_at`, `http_status`, `content_hash`, `job_run_id`, and `storage_path`.
  - Naming rules are defined for event, fighter, fight, and stats artifacts.
  - The contract distinguishes raw capture from parsed tabular outputs in `data/`.
- **Dependencies:** T1.1.1
- **Complexity:** S
- **Risk:** Low
- **Notes:** Keep the contract simple. CSV or parquet manifests are both acceptable, but one format should be chosen and used consistently.

#### T1.1.3 Standardize local execution commands
- **Description:** Normalize how the team runs acquisition jobs locally so all future tickets have one entrypoint. The existing Makefile and Scrapy commands should be reused, not replaced without reason.
- **Status:** DONE — see `docs/runbook.md` and extended `scraper/UFC-Web-Scraping-main/Makefile`.
- **Acceptance Criteria:**
  - One documented command exists for each Phase 1 acquisition flow.
  - The command surface supports full runs and filtered runs by event URL, event ID, or small sample.
  - The project README or runbook includes exact commands for setup, crawl, incremental crawl, and sample validation.
  - The execution path clearly states where raw artifacts, manifests, and parsed outputs are written.
- **Dependencies:** T1.1.2
- **Complexity:** S
- **Risk:** Low
- **Notes:** Prefer extending the existing `Makefile` rather than inventing a second orchestration layer.

## Shared Crawler Hardening

#### T1.2.1 Harden shared crawl settings for source safety
- **Description:** Review and harden the shared crawler settings so all Phase 1 jobs use the same rate limits, retry behavior, timeouts, and caching policy. Current settings already include `ROBOTSTXT_OBEY = True`, `CONCURRENT_REQUESTS_PER_DOMAIN = 1`, and `DOWNLOAD_DELAY = 1`; this ticket makes that behavior explicit and consistent across spiders.
- **Status:** TODO
- **Acceptance Criteria:**
  - Shared settings define retry count, request timeout, user agent, and throttling behavior centrally.
  - All acquisition spiders inherit the same baseline source-safety controls.
  - Per-spider overrides are documented and justified.
  - A smoke run confirms that the crawler respects one-request-per-domain behavior and bounded retries.
- **Dependencies:** T1.1.3
- **Complexity:** M
- **Risk:** Medium
- **Notes:** Prefer central configuration in `ufc_scraper/ufc_scraper/settings.py`; keep spider `custom_settings` only where necessary.

#### T1.2.2 Implement raw page capture and fetch metadata pipeline
- **Description:** Add a shared pipeline that stores raw responses before or alongside parsed records. Today the project primarily writes parsed CSV outputs; Phase 1 needs raw HTML plus fetch metadata for auditability and reprocessing.
- **Status:** TODO
- **Acceptance Criteria:**
  - Successful fetches write raw content to `data/raw/ufcstats/<entity_type>/...`.
  - A manifest row is written for every raw capture with `source_url`, `fetched_at`, `http_status`, `content_hash`, `job_run_id`, and `storage_path`.
  - Failed fetches still produce an error or manifest record with failure status and source URL.
  - Parsed CSV output remains usable after this change.
- **Dependencies:** T1.2.1, T1.1.2
- **Complexity:** M
- **Risk:** Medium
- **Notes:** This is the key gap between the current scraper and the Phase 1 acquisition target.

#### T1.2.3 Replace CSV-only incremental logic with manifest-aware idempotency
- **Description:** The current `IncrementalCrawlMixin` skips records by existing parsed CSV IDs. Extend this so reruns and resume behavior are driven by canonical manifests and content hashes, not only by current CSV contents.
- **Status:** TODO
- **Acceptance Criteria:**
  - Incremental mode can skip already-captured pages using manifest state, not only parsed CSV IDs.
  - Restarting an interrupted job does not duplicate raw artifacts or manifest entries.
  - Re-fetching a page with unchanged content is either skipped or recorded as unchanged according to the chosen contract.
  - Logs show counts for fetched, skipped, changed, and failed URLs.
- **Dependencies:** T1.2.2
- **Complexity:** M
- **Risk:** Medium
- **Notes:** Update `ufc_scraper/ufc_scraper/spiders/incremental.py` rather than bolting on a second incremental system.

#### T1.2.4 Add smoke-run harness for acquisition jobs
- **Description:** Create a minimal test/run harness that executes a constrained sample of each acquisition path and verifies files land in the expected locations. This should be fast enough to run during development.
- **Status:** TODO
- **Acceptance Criteria:**
  - A sample run can fetch a small number of events, fighters, and fight stats without running a full backfill.
  - The smoke harness verifies raw artifacts, manifest rows, and parsed outputs are all produced.
  - Failure in any required output path fails the smoke run.
  - The harness is documented and repeatable on a clean checkout.
- **Dependencies:** T1.2.2, T1.2.3
- **Complexity:** M
- **Risk:** Low
- **Notes:** Keep it small. This is a developer validation tool, not the full historical load.

## Event Discovery and Event Detail Capture

#### T1.3.1 Extend event discovery to cover the full Phase 1 event scope
- **Description:** The current event spider starts from completed events. Extend discovery so Phase 1 can build a canonical registry for both historical and upcoming UFC events, or document a fallback seed path if the public source only fully exposes completed events.
- **Status:** TODO
- **Acceptance Criteria:**
  - Event discovery supports completed events and an explicit strategy for upcoming events.
  - Event URLs are normalized into a canonical manifest.
  - Duplicate event URLs and duplicate event IDs are removed during discovery.
  - Discovery output contains `source_event_id` when derivable, plus event name/date candidates and event status.
- **Dependencies:** T1.2.1, T1.1.2
- **Complexity:** M
- **Risk:** Medium
- **Notes:** Current `crawl_events` starts from `statistics/events/completed?page=all`; upcoming discovery may require a second seed source or a maintained seed list.

#### T1.3.2 Build the canonical events manifest
- **Description:** Persist discovery results into one canonical manifest that downstream jobs read instead of rediscovering event URLs independently.
- **Status:** TODO
- **Acceptance Criteria:**
  - A manifest file exists at a stable path such as `data/manifests/events_manifest.csv`.
  - Each record includes canonical event URL, source event ID, event status, discovery timestamp, and dedupe key.
  - The manifest supports incremental refresh without duplicating prior event rows.
  - Downstream event-detail crawling can read directly from this manifest.
- **Dependencies:** T1.3.1, T1.2.3
- **Complexity:** S
- **Risk:** Low
- **Notes:** This ticket is the concrete implementation of high-level ticket `UFC-001`.

#### T1.3.3 Capture event detail pages and ordered fight-card listings
- **Description:** Extend the event-detail acquisition flow so each event page is captured raw and its parsed output includes event metadata plus ordered fight-card references for all bouts on the card.
- **Status:** TODO
- **Acceptance Criteria:**
  - Each event detail page is stored as a raw artifact with fetch metadata.
  - Parsed event output includes event name, date, location, and ordered fight-card rows.
  - Parsed event output captures fight detail URLs for each listed bout.
  - The run can be filtered to a single event manifest row for debugging.
- **Dependencies:** T1.3.2, T1.2.2
- **Complexity:** M
- **Risk:** Medium
- **Notes:** This is the concrete implementation core of high-level ticket `UFC-002`.

#### T1.3.4 Add event acquisition validation and coverage reporting
- **Description:** Build a report that reconciles discovered events, fetched event pages, and parsed event outputs so the team can see what is missing before moving to downstream fighter and fight acquisition.
- **Status:** TODO
- **Acceptance Criteria:**
  - A report exists showing total discovered events, fetched event pages, parse failures, and missing outputs.
  - The report flags duplicate event IDs, blank event dates, and malformed fight-card listings.
  - The report can be generated after any run without manual data cleanup.
  - A run is considered failed if required event metadata fields are missing above a defined threshold.
- **Dependencies:** T1.3.3
- **Complexity:** S
- **Risk:** Low
- **Notes:** Do not wait until Phase 2 to discover broken event coverage.

## Fighter Profile Acquisition

#### T1.4.1 Build the fighter profile queue from current acquisition outputs
- **Description:** Create a single fighter queue from fighter listing pages, event-linked fighter URLs, and existing CSV seeds. The queue should represent the set of fighter profile pages Phase 1 intends to capture.
- **Status:** TODO
- **Acceptance Criteria:**
  - A fighter queue exists at a stable path such as `data/manifests/fighter_queue.csv`.
  - The queue deduplicates fighter URLs across A-Z roster pages and event-linked references.
  - Each queued fighter has a canonical profile URL and a stable dedupe key.
  - The queue can be regenerated without duplicating prior entries.
- **Dependencies:** T1.3.3, T1.2.3
- **Complexity:** M
- **Risk:** Medium
- **Notes:** The current `crawl_fighters` spider uses A-Z listing pages; this ticket makes queue creation explicit and auditable.

#### T1.4.2 Capture fighter profile pages and source IDs
- **Description:** Extend fighter acquisition so each fighter profile page is stored raw and parsed into a structured row carrying identity fields needed later for joins and deduplication.
- **Status:** TODO
- **Acceptance Criteria:**
  - Each queued fighter profile page is stored as raw HTML with fetch metadata.
  - Parsed fighter output includes `fighter_id` or source-equivalent ID when available, fighter name, nickname if present, DOB if present, height, reach, stance, nationality, and profile URL.
  - The crawler can run incrementally against the fighter queue.
  - Parse failures and missing-page failures are surfaced in the manifest or report output.
- **Dependencies:** T1.4.1, T1.2.2
- **Complexity:** M
- **Risk:** Medium
- **Notes:** This is the concrete implementation core of high-level ticket `UFC-003`.

#### T1.4.3 Add fighter missing-data and identity review flags
- **Description:** Build review outputs for fighters with sparse bios, conflicting names, or incomplete physical attributes. Phase 1 should not try to solve all identity problems, but it must surface them explicitly.
- **Status:** TODO
- **Acceptance Criteria:**
  - Fighters missing critical fields such as name or source ID are flagged.
  - Fighters with duplicate names but different source URLs or IDs are flagged for review.
  - A review file is generated with the reason each fighter was flagged.
  - The scraper run completes even when flagged fighters exist.
- **Dependencies:** T1.4.2
- **Complexity:** S
- **Risk:** Medium
- **Notes:** This prevents later warehouse identity work from becoming guesswork.

## Fight Stats Acquisition

#### T1.5.1 Build the completed-fight stats queue
- **Description:** Generate a queue of completed fights that should have stats pages or explicit no-stats status, using the event-detail outputs as the source of truth.
- **Status:** TODO
- **Acceptance Criteria:**
  - A stats queue exists at a stable path such as `data/manifests/fight_stats_queue.csv`.
  - Each row contains event ID, fight ID, fight detail URL, and a target stats status field.
  - Queue generation excludes future fights and handles no-contest or cancelled rows safely.
  - The queue can be refreshed incrementally from the canonical event outputs.
- **Dependencies:** T1.3.3, T1.3.4
- **Complexity:** S
- **Risk:** Low
- **Notes:** Do not let the stats spiders rediscover fights independently once canonical event outputs exist.

#### T1.5.2 Capture fight-level stats pages
- **Description:** Extend the fight stats acquisition path so each eligible completed bout writes a raw fight page artifact plus parsed aggregate fighter-vs-fighter statistics.
- **Status:** TODO
- **Acceptance Criteria:**
  - Each eligible fight stats page is stored as a raw artifact with fetch metadata.
  - Parsed outputs include both fighter rows for the bout and carry the fight ID needed for joins.
  - Missing stats pages are recorded explicitly rather than silently dropped.
  - Incremental runs skip already-captured unchanged fight pages.
- **Dependencies:** T1.5.1, T1.2.2, T1.2.3
- **Complexity:** M
- **Risk:** Medium
- **Notes:** This is the concrete implementation core of high-level ticket `UFC-004` for aggregate fight stats.

#### T1.5.3 Capture round-level fight stats pages
- **Description:** Extend the round-level acquisition path so the project stores raw fight pages once and emits parsed per-round fighter statistics where round tables exist.
- **Status:** TODO
- **Acceptance Criteria:**
  - Round-level stats are produced for fights where round tables are available.
  - Parsed round-level outputs carry fight ID, fighter ID or side, and round number.
  - The run records when a fight has aggregate stats but no usable round-level table.
  - Round-level scraping reuses the same raw capture strategy instead of duplicating fetches unnecessarily.
- **Dependencies:** T1.5.2
- **Complexity:** M
- **Risk:** Medium
- **Notes:** Current repo already has a separate `crawl_fight_stats_by_round` spider; reuse it, but make its acquisition state visible.

#### T1.5.4 Add stats coverage reconciliation report
- **Description:** Produce a coverage report reconciling completed fights, aggregate stats coverage, round-level stats coverage, and failure reasons. This closes the loop on whether acquisition is good enough for Phase 2.
- **Status:** TODO
- **Acceptance Criteria:**
  - A report shows total completed fights, fights with aggregate stats, fights with round-level stats, and missing/failed pages.
  - The report identifies whether missing data is source absence, fetch failure, or parse failure.
  - The report can be generated from manifests and outputs without rerunning the crawl.
  - The report is stored under `data/reports/`.
- **Dependencies:** T1.5.2, T1.5.3
- **Complexity:** S
- **Risk:** Low
- **Notes:** This is the artifact Phase 2 will use to decide how much missingness handling is required.

## Phase Closeout

#### T1.6.1 Run a bounded historical backfill for validation
- **Description:** Execute a bounded backfill over a realistic sample, such as the last 25 to 50 UFC events, to validate the Phase 1 acquisition flow end to end before attempting a full history run.
- **Status:** TODO
- **Acceptance Criteria:**
  - A bounded backfill range is chosen and documented.
  - The run produces raw artifacts, manifests, parsed outputs, and reports for events, fighters, and fight stats.
  - Failure counts and missing-data counts are reviewed and summarized.
  - Any blocking defect discovered during the run is turned into a follow-up ticket before full backfill approval.
- **Dependencies:** T1.3.4, T1.4.3, T1.5.4
- **Complexity:** M
- **Risk:** Medium
- **Notes:** Do not jump from unit tests straight to full-history crawling.

#### T1.6.2 Publish the Phase 1 acquisition runbook and handoff checklist
- **Description:** Document how to run, monitor, resume, and validate Phase 1 acquisition, and define the explicit handoff conditions for Phase 2 parsing and warehouse ingestion work.
- **Status:** TODO
- **Acceptance Criteria:**
  - A runbook exists covering setup, crawl commands, incremental updates, output locations, and failure triage.
  - The runbook includes restart/resume instructions and expected report outputs.
  - A Phase 1 handoff checklist states what files and reports Phase 2 can rely on.
  - The document names unresolved acquisition risks that remain open after Phase 1.
- **Dependencies:** T1.6.1
- **Complexity:** S
- **Risk:** Low
- **Notes:** This closes Phase 1. If the handoff contract is unclear, Phase 2 will waste time reverse-engineering acquisition behavior.

---

## Appendix: T1.1.1 Scraper/Data Baseline Audit

_Completed 2026-03-17._

### Spiders

| Spider name | Class | Source URL(s) | Output file |
|---|---|---|---|
| `crawl_events` | `CrawlEvents` | `ufcstats.com/statistics/events/completed?page=all` | `data/events.csv` |
| `crawl_fights` | `CrawlFights` | Same event listing → individual fight-detail pages | `data/fights.csv` |
| `crawl_fighters` | `CrawlFighters` | `ufcstats.com/statistics/fighters?char={a-z}&page=all` → profile pages | `data/fighters.csv` |
| `crawl_fight_stats` | `CrawlFightStats` | Events → fights → aggregate stats tables | `data/fight_stats.csv` |
| `crawl_fight_stats_by_round` | `CrawlFightStatsByRound` | Events → fights → round-by-round stats tables | `data/fight_stats_by_round.csv` |

All five spiders inherit `IncrementalCrawlMixin` (in `spiders/incremental.py`), which deduplicates by loading known IDs from the existing parsed CSV before crawling.

### Makefile entry points (`scraper/UFC-Web-Scraping-main/Makefile`)

| Command | Effect |
|---|---|
| `make crawl_all` | Full crawl of all five spiders |
| `make crawl_<spider>` | Single-spider crawl |
| `make update_all` | Incremental crawl of all five spiders |
| `make update_<spider>` | Incremental single-spider crawl |

`DATA_DIR` is set to `../../data`, so outputs land in the repo-root `data/` directory.

### CSV outputs mapped to spiders and source pages

| File | Grain | Row count (repo-root `data/`) | Key fields |
|---|---|---|---|
| `data/events.csv` | One row per event | 765 | `event_id`, `name`, `date`, `city`, `state`, `country`, `fights` (comma-sep fight IDs) |
| `data/fights.csv` | One row per fight | 8,551 | `fight_id`, `event_id`, `fighter_1_id`, `fighter_2_id`, `finish_method`, `weight_class`, `referee` |
| `data/fighters.csv` | One row per fighter | 4,453 | `fighter_id`, `full_name`, `height_*`, `reach_*`, `stance`, `dob`, `record` |
| `data/fight_stats.csv` | One row per fighter per fight (aggregate) | 17,103 | `fight_stat_id`, `fight_id`, `fighter_id`, full strike/takedown/control breakdown |
| `data/fight_stats_by_round.csv` | One row per fighter per round per fight | 40,321 | `fight_stat_by_round_id`, `fight_id`, `fighter_id`, `round`, same stat columns as aggregate |

A secondary copy exists under `scraper/data/` (4 events, 40 fights, 441 fight_stats, 559 fight_stats_by_round rows — recent partial scrapes from March 2026). `scraper/data/fighters.csv` is header-only.

### What is already covered

- Complete historical event listing and per-event metadata (name, date, location, ordered fight IDs).
- Per-fight metadata: fighters, outcome, weight class, finish method, round, time, referee, judges.
- Fighter roster via A-Z listing pages plus profile detail parsing (name, nickname, DOB, height, weight, reach, stance, record).
- Aggregate (fight-level) strike, takedown, control, and knockdown statistics per fighter.
- Round-by-round statistics with the same stat dimensions.
- Incremental update support via ID-based deduplication.
- Rate limiting, autothrottle, and `ROBOTSTXT_OBEY` controls already in place.
- Deterministic UUID5 IDs stable across re-runs.

### What is missing or must be extended for Phase 1

| Gap | Relevant Phase 1 tickets |
|---|---|
| **Raw page capture absent.** No raw HTML is stored. Parsers write directly to CSV. Reprocessing or auditing requires a full re-crawl. | T1.2.2 |
| **No fetch metadata.** `scraped_at` is present in CSVs but there is no per-fetch record of `http_status`, `content_hash`, `job_run_id`, or `storage_path`. | T1.2.2, T1.1.2 |
| **Incremental logic is CSV-only.** `IncrementalCrawlMixin` deduplicates against current parsed CSV contents, not a manifest. Interrupted runs can silently miss pages. | T1.2.3 |
| **No canonical events manifest.** Downstream spiders each rediscover event URLs independently rather than reading from a single authoritative event list. | T1.3.1, T1.3.2 |
| **Upcoming events not in scope.** `crawl_events` seeds only from the completed-events page; there is no discovery path for scheduled upcoming cards. | T1.3.1 |
| **No manifest or report outputs.** There is no machine-readable record of what was attempted, what succeeded, and what failed per run. | T1.1.2, T1.3.4, T1.4.3, T1.5.4 |
| **Fighter queue is implicit.** The fighter spider self-discovers via A-Z pages with no explicit queue file that downstream jobs can inspect or filter. | T1.4.1 |
| **No fight-stats queue.** Stats spiders rediscover fights via event traversal; there is no stable queue of fights that are eligible for stats capture. | T1.5.1 |
| **No smoke-run harness.** There is no documented constrained test run to validate the full acquisition path without a full historical crawl. | T1.2.4 |
| **Missing fighter data.** `reach_in`/`reach_cm`, `stance`, and `dob` are optional and sparse for many fighters; no review flags are generated. | T1.4.3 |
| **`title_fight` and `bonus` flags absent.** The `fights` entity and `Fight` dataclass have no `is_title_fight` or `is_bonus` fields despite these being relevant features in `ufc-predictor.md`. | Scope TBD |
| **No `card_order` field.** Fight ordering within a card is not stored; the event entity holds a comma-separated `fights` list but no position index per fight. | Scope TBD |
| **`nationality` and `gym` absent from fighter profiles.** `ufc-predictor.md` calls for `nationality` and `gym_name`; these are not parsed by `FighterInfoParser`. | Scope TBD |

### Key architectural note: parsed CSVs, not raw-page captures

**The current scraper produces only parsed tabular CSV outputs.** There are no stored raw HTML files, no per-fetch status records, and no content hashes. Every row in `data/*.csv` is a post-parse Python dataclass serialised to CSV at crawl time. This means:

- Any change to parser logic requires a full re-crawl to reprocess historical data.
- There is no audit trail linking a CSV row back to the exact HTTP response that produced it.
- Failures during parsing are not persisted; they appear only in Scrapy's console log.

This gap is the primary structural difference between the current state and the Phase 1 acquisition target defined in `ufc-predictor.md` and T1.2.2. All other Phase 1 hardening work (manifests, reports, incremental idempotency) depends on closing this gap first.

---

## Phase 1 Definition of Done

Phase 1 is done when the following are true:

- event discovery is canonical and no downstream spider has to rediscover event URLs on its own;
- raw page capture exists for event pages, fighter profile pages, and fight stats pages;
- manifests and reports exist for events, fighters, and fight stats;
- reruns are idempotent and resume safely after interruption;
- missing pages and parse failures are visible in reports, not hidden in logs;
- a bounded backfill has been executed successfully and documented;
- Phase 2 can consume Phase 1 outputs without guessing where source data came from.
