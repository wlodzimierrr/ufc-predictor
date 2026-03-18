PROJECT_DIR := ufc_scraper
PYTHON ?= python3
CRAWL := $(PYTHON) -m scrapy crawl
CRAWL_LIST := events fighters fights fight_stats fight_stats_by_round
DATA_DIR := ../../data
N ?= 10
# Bounded backfill: CLOSESPIDER_PAGECOUNT for the events spider.
# 1 listing page + N-1 event detail pages ≈ N-1 events.
# Default 51 → ~50 events.  Override: make backfill_sample BACKFILL_N=26
BACKFILL_N ?= 51
.PHONY: crawl_all crawl_all_output update_all crawl_csv_all smoke check backfill_sample

# Crawl and output to a specific format, e.g. csv, json
crawl_with_output_%:
	cd $(PROJECT_DIR) && \
	mkdir -p $(DATA_DIR) && \
	$(CRAWL) crawl_$* $(if $(OUTPUT),-O $(DATA_DIR)/$*.$(OUTPUT)) $(ARGS)


update_%:
	cd $(PROJECT_DIR) && \
	mkdir -p $(DATA_DIR) && \
	$(CRAWL) crawl_$* -a incremental=1 -o $(DATA_DIR)/$*.csv $(ARGS)


crawl_all_output:
	@for c in $(CRAWL_LIST); do \
		$(MAKE) crawl_with_output_$$c OUTPUT=$(OUTPUT) $(ARGS); \
	done


update_all:
	@for c in $(CRAWL_LIST); do \
		$(MAKE) update_$$c $(ARGS); \
	done


crawl_%:
	cd $(PROJECT_DIR) && \
	$(CRAWL) crawl_$* $(ARGS)


crawl_all:
	@for c in $(CRAWL_LIST); do \
		$(MAKE) crawl_$$c $(ARGS); \
	done


# -- Phase 1 additions -------------------------------------------------------

# Full crawl, write CSV output (single spider). Overwrites existing file.
# Usage: make crawl_csv_events
crawl_csv_%:
	$(MAKE) crawl_with_output_$* OUTPUT=csv


# Full crawl, write CSV output (all spiders in dependency order).
crawl_csv_all:
	@for c in $(CRAWL_LIST); do \
		$(MAKE) crawl_csv_$$c; \
	done


# Bounded sample run: fetch at most N pages then stop (default N=10).
# Use for development validation without a full history crawl.
# Usage: make sample_events N=5
sample_%:
	cd $(PROJECT_DIR) && \
	mkdir -p $(DATA_DIR) && \
	$(CRAWL) crawl_$* -s CLOSESPIDER_PAGECOUNT=$(N) -O $(DATA_DIR)/$*.csv $(ARGS)


# Validate output artifacts from the most recent acquisition run.
# Checks manifest, raw HTML files, and parsed CSVs.  Exits non-zero on failure.
check:
	$(PYTHON) smoke_check.py


# Smoke run: events spider (5 pages) then output validation.
# Passes only when all mandatory artifacts are produced.
smoke:
	$(MAKE) sample_events N=5
	$(MAKE) check


# Build the canonical fight stats queue from local data sources.
# Reads fights.csv, events_manifest.csv, and events.csv; no network calls.
# Usage: make build_stats_queue
build_stats_queue:
	$(PYTHON) build_fight_stats_queue.py


# Fighter missing-data and identity review: flags sparse bios, duplicate names,
# and missing critical fields.  Always exits 0 — flags are informational.
# Writes data/reports/fighter_review.csv.
review_fighters:
	$(PYTHON) fighter_review.py


# Build the canonical fighter profile queue from local data sources.
# Reads data/fighters.csv and data/raw/ufcstats/fights/*.html; no network calls.
# Usage: make build_queue
build_queue:
	$(PYTHON) build_fighter_queue.py


# Fight stats coverage report: reconciles queue, fetch log, and parsed CSVs.
# Writes data/reports/stats_coverage.csv.  Exits non-zero if miss rate > threshold.
# Usage: make report_stats  |  make report_stats THRESHOLD=0.10
report_stats:
	$(PYTHON) stats_coverage_report.py $(if $(THRESHOLD),--threshold $(THRESHOLD))


# Event coverage report: reconciles manifest, fetch log, and parsed CSV.
# Pass THRESHOLD=0.10 to override the default 5% miss-rate tolerance.
# Usage: make report_events  |  make report_events THRESHOLD=0.10
report_events:
	$(PYTHON) event_coverage_report.py $(if $(THRESHOLD),--threshold $(THRESHOLD))


# Bounded end-to-end backfill (T1.6.1).
# Runs the full acquisition pipeline over the most recent ~50 events:
#   1. events spider   — capped at BACKFILL_N HTTP responses (default 51 ≈ 50 events)
#   2. build queues    — fight stats queue + fighter queue from local data
#   3. incremental fetch — fight_stats + fight_stats_by_round for newly queued fights
#   4. all reports     — event coverage, fighter review, stats coverage
#
# Override event sample size:  make backfill_sample BACKFILL_N=26   (~25 events)
# Usage: make backfill_sample
backfill_sample:
	@echo "=== T1.6.1 Bounded backfill  (CLOSESPIDER_PAGECOUNT=$(BACKFILL_N)) ==="
	$(MAKE) sample_events N=$(BACKFILL_N)
	$(MAKE) build_stats_queue
	$(MAKE) build_queue
	$(MAKE) update_fight_stats
	$(MAKE) update_fight_stats_by_round
	$(MAKE) report_events
	$(MAKE) review_fighters
	$(MAKE) report_stats
	@echo "=== Bounded backfill complete — review report output above. ==="
