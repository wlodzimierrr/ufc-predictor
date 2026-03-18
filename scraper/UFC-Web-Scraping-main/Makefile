PROJECT_DIR := ufc_scraper
PYTHON ?= python3
CRAWL := $(PYTHON) -m scrapy crawl
CRAWL_LIST := events fighters fights fight_stats fight_stats_by_round
DATA_DIR := ../../data
N ?= 10
.PHONY: crawl_all crawl_all_output update_all crawl_csv_all smoke check

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


# Event coverage report: reconciles manifest, fetch log, and parsed CSV.
# Pass THRESHOLD=0.10 to override the default 5% miss-rate tolerance.
# Usage: make report_events  |  make report_events THRESHOLD=0.10
report_events:
	$(PYTHON) event_coverage_report.py $(if $(THRESHOLD),--threshold $(THRESHOLD))
