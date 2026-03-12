PROJECT_DIR := ufc_scraper
PYTHON ?= python3
CRAWL := $(PYTHON) -m scrapy crawl
CRAWL_LIST := events fighters fights fight_stats fight_stats_by_round
DATA_DIR := ../../data
.PHONY: crawl_all crawl_all_output update_all

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
