PYTHON ?= python3

# ── Warehouse ──────────────────────────────────────────────────────────────────

migrate:
	$(PYTHON) warehouse/migrate.py

load_events:
	$(PYTHON) warehouse/load_events.py

load_fights:
	$(PYTHON) warehouse/load_fights.py

load_fighters:
	$(PYTHON) warehouse/load_fighters.py

load_stats:
	$(PYTHON) warehouse/load_fight_stats.py

load_upcoming:
	$(PYTHON) warehouse/load_upcoming_fights.py

load_all: load_events load_fighters load_fights load_stats

validate_integrity:
	$(PYTHON) warehouse/validate_integrity.py

validate_consistency:
	$(PYTHON) warehouse/validate_consistency.py

warehouse_check: validate_integrity validate_consistency

warehouse_up: migrate load_all warehouse_check

# ── Features ──────────────────────────────────────────────────────────────────

build_features:
	$(PYTHON) features/pipeline.py

test_leakage:
	$(PYTHON) -m pytest features/tests/test_leakage.py -v

validate_features:
	$(PYTHON) features/validate_features.py

build_upcoming_features:
	$(PYTHON) features/build_upcoming.py

features_up: build_features test_leakage validate_features

# ── Modeling ─────────────────────────────────────────────────────────────────

train_logreg:
	$(PYTHON) modeling/train_logreg.py

train_lgbm:
	$(PYTHON) modeling/train_lgbm.py

train_lgbm_v2:
	$(PYTHON) modeling/train_lgbm_v2.py

train_xgb:
	$(PYTHON) modeling/train_xgb.py

train_ensemble:
	$(PYTHON) modeling/train_ensemble.py

compare_models:
	$(PYTHON) modeling/compare.py

uncertainty_analysis:
	$(PYTHON) modeling/uncertainty.py

error_analysis:
	$(PYTHON) modeling/error_analysis.py

score_upcoming:
	$(PYTHON) modeling/score_upcoming.py

predict:
	$(PYTHON) predict.py

review_event:
	$(PYTHON) modeling/post_event_review.py --event "$(EVENT)"

train_all_v2: train_lgbm_v2 train_xgb train_ensemble compare_models

# ── Post-event refresh ────────────────────────────────────────────────────────
# Full refresh: re-scrape with no cache, reload warehouse, rebuild predictions.
# Usage: make post_event

SCRAPER_DIR := scraper/UFC-Web-Scraping-main

refresh_scrape:
	cd $(SCRAPER_DIR) && $(MAKE) refresh_all

post_event: refresh_scrape load_all load_upcoming build_upcoming_features predict

# ── End-to-end prediction pipeline ──────────────────────────────────────────

test_integration:
	$(PYTHON) -m pytest tests/test_integration_pipeline.py -v

predict_pipeline: load_upcoming build_upcoming_features score_upcoming

.PHONY: migrate load_events load_fights load_fighters load_stats load_upcoming load_all \
        validate_integrity validate_consistency warehouse_check warehouse_up \
        build_features build_upcoming_features test_leakage validate_features features_up \
        train_logreg train_lgbm train_lgbm_v2 train_xgb train_ensemble \
        compare_models score_upcoming predict review_event \
        uncertainty_analysis error_analysis train_all_v2 \
        test_integration predict_pipeline \
        refresh_scrape post_event
