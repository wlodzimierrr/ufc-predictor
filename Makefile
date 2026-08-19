PYTHON ?= python3
BETTING_BACKTEST_COMPARE_ARGS ?= --line-type opening --odds-policy latest-before-prediction

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

load_all: load_events load_fighters load_fights load_stats load_upcoming

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

review_all_events:
	$(PYTHON) modeling/backtest_past_events.py

backtest_past: review_all_events

pre_event_log:
	$(PYTHON) modeling/build_pre_event_prediction_log.py

train_all_v2: train_lgbm_v2 train_xgb train_ensemble compare_models

# ── Betting ─────────────────────────────────────────────────────────────────

load_odds:
	$(PYTHON) warehouse/load_fight_odds.py $(ARGS)

adapt_kaggle_odds:
	$(PYTHON) warehouse/adapt_kaggle_odds.py $(ARGS)

betting_recommendations:
	$(PYTHON) betting/recommend.py $(ARGS)

betting_recommendations_compare:
	$(PYTHON) betting/recommend.py --report-dir data/reports/betting_default $(ARGS)
	$(PYTHON) betting/recommend.py --config configs/betting_conservative_candidate.toml $(ARGS)

betting_backtest:
	$(PYTHON) betting/backtest.py $(ARGS)

betting_backtest_compare:
	$(PYTHON) betting/backtest.py --max-one-bet-per-fight --report-dir data/reports/betting_default $(BETTING_BACKTEST_COMPARE_ARGS) $(ARGS)
	$(PYTHON) betting/backtest.py --max-one-bet-per-fight --config configs/betting_conservative_candidate.toml $(BETTING_BACKTEST_COMPARE_ARGS) $(ARGS)

betting_tune_policy:
	$(PYTHON) betting/tune_policy.py $(ARGS)

test_betting:
	$(PYTHON) -m pytest betting/tests warehouse/tests/test_adapt_kaggle_odds.py warehouse/tests/test_load_fight_odds.py warehouse/tests/test_betting_odds_migration.py -v

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
        compare_models score_upcoming predict review_event review_all_events backtest_past pre_event_log \
        uncertainty_analysis error_analysis train_all_v2 \
        load_odds adapt_kaggle_odds betting_recommendations betting_recommendations_compare \
        betting_backtest betting_backtest_compare betting_tune_policy test_betting \
        test_integration predict_pipeline \
        refresh_scrape post_event
