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

load_all: load_events load_fighters load_fights load_stats

validate_integrity:
	$(PYTHON) warehouse/validate_integrity.py

.PHONY: migrate load_events load_fights load_fighters load_stats load_all validate_integrity
