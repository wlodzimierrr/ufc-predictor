# Phase 2 Execution Tickets

Phase 2 in [ufc-predictor.md](ufc-predictor.md) is `Data Modeling and Storage`. This file turns
that scope into a real implementation backlog.

These tickets assume:

- Phase 1 is complete and signed off (see `docs/tickets.md`, T1.6.2).
- The canonical outputs in `data/` are the source of truth: `events.csv`, `fights.csv`,
  `fighters.csv`, `fight_stats.csv`, `fight_stats_by_round.csv`.
- Phase 2 ends when all five tables are loaded into Postgres, validated, and documented —
  ready for Phase 3 feature engineering.
- Python and SQL are the implementation languages. No ORM; plain SQL DDL and psycopg2 or
  SQLAlchemy Core for loads.

Recommended execution order:

1. Foundation — Postgres environment + normalization contract
2. Schema — DDL tables, indexes, constraints
3. Load — one loader per table group, idempotent upserts
4. Validation — integrity and consistency checks
5. Phase closeout

---

## Foundation

#### T2.1.1 Set up local Postgres environment
- **Description:** Provision a local Postgres instance for development and define the connection
  configuration pattern. All Phase 2 scripts must connect via a shared config so no connection
  strings are hardcoded.
- **Status:** DONE
- **Acceptance Criteria:**
  - A `docker-compose.yml` at the repo root (or `infra/`) starts a Postgres 16 container with
    a named volume for persistence.
  - A `.env.example` documents the required environment variables: `POSTGRES_HOST`,
    `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`.
  - A `warehouse/db.py` module exposes a `get_connection()` helper that reads from env vars and
    returns a psycopg2 connection. No hardcoded credentials anywhere.
  - `docker compose up -d && python warehouse/db.py` confirms a successful connection.
- **Dependencies:** T1.6.2
- **Complexity:** S
- **Risk:** Low
- **Notes:** docker-compose skipped — Postgres 17 already running on homelab at
  `homelab-database.homelab.local`. Connection config implemented instead: `.env.example`
  documents the five required vars; real `.env` is gitignored. `warehouse/db.py` exposes
  `get_connection()` reading from env vars (with optional python-dotenv support).
  `python3 warehouse/db.py` confirmed connection to Postgres 17.9 on homelab.

---

#### T2.1.2 Define and document the normalization rules
- **Description:** Before writing any loader code, document the exact mapping from each CSV
  column to its target DB column: type conversions, enum values, null policy, and derived fields.
  This prevents the loaders from each making independent decisions about edge cases.
- **Status:** DONE
- **Acceptance Criteria:**
  - A `docs/normalization-rules.md` file covers every non-trivial transformation:
    - `fights.bout_type` → `weight_class` (strip "Bout"/"Title Bout" suffix) and
      `is_title_fight` (bool, true if "Title" in `bout_type`).
    - `fights.fighter_1_outcome` / `fighter_2_outcome` → `winner_fighter_id` (UUID of the "W"
      fighter) and `result_type` enum (`win` / `draw` / `nc`). Document what outcome strings
      map to draw and nc.
    - `fights.finish_time_minute` + `finish_time_second` → `finish_time_seconds` (int).
    - `fights.primary_finish_method` + `secondary_finish_method` → final `finish_method`
      enum values (e.g. `decision_unanimous`, `decision_split`, `ko_tko`, `submission`,
      `technical_submission`, `dq`, `nc`).
    - `fight_stats.control_time_minutes` + `control_time_seconds` → `control_time_seconds`
      (int); applies to both aggregate and by-round tables.
    - `fighters`: which columns to load; `wins`, `losses`, `draws`, `no_contests` are scraped
      career totals and are **not** loaded into the warehouse (they will be computed per-fight
      in Phase 3 feature engineering).
    - Null policy: empty string in CSV → `NULL` in DB for all nullable fields.
  - The document lists the exact enum vocabularies for `result_type`, `finish_method`, and
    `weight_class` so loader and schema agree.
- **Dependencies:** T2.1.1
- **Complexity:** S
- **Risk:** Low
- **Notes:** Look at the actual distinct values in the CSVs before writing the doc. The
  `bout_type` and `finish_method` columns have real edge cases (interim title, TKO - Doctor's
  Stoppage, overturned results, etc.) that must be handled explicitly, not silently dropped.

---

## Schema

#### T2.2.1 Create DDL for events and fights tables
- **Description:** Write the Postgres DDL for the `events` and `fights` tables per the
  normalization rules from T2.1.2.
- **Status:** DONE
- **Acceptance Criteria:**
  - `warehouse/sql/001_events.sql` creates the `events` table:

    | Column | Type | Notes |
    |---|---|---|
    | `event_id` | `uuid PRIMARY KEY` | from CSV |
    | `event_name` | `text NOT NULL` | |
    | `event_date` | `date NOT NULL` | from `date_formatted` |
    | `city` | `text` | |
    | `state` | `text` | nullable |
    | `country` | `text` | |
    | `event_status` | `text` | `completed` / `upcoming` |
    | `source_url` | `text NOT NULL` | |
    | `scraped_at` | `timestamptz` | |

  - `warehouse/sql/002_fights.sql` creates the `fights` table:

    | Column | Type | Notes |
    |---|---|---|
    | `fight_id` | `uuid PRIMARY KEY` | from CSV |
    | `event_id` | `uuid NOT NULL REFERENCES events` | |
    | `fighter_1_id` | `uuid NOT NULL` | FK added after fighters table exists |
    | `fighter_2_id` | `uuid NOT NULL` | FK added after fighters table exists |
    | `winner_fighter_id` | `uuid` | nullable; NULL for draws and NC |
    | `result_type` | `text NOT NULL` | `win` / `draw` / `nc` |
    | `weight_class` | `text` | nullable; NULL for ~20 early UFC tournament bouts |
    | `is_title_fight` | `boolean NOT NULL DEFAULT false` | |
    | `is_interim_title` | `boolean NOT NULL DEFAULT false` | |
    | `scheduled_rounds` | `smallint` | from `num_rounds` |
    | `finish_method` | `text` | `decision` / `ko_tko` / `submission` / `doctor_stoppage` / `overturned` / `could_not_continue` / `dq` / `other` |
    | `finish_detail` | `text` | `secondary_finish_method` stored as free text; nullable |
    | `finish_round` | `smallint` | |
    | `finish_time_seconds` | `smallint` | computed from minute + second |
    | `referee` | `text` | nullable |
    | `source_url` | `text NOT NULL` | |
    | `scraped_at` | `timestamptz` | |

  - Both scripts are idempotent (`CREATE TABLE IF NOT EXISTS`).
- **Dependencies:** T2.1.2
- **Complexity:** S
- **Risk:** Low
- **Notes:** FK from `fights` to `fighters` is deferred until `fighters` table is created in
  T2.2.2. Use `ALTER TABLE` in T2.2.4 to add it once both tables exist.
- **Implementation:** Two adjustments from the ticket spec, both driven by normalization rules:
  `weight_class` is nullable (not `NOT NULL`) because ~20 early UFC tournament bouts have no
  extractable weight class. Added `is_interim_title boolean NOT NULL DEFAULT false` and
  `finish_detail text` (stores `secondary_finish_method` free text) per the normalization rules.

---

#### T2.2.2 Create DDL for the fighters table
- **Description:** Write the Postgres DDL for the `fighters` table.
- **Status:** DONE
- **Acceptance Criteria:**
  - `warehouse/sql/003_fighters.sql` creates the `fighters` table:

    | Column | Type | Notes |
    |---|---|---|
    | `fighter_id` | `uuid PRIMARY KEY` | from CSV |
    | `full_name` | `text NOT NULL` | |
    | `first_name` | `text` | nullable |
    | `last_name` | `text` | from `last_names` in CSV |
    | `nickname` | `text` | nullable |
    | `height_cm` | `numeric(5,2)` | nullable |
    | `weight_lbs` | `numeric(5,1)` | nullable |
    | `reach_cm` | `numeric(5,2)` | nullable |
    | `stance` | `text` | nullable |
    | `dob` | `date` | nullable; from `dob_formatted` |
    | `source_url` | `text NOT NULL` | |
    | `scraped_at` | `timestamptz` | |

  - `wins`, `losses`, `draws`, `no_contests` from the CSV are **not** stored; these are
    current career totals at scrape time and will be computed per-fight in Phase 3.
  - Script is idempotent.
- **Dependencies:** T2.1.2
- **Complexity:** S
- **Risk:** Low
- **Notes:** `last_names` in the CSV becomes `last_name` in the DB (singular, consistent with
  convention). Empty strings must be treated as NULL.

---

#### T2.2.3 Create DDL for fight stats tables
- **Description:** Write the Postgres DDL for `fight_stats_aggregate` and
  `fight_stats_by_round`.
- **Status:** DONE
- **Acceptance Criteria:**
  - `warehouse/sql/004_fight_stats.sql` creates both tables. Both share the same stat columns;
    `fight_stats_by_round` adds `round`.
  - Shared stat columns (both tables):

    | Column | Type | Notes |
    |---|---|---|
    | `fight_id` | `uuid NOT NULL REFERENCES fights` | |
    | `fighter_id` | `uuid NOT NULL REFERENCES fighters` | |
    | `knockdowns` | `smallint NOT NULL DEFAULT 0` | |
    | `total_strikes_landed` | `smallint NOT NULL DEFAULT 0` | |
    | `total_strikes_attempted` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_landed` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_attempted` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_head_landed` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_head_attempted` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_body_landed` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_body_attempted` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_leg_landed` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_leg_attempted` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_distance_landed` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_distance_attempted` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_clinch_landed` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_clinch_attempted` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_ground_landed` | `smallint NOT NULL DEFAULT 0` | |
    | `sig_strikes_ground_attempted` | `smallint NOT NULL DEFAULT 0` | |
    | `takedowns_landed` | `smallint NOT NULL DEFAULT 0` | |
    | `takedowns_attempted` | `smallint NOT NULL DEFAULT 0` | |
    | `control_time_seconds` | `smallint NOT NULL DEFAULT 0` | computed: minutes×60 + seconds |
    | `submissions_attempted` | `smallint NOT NULL DEFAULT 0` | |
    | `reversals` | `smallint NOT NULL DEFAULT 0` | |
    | `scraped_at` | `timestamptz` | |

  - `fight_stats_aggregate` PK: `fight_stat_id uuid PRIMARY KEY` (from CSV).
  - `fight_stats_by_round` PK: `fight_stat_by_round_id uuid PRIMARY KEY` (from CSV); adds
    `round smallint NOT NULL`.
  - Both scripts are idempotent.
- **Dependencies:** T2.2.1, T2.2.2
- **Complexity:** S
- **Risk:** Low

---

#### T2.2.4 Add indexes, foreign key constraints, and migration runner
- **Description:** Add the FK from `fights` to `fighters` (deferred from T2.2.1), create
  performance indexes, and write a single migration runner that applies all DDL in order.
- **Status:** DONE
- **Acceptance Criteria:**
  - `warehouse/sql/005_constraints_and_indexes.sql`:
    - Adds FK from `fights.fighter_1_id` and `fights.fighter_2_id` to `fighters.fighter_id`.
    - Adds FK from `fights.winner_fighter_id` to `fighters.fighter_id` (deferrable, nullable).
    - Creates indexes: `fights(event_id)`, `fights(fighter_1_id)`, `fights(fighter_2_id)`,
      `fight_stats_aggregate(fight_id)`, `fight_stats_aggregate(fighter_id)`,
      `fight_stats_by_round(fight_id, fighter_id, round)`.
  - `warehouse/migrate.py` applies SQL files in `warehouse/sql/` in filename order. Idempotent:
    tracks applied migrations in a `schema_migrations` table; skips already-applied files.
  - Running `python warehouse/migrate.py` on a fresh DB creates all tables and indexes cleanly.
  - Running it a second time is a no-op and exits 0.
- **Dependencies:** T2.2.1, T2.2.2, T2.2.3
- **Complexity:** M
- **Risk:** Low
- **Notes:** Keep the migration runner simple — no third-party migration libraries. A
  `schema_migrations(filename text PRIMARY KEY, applied_at timestamptz)` table is enough.

---

## Load

#### T2.3.1 Implement the normalization and transformation layer
- **Description:** Write the Python transformation functions that convert raw CSV rows into
  typed, DB-ready dicts. These functions are the single place where all normalization rules
  from T2.1.2 are applied.
- **Status:** DONE
- **Acceptance Criteria:**
  - `warehouse/transform.py` contains one function per table:
    - `transform_event(row: dict) -> dict`
    - `transform_fight(row: dict) -> dict`
    - `transform_fighter(row: dict) -> dict`
    - `transform_fight_stat(row: dict) -> dict` (used for both aggregate and by-round rows)
  - Each function:
    - Parses dates from ISO strings to `datetime.date`.
    - Converts empty strings to `None`.
    - Derives `weight_class` and `is_title_fight` from `bout_type`.
    - Derives `winner_fighter_id` and `result_type` from `fighter_1_outcome` / `fighter_2_outcome`.
    - Computes `finish_time_seconds` and `control_time_seconds`.
    - Maps `primary_finish_method` + `secondary_finish_method` to the `finish_method` enum
      from the normalization rules doc.
  - Unit tests in `warehouse/tests/test_transform.py` cover each function with at least:
    - A standard win row (KO/TKO, decision, submission).
    - A draw row.
    - A no-contest row.
    - A row with empty/null physical attributes.
    - A title-fight `bout_type`.
- **Dependencies:** T2.1.2
- **Complexity:** M
- **Risk:** Medium
- **Notes:** Keep transform functions pure (no DB calls, no file I/O). This makes them easy to
  test and reuse in the upcoming-fight scoring pipeline later.

---

#### T2.3.2 Implement the upsert helper
- **Description:** Write a generic idempotent upsert function used by all loaders. All load
  jobs must be re-runnable without creating duplicates.
- **Status:** DONE
- **Acceptance Criteria:**
  - `warehouse/db.py` gains an `upsert(conn, table, rows, pk_columns)` function that:
    - Builds and executes `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` for each batch of rows.
    - Accepts a list of dicts; columns are inferred from dict keys.
    - Handles `scraped_at` by keeping the more recent value on conflict.
  - Unit test confirms that inserting the same row twice leaves exactly one row in the table.
- **Dependencies:** T2.1.1
- **Complexity:** S
- **Risk:** Low
- **Notes:** Use `execute_values` from psycopg2.extras for batch inserts. A batch size of 500
  rows is a sensible default.

---

#### T2.3.3 Load events and fights
- **Description:** Implement and run the loaders for `events` and `fights`.
- **Status:** DONE
- **Acceptance Criteria:**
  - `warehouse/load_events.py`: reads `data/events.csv` + `data/manifests/events_manifest.csv`
    (for `event_status`), applies `transform_event()`, upserts into `events`.
  - `warehouse/load_fights.py`: reads `data/fights.csv`, applies `transform_fight()`, upserts
    into `fights`.
  - `make load_events && make load_fights` succeeds with exit 0.
  - Row counts in DB match (or are a known subset of) CSV row counts.
  - No FK violations — all `event_id` values in `fights` exist in `events`.
- **Dependencies:** T2.2.4, T2.3.1, T2.3.2
- **Complexity:** M
- **Risk:** Low
- **Notes:** Load order matters: `events` before `fights`. The events manifest provides
  `event_status`; if a fight's `event_id` is absent from `events` the load should log a warning
  and skip that row, not crash.

---

#### T2.3.4 Load fighters
- **Description:** Implement and run the fighters loader.
- **Status:** DONE — implemented as part of T2.3.3 (required before fights due to FK constraints)
- **Acceptance Criteria:**
  - `warehouse/load_fighters.py`: reads `data/fighters.csv`, applies `transform_fighter()`,
    upserts into `fighters`.
  - `make load_fighters` succeeds with exit 0.
  - Row count in DB ≥ row count in CSV (all fighters loaded).
  - All `fighter_1_id` and `fighter_2_id` values in the `fights` table have a corresponding
    row in `fighters` after the load.
- **Dependencies:** T2.2.4, T2.3.1, T2.3.2
- **Complexity:** S
- **Risk:** Low
- **Notes:** Fighters can be loaded independently of events/fights (no FK dependency on load
  order for fighters, only for the FK check post-load).

---

#### T2.3.5 Load fight stats (aggregate and by round)
- **Description:** Implement and run the loaders for both stats tables.
- **Status:** DONE
- **Acceptance Criteria:**
  - `warehouse/load_fight_stats.py`: reads `data/fight_stats.csv` and
    `data/fight_stats_by_round.csv`, applies `transform_fight_stat()` to each, upserts into
    `fight_stats_aggregate` and `fight_stats_by_round` respectively.
  - `make load_fight_stats` succeeds with exit 0.
  - All `fight_id` values in both stats tables exist in `fights`.
  - Row count: `fight_stats_aggregate` ≈ 2 × (queued fights with stats); by-round count ≈
    aggregate count × average rounds (typically 3–5×).
- **Dependencies:** T2.3.3, T2.3.4
- **Complexity:** S
- **Risk:** Low
- **Notes:** Load after events, fights, and fighters so FK constraints are satisfied.

---

## Validation

#### T2.4.1 Post-load integrity checks
- **Description:** Write and run a validation script that confirms the loaded warehouse is
  internally consistent: row counts match expectations, FK relationships are intact, and no
  orphaned rows exist.
- **Status:** DONE
- **Acceptance Criteria:**
  - `warehouse/validate_integrity.py` checks and reports on:
    - Row counts for all five tables vs expected ranges (derived from CSV counts).
    - FK completeness: all `event_id`, `fighter_id`, `fight_id` references resolve.
    - Orphaned fight stats: `fight_id` in `fight_stats_aggregate` not found in `fights`.
    - Fights with no stats rows: fights in `fights` table absent from `fight_stats_aggregate`
      (expected for ~0.2% of fights; report count).
    - Fighters referenced in `fights` but absent from `fighters` table.
  - Script prints PASS/FAIL per check, exits non-zero if any FK check fails.
  - `make validate_integrity` runs the script.
- **Dependencies:** T2.3.3, T2.3.4, T2.3.5
- **Complexity:** M
- **Risk:** Low

---

#### T2.4.2 Consistency checks
- **Description:** Validate logical consistency within the data: aggregate stats approximately
  match round sums, result states are mutually valid, and weight classes are within the known
  vocabulary.
- **Status:** DONE
- **Acceptance Criteria:**
  - `warehouse/validate_consistency.py` checks:
    - For each fight with both aggregate and round stats: aggregate `sig_strikes_landed` ≈ sum
      of per-round values (within ±1 for rounding). Report fights where the difference exceeds
      threshold.
    - `result_type = 'win'` ↔ `winner_fighter_id IS NOT NULL`.
    - `result_type IN ('draw', 'nc')` ↔ `winner_fighter_id IS NULL`.
    - `finish_round <= scheduled_rounds` for all fights.
    - `weight_class` values are all within the known UFC weight-class vocabulary (log any
      unrecognized values as warnings, not failures).
  - Script prints PASS/FAIL per check. Aggregate-round discrepancies exit 0 but print a
    summary count (some minor discrepancies are expected from the source data).
  - `make validate_consistency` runs the script.
- **Dependencies:** T2.4.1
- **Complexity:** M
- **Risk:** Low
- **Notes:** The aggregate ≈ round-sum check is a data quality signal, not a hard blocker.
  Flag fights that fail but do not fail the whole run — they are known to exist in ufcstats.com
  source data.

---

## Phase Closeout

#### T2.5.1 Add Makefile targets and publish Phase 2 runbook
- **Description:** Wire up the Phase 2 commands into the Makefile and update the docs runbook
  to cover warehouse setup, migration, load, and validation.
- **Status:** DONE
- **Acceptance Criteria:**
  - `Makefile` (repo root or `warehouse/Makefile`) gains targets:
    - `migrate` — runs `python warehouse/migrate.py`
    - `load_events`, `load_fights`, `load_fighters`, `load_fight_stats`
    - `load_all` — runs all four loaders in dependency order
    - `validate_integrity`, `validate_consistency`
    - `warehouse_check` — runs both validators in sequence
    - `warehouse_up` — full sequence: migrate → load_all → warehouse_check
  - `docs/runbook.md` gains a **Phase 2 Warehouse** section covering: setup (`docker compose
    up`), migration, full load, incremental load (re-running loaders is idempotent), and
    validation.
  - A **Phase 2 handoff checklist** section documents what Phase 3 (feature engineering) can
    rely on:
    - All five warehouse tables loaded and FK-validated.
    - `validate_integrity` exits 0.
    - Known data quality exceptions documented (0.2% fights with no stats, ~14% sparse fighter
      bios, 7 duplicate-name pairs).
- **Dependencies:** T2.4.2
- **Complexity:** S
- **Risk:** Low
