# Phase 3 Execution Tickets

Phase 3 in [ufc-predictor.md](ufc-predictor.md) is `Feature Engineering`. This file turns
that scope into a real implementation backlog.

**Goal:** Transform the warehouse tables (events, fighters, fights, fight_stats) into
model-ready feature rows — one per fight, containing pre-fight snapshots for both fighters
with difference, ratio, and matchup features. Every feature must use only information
available before the fight date (leakage prevention).

**Architecture decisions:**
- Features are computed in Python, not SQL. Complex logic (exponential decay, rolling windows,
  Elo) is easier to test and iterate on in Python.
- All data is loaded into memory once and processed in chronological order. This avoids ~17k
  DB round-trips and naturally supports Elo-style sequential updates.
- Feature modules live in `features/` as pure functions. No DB calls inside feature code.
- Two output tables: `fighter_snapshots` (one row per fighter per fight they participate in)
  and `bout_features` (one row per fight, model-ready).

**Data baseline (from Phase 2 handoff):**
- 764 events (1994–2026), 4,452 fighters, 8,550 fights, 8,531 fights with stats
- Median fighter has 4 fights; 25th percentile = 2, 95th = 19
- 89% of fighters have DOB; 56% have both height and reach
- 0.2% of fights have no stats rows (19 fights)

---

## T3.1 — Foundation

#### T3.1.1 Feature catalog document
- **Description:** Define every feature that will be computed, its formula, source columns,
  null-handling strategy, and feature family. This document is the single reference for what
  the feature pipeline produces.
- **Status:** DONE
- **Acceptance Criteria:**
  - `docs/feature-catalog.md` lists every feature grouped by family:
    - Career aggregates (~20 features)
    - Rolling windows (~15 features × 3 windows = ~45)
    - Exponentially decayed metrics (~10 features)
    - Physical / demographic / activity (~10 features)
    - Elo and opponent-adjusted (~5 features)
    - Bout-level difference and ratio (~20 features)
  - Each feature entry includes: name, formula, source table/columns, null handling, and type
    (int / float / bool / categorical).
  - Missingness strategy documented: which features get a `_missing` indicator flag, which
    use imputation, which are simply NULL.
  - Leakage prevention rule stated per family (cutoff = event_date, exclude target fight).
- **Dependencies:** Phase 2 complete
- **Complexity:** M
- **Risk:** Low
- **Notes:** This is a design document, not code. It serves as the spec for T3.2.x–T3.4.x
  and should be updated if features change during implementation.

#### T3.1.2 Feature schema DDL and migration
- **Description:** Create the database tables that store computed features.
- **Status:** DONE
- **Acceptance Criteria:**
  - `warehouse/sql/007_feature_tables.sql` creates:
    - `fighter_snapshots` — one row per fighter per fight they participate in:
      - PK: `(fighter_id, fight_id)`
      - `as_of_date` (date) — the event_date of the fight (cutoff)
      - ~100 feature columns (career, rolling, decayed, physical, Elo)
      - `feature_version` (smallint) — schema version for reproducibility
      - `computed_at` (timestamptz)
    - `bout_features` — one row per fight, model-ready:
      - PK: `fight_id`
      - `event_date`, `weight_class`, `is_title_fight` (metadata)
      - `fighter_1_id`, `fighter_2_id` (for joining back to fighters)
      - ~60 difference/ratio/matchup columns
      - `label` (smallint) — 1 if fighter_1 wins, 0 if fighter_2 wins, NULL for draw/NC
      - `feature_version`, `computed_at`
  - `make migrate` applies the new migration.
  - Indexes on `fighter_snapshots(fighter_id)` and `bout_features(event_date)`.
- **Dependencies:** T3.1.1
- **Complexity:** S
- **Risk:** Low
- **Notes:** Exact column list will come from T3.1.1. The schema can be updated with
  additional migrations as features are added during implementation.

---

## T3.2 — Fighter Feature Modules

All feature modules are pure Python functions in `features/`. They receive pre-loaded fight
history data (lists of dicts) and return feature dicts. No DB calls, no file I/O.

#### T3.2.1 Data loader and fight history index
- **Description:** Load all warehouse data into memory and build an index that provides each
  fighter's chronologically ordered fight history up to any cutoff date. This is the shared
  foundation for all feature modules.
- **Status:** DONE
- **Acceptance Criteria:**
  - `features/data_loader.py` contains:
    - `load_all_data(conn) -> WarehouseData` — loads events, fights, fighters, and
      fight_stats_aggregate into memory as lists of dicts (or dataclass).
    - `WarehouseData` (dataclass or named tuple) holding all four datasets plus lookup indexes.
  - `features/history.py` contains:
    - `FightHistory` — a dict (or dataclass) representing one historical fight for a fighter:
      fight metadata, the fighter's stats, and the opponent's stats.
    - `build_fighter_index(data: WarehouseData) -> dict[str, list[FightHistory]]` — returns
      `{fighter_id: [FightHistory, ...]}` sorted by event_date ascending.
    - `get_history(index, fighter_id, cutoff_date) -> list[FightHistory]` — returns all fights
      for the fighter before (strictly less than) the cutoff date.
  - Unit tests confirm:
    - A fighter with 5 fights returns 0, 1, 2, 3, 4 history entries at successive cutoffs.
    - The target fight is never included in the returned history.
    - Fights on the same date as the cutoff are excluded.
- **Dependencies:** T3.1.1
- **Complexity:** M
- **Risk:** Low
- **Notes:** Using in-memory data avoids ~17k DB queries. Total data fits in <100 MB RAM.
  The index is built once and reused for all 8,550 fights.

#### T3.2.2 Career aggregate features
- **Description:** Compute cumulative career statistics from a fighter's full pre-fight history.
- **Status:** DONE
- **Acceptance Criteria:**
  - `features/career.py` contains `compute_career_features(history: list[FightHistory]) -> dict`
    returning:
    - Record: `total_fights`, `wins`, `losses`, `draws`, `no_contests`, `win_rate`
    - Finish profile: `ko_tko_wins`, `sub_wins`, `dec_wins`, `ko_tko_losses`, `sub_losses`,
      `dec_losses` (raw counts + rates as fraction of total fights)
    - Title: `title_fights`, `title_wins`
    - Time: `avg_fight_time_seconds`, `total_cage_time_seconds`
    - Streaks: `current_win_streak`, `current_lose_streak`
    - Striking: `career_sig_strikes_landed_per_min`, `career_sig_strikes_absorbed_per_min`,
      `career_sig_strike_accuracy`, `career_sig_strike_defense`
    - Grappling: `career_takedown_accuracy`, `career_takedown_defense`,
      `career_takedowns_per_15min`, `career_submissions_per_15min`
    - Dominance: `career_knockdowns_per_fight`, `career_control_time_per_fight`
  - All rates return `None` when the denominator is zero (e.g. no fights yet).
  - Unit tests cover: 0-fight history (debut), 1-fight history, multi-fight history with
    mixed outcomes (KO win, decision loss, draw, NC).
- **Dependencies:** T3.2.1
- **Complexity:** M
- **Risk:** Low
- **Notes:** Fight time is computed as `finish_round * 5*60 + finish_time_seconds` for the
  actual elapsed time. Per-minute rates use total cage time as the denominator.

#### T3.2.3 Rolling window features
- **Description:** Compute statistics over the most recent 1, 3, and 5 fights.
- **Status:** DONE
- **Acceptance Criteria:**
  - `features/rolling.py` contains
    `compute_rolling_features(history: list[FightHistory], windows=[1, 3, 5]) -> dict`
    returning for each window size N:
    - `last{N}_wins` — wins in last N fights
    - `last{N}_sig_strikes_landed_per_min`
    - `last{N}_sig_strikes_absorbed_per_min`
    - `last{N}_sig_strike_accuracy`
    - `last{N}_takedown_accuracy`
    - `last{N}_control_time_per_fight`
    - `last{N}_knockdowns_per_fight`
    - `last{N}_finish_rate` — fraction of last N fights ending by finish (KO/sub)
  - If a fighter has fewer than N fights, the window uses all available fights. The feature
    value is still computed (not NULL), but a `has_N_fights` boolean flag is included.
  - Unit tests cover: fighter with exactly 1, 3, and 10 prior fights.
- **Dependencies:** T3.2.1
- **Complexity:** M
- **Risk:** Low
- **Notes:** Rolling windows use the most recent N fights from the history list (which is
  already sorted ascending by date — take the last N entries).

#### T3.2.4 Exponentially decayed metrics
- **Description:** Compute recency-weighted versions of key statistics using exponential decay.
- **Status:** DONE
- **Acceptance Criteria:**
  - `features/decay.py` contains
    `compute_decayed_features(history: list[FightHistory], half_life_days=365) -> dict`
    returning:
    - `decay_sig_strike_rate` — recency-weighted sig strikes landed per minute
    - `decay_sig_strike_accuracy`
    - `decay_takedown_rate` — recency-weighted takedowns per 15 min
    - `decay_takedown_accuracy`
    - `decay_control_time_per_fight`
    - `decay_knockdowns_per_fight`
    - `decay_win_rate` — recency-weighted win probability
    - `decay_finish_rate`
  - Decay weight for fight i: `w_i = 2^(−days_since_fight_i / half_life_days)`
  - Each metric is a weighted average: `sum(w_i * value_i) / sum(w_i)`
  - The `cutoff_date` (the date of the fight being predicted) is passed in so that
    `days_since_fight_i = (cutoff_date - fight_i.event_date).days`.
  - All features return `None` for 0-fight history.
  - Unit tests cover: recent fights get more weight than old fights; single-fight history;
    half-life boundary (a fight exactly half_life_days ago gets weight 0.5).
- **Dependencies:** T3.2.1
- **Complexity:** M
- **Risk:** Low
- **Notes:** Half-life of 365 days means a fight 1 year ago has weight 0.5, 2 years ago
  has weight 0.25, etc. This parameter can be tuned in Phase 4. The function should accept
  `half_life_days` as a parameter so it's easy to experiment.

#### T3.2.5 Physical, demographic, and activity features
- **Description:** Compute features from fighter profile data and fight timing.
- **Status:** DONE
- **Acceptance Criteria:**
  - `features/physical.py` contains
    `compute_physical_features(fighter: dict, history: list[FightHistory], cutoff_date) -> dict`
    returning:
    - **Physical:** `height_cm`, `reach_cm`, `weight_lbs`, `reach_to_height_ratio`
    - **Demographic:** `age_at_fight` (years, from DOB and cutoff_date), `age_squared`
    - **Stance:** `stance` (categorical: orthodox / southpaw / switch / None)
    - **Activity:** `days_since_last_fight` (NULL for debut), `is_long_layoff` (>365 days),
      `fights_per_year` (career fights / career span in years)
    - **Experience:** `is_debut`, `ufc_fight_count` (same as total_fights from career),
      `five_round_experience` (count of 5-round fights), `title_fight_experience` (count)
    - **Missingness flags:** `height_missing`, `reach_missing`, `dob_missing` (booleans)
  - All physical attributes pass through as-is from the fighters table (no imputation here —
    that's a modeling concern in Phase 4).
  - Unit tests cover: fighter with full profile, fighter with all-null profile, debut fighter.
- **Dependencies:** T3.2.1
- **Complexity:** S
- **Risk:** Low
- **Notes:** Missingness flags let Phase 4 models decide how to handle sparse data.
  56% of fighters have both height and reach; 89% have DOB.

---

## T3.3 — Ratings and Opponent Adjustment

#### T3.3.1 Elo rating system
- **Description:** Implement a sequential Elo rating that processes all fights in date order
  and produces a pre-fight Elo rating for each fighter.
- **Status:** DONE
- **Acceptance Criteria:**
  - `features/elo.py` contains:
    - `compute_all_elos(fights: list[dict], k=32, initial=1500) -> dict[str, dict[str, float]]`
      — processes all fights in date order, returns `{fight_id: {fighter_id: pre_fight_elo}}`
      for every fighter in every fight.
    - Standard Elo formula: `E_a = 1 / (1 + 10^((R_b - R_a) / 400))`, update by
      `R_a_new = R_a + K * (S_a - E_a)` where S_a = 1 for win, 0.5 for draw, 0 for loss.
    - No-contests are skipped (no rating change).
    - Draws give 0.5 to both fighters.
  - Pre-fight Elo is stored as a feature in the fighter snapshot.
  - Additionally computes: `elo_change_last_fight` (how much Elo changed from their most
    recent fight), `opponent_pre_fight_elo` (opponent's Elo going into this fight).
  - Unit tests:
    - Two fighters, one wins: winner's Elo goes up, loser's goes down.
    - Draw: both move toward each other.
    - NC: no change.
    - Fighter with 0 prior fights starts at `initial`.
- **Dependencies:** T3.2.1
- **Complexity:** M
- **Risk:** Low
- **Notes:** K-factor of 32 is a reasonable starting point for MMA (high variance sport).
  Can be tuned in Phase 4. The Elo computation must process fights in strict chronological
  order — it's a single sequential pass over all fights, not per-fighter.

#### T3.3.2 Opponent-adjusted metrics
- **Description:** Adjust a fighter's key stats relative to their opponents' baseline allowed
  rates. A fighter who lands 5 sig strikes/min against opponents who typically allow only
  3/min is performing above expectation.
- **Status:** DONE
- **Acceptance Criteria:**
  - `features/opponent.py` contains
    `compute_opponent_adjusted(history: list[FightHistory], fighter_index) -> dict` returning:
    - `opp_adjusted_sig_strike_rate` — fighter's sig_strikes_landed_per_min divided by the
      career average sig_strikes_absorbed_per_min of each opponent (geometric mean across
      fights, or simple mean).
    - `opp_adjusted_takedown_rate` — similarly adjusted takedown rate.
    - `opp_adjusted_control_rate` — similarly adjusted control time.
    - `avg_opponent_elo` — mean pre-fight Elo of all past opponents (requires Elo data).
    - `avg_opponent_win_rate` — mean career win rate of all past opponents at the time of
      the fight.
  - Returns `None` for all features if history is empty.
  - Unit tests cover: fighter with opponents of varying quality; debut (no opponents).
- **Dependencies:** T3.2.2, T3.3.1
- **Complexity:** M
- **Risk:** Medium — opponent baseline rates require looking up each opponent's career stats,
  which means the feature index must be accessible.
- **Notes:** This is the most complex feature family. The opponent's "allowed rate" is their
  career average of that stat _absorbed_ across their own prior fights. For opponent Elo,
  use the pre-fight Elo snapshot from T3.3.1.

---

## T3.4 — Feature Pipeline

#### T3.4.1 Snapshot and bout feature row builder
- **Description:** Orchestrate all feature modules into a complete fighter snapshot, then
  merge two fighter snapshots into a model-ready bout feature row.
- **Status:** DONE
- **Acceptance Criteria:**
  - `features/snapshot.py` contains:
    - `build_fighter_snapshot(fighter, history, cutoff_date, elo_data, fighter_index) -> dict`
      — calls `compute_career_features()`, `compute_rolling_features()`,
      `compute_decayed_features()`, `compute_physical_features()`,
      `compute_opponent_adjusted()`, and merges results with Elo data into one flat dict.
  - `features/bout.py` contains:
    - `build_bout_features(fight, snapshot_a, snapshot_b) -> dict` — creates a model-ready
      row with:
      - **Metadata:** `fight_id`, `event_date`, `weight_class`, `is_title_fight`,
        `scheduled_rounds`, `fighter_1_id`, `fighter_2_id`
      - **Difference features (A − B):** `age_diff`, `height_diff`, `reach_diff`,
        `elo_diff`, `win_rate_diff`, `career_sig_strike_rate_diff`,
        `career_takedown_rate_diff`, `career_control_time_diff`, `experience_diff`
      - **Ratio features (A / B, with 0-safe division):** `experience_ratio`,
        `win_rate_ratio`, `sig_strike_accuracy_ratio`, `takedown_accuracy_ratio`
      - **Matchup flags:** `stance_matchup` (e.g. "orthodox_vs_southpaw"),
        `is_reach_advantage_a` (bool), `is_experience_advantage_a` (bool)
      - **Label:** `label = 1` if fighter_1 wins, `0` if fighter_2 wins, `NULL` for draw/NC
      - **Feature version and timestamp**
  - Unit tests: build a snapshot from mock history, build a bout row from two snapshots,
    verify difference features are actually A − B.
- **Dependencies:** T3.2.2, T3.2.3, T3.2.4, T3.2.5, T3.3.1, T3.3.2
- **Complexity:** M
- **Risk:** Low
- **Notes:** Difference features must be consistent: always fighter_1 minus fighter_2.
  The model in Phase 4 will handle symmetry (a negative diff means fighter_2 is favored).

#### T3.4.2 Full build pipeline and persistence
- **Description:** Wire up the full pipeline: load data → compute Elos → build snapshots
  and bout rows for all fights → persist to database.
- **Status:** DONE
- **Acceptance Criteria:**
  - `features/pipeline.py` contains:
    - `build_all_features(conn) -> tuple[int, int]` — returns (snapshots_written,
      bout_rows_written):
      1. Load all warehouse data into memory via `load_all_data()`.
      2. Build fighter index via `build_fighter_index()`.
      3. Compute all Elos via `compute_all_elos()`.
      4. For each fight in chronological order:
         a. Get fighter_1 and fighter_2 history before cutoff.
         b. Build fighter_1 snapshot and fighter_2 snapshot.
         c. Build bout feature row.
         d. Collect into batches.
      5. Upsert all `fighter_snapshots` rows.
      6. Upsert all `bout_features` rows.
  - `make build_features` runs the pipeline.
  - Pipeline logs progress every 1000 fights.
  - Full build completes in under 5 minutes on the homelab.
  - Re-running is idempotent (upsert on PK).
  - Row counts after build:
    - `fighter_snapshots`: ~17,100 rows (2 per fight × 8,550 fights)
    - `bout_features`: ~8,550 rows (one per fight)
- **Dependencies:** T3.1.2, T3.4.1
- **Complexity:** M
- **Risk:** Medium — performance and memory usage need attention for 8,550 fights.
- **Notes:** The pipeline processes fights in date order. Elo is computed in a single pass
  first, then snapshots are built per-fight. The batch upsert at the end uses the existing
  `warehouse.db.upsert()` function.

---

## T3.5 — Validation

#### T3.5.1 Leakage prevention tests
- **Description:** Write tests that prove no feature uses data from the target fight or any
  fight after the cutoff date.
- **Status:** TODO
- **Acceptance Criteria:**
  - `features/tests/test_leakage.py` contains:
    - **Temporal exclusion test:** For a known fight on date D, verify that the fighter's
      snapshot contains no information from fights on or after date D.
    - **Target fight exclusion test:** Verify that the target fight's stats are not included
      in either fighter's snapshot features.
    - **Monotonic history test:** For a fighter with 10 fights, verify that `total_fights` in
      their snapshot increases by exactly 1 for each successive fight they participate in.
    - **Elo causality test:** Verify that a fighter's pre-fight Elo at fight N reflects only
      the outcomes of fights 1 through N−1.
    - **Label isolation test:** Verify that `bout_features.label` is derived solely from
      `fights.result_type` and `fights.winner_fighter_id`, not from any feature column.
  - All tests use real warehouse data (integration tests against the homelab DB).
  - `make test_leakage` runs the suite; exits non-zero on any failure.
- **Dependencies:** T3.4.2
- **Complexity:** M
- **Risk:** Low
- **Notes:** Leakage is the single biggest risk in fight prediction modeling. These tests
  are the safety net. They should be run after every feature pipeline change.

#### T3.5.2 Feature quality and distribution checks
- **Description:** Validate feature distributions, missingness rates, and correlations.
- **Status:** TODO
- **Acceptance Criteria:**
  - `features/validate_features.py` checks and reports:
    - **Missingness rates:** percentage of NULL values per feature column; flag any feature
      with >50% NULL as a warning.
    - **Distribution sanity:** mean, std, min, max, p5, p95 for all numeric features; flag
      any feature with zero variance.
    - **Correlation with label:** Pearson correlation of each feature with `label`; features
      with |r| > 0.5 are suspicious (potential leakage or trivially predictive).
    - **Row counts:** `fighter_snapshots` ≈ 2 × fights count; `bout_features` ≈ fights count.
    - **Feature completeness:** bout_features rows where all core difference features are
      non-NULL (expected: high percentage for experienced fighters, lower for debuts).
  - Script prints a summary report and exits 0.
  - `make validate_features` runs the script.
- **Dependencies:** T3.4.2
- **Complexity:** S
- **Risk:** Low
- **Notes:** This is an informational check, not a hard pass/fail gate. It helps catch
  feature engineering bugs early (e.g. a feature that's always 0, or always NULL).

---

## T3.6 — Closeout

#### T3.6.1 Makefile targets and Phase 3 runbook
- **Description:** Add feature pipeline commands to the Makefile and document the Phase 3
  workflow in the runbook.
- **Status:** TODO
- **Acceptance Criteria:**
  - `Makefile` gains targets:
    - `build_features` — runs `features/pipeline.py`
    - `test_leakage` — runs leakage tests
    - `validate_features` — runs feature quality checks
    - `features_up` — full sequence: build_features → test_leakage → validate_features
  - `docs/runbook.md` gains a **Phase 3 Feature Engineering** section covering:
    - How to run the feature pipeline
    - How to add a new feature (which files to modify)
    - Leakage prevention rules
    - Known limitations (sparse physical data, debut fighters)
  - **Phase 3 handoff checklist** documents what Phase 4 (modeling) can rely on:
    - `fighter_snapshots` and `bout_features` tables populated
    - Leakage tests passing
    - Feature quality report reviewed
    - Known missingness rates documented
- **Dependencies:** T3.5.1, T3.5.2
- **Complexity:** S
- **Risk:** Low

---

## Dependency graph

```
T3.1.1 (feature catalog)
  └→ T3.1.2 (schema DDL)
  └→ T3.2.1 (data loader + history index)
       ├→ T3.2.2 (career aggregates)
       ├→ T3.2.3 (rolling windows)
       ├→ T3.2.4 (decayed metrics)
       ├→ T3.2.5 (physical/demographic/activity)
       └→ T3.3.1 (Elo)
            └→ T3.3.2 (opponent-adjusted)
                 └→ T3.4.1 (snapshot + bout builder)
                      └→ T3.4.2 (build pipeline)
                           ├→ T3.5.1 (leakage tests)
                           ├→ T3.5.2 (feature quality)
                           └→ T3.6.1 (closeout)
```

**Parallelizable:** T3.2.2, T3.2.3, T3.2.4, T3.2.5 can all be implemented in parallel
once T3.2.1 is done.

**Critical path:** T3.2.1 → T3.2.2 → T3.3.1 → T3.3.2 → T3.4.1 → T3.4.2 → T3.5.1

---

## Complexity summary

| Size | Tickets |
|------|---------|
| S    | T3.1.2, T3.2.5, T3.5.2, T3.6.1 |
| M    | T3.1.1, T3.2.1, T3.2.2, T3.2.3, T3.2.4, T3.3.1, T3.3.2, T3.4.1, T3.4.2, T3.5.1 |

Total: 13 tickets (4S + 10M)
