# UFC Fight Prediction System Project Plan

## 1. Project Overview

The goal is to build a pre-fight UFC prediction system that estimates the probability of Fighter A defeating Fighter B from historical outcomes, fighter profiles, and fight statistics. The first production version should optimize for robustness, interpretability, and maintainability.

The system serves three business and analytics goals:

- Create a clean historical UFC warehouse that unifies events, fights, fighters, and statistics.
- Produce calibrated pre-fight win probabilities for upcoming bouts.
- Provide analyst-facing explanations for why the model favors one fighter over another.

Descriptive statistics summarize what has happened, such as striking differential or recent win streak. Predictive modeling estimates what is likely to happen next from those signals. The platform should support both: historical analysis and forward-looking projections.

Expected outputs of the system:

- `p_win_fighter_a`: modeled probability that Fighter A wins.
- `p_win_fighter_b`: complementary probability for Fighter B.
- Calibration-aware confidence indicators and uncertainty flags.
- Model explanations, including top contributing features and directional drivers.
- Historical evaluation reports for model comparison, calibration, and drift review.

## 2. Product Scope

### In Scope

- Scraping UFC event pages, fight pages, fighter profile pages, and fight statistics pages.
- Building a historical relational dataset in Postgres with clear primary and foreign keys.
- Creating fighter-level career, rolling-window, and exponentially decayed pre-fight features.
- Training machine learning models for pre-fight binary outcome prediction.
- Scoring future UFC fights in batch for upcoming cards.
- Evaluating model quality, probability calibration, and explainability outputs.

### Out of Scope

- Live in-fight betting predictions or round-by-round streaming inference.
- Real-time stream ingestion from live feeds.
- Coverage of non-UFC promotions unless explicitly added later.
- Fully automated sportsbook execution or wager placement.
- Medical, injury, or judging intelligence that requires proprietary data not yet sourced.
- Autonomous retraining in production without analyst review.

## 3. Data Sources and Data Domains

The platform should organize source data into four primary domains.

| Domain | Key Fields | Grain | Likely Joins | Common Data Quality Issues |
| --- | --- | --- | --- | --- |
| Event Data | event name, event date, venue, city, country, card order, event URL | One row per event | `events.event_id` to `fights.event_id` | duplicate events, naming drift, postponed cards, time zone ambiguity |
| Fight Information | fighters, winner, loser, weight class, rounds, finish method, decision type, title flag, bonus flags, referee | One row per fight | `fights.fight_id` to `events`, `fighters`, and aggregate stats | overturned results, draws, inconsistent finish labels |
| Fighter Profiles | fighter name, nickname, DOB, height, reach, stance, weight, nationality, gym, record, debut date | One row per fighter per scrape version | `fighters.fighter_id` to `fights` and snapshots | name variants, missing reach, changing gym data, DOB discrepancies |
| Fight Statistics | sig strikes, total strikes, takedowns, submission attempts, control time, knockdowns, target splits, position splits | One row per fighter per round, plus one row per fighter-fight aggregate | `fight_stats_by_round.fight_id` plus fighter role | incomplete stats, time-format issues, stat corrections |

### Core Entities

| Entity | Purpose | Notes |
| --- | --- | --- |
| `event` | Represents a UFC event or card | Event-level dimension keyed by date and source identifier |
| `fight` | Represents a single bout | Connects event, two fighters, and outcome metadata |
| `fighter` | Represents an athlete identity | Must support alias handling and external IDs |
| `fighter_fight_stats` | Represents fighter-specific performance within a fight | Best modeled as aggregate and per-round tables |
| `opponent_history` | Derived view of prior opponents and opponent strength | Used for schedule strength and matchup features |
| `model_feature_snapshot` | Frozen pre-fight feature set used for training or inference | Main protection against leakage |

## 4. Data Model and Storage Design

Postgres should be the primary warehouse for normalized data and derived feature snapshots. Python should own scraping, parsing, feature generation, training, and inference.

### Implemented Raw Files

The current scraper writes five canonical CSV files at the repo root `data/` directory.

| File | Grain | Actual Columns |
| --- | --- | --- |
| `data/events.csv` | one row per event | `scraped_at`, `event_id`, `url`, `name`, `date`, `date_formatted`, `city`, `state`, `country`, `fights`, `fight_urls`, `event_status` |
| `data/fights.csv` | one row per fight | `scraped_at`, `fight_id`, `event_id`, `url`, `fighter_1_id`, `fighter_2_id`, `fighter_1_outcome`, `fighter_2_outcome`, `bout_type`, `weight_class`, `num_rounds`, `finish_method`, `primary_finish_method`, `secondary_finish_method`, `finish_round`, `finish_time_minute`, `finish_time_second`, `referee`, `judge_1`, `judge_2`, `judge_3` |
| `data/fighters.csv` | one row per fighter | `scraped_at`, `fighter_id`, `url`, `full_name`, `first_name`, `last_names`, `nickname`, `height_ft`, `height_in`, `height_cm`, `weight_lbs`, `reach_in`, `reach_cm`, `stance`, `dob`, `dob_formatted`, `record`, `wins`, `losses`, `draws`, `no_contests`, `fight_ids` |
| `data/fight_stats.csv` | one row per fighter per fight | `scraped_at`, `fight_stat_id`, `fight_id`, `fighter_id`, `url`, `total_strikes_landed`, `total_strikes_attempted`, `significant_strikes_landed`, `significant_strikes_attempted`, `significant_strikes_landed_head`, `significant_strikes_attempted_head`, `significant_strikes_landed_body`, `significant_strikes_attempted_body`, `significant_strikes_landed_leg`, `significant_strikes_attempted_leg`, `significant_strikes_landed_distance`, `significant_strikes_attempted_distance`, `significant_strikes_landed_clinch`, `significant_strikes_attempted_clinch`, `significant_strikes_landed_ground`, `significant_strikes_attempted_ground`, `knockdowns`, `takedowns_landed`, `takedowns_attempted`, `control_time_minutes`, `control_time_seconds`, `submissions_attempted`, `reversals` |
| `data/fight_stats_by_round.csv` | one row per fighter per round per fight | `scraped_at`, `fight_stat_by_round_id`, `fight_id`, `fighter_id`, `round`, `total_strikes_landed`, `total_strikes_attempted`, `significant_strikes_landed`, `significant_strikes_attempted`, `significant_strikes_landed_head`, `significant_strikes_attempted_head`, `significant_strikes_landed_body`, `significant_strikes_attempted_body`, `significant_strikes_landed_leg`, `significant_strikes_attempted_leg`, `significant_strikes_landed_distance`, `significant_strikes_attempted_distance`, `significant_strikes_landed_clinch`, `significant_strikes_attempted_clinch`, `significant_strikes_landed_ground`, `significant_strikes_attempted_ground`, `knockdowns`, `takedowns_landed`, `takedowns_attempted`, `control_time_minutes`, `control_time_seconds`, `submissions_attempted`, `reversals` |

### Implemented Warehouse Tables

The current warehouse schema is defined in `warehouse/sql/*.sql`. These are the actual normalized tables in the repo today.

| Table | Primary Key | Important Foreign Keys | Actual Columns |
| --- | --- | --- | --- |
| `events` | `event_id` | none | `event_id`, `event_name`, `event_date`, `city`, `state`, `country`, `event_status`, `source_url`, `scraped_at` |
| `fights` | `fight_id` | `event_id`, `fighter_1_id`, `fighter_2_id`, `winner_fighter_id` | `fight_id`, `event_id`, `fighter_1_id`, `fighter_2_id`, `winner_fighter_id`, `result_type`, `weight_class`, `is_title_fight`, `is_interim_title`, `scheduled_rounds`, `finish_method`, `finish_detail`, `finish_round`, `finish_time_seconds`, `referee`, `source_url`, `scraped_at` |
| `fighters` | `fighter_id` | none | `fighter_id`, `full_name`, `first_name`, `last_name`, `nickname`, `height_cm`, `weight_lbs`, `reach_cm`, `stance`, `dob`, `source_url`, `scraped_at` |
| `fight_stats_aggregate` | `fight_stat_id` | `fight_id`, `fighter_id` | `fight_stat_id`, `fight_id`, `fighter_id`, `knockdowns`, `total_strikes_landed`, `total_strikes_attempted`, `sig_strikes_landed`, `sig_strikes_attempted`, `sig_strikes_head_landed`, `sig_strikes_head_attempted`, `sig_strikes_body_landed`, `sig_strikes_body_attempted`, `sig_strikes_leg_landed`, `sig_strikes_leg_attempted`, `sig_strikes_distance_landed`, `sig_strikes_distance_attempted`, `sig_strikes_clinch_landed`, `sig_strikes_clinch_attempted`, `sig_strikes_ground_landed`, `sig_strikes_ground_attempted`, `takedowns_landed`, `takedowns_attempted`, `control_time_seconds`, `submissions_attempted`, `reversals`, `source_url`, `scraped_at` |
| `fight_stats_by_round` | `fight_stat_by_round_id` | `fight_id`, `fighter_id` | `fight_stat_by_round_id`, `fight_id`, `fighter_id`, `round`, `knockdowns`, `total_strikes_landed`, `total_strikes_attempted`, `sig_strikes_landed`, `sig_strikes_attempted`, `sig_strikes_head_landed`, `sig_strikes_head_attempted`, `sig_strikes_body_landed`, `sig_strikes_body_attempted`, `sig_strikes_leg_landed`, `sig_strikes_leg_attempted`, `sig_strikes_distance_landed`, `sig_strikes_distance_attempted`, `sig_strikes_clinch_landed`, `sig_strikes_clinch_attempted`, `sig_strikes_ground_landed`, `sig_strikes_ground_attempted`, `takedowns_landed`, `takedowns_attempted`, `control_time_seconds`, `submissions_attempted`, `reversals`, `source_url`, `scraped_at` |

### Implemented Feature Tables

The current feature layer uses two tables: `fighter_snapshots` and `bout_features`. The full reference now lives in `docs/data_dictionary.md`; the lists below are the actual column names from the warehouse SQL.

#### `fighter_snapshots`

- Identifiers and metadata: `fighter_id`, `fight_id`, `as_of_date`, `feature_version`, `computed_at`
- Career aggregates: `career_fights`, `career_wins`, `career_losses`, `career_draws`, `career_nc`, `career_win_rate`, `career_finish_rate`, `career_ko_tko_wins`, `career_sub_wins`, `career_dec_wins`, `career_ko_tko_losses`, `career_sub_losses`, `career_title_fights`, `career_title_wins`, `career_minutes`, `career_sig_strikes_landed_pm`, `career_sig_strikes_absorbed_pm`, `career_sig_strike_accuracy`, `career_sig_strike_defense`, `career_takedown_accuracy`, `career_takedown_defense`, `career_sub_attempts_pm`, `career_control_rate`, `career_knockdowns_pm`
- Rolling window last 1: `win_rate_last1`, `finish_rate_last1`, `sig_strikes_landed_pm_last1`, `sig_strikes_absorbed_pm_last1`, `sig_strike_accuracy_last1`, `sig_strike_defense_last1`, `takedown_landed_pm_last1`, `takedown_accuracy_last1`, `takedown_defense_last1`, `control_rate_last1`, `knockdowns_pm_last1`, `knockdowns_absorbed_pm_last1`, `sub_attempts_pm_last1`, `avg_fight_time_last1`, `streak_last1`
- Rolling window last 3: `win_rate_last3`, `finish_rate_last3`, `sig_strikes_landed_pm_last3`, `sig_strikes_absorbed_pm_last3`, `sig_strike_accuracy_last3`, `sig_strike_defense_last3`, `takedown_landed_pm_last3`, `takedown_accuracy_last3`, `takedown_defense_last3`, `control_rate_last3`, `knockdowns_pm_last3`, `knockdowns_absorbed_pm_last3`, `sub_attempts_pm_last3`, `avg_fight_time_last3`, `streak_last3`
- Rolling window last 5: `win_rate_last5`, `finish_rate_last5`, `sig_strikes_landed_pm_last5`, `sig_strikes_absorbed_pm_last5`, `sig_strike_accuracy_last5`, `sig_strike_defense_last5`, `takedown_landed_pm_last5`, `takedown_accuracy_last5`, `takedown_defense_last5`, `control_rate_last5`, `knockdowns_pm_last5`, `knockdowns_absorbed_pm_last5`, `sub_attempts_pm_last5`, `avg_fight_time_last5`, `streak_last5`
- Exponentially decayed metrics: `sig_strikes_landed_pm_decay`, `sig_strikes_absorbed_pm_decay`, `sig_strike_accuracy_decay`, `sig_strike_defense_decay`, `takedown_landed_pm_decay`, `takedown_accuracy_decay`, `takedown_defense_decay`, `control_rate_decay`, `knockdowns_pm_decay`, `win_rate_decay`
- Physical and activity: `age`, `age_squared`, `height_cm`, `reach_cm`, `reach_to_height`, `is_orthodox`, `is_southpaw`, `days_since_last_fight`, `is_long_layoff`, `is_short_notice`, `is_debut`, `age_missing`, `height_reach_missing`
- Elo and opponent-adjusted: `elo_rating`, `elo_opponent`, `elo_diff`, `opp_avg_elo`, `opp_adj_sig_strike_accuracy`
- Phase 5 v2 additions: `slope_sig_strikes_last5`, `slope_td_accuracy_last5`, `slope_control_rate_last5`, `std_sig_strikes_last5`, `std_td_accuracy_last5`, `fights_per_year_last3`

#### `bout_features`

- Identifiers and metadata: `fight_id`, `fighter_1_id`, `fighter_2_id`, `event_date`, `weight_class`, `is_title_fight`, `scheduled_rounds`, `label`, `feature_version`, `computed_at`
- Difference features v1: `diff_elo`, `diff_career_wins`, `diff_career_fights`, `diff_career_win_rate`, `diff_career_finish_rate`, `diff_career_sig_strikes_landed_pm`, `diff_career_sig_strike_accuracy`, `diff_career_takedown_accuracy`, `diff_career_control_rate`, `diff_age`, `diff_height_cm`, `diff_reach_cm`, `diff_days_since_last_fight`, `diff_win_rate_last3`, `diff_sig_strikes_landed_pm_last3`, `diff_takedown_accuracy_last3`, `diff_control_rate_last3`, `diff_sig_strikes_landed_pm_decay`, `diff_win_rate_decay`, `diff_opp_avg_elo`
- Ratio features: `ratio_career_wins`, `ratio_career_fights`, `ratio_career_sig_strikes_landed_pm`, `ratio_career_control_rate`, `ratio_elo`
- Matchup flags: `is_orthodox_vs_southpaw`, `both_debuting`
- Phase 5 v2 additions: `diff_career_ko_rate`, `diff_career_sub_rate`, `diff_career_decision_rate`, `diff_career_sig_strikes_absorbed_pm`, `diff_career_sig_strike_defense`, `diff_career_takedown_defense`, `diff_title_fight_count`, `diff_five_round_fights`, `diff_reach_height_ratio`, `diff_fights_per_year_last3`, `diff_slope_sig_strikes_last5`, `diff_slope_td_accuracy_last5`, `diff_slope_control_rate_last5`, `diff_std_sig_strikes_last5`, `diff_std_td_accuracy_last5`, `f1_is_southpaw`, `f2_is_southpaw`, `weight_class_rank`

### Keys and Relationship Design

- Use surrogate integer or UUID primary keys for internal joins.
- Preserve source-specific identifiers in dedicated columns for traceability and re-scrapes.
- Model each fight with an explicit fighter ordering and derive side-invariant features as differences and ratios.
- Enforce foreign keys from `fights` to `events` and `fighters`, and from stats tables to `fights` and `fighters`.

### Versioning and Snapshots

- Raw scrape tables should keep `scraped_at`, source URL, and optional page hash.
- Derived feature tables must be versioned by `feature_version` and `snapshot_date`.
- A training row should be reproducible from `(fight_id, feature_version, snapshot_date)`.
- Upcoming fight rows should be stored separately from historical labels to avoid accidental contamination.

## 5. Data Collection Pipeline

The ingestion pipeline should move from source discovery to validated warehouse loads.

### Workflow

1. Discover event, fight, and fighter pages from UFC schedules and archives.
2. Crawl pages with rate limits, retries, and backoff.
3. Parse HTML into typed Python objects.
4. Normalize units, enums, names, dates, and time formats.
5. Deduplicate with external IDs plus event and fighter context.
6. Load records into Postgres staging and warehouse tables.
7. Run validation checks before downstream feature jobs.

### Design Requirements

- Scraping must respect source terms, robots constraints where applicable, and site reliability limits.
- Separate fetch logic from parse logic so layout changes stay isolated.
- Store URL, scrape timestamp, HTTP status, and content hash for auditability.

### Reliability Considerations

- Retry transient failures with bounded exponential backoff.
- Mark hard parser failures with source page identifiers.
- Use idempotent upserts for warehouse loads.
- Detect page changes via normalized HTML or payload hashes.
- Support historical backfills and incremental updates after new events.

### Validation Checks

- Event date must exist and be valid.
- Each fight must map to one event and two fighters.
- Result states must be mutually consistent.
- Aggregate stats should approximately match round sums.
- Physical attributes should use one unit system.
- Duplicate fighter identities should be flagged.

## 6. Feature Engineering Strategy

Raw data should be transformed into fighter-level pre-fight snapshots, then merged into bout-level training rows. Every feature must represent information available strictly before the fight date.

### Core Feature Families

- Fighter career aggregates: wins, losses, finish rates, UFC bouts, cage time, title fight count.
- Rolling window features: last 1, 3, and 5 fights for striking differential, takedown success, control share, and outcomes.
- Exponentially decayed metrics: recency-weighted striking, takedown, pace, and damage metrics.
- Opponent-strength adjusted metrics: performance relative to opponent baseline allowed rates and quality proxies such as Elo.
- Difference features: Fighter A minus Fighter B for age, reach, pace, finish rate, control rate, and adjusted efficiencies.
- Ratio features: strike accuracy, takedown attempt, win-rate, and experience ratios.
- Style matchup features: striker-versus-grappler indicators, stance matchups, and distance versus control preference.
- Rest and inactivity features: days since last fight, long layoff, quick turnaround.
- Age and physical attributes: age, age squared, height, reach, reach-to-height ratio.
- Experience features: UFC debut age, UFC fight count, five-round experience, title fight experience.
- Recent form features: last-fight result, last-three-fight points, recency-weighted streak, recent knockdown and submission patterns.

### Feature Construction Principles

- Build fighter-centric snapshots first, then create symmetric fight rows.
- Include missingness indicators for sparse profile fields and incomplete stats.
- Normalize count stats into per-minute or per-opportunity rates.
- Include process metrics such as control, pace, defense, and adjusted efficiency.
- Prefer stable transformations over bespoke composite scores.

### Leakage Prevention

Feature generation must use a cutoff equal to the scheduled fight date. The pipeline must exclude the target fight and any later fights from aggregates, rolling windows, opponent-strength calculations, and profile updates. Post-fight fields from the target bout must never enter the training row. Leakage tests should assert that each snapshot uses only records with earlier event dates.

## 7. Modeling Approach

This is a binary probabilistic prediction problem over structured tabular sports data. The model stack should start with interpretable baselines and add more expressive methods only when they provide measurable lift.

| Model | Strengths | Weaknesses | Interpretability | Suitability |
| --- | --- | --- | --- | --- |
| Logistic Regression | simple, stable, fast, strong baseline for calibrated probabilities | linear decision boundary, weaker on nonlinear interactions | high | excellent MVP benchmark |
| Random Forest | captures nonlinearities and interactions, robust to mixed features | weaker probability calibration, larger models, less smooth generalization | medium | useful benchmark, not ideal final probability model |
| Gradient Boosted Trees (XGBoost or LightGBM) | strong tabular performance, handles nonlinearities, supports missing data | can overfit without time-aware validation, needs calibration review | medium | strong primary candidate |
| Elo/Glicko Rating | intuitive, easy to explain, naturally sequential | limited feature depth, ignores rich stat interactions | high | strong benchmark and useful feature source |
| Bayesian Hierarchical Model | handles partial pooling and uncertainty well, useful for sparse fighters | slower development and inference, higher implementation complexity | medium to high | promising for later uncertainty modeling |
| Feedforward Neural Network | can learn nonlinear combinations of dense features | more tuning, weaker transparency, often not better than GBDT on tabular data | low to medium | lower MVP priority |
| Sequence Model over Fight History | directly models order and evolution of fights | data-hungry, higher complexity, harder debugging and leakage control | low | research track, not MVP |

### Recommended Starting Point

The MVP should include Logistic Regression as the transparency-first baseline, Elo or Glicko as a lightweight sequential benchmark, and LightGBM as the primary production candidate. This stack balances maintainability, interpretability, and expected predictive strength.

## 8. Training and Validation Design

Training must mirror live deployment: predict each fight using only prior history.

### Pipeline Rules

- Split train, validation, and test sets by fight date, never randomly.
- Use rolling-window cross-validation for model selection.
- Retain fighters with short histories and expose sparse-history flags.
- Scale numeric features only for models that require it.
- Address imbalance only if target filtering creates material skew.
- Tune hyperparameters on time-based folds, not shuffled folds.

### Suggested Temporal Design

- Training: earliest history through cutoff date T1.
- Validation: next time block T1 to T2.
- Test: final holdout block T2 onward.
- Rolling CV: repeat on sequential windows to measure stability across eras.

### Pseudocode: Generating Pre-Fight Snapshots

```text
for fight in fights ordered by event_date:
    cutoff_date = fight.event_date
    fighter_a_history = all prior fights for fighter_a where event_date < cutoff_date
    fighter_b_history = all prior fights for fighter_b where event_date < cutoff_date

    fighter_a_snapshot = build_fighter_features(fighter_a_history, cutoff_date)
    fighter_b_snapshot = build_fighter_features(fighter_b_history, cutoff_date)

    bout_row = combine_snapshots(
        fighter_a_snapshot,
        fighter_b_snapshot,
        metadata_available_pre_fight_only
    )

    if fight outcome is known:
        attach label
    store bout_row
```

### Pseudocode: Training the Model

```text
feature_rows = load_historical_feature_snapshots(feature_version)
splits = make_time_based_splits(feature_rows, by="fight_date")

for split in splits:
    train_df, valid_df = split.train, split.valid
    preprocessors = fit_preprocessing(train_df)
    x_train = preprocessors.transform(train_df.features)
    x_valid = preprocessors.transform(valid_df.features)

    model = fit_model(x_train, train_df.label, hyperparameters)
    valid_pred = model.predict_proba(x_valid)
    evaluate(split, valid_df.label, valid_pred)

select best configuration
refit on train_plus_validation
persist model artifact, feature version, metrics, and calibration report
```

### Pseudocode: Scoring Future Fights

```text
upcoming = load_scheduled_fights_without_results()

for fight in upcoming:
    snapshot = build_pre_fight_feature_row(fight, as_of_date=today)
    transformed = preprocess_with_saved_artifacts(snapshot)
    prediction = model.predict_proba(transformed)
    explanation = explain_prediction(model, snapshot)
    store prediction and explanation
```

## 9. Evaluation Metrics

The system should be judged primarily on probability quality, not just pick accuracy.

### Core Metrics

- Accuracy: useful for directional correctness but insensitive to confidence quality.
- Log Loss: penalizes overconfident wrong predictions and is the primary optimization candidate.
- Brier Score: measures squared error of predicted probabilities and supports calibration assessment.
- ROC AUC: measures ranking ability across thresholds.
- Precision and Recall: relevant only for thresholded decision use cases, not as the primary objective.
- Calibration plots: compare predicted win probability buckets to realized outcomes.
- Reliability analysis: measure expected calibration error and review overconfidence in specific probability bands.

Probability calibration matters more than pure accuracy because this is a projection system. A poorly calibrated model can mislead analysts even when its hit rate is acceptable.

### Benchmark Comparisons

- Naive favorite baseline: if bookmaker odds are unavailable, use fighter with better raw UFC win percentage or higher ranking proxy when available.
- Simple Elo baseline: sequential rating model with pre-fight expected win probability.
- Previous-fight-winner heuristic: choose the fighter who won their most recent bout.
- Market odds benchmark: compare model log loss and calibration to closing betting probabilities if acquired later.

## 10. Prediction Workflow for Future Fights

Future fight scoring should run as a batch pre-fight process for scheduled cards.

### Workflow

1. Ingest upcoming event and scheduled fight card data.
2. Resolve fighter identities to the `fighters` table.
3. Build a pre-fight snapshot for each fighter using data strictly before the event date.
4. Join Fighter A and Fighter B snapshots into one feature row with difference, ratio, and matchup features.
5. Score each fight with the selected production model and save probabilities plus explanations.
6. Mark sparse-history or missing-data cases with uncertainty flags for analyst review.

### Special Cases

- Debuting fighters: use profile-only features, camp/nationality if available, default priors, and explicit `debut_flag`.
- Missing physical attributes: impute conservatively and emit missingness indicators.
- Late opponent changes: regenerate feature rows at scoring time from the latest official matchup.
- Batch scoring: run per card, but persist fight-level results independently so re-runs do not require full card recomputation.

### History Update Logic

After each completed event:

- load official results and fight stats;
- update event, fight, and stats tables;
- rebuild affected fighter history snapshots;
- append the new fights to the historical training corpus for the next scheduled retrain.

## 11. Explainability and Analysis

Analysts should be able to inspect both global model behavior and single-fight predictions.

### Required Analysis Views

- Feature importance: global importance from LightGBM gain metrics and permutation importance.
- SHAP values: local explanation for each upcoming fight and global summary for major drivers.
- Calibration review: reliability curves, calibration tables, and confidence bucket analysis.
- Error analysis by weight class: detect whether heavyweights, flyweights, or women's divisions show different error patterns.
- Error analysis by veteran versus newcomer fights: sparse-history bouts should be segmented explicitly.
- Drift detection over time: compare feature distributions, target rates, and model residuals by season or year.

### Operational Expectations

- Persist explanation artifacts alongside predictions.
- Review top false positives and false negatives after each major event batch.

## 12. System Architecture

```text
/data
  /raw
  /interim
  /processed
/scrapers
/parsers
/models
/features
/training
/inference
/evaluation
/notebooks
/tests
/docs
```

Directory responsibilities:

| Directory | Responsibility |
| --- | --- |
| `/data/raw` | raw scraped pages, raw extracts, source manifests |
| `/data/interim` | normalized intermediate files before warehouse load |
| `/data/processed` | exported training datasets, feature snapshots, scored outputs |
| `/scrapers` | HTTP fetchers, source discovery, retry logic, crawl jobs |
| `/parsers` | HTML parsers and normalization logic for events, fights, fighters, stats |
| `/models` | model definitions, benchmark implementations, calibration wrappers |
| `/features` | snapshot builders, rolling aggregates, decayed metrics, leakage tests |
| `/training` | train scripts, split logic, hyperparameter tuning, artifact management |
| `/inference` | future fight scoring jobs, batch runners, prediction persistence |
| `/evaluation` | metrics, benchmark reports, SHAP analysis, drift detection |
| `/notebooks` | exploratory analysis and validation notebooks, not production logic |
| `/tests` | unit, integration, data validation, and leakage-prevention tests |
| `/docs` | design docs, runbooks, data dictionary, model cards |

## 13. Milestones and Engineering Tickets

### Phase 1: Data Acquisition

| Ticket ID | Title | Summary | Deliverables | Dependencies |
| --- | --- | --- | --- | --- |
| UFC-001 | Event Source Discovery | Create registry of historical and upcoming event pages. | manifest, discovery script, selectors | none |
| UFC-002 | Event Page Scraper | Scrape event metadata and card listings with rate limits and retries. | scraper module, raw page storage, tests | UFC-001 |
| UFC-003 | Fighter Profile Scraper | Scrape fighter profile pages and source identifiers. | scraper, raw profile capture, parser fixtures | UFC-001 |
| UFC-004 | Fight Stats Scraper | Scrape fight-level and round-level statistics for completed bouts. | stats scraper, raw pages, retry logic | UFC-002 |

### Phase 2: Data Modeling and Storage

| Ticket ID | Title | Summary | Deliverables | Dependencies |
| --- | --- | --- | --- | --- |
| UFC-005 | Postgres Schema Creation | Create warehouse schema, constraints, and indexes. | DDL scripts, migrations, schema doc | UFC-002, UFC-003, UFC-004 |
| UFC-006 | Parsing and Normalization Layer | Convert raw scraped content into normalized records. | parsers, enum mappings, tests | UFC-002, UFC-003, UFC-004 |
| UFC-007 | Warehouse Load Jobs | Implement idempotent upsert jobs for warehouse tables. | load scripts, upsert logic, validation checks | UFC-005, UFC-006 |

### Phase 3: Feature Engineering

| Ticket ID | Title | Summary | Deliverables | Dependencies |
| --- | --- | --- | --- | --- |
| UFC-008 | Fighter History Snapshot Generator | Build per-fighter pre-fight snapshot table keyed by cutoff date. | snapshot job, history schema, tests | UFC-007 |
| UFC-009 | Rolling and Decayed Metrics Module | Compute rolling-window and exponentially decayed metrics. | feature library, config, tests | UFC-008 |
| UFC-010 | Opponent-Adjusted Metrics Module | Add schedule-strength and opponent-adjusted features. | adjustment logic, comparison notebook | UFC-008 |
| UFC-011 | Bout Feature Row Builder | Merge two fighter snapshots into model-ready bout rows. | feature row table, export job, schema doc | UFC-009, UFC-010 |

### Phase 4: Baseline Modeling

| Ticket ID | Title | Summary | Deliverables | Dependencies |
| --- | --- | --- | --- | --- |
| UFC-012 | Leakage Validation Tests | Ensure snapshots exclude target-fight information. | leakage test suite, CI checks | UFC-011 |
| UFC-013 | Time-Based Split Module | Implement temporal split and rolling validation utilities. | split library, fold config | UFC-011 |
| UFC-014 | Logistic Regression Trainer | Train and evaluate regularized logistic regression baseline. | training script, model artifact, metric report | UFC-012, UFC-013 |
| UFC-015 | Elo Benchmark Module | Build sequential Elo or Glicko benchmark. | benchmark module, evaluation report | UFC-013 |

### Phase 5: Advanced Modeling

| Ticket ID | Title | Summary | Deliverables | Dependencies |
| --- | --- | --- | --- | --- |
| UFC-016 | LightGBM Training Pipeline | Train boosted tree model with temporal validation. | LightGBM trainer, tuned config, metrics | UFC-012, UFC-013 |
| UFC-017 | Random Forest Benchmark | Add random forest benchmark for nonlinear comparison. | benchmark model, comparison report | UFC-013 |
| UFC-018 | Bayesian Model Research Spike | Prototype hierarchical model for sparse-history fighters. | research notebook, feasibility memo | UFC-014, UFC-015 |
| UFC-019 | Sequence Model Research Spike | Evaluate sequential history modeling for later versions. | experiment plan, comparison memo | UFC-011 |

### Phase 6: Evaluation and Calibration

| Ticket ID | Title | Summary | Deliverables | Dependencies |
| --- | --- | --- | --- | --- |
| UFC-020 | Metrics and Benchmark Framework | Standardize metrics and benchmark comparison reporting. | evaluation library, report templates | UFC-014, UFC-015, UFC-016 |
| UFC-021 | Calibration Analysis Pipeline | Generate reliability curves and calibration artifacts. | calibration notebook, calibration module | UFC-020 |
| UFC-022 | Error and Drift Analysis | Analyze errors by weight class, experience cohort, and time period. | drift report, segmented analysis | UFC-020 |

### Phase 7: Prediction Interface

| Ticket ID | Title | Summary | Deliverables | Dependencies |
| --- | --- | --- | --- | --- |
| UFC-023 | Upcoming Fight Ingestion Job | Ingest and persist scheduled future fights separately. | upcoming fight loader, validation checks | UFC-007 |
| UFC-024 | Batch Scoring Pipeline | Score upcoming cards and persist outputs. | inference job, prediction table writes | UFC-016, UFC-021, UFC-023 |
| UFC-025 | Prediction CLI or API | Expose fight-level and card-level predictions to analysts. | CLI or lightweight API, usage doc | UFC-024 |

### Phase 8: Visualization and Reporting

| Ticket ID | Title | Summary | Deliverables | Dependencies |
| --- | --- | --- | --- | --- |
| UFC-026 | Model Card and Documentation | Document data sources, features, caveats, and results. | model card, data dictionary, runbook | UFC-020, UFC-021 |
| UFC-027 | Upcoming Card Dashboard | Build analyst dashboard for probabilities, confidence, and explanations. | dashboard prototype, query layer, screenshots | UFC-024, UFC-025 |
| UFC-028 | Post-Event Review Notebook | Create reusable notebook for post-event review. | notebook template, report checklist | UFC-024, UFC-022 |

## 14. Risks and Open Questions

### Key Risks

- Data quality inconsistency across historical eras may weaken feature stability.
- Incomplete historical stats can bias engineered metrics toward recent fights.
- Fighter name matching and aliases may create duplicate or merged identities.
- Website structure changes can silently break scrapers and parsers.
- Leakage from post-fight fields is a major modeling failure mode.
- Small sample sizes for debuting or low-activity fighters will inflate uncertainty.
- Fighter styles evolve over time, reducing the relevance of older performance data.
- Late cancellations and opponent changes can invalidate precomputed features.

### Open Questions

- Which public source or combination of sources should be treated as the system of record?
- Should women’s and men’s divisions share one model or use segmented models?
- How should draws, no contests, and overturned decisions be labeled for training?
- Should five-round main events be modeled jointly with three-round fights or separately?
- When market odds become available, should they remain only a benchmark or become a model input?
- What level of manual review is acceptable for fighter identity resolution and sparse-data fights?

## 15. Future Enhancements

Once the MVP is stable, the following extensions are reasonable:

- Betting market line comparison and closing-line value analysis.
- Ensemble modeling across Logistic Regression, Elo, and LightGBM.
- Fighter embeddings learned from career trajectories and opponent graphs.
- Graph-based features using shared opponents and transitive performance signals.
- NLP features from fight commentary, interviews, or news if source quality is defensible.
- Injury, camp change, and short-notice signals when reliable pre-fight sources are available.
- Simulation of full fight cards for parlay or event-level scenario analysis.
- Expansion to other MMA promotions through promotion-specific source adapters and model segmentation.

Recommended MVP path: build the warehouse, enforce leakage-safe snapshots, ship interpretable baselines, and promote LightGBM only after calibration and benchmark review confirm lift.
