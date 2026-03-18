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

### Recommended Tables

| Table | Primary Key | Important Foreign Keys | Suggested Fields |
| --- | --- | --- | --- |
| `events` | `event_id` | none | `source_event_id`, `event_name`, `event_date`, `venue`, `city`, `country`, `scraped_at`, `source_url` |
| `fights` | `fight_id` | `event_id`, `fighter_1_id`, `fighter_2_id`, `winner_fighter_id` | `card_order`, `weight_class`, `scheduled_rounds`, `completed_rounds`, `result_type`, `method`, `decision_type`, `is_title_fight`, `is_bonus_fight`, `referee` |
| `fighters` | `fighter_id` | none | `source_fighter_id`, `fighter_name`, `nickname`, `date_of_birth`, `height_cm`, `reach_cm`, `stance`, `nationality`, `gym_name`, `ufc_debut_date`, `last_profile_scraped_at` |
| `fight_stats_by_round` | `fight_stat_round_id` | `fight_id`, `fighter_id` | `round_number`, `sig_str_landed`, `sig_str_attempted`, `total_str_landed`, `total_str_attempted`, `td_landed`, `td_attempted`, `sub_attempts`, `knockdowns`, `control_seconds`, `head_landed`, `body_landed`, `leg_landed`, `distance_landed`, `clinch_landed`, `ground_landed` |
| `fight_stats_aggregate` | `fight_stat_agg_id` | `fight_id`, `fighter_id` | fight-level totals plus derived efficiencies, pace per minute, strike differential, control share |
| `fighter_history_snapshot` | `history_snapshot_id` | `fighter_id`, optionally `fight_id` | `as_of_date`, `career_fights`, `career_wins`, `career_losses`, `career_minutes`, rolling and decayed metrics, opponent strength summaries |
| `upcoming_fight_feature_set` | `feature_set_id` | `event_id`, `fight_id`, `fighter_1_id`, `fighter_2_id` | `snapshot_date`, model-ready features, missingness flags, uncertainty flags, `model_version_target` |

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
