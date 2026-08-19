# Data Dictionary

Complete reference for all warehouse tables, columns, feature definitions, and enum values.

---

## Warehouse Tables

### events

One row per UFC event (completed or upcoming). Source: `data/events.csv` + `data/manifests/events_manifest.csv`.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `event_id` | uuid | PK | Unique event identifier from source |
| `event_name` | text | NOT NULL | e.g. "UFC 300: Pereira vs. Hill" |
| `event_date` | date | NOT NULL | Scheduled date of the event |
| `city` | text | yes | Venue city |
| `state` | text | yes | Venue state/province |
| `country` | text | yes | Venue country |
| `event_status` | text | yes | `completed` or `upcoming` |
| `source_url` | text | NOT NULL | ufcstats.com event page URL |
| `scraped_at` | timestamptz | yes | When the record was last scraped |

---

### fighters

One row per fighter. Career record columns are excluded — they are computed per-fight in feature engineering.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `fighter_id` | uuid | PK | Unique fighter identifier from source |
| `full_name` | text | NOT NULL | Full display name |
| `first_name` | text | yes | First name |
| `last_name` | text | yes | Last name |
| `nickname` | text | yes | Fighter nickname |
| `height_cm` | numeric(5,2) | yes | Height in centimeters |
| `weight_lbs` | numeric(5,1) | yes | Weight in pounds |
| `reach_cm` | numeric(5,2) | yes | Reach in centimeters |
| `stance` | text | yes | Fighting stance (see enums) |
| `dob` | date | yes | Date of birth |
| `source_url` | text | NOT NULL | ufcstats.com fighter page URL |
| `scraped_at` | timestamptz | yes | When the record was last scraped |

---

### fights

One row per bout. Links to events and fighters.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `fight_id` | uuid | PK | Unique fight identifier from source |
| `event_id` | uuid | NOT NULL | FK to `events.event_id` |
| `fighter_1_id` | uuid | NOT NULL | FK to `fighters.fighter_id` (deferred) |
| `fighter_2_id` | uuid | NOT NULL | FK to `fighters.fighter_id` (deferred) |
| `winner_fighter_id` | uuid | yes | FK to `fighters.fighter_id`. NULL for draws, NC, upcoming |
| `result_type` | text | NOT NULL | Fight outcome type (see enums) |
| `weight_class` | text | yes | Weight division (see enums). NULL for early tournament bouts |
| `is_title_fight` | boolean | NOT NULL | Whether this was a title fight |
| `is_interim_title` | boolean | NOT NULL | Whether this was an interim title fight |
| `scheduled_rounds` | smallint | yes | Number of scheduled rounds (3 or 5) |
| `finish_method` | text | yes | How the fight ended (see enums) |
| `finish_detail` | text | yes | Secondary method detail (free text) |
| `finish_round` | smallint | yes | Round in which the fight ended |
| `finish_time_seconds` | smallint | yes | Time in the finish round (seconds) |
| `referee` | text | yes | Referee name |
| `source_url` | text | NOT NULL | ufcstats.com fight page URL |
| `scraped_at` | timestamptz | yes | When the record was last scraped |

---

### fight_stats_aggregate

One row per fighter per fight (2 rows per fight). Fight-level performance totals.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `fight_stat_id` | uuid | PK | Unique stat row identifier |
| `fight_id` | uuid | NOT NULL | FK to `fights.fight_id` |
| `fighter_id` | uuid | NOT NULL | FK to `fighters.fighter_id` |
| `knockdowns` | smallint | NOT NULL | Total knockdowns scored |
| `total_strikes_landed` | smallint | NOT NULL | All strikes landed |
| `total_strikes_attempted` | smallint | NOT NULL | All strikes attempted |
| `sig_strikes_landed` | smallint | NOT NULL | Significant strikes landed |
| `sig_strikes_attempted` | smallint | NOT NULL | Significant strikes attempted |
| `sig_strikes_head_landed` | smallint | NOT NULL | Sig strikes to head landed |
| `sig_strikes_head_attempted` | smallint | NOT NULL | Sig strikes to head attempted |
| `sig_strikes_body_landed` | smallint | NOT NULL | Sig strikes to body landed |
| `sig_strikes_body_attempted` | smallint | NOT NULL | Sig strikes to body attempted |
| `sig_strikes_leg_landed` | smallint | NOT NULL | Sig strikes to legs landed |
| `sig_strikes_leg_attempted` | smallint | NOT NULL | Sig strikes to legs attempted |
| `sig_strikes_distance_landed` | smallint | NOT NULL | Sig strikes at distance landed |
| `sig_strikes_distance_attempted` | smallint | NOT NULL | Sig strikes at distance attempted |
| `sig_strikes_clinch_landed` | smallint | NOT NULL | Sig strikes in clinch landed |
| `sig_strikes_clinch_attempted` | smallint | NOT NULL | Sig strikes in clinch attempted |
| `sig_strikes_ground_landed` | smallint | NOT NULL | Sig strikes on ground landed |
| `sig_strikes_ground_attempted` | smallint | NOT NULL | Sig strikes on ground attempted |
| `takedowns_landed` | smallint | NOT NULL | Successful takedowns |
| `takedowns_attempted` | smallint | NOT NULL | Takedown attempts |
| `control_time_seconds` | smallint | NOT NULL | Ground control time in seconds |
| `submissions_attempted` | smallint | NOT NULL | Submission attempts |
| `reversals` | smallint | NOT NULL | Reversals |
| `source_url` | text | yes | Source page URL |
| `scraped_at` | timestamptz | yes | When scraped |

---

### fight_stats_by_round

One row per fighter per round per fight. Same stat columns as `fight_stats_aggregate` plus `round`.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `fight_stat_by_round_id` | uuid | PK | Unique stat row identifier |
| `fight_id` | uuid | NOT NULL | FK to `fights.fight_id` |
| `fighter_id` | uuid | NOT NULL | FK to `fighters.fighter_id` |
| `round` | smallint | NOT NULL | Round number (1-5) |
| *(all stat columns)* | | | Same as `fight_stats_aggregate` |

---

### fighter_snapshots

One row per fighter per fight. Pre-fight feature snapshot computed using only data available before the fight date. PK: `(fighter_id, fight_id)`.

#### Identifiers and metadata

| Column | Description |
|---|---|
| `fighter_id` | FK to `fighters.fighter_id` |
| `fight_id` | FK to `fights.fight_id` |
| `as_of_date` | Event date used as the feature cutoff |
| `feature_version` | Feature schema version |
| `computed_at` | Timestamp when the feature row was computed |

#### Career aggregates

| Column | Description |
|---|---|
| `career_fights` | Total fights before this bout |
| `career_wins` | Total wins |
| `career_losses` | Total losses |
| `career_draws` | Total draws |
| `career_nc` | Total no-contests |
| `career_win_rate` | wins / fights |
| `career_finish_rate` | (KO/TKO + sub wins) / wins |
| `career_ko_tko_wins` | KO/TKO victories |
| `career_sub_wins` | Submission victories |
| `career_dec_wins` | Decision victories |
| `career_ko_tko_losses` | KO/TKO losses |
| `career_sub_losses` | Submission losses |
| `career_title_fights` | Title fights competed in |
| `career_title_wins` | Title fights won |
| `career_minutes` | Total cage time in minutes |
| `career_sig_strikes_landed_pm` | Career sig strikes landed per minute |
| `career_sig_strikes_absorbed_pm` | Career sig strikes absorbed per minute |
| `career_sig_strike_accuracy` | Career sig strikes landed / attempted |
| `career_sig_strike_defense` | 1 - (opponent sig strikes landed / attempted) |
| `career_takedown_accuracy` | Career takedowns landed / attempted |
| `career_takedown_defense` | 1 - (opponent takedowns landed / attempted) |
| `career_sub_attempts_pm` | Submission attempts per 15 minutes |
| `career_control_rate` | Control time (seconds) per fight |
| `career_knockdowns_pm` | Knockdowns per fight |

#### Rolling windows (last 1, 3, 5 fights)

Each metric is computed for the last N fights. Column pattern: `{metric}_last{N}`.

| Metric | Description |
|---|---|
| `win_rate` | Wins / N |
| `finish_rate` | Finish wins / wins in window |
| `sig_strikes_landed_pm` | Sig strikes landed per minute |
| `sig_strikes_absorbed_pm` | Sig strikes absorbed per minute |
| `sig_strike_accuracy` | Sig strike accuracy |
| `sig_strike_defense` | Sig strike defense |
| `takedown_landed_pm` | Takedowns landed per minute |
| `takedown_accuracy` | Takedown accuracy |
| `takedown_defense` | Takedown defense |
| `control_rate` | Control time per fight |
| `knockdowns_pm` | Knockdowns per fight |
| `knockdowns_absorbed_pm` | Knockdowns absorbed per fight |
| `sub_attempts_pm` | Submission attempts per minute |
| `avg_fight_time` | Average fight duration in minutes |
| `streak` | Current win/loss streak over the window |

#### Exponentially decayed metrics (alpha = 0.85)

Column pattern: `{metric}_decay`. More recent fights weighted exponentially higher.

| Metric | Description |
|---|---|
| `sig_strikes_landed_pm_decay` | Recency-weighted sig strike rate |
| `sig_strikes_absorbed_pm_decay` | Recency-weighted sig strikes absorbed rate |
| `sig_strike_accuracy_decay` | Recency-weighted sig strike accuracy |
| `sig_strike_defense_decay` | Recency-weighted sig strike defense |
| `takedown_landed_pm_decay` | Recency-weighted takedown rate |
| `takedown_accuracy_decay` | Recency-weighted takedown accuracy |
| `takedown_defense_decay` | Recency-weighted takedown defense |
| `control_rate_decay` | Recency-weighted control time |
| `knockdowns_pm_decay` | Recency-weighted knockdown rate |
| `win_rate_decay` | Recency-weighted win rate |

#### v2 trend features (Phase 5)

| Column | Description |
|---|---|
| `slope_sig_strikes_last5` | Linear trend slope of sig strike rate over last 5 fights |
| `slope_td_accuracy_last5` | Linear trend slope of takedown accuracy over last 5 fights |
| `slope_control_rate_last5` | Linear trend slope of control rate over last 5 fights |
| `std_sig_strikes_last5` | Standard deviation of sig strike rate over last 5 fights |
| `std_td_accuracy_last5` | Standard deviation of takedown accuracy over last 5 fights |
| `fights_per_year_last3` | Fight frequency: fights per year over last 3 fights |

#### Physical, demographic, activity

| Column | Description |
|---|---|
| `age` | Age at fight date (years) |
| `age_squared` | Age squared (captures non-linear aging effects) |
| `height_cm` | Fighter height |
| `reach_cm` | Fighter reach |
| `reach_to_height` | Reach / height ratio |
| `is_orthodox` | Orthodox stance flag |
| `is_southpaw` | Southpaw stance flag |
| `days_since_last_fight` | Days since previous fight |
| `is_long_layoff` | True if > 365 days since last fight |
| `is_short_notice` | Short-notice fight flag |
| `is_debut` | True if this is the fighter's first fight in the dataset |
| `age_missing` | True if DOB is unknown |
| `height_reach_missing` | True if height or reach is unknown |

#### Elo and opponent strength

| Column | Description |
|---|---|
| `elo_rating` | Pre-fight Elo rating (starts at 1500) |
| `elo_opponent` | Opponent's pre-fight Elo rating |
| `elo_diff` | elo_rating - elo_opponent |
| `opp_avg_elo` | Average Elo of prior opponents |
| `opp_adj_sig_strike_accuracy` | Sig strike accuracy adjusted for opponent quality |

---

### bout_features

One row per fight. Model-ready features derived from two fighter snapshots. PK: `fight_id`.

#### Identifiers and metadata

| Column | Description |
|---|---|
| `fight_id` | PK, FK to `fights.fight_id` |
| `fighter_1_id` | FK to `fighters.fighter_id` |
| `fighter_2_id` | FK to `fighters.fighter_id` |
| `event_date` | Fight date |
| `weight_class` | Weight division |
| `is_title_fight` | Title fight flag |
| `scheduled_rounds` | 3 or 5 |
| `label` | 1 = fighter_1 won, 0 = fighter_2 won, NULL = draw/NC |
| `feature_version` | 1 (v1) or 2 (v2) |
| `computed_at` | Timestamp when the feature row was computed |

#### Difference features (fighter_1 - fighter_2)

| Column | Description |
|---|---|
| `diff_elo` | Elo rating difference |
| `diff_career_wins` | Win count difference |
| `diff_career_fights` | Fight count difference |
| `diff_career_win_rate` | Win rate difference |
| `diff_career_finish_rate` | Finish rate difference |
| `diff_career_sig_strikes_landed_pm` | Sig strike rate difference |
| `diff_career_sig_strike_accuracy` | Sig strike accuracy difference |
| `diff_career_takedown_accuracy` | Takedown accuracy difference |
| `diff_career_control_rate` | Control time per fight difference |
| `diff_age` | Age difference |
| `diff_height_cm` | Height difference |
| `diff_reach_cm` | Reach difference |
| `diff_days_since_last_fight` | Days since last fight difference |
| `diff_win_rate_last3` | Rolling 3-fight win rate difference |
| `diff_sig_strikes_landed_pm_last3` | Rolling 3-fight sig strike rate difference |
| `diff_takedown_accuracy_last3` | Rolling 3-fight takedown accuracy difference |
| `diff_control_rate_last3` | Rolling 3-fight control rate difference |
| `diff_sig_strikes_landed_pm_decay` | Decayed sig strike rate difference |
| `diff_win_rate_decay` | Decayed win rate difference |
| `diff_opp_avg_elo` | Average opponent Elo difference |

#### v2 difference features (Phase 5)

| Column | Description |
|---|---|
| `diff_career_ko_rate` | KO/TKO win rate difference |
| `diff_career_sub_rate` | Submission win rate difference |
| `diff_career_decision_rate` | Decision win rate difference |
| `diff_career_sig_strikes_absorbed_pm` | Sig strikes absorbed rate difference |
| `diff_career_sig_strike_defense` | Sig strike defense difference |
| `diff_career_takedown_defense` | Takedown defense difference |
| `diff_title_fight_count` | Title fight experience difference |
| `diff_five_round_fights` | Five-round fight experience difference |
| `diff_reach_height_ratio` | Reach-to-height ratio difference |
| `diff_fights_per_year_last3` | Fight frequency difference |
| `diff_slope_sig_strikes_last5` | Sig strike trend difference |
| `diff_slope_td_accuracy_last5` | Takedown accuracy trend difference |
| `diff_slope_control_rate_last5` | Control rate trend difference |
| `diff_std_sig_strikes_last5` | Sig strike volatility difference |
| `diff_std_td_accuracy_last5` | Takedown accuracy volatility difference |

#### Ratio features (fighter_1 / (fighter_1 + fighter_2))

| Column | Description |
|---|---|
| `ratio_career_wins` | Win count ratio |
| `ratio_career_fights` | Fight count ratio |
| `ratio_career_sig_strikes_landed_pm` | Sig strike rate ratio |
| `ratio_career_control_rate` | Control rate ratio |
| `ratio_elo` | Elo rating ratio |

#### Matchup and metadata features

| Column | Description |
|---|---|
| `is_orthodox_vs_southpaw` | True if one orthodox, one southpaw |
| `both_debuting` | True if both fighters debuting |
| `f1_is_southpaw` | Fighter 1 southpaw flag |
| `f2_is_southpaw` | Fighter 2 southpaw flag |
| `weight_class_rank` | Ordinal weight class encoding (1=strawweight to 9=heavyweight) |

#### Debut prior features (computed at training/inference time, not in DB)

| Column | Description |
|---|---|
| `debut_prior_win_prob_f1` | Fixed at 0.5 for debut bouts, NaN otherwise |
| `debut_reach_adv` | Reach difference as z-score by weight class, NaN for non-debut |
| `debut_height_adv` | Height difference as z-score by weight class, NaN for non-debut |

---

### predictions

One row per scored fight per model run. Stores upcoming-fight predictions produced by the production scoring pipeline. PK: `(fight_id, scored_at)`.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `fight_id` | uuid | PK, NOT NULL | FK to `fights.fight_id` |
| `event_date` | date | yes | Date of the event being scored |
| `fighter_1_id` | uuid | NOT NULL | FK to `fighters.fighter_id` |
| `fighter_2_id` | uuid | NOT NULL | FK to `fighters.fighter_id` |
| `fighter_1_name` | text | yes | Fighter 1 display name copied for dashboard/export convenience |
| `fighter_2_name` | text | yes | Fighter 2 display name copied for dashboard/export convenience |
| `weight_class` | text | yes | Bout weight division |
| `predicted_prob_f1` | numeric(6,4) | yes | Raw model probability that fighter_1 wins |
| `calibrated_prob_f1` | numeric(6,4) | yes | Calibrated probability after Platt scaling |
| `confidence_tier` | text | yes | `high`, `medium`, or `toss-up` |
| `is_uncertain` | boolean | yes | True if the fight is in the model's uncertain probability band |
| `model_name` | text | NOT NULL | Name of the model used for scoring |
| `model_artifact` | text | yes | Path to the model artifact used for scoring |
| `scored_at` | timestamptz | PK, NOT NULL | Timestamp for the scoring run |

Indexes:

- `idx_predictions_event_date` on `event_date`
- `idx_predictions_scored_at` on `scored_at DESC`

---

### fight_odds

One row per observed fighter-side moneyline price. Stores raw imported odds observations; derived probabilities live in views so odds imports remain auditable. Source: `data/odds/fight_odds.csv` loaded by `warehouse/load_fight_odds.py`. PK: `(fight_id, fighter_id, bookmaker, market, line_type, odds_timestamp)`.

Odds timestamps are required for leakage-safe betting backtests. A backtest may only use odds that were observed before the event or before the configured decision cutoff; odds without a trustworthy `odds_timestamp` should not be used for historical profit/loss claims.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `fight_id` | uuid | PK, NOT NULL | FK to `fights.fight_id` |
| `event_id` | uuid | NOT NULL | FK to `events.event_id`; also checked against the fight's event |
| `fighter_id` | uuid | PK, NOT NULL | FK to `fighters.fighter_id`; fighter whose price is listed |
| `opponent_fighter_id` | uuid | NOT NULL | FK to `fighters.fighter_id`; opposite side of the fight |
| `bookmaker` | text | PK, NOT NULL | Sportsbook or odds source name |
| `market` | text | PK, NOT NULL | V1 supports `moneyline` only |
| `line_type` | text | PK, NOT NULL | `opening`, `current`, `closing`, or `unknown` |
| `odds_timestamp` | timestamptz | PK, NOT NULL | When the price was observed or imported from the source |
| `american_odds` | integer | yes | Raw American odds, if supplied; cannot be `0` |
| `decimal_odds` | numeric(10,4) | yes | Raw decimal odds, if supplied; must be greater than `1.0` |
| `source` | text | NOT NULL | Manual/import/API source label |
| `source_url` | text | yes | Optional odds source URL or reference |
| `imported_at` | timestamptz | NOT NULL | When the row entered this project |

Constraints:

- At least one of `american_odds` or `decimal_odds` must be supplied.
- `market` is restricted to `moneyline` for V1.
- `line_type` is restricted to `opening`, `current`, `closing`, or `unknown`.
- `fighter_id` and `opponent_fighter_id` must differ.
- `bookmaker` and `source` must be nonblank.

---

### fight_odds_normalized

Warehouse view that derives normalized decimal odds and raw implied probability from `fight_odds`.

| Column | Description |
|---|---|
| `normalized_decimal_odds` | Decimal odds used for implied probability, EV, and staking calculations |
| `implied_probability` | Raw market implied probability, `1 / normalized_decimal_odds` |
| *(all raw odds columns)* | Passed through from `fight_odds` |

---

### latest_fight_odds

Warehouse view with the latest `current` line per `(fight_id, fighter_id, bookmaker, market)`, ordered by `odds_timestamp DESC` then `imported_at DESC`.

Key columns:

- `fight_id`
- `event_id`
- `fighter_id`
- `opponent_fighter_id`
- `bookmaker`
- `market`
- `line_type`
- `odds_timestamp`
- `american_odds`
- `decimal_odds`
- `normalized_decimal_odds`
- `implied_probability`
- `source`
- `source_url`
- `imported_at`

---

### fight_odds_no_vig

Warehouse view that emits no-vig probabilities only for exact two-sided reciprocal moneyline groups. A group is valid only when the same `(fight_id, event_id, bookmaker, market, line_type, odds_timestamp)` has exactly two distinct fighters and each row names the other fighter as its opponent.

| Column | Description |
|---|---|
| `fight_id` | Fight identifier |
| `event_id` | Event identifier |
| `fighter_id` | Fighter whose no-vig probability is listed |
| `opponent_fighter_id` | Opposite fighter in the same two-sided market |
| `bookmaker` | Sportsbook or odds source |
| `market` | `moneyline` |
| `line_type` | `opening`, `current`, `closing`, or `unknown` |
| `odds_timestamp` | Observation timestamp for leakage-safe filtering |
| `normalized_decimal_odds` | Decimal odds used for calculations |
| `implied_probability` | Raw implied probability before vig removal |
| `overround` | Sum of the two raw implied probabilities |
| `no_vig_implied_probability` | Fighter implied probability after normalizing by `overround` |

---

### Betting report columns

The Phase 8 betting reports are generated separately from model accuracy reports. Current planned outputs include `data/reports/betting_recommendations.csv`, `betting_event_summary.csv`, `betting_backtest_fights.csv`, `betting_backtest_events.csv`, and `betting_backtest_summary.csv`.

Common recommendation/backtest columns:

| Column | Description |
|---|---|
| `event_id` | Event identifier |
| `event_name` | Event display name |
| `event_date` | Event date |
| `fight_id` | Fight identifier |
| `fighter_id` | Evaluated fighter |
| `fighter_name` | Evaluated fighter display name |
| `opponent_fighter_id` | Opposing fighter identifier |
| `opponent_fighter_name` | Opposing fighter display name |
| `bookmaker` | Sportsbook or odds source |
| `market` | V1 value: `moneyline` |
| `line_type` | Odds line type used for the recommendation/backtest |
| `odds_timestamp` | Odds observation timestamp; required for leakage-safe backtests |
| `model_probability` | Calibrated model probability for the evaluated fighter |
| `market_implied_probability` | Raw implied probability from offered odds |
| `no_vig_market_probability` | Market probability after removing two-sided vig |
| `edge` | `model_probability - no_vig_market_probability` |
| `ev_per_unit` | Expected profit/loss per unit staked |
| `ev_percent` | Same value as `ev_per_unit`, formatted as a percentage at presentation time |
| `full_kelly_fraction` | Uncapped Kelly stake fraction |
| `fractional_kelly_fraction` | Kelly fraction after applying configured multiplier |
| `final_stake_fraction` | Final bankroll fraction after tier, single-bet, and event caps |
| `stake_amount` | Currency stake amount when bankroll is supplied |
| `decision` | `bet` or `pass` |
| `reason_codes` | Pipe-delimited reason codes, for example `positive_edge|positive_ev|fractional_kelly` |

Backtest-only columns:

| Column | Description |
|---|---|
| `actual_winner_fighter_id` | Actual winner when the fight resolved with a win/loss result |
| `bet_result` | `win`, `loss`, `push`, or `no_bet` |
| `profit_loss_units` | Profit/loss in stake units |
| `bankroll_before` | Simulated bankroll before the bet |
| `bankroll_after` | Simulated bankroll after the bet resolves |

---

### latest_predictions

Dashboard-facing view with the most recent scored prediction per fight. Source: `warehouse/sql/012_prediction_dashboard_views.sql`.

Key columns:

- `fight_id`
- `event_date`
- `fighter_1_name`
- `fighter_2_name`
- `predicted_prob_f1`
- `calibrated_prob_f1`
- `predicted_label`
- `predicted_winner_name`
- `confidence_tier`
- `is_uncertain`
- `model_name`
- `model_artifact`
- `scored_at`

---

### current_event_predictions

Dashboard-facing view for upcoming/current event predictions. Joins `latest_predictions` to `fights` and `events`, then keeps upcoming events or events dated today or later. Source: `warehouse/sql/012_prediction_dashboard_views.sql`.

Key columns:

- Event fields: `event_id`, `event_name`, `event_date`, `city`, `state`, `country`, `event_status`
- Fight fields: `fight_id`, `is_title_fight`, `is_interim_title`, `scheduled_rounds`, `weight_class`
- Prediction fields: `predicted_prob_f1`, `calibrated_prob_f1`, `predicted_label`, `predicted_winner_name`, `confidence_tier`, `is_uncertain`, `model_name`, `model_artifact`, `scored_at`

---

### pre_event_prediction_fights

Fight-level audit view for predictions that existed before event day. Keeps the latest prediction per fight where `predictions.scored_at::date < predictions.event_date`, then joins fight results when available. Source: `warehouse/sql/012_prediction_dashboard_views.sql`.

Key columns:

- `event_id`
- `event_name`
- `event_date`
- `fight_id`
- `fighter_1_name`
- `fighter_2_name`
- `weight_class`
- `scored_at`
- `pre_event_evidence`
- `predicted_prob_f1`
- `calibrated_prob_f1`
- `predicted_label`
- `predicted_winner_name`
- `confidence_tier`
- `actual_label`
- `actual_winner_name`
- `resolved`
- `correct`
- `is_correct` (`correct` alias used by the dashboard app)
- `model_name`
- `model_artifact`

---

### pre_event_prediction_events

Event-level post-event performance view derived from resolved rows in `pre_event_prediction_fights`. Source: `warehouse/sql/012_prediction_dashboard_views.sql`.

Key columns:

- `event_id`
- `event_name`
- `event_date`
- `model_name`
- `pre_event_evidence`
- `n_predicted_fights`
- `correct`
- `accuracy`
- `log_loss`
- `brier_score`
- `first_scored_at`
- `last_scored_at`
- `high_count`
- `medium_count`
- `toss_up_count`
- `high_accuracy`
- `medium_accuracy`
- `toss_up_accuracy`
- `event_status`
- `location`
- `event_location` (`location` alias used by the dashboard app)

---

### fighter_career_summary

Dashboard-facing view built from the latest `fighter_snapshots` row per fighter, joined to fighter bio data and the latest fight result. Source: `warehouse/sql/011_fighter_career_summary.sql`.

| Column | Description |
|---|---|
| `fighter_id` | Fighter identifier |
| `full_name` | Fighter display name |
| `first_name` | First name |
| `last_name` | Last name |
| `nickname` | Fighter nickname |
| `height_cm` | Height in centimeters |
| `weight_lbs` | Weight in pounds |
| `reach_cm` | Reach in centimeters |
| `stance` | Fighting stance |
| `dob` | Date of birth |
| `total_fights` | Career fights including latest fight |
| `total_wins` | Career wins including latest fight |
| `total_losses` | Career losses including latest fight |
| `total_draws` | Career draws including latest fight |
| `career_nc` | Career no-contests |
| `win_rate` | total_wins / total_fights |
| `finish_rate` | Career finish rate from latest snapshot |
| `career_ko_tko_wins` | KO/TKO wins |
| `career_sub_wins` | Submission wins |
| `career_dec_wins` | Decision wins |
| `career_ko_tko_losses` | KO/TKO losses |
| `career_sub_losses` | Submission losses |
| `career_title_fights` | Title fights competed in |
| `career_title_wins` | Title fights won |
| `total_minutes` | Career cage time in minutes |
| `sig_strikes_landed_pm` | Career significant strikes landed per minute |
| `sig_strikes_absorbed_pm` | Career significant strikes absorbed per minute |
| `sig_strike_accuracy` | Career significant strike accuracy |
| `sig_strike_defense` | Career significant strike defense |
| `takedown_accuracy` | Career takedown accuracy |
| `takedown_defense` | Career takedown defense |
| `control_rate` | Career control rate |
| `knockdowns_pm` | Career knockdowns per minute/fight metric from snapshots |
| `elo_rating` | Latest Elo rating |
| `win_rate_last3` | Rolling 3-fight win rate |
| `win_rate_last5` | Rolling 5-fight win rate |
| `sig_strikes_landed_pm_last3` | Rolling 3-fight significant strike rate |
| `sig_strikes_landed_pm_last5` | Rolling 5-fight significant strike rate |
| `takedown_accuracy_last3` | Rolling 3-fight takedown accuracy |
| `takedown_accuracy_last5` | Rolling 5-fight takedown accuracy |
| `control_rate_last3` | Rolling 3-fight control rate |
| `control_rate_last5` | Rolling 5-fight control rate |
| `streak_last5` | Recent streak over last 5 fights |
| `age` | Latest snapshot age |
| `days_since_last_fight` | Days since previous fight as of latest snapshot |
| `last_fight_outcome` | Latest fight outcome from the fighter perspective |
| `last_fight_method` | Latest fight finish method |
| `last_event_name` | Latest event name |
| `last_fight_date` | Latest fight date |

---

## Enum Values

### result_type (fights)

| Value | Description |
|---|---|
| `win` | One fighter won (winner_fighter_id is set) |
| `draw` | Fight ended in a draw |
| `nc` | No contest |
| `upcoming` | Fight has not yet occurred |

### event_status (events)

| Value | Description |
|---|---|
| `completed` | Event has taken place |
| `upcoming` | Event is scheduled but has not occurred |

### weight_class (fights, bout_features)

| Value | Weight class rank |
|---|---|
| `strawweight` | 1 |
| `flyweight` | 2 |
| `bantamweight` | 3 |
| `featherweight` | 4 |
| `lightweight` | 5 |
| `welterweight` | 6 |
| `middleweight` | 7 |
| `light_heavyweight` | 8 |
| `heavyweight` | 9 |
| `women_strawweight` | 1 |
| `women_flyweight` | 2 |
| `women_bantamweight` | 3 |
| `women_featherweight` | 4 |
| `catch_weight` | NULL |
| `open_weight` | NULL |

### stance (fighters)

| Value | Description |
|---|---|
| `orthodox` | Right-handed stance (left foot forward) |
| `southpaw` | Left-handed stance (right foot forward) |
| `switch` | Switches between stances |

### finish_method (fights)

| Value | Description |
|---|---|
| `decision` | Went to judges' scorecards |
| `ko_tko` | Knockout or technical knockout |
| `submission` | Submission finish |
| `doctor_stoppage` | Stopped by ringside doctor |
| `overturned` | Result overturned (e.g. failed drug test) |
| `could_not_continue` | Fighter could not continue (injury) |
| `dq` | Disqualification |
| `other` | Other finish method |

### confidence_tier (predictions)

| Value | Probability range | Description |
|---|---|---|
| `high` | p <= 0.30 or p >= 0.70 | Model is relatively confident |
| `medium` | 0.30-0.40 or 0.60-0.70 | Moderate confidence |
| `toss-up` | 0.40-0.60 | Near coin-flip, low signal |

### market (fight_odds)

| Value | Description |
|---|---|
| `moneyline` | Fighter winner market. V1 does not support props, totals, methods, rounds, or parlays |

### line_type (fight_odds)

| Value | Description |
|---|---|
| `opening` | Opening line |
| `current` | Current/latest observed line |
| `closing` | Closing line |
| `unknown` | Line type not known from source |

---

## Feature Set Versions

### v1 (29 features)

The original feature set from Phase 4. Used by Logistic Regression and LightGBM v1.

Columns: `diff_elo`, `diff_career_wins`, `diff_career_fights`, `diff_career_win_rate`, `diff_career_finish_rate`, `diff_career_sig_strikes_landed_pm`, `diff_career_sig_strike_accuracy`, `diff_career_takedown_accuracy`, `diff_career_control_rate`, `diff_age`, `diff_height_cm`, `diff_reach_cm`, `diff_days_since_last_fight`, `diff_win_rate_last3`, `diff_sig_strikes_landed_pm_last3`, `diff_takedown_accuracy_last3`, `diff_control_rate_last3`, `diff_sig_strikes_landed_pm_decay`, `diff_win_rate_decay`, `diff_opp_avg_elo`, `ratio_career_wins`, `ratio_career_fights`, `ratio_career_sig_strikes_landed_pm`, `ratio_career_control_rate`, `ratio_elo`, `is_title_fight`, `scheduled_rounds`, `is_orthodox_vs_southpaw`, `both_debuting`.

### v2 + debut priors (50 features)

Extended feature set from Phase 5. Used by LightGBM v2, XGBoost, and the production model. Adds 18 v2 columns + 3 debut prior columns to v1.

Full column list is in `models/xgb/*/metadata.json:feature_cols`.

---

## Relationships

```
events  1──N  fights
fighters  1──N  fights (as fighter_1_id or fighter_2_id)
fights  1──2  fight_stats_aggregate (one per fighter)
fights  1──N  fight_stats_by_round (one per fighter per round)
fights  1──2  fighter_snapshots (one per fighter)
fights  1──1  bout_features
fights  1──N  predictions (one per scoring run)
fights  1──N  fight_odds (one per fighter/bookmaker/line timestamp)
fight_odds  N──1  fight_odds_normalized view source
fight_odds  N──1  latest_fight_odds view source
fight_odds  N──1  fight_odds_no_vig view source for exact two-sided market groups
predictions  N──1  latest_predictions view source
predictions + fights + events  N──1  current_event_predictions view source
predictions + fights + events  N──1  pre_event_prediction_fights view source
pre_event_prediction_fights  N──1  pre_event_prediction_events view source
fighter_snapshots  N──1  fighter_career_summary view source
```
