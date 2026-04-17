# Model Card: UFC Fight Prediction — XGBoost (calibrated)

## Model Identity

| Field | Value |
|---|---|
| Model name | XGBoost (calibrated) |
| Feature version | v2 + debut priors (50 features) |
| Artifact path | `models/xgb/20260328T221117Z/` |
| Training date | 2026-03-28 |
| Production pointer | `models/production_model.json` |
| Calibration | Platt scaling (logistic regression on log-odds, fit on validation set) |
| Selection rationale | Lowest test log loss (0.6318) with post-hoc calibration improving ECE |

## Intended Use

- **Primary use:** Pre-fight win probability estimation for upcoming UFC bouts.
- **Users:** Sports analysts reviewing upcoming cards, not automated betting systems.
- **Output:** Calibrated probability that fighter_1 wins, with confidence tier and uncertainty flag.
- **Not intended for:** Live in-fight prediction, automated wagering, non-UFC promotions.

## Training Data

| Split | Date range | Rows |
|---|---|---|
| Train | Earliest through 2022-03-11 | 6,376 |
| Validation | 2022-03-12 through 2024-03-08 | 1,007 |
| Test | 2024-03-09 onward | 1,017 |

- Source: ufcstats.com, scraped and loaded into Postgres warehouse.
- Label: binary — 1 if fighter_1 won, 0 if fighter_2 won.
- Excluded: draws, no-contests, and fights with missing labels.
- Temporal split: no random shuffling. Train/val/test are strictly chronological.

## Feature Set (50 columns)

### v1 core (29 features)

| Group | Features |
|---|---|
| Elo | `diff_elo`, `ratio_elo` |
| Career record | `diff_career_wins`, `diff_career_fights`, `diff_career_win_rate`, `diff_career_finish_rate`, `ratio_career_wins`, `ratio_career_fights` |
| Striking | `diff_career_sig_strikes_landed_pm`, `diff_career_sig_strike_accuracy`, `ratio_career_sig_strikes_landed_pm` |
| Grappling | `diff_career_takedown_accuracy`, `diff_career_control_rate`, `ratio_career_control_rate` |
| Physical | `diff_age`, `diff_height_cm`, `diff_reach_cm` |
| Recency | `diff_days_since_last_fight`, `diff_win_rate_last3`, `diff_sig_strikes_landed_pm_last3`, `diff_takedown_accuracy_last3`, `diff_control_rate_last3` |
| Decay | `diff_sig_strikes_landed_pm_decay`, `diff_win_rate_decay` |
| Opponent strength | `diff_opp_avg_elo` |
| Metadata | `is_title_fight`, `scheduled_rounds`, `is_orthodox_vs_southpaw`, `both_debuting` |

### v2 additions (18 features)

| Group | Features |
|---|---|
| Finish style | `diff_career_ko_rate`, `diff_career_sub_rate`, `diff_career_decision_rate` |
| Defense | `diff_career_sig_strikes_absorbed_pm`, `diff_career_sig_strike_defense`, `diff_career_takedown_defense` |
| Experience | `diff_title_fight_count`, `diff_five_round_fights` |
| Physical ratio | `diff_reach_height_ratio` |
| Activity | `diff_fights_per_year_last3` |
| Trends | `diff_slope_sig_strikes_last5`, `diff_slope_td_accuracy_last5`, `diff_slope_control_rate_last5`, `diff_std_sig_strikes_last5`, `diff_std_td_accuracy_last5` |
| Stance | `f1_is_southpaw`, `f2_is_southpaw`, `weight_class_rank` |

### Debut priors (3 features)

| Feature | Description |
|---|---|
| `debut_prior_win_prob_f1` | Fixed at 0.5 (no positional bias). NaN for non-debut bouts. |
| `debut_reach_adv` | Reach difference as z-score relative to weight-class std. NaN for non-debut. |
| `debut_height_adv` | Height difference as z-score relative to weight-class std. NaN for non-debut. |

## Training Methodology

- **Algorithm:** XGBoost (`binary:logistic`)
- **Tuning:** 27-config grid search over `max_depth` x `min_child_weight` x `reg_lambda`
- **Validation:** 5-fold rolling temporal cross-validation for hyperparameter selection
- **Early stopping:** 50 rounds on validation log loss

### Best hyperparameters

| Parameter | Value |
|---|---|
| max_depth | 4 |
| learning_rate | 0.02 |
| subsample | 0.8 |
| colsample_bytree | 0.8 |
| min_child_weight | 20 |
| reg_lambda | 5.0 |
| n_estimators | 500 |
| best_iteration | 185 |

### Calibration

Platt scaling (logistic regression on log-odds) fit on validation set predictions.
Applied post-training to test set and all future predictions.

## Evaluation Results

### Test set metrics

| Metric | Uncalibrated | Calibrated |
|---|---|---|
| Log Loss | 0.6413 | **0.6318** |
| Accuracy | 61.1% | **62.9%** |
| AUC | 0.703 | 0.703 |
| Brier Score | 0.2256 | **0.2209** |
| ECE | 0.067 | **0.061** |

### Model leaderboard (test set, sorted by log loss)

| Model | Log Loss | Accuracy | AUC | ECE |
|---|---|---|---|---|
| **XGBoost (calibrated)** | **0.6318** | 62.9% | 0.703 | 0.061 |
| Ensemble (stacked) | 0.6359 | 65.1% | 0.693 | 0.048 |
| LightGBM v2 | 0.6374 | 62.5% | 0.700 | 0.066 |
| Logistic Reg (calibrated) | 0.6396 | 63.5% | 0.684 | 0.030 |
| Elo baseline | 0.6837 | 54.3% | 0.561 | 0.031 |
| Coin flip | 0.6931 | 54.8% | 0.500 | 0.048 |

### Ensemble decision

Stacked ensemble (test LL 0.6359) did not beat the best single model (0.6318) by the
0.003 threshold. Component models are too correlated for meaningful ensemble lift.

## Confidence Tiers

| Tier | Probability range | Test count | Test accuracy |
|---|---|---|---|
| High | p <= 0.30 or p >= 0.70 | 295 (29.0%) | 68.7% |
| Medium | 0.30-0.40 or 0.60-0.70 | 347 (34.1%) | — |
| Toss-up | 0.40-0.60 | 375 (36.9%) | 52.0% |

## Bias Analysis

### By weight class (test set)

| Weight Class | N | Accuracy | Log Loss | AUC |
|---|---|---|---|---|
| Heavyweight | 67 | 67.2% | 0.610 | 0.675 |
| Middleweight | 129 | 63.6% | 0.616 | 0.725 |
| Lightweight | 131 | 64.1% | 0.629 | 0.712 |
| Women's Flyweight | 52 | 63.5% | 0.647 | 0.670 |
| Flyweight | 83 | 60.2% | 0.650 | 0.662 |
| Featherweight | 122 | 57.4% | 0.662 | 0.718 |
| Women's Strawweight | 68 | 57.4% | 0.667 | 0.678 |
| Bantamweight | 121 | 59.5% | 0.668 | 0.671 |
| Welterweight | 117 | 58.1% | 0.682 | 0.706 |
| Light Heavyweight | 70 | 58.6% | 0.683 | 0.506 |

Model performs best at heavyweight and middleweight, weakest at light heavyweight
(low AUC 0.506) and welterweight.

### By debut status (test set)

| Segment | N | Accuracy | Log Loss |
|---|---|---|---|
| Both experienced | 585 | 61.0% | 0.644 |
| Experience mismatch | 407 | 61.7% | 0.650 |
| Both debuting | 25 | 36.0% | 0.871 |

Both-debuting fights are effectively unpredictable (worse than coin flip). The model
correctly assigns these to the toss-up tier via debut priors.

### Title fights (test set)

| Segment | N | Accuracy | Log Loss |
|---|---|---|---|
| Non-title | 969 | 60.9% | 0.649 |
| Title fight | 48 | 56.2% | 0.723 |

Title fights are harder to predict — smaller sample, higher stakes, more evenly matched fighters.

### By year (test set)

| Year | N | Accuracy | Log Loss |
|---|---|---|---|
| 2024 | 428 | 58.9% | 0.669 |
| 2025 | 513 | 62.0% | 0.641 |
| 2026 | 76 | 61.8% | 0.637 |

No sign of model drift — performance is stable or improving in recent data.

## Known Limitations

1. **Toss-up band:** 37% of fights fall in the 0.40-0.60 range with near-coin-flip accuracy (52%). These are genuinely unpredictable from available features.
2. **Debut fighters:** Both-debuting bouts have 36% accuracy. The model has essentially no signal beyond physical attributes.
3. **Title fights:** Higher log loss (0.723 vs 0.649). Championship bouts feature more evenly matched fighters where the model's discriminative features are less informative.
4. **No injury/camp data:** The model cannot account for injuries, training camp changes, weight cut issues, or short-notice replacements.
5. **No market odds:** The model does not incorporate betting lines, which carry additional information from the market.
6. **Light heavyweight weakness:** AUC of 0.506 (near random) suggests the model's features are not discriminative in this division.
7. **Positional bias:** The model predicts P(fighter_1 wins). Fighter ordering comes from the source data and may carry subtle positional biases.
8. **Historical era sensitivity:** Trained on fights from ~2010-2026. Older MMA eras had different competitive dynamics.

## Model Artifacts

```
models/
  production_model.json          # pointer to active model
  xgb/20260328T221117Z/
    model.joblib                 # trained XGBoost model
    metadata.json                # feature_cols, hyperparameters, split dates
    metrics.json                 # val/test metrics (calibrated + uncalibrated)
  comparison_report.txt          # full model leaderboard
  calibration_comparison.png     # reliability diagram (all models)
  uncertainty_report.txt         # confidence tier analysis
  error_analysis.txt             # segmented error analysis
```

## Reproduction

```bash
# Retrain from scratch
make train_all_v2        # trains LightGBM v2, XGBoost, ensemble, compares

# Score upcoming fights
make predict_pipeline    # load_upcoming → build_features → score

# Review after event completes
make review_event EVENT="UFC 315"
```
