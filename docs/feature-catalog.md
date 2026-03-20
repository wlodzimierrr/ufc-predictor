# Feature Catalog

This document defines every feature computed by the Phase 3 pipeline. It serves as the
single reference for feature modules (`features/`), output tables (`fighter_snapshots`,
`bout_features`), and downstream modeling.

**Conventions:**
- All features use only information available **strictly before** the fight's `event_date`.
- The target fight is always excluded from the fighter's history.
- "Prior fights" = all of a fighter's completed fights where `event_date < cutoff_date`.
- Rate features use `_rate` suffix; per-minute features use `_pm` suffix.
- Rolling features use `_last{N}` suffix (e.g., `sig_strikes_landed_pm_last3`).
- Decayed features use `_decay` suffix.
- Bout-level difference features use `diff_` prefix; ratio features use `ratio_` prefix.

---

## 1. Career Aggregate Features

**Family:** `career`
**Source:** `fights`, `fight_stats_aggregate`, `events`
**Leakage rule:** Only fights with `event_date < cutoff_date`.
**Null handling:** All default to 0 for a debuting fighter (no prior fights).

| # | Feature | Type | Formula | Notes |
|---|---------|------|---------|-------|
| 1 | `career_fights` | int | count of prior fights | |
| 2 | `career_wins` | int | count of prior wins | |
| 3 | `career_losses` | int | count of prior losses | |
| 4 | `career_draws` | int | count of prior draws | |
| 5 | `career_nc` | int | count of prior no-contests | |
| 6 | `career_win_rate` | float | `career_wins / career_fights` | NULL if `career_fights = 0` |
| 7 | `career_finish_rate` | float | `(ko_tko_wins + sub_wins) / career_wins` | NULL if `career_wins = 0` |
| 8 | `career_ko_tko_wins` | int | wins by KO/TKO | |
| 9 | `career_sub_wins` | int | wins by submission | |
| 10 | `career_dec_wins` | int | wins by decision | |
| 11 | `career_ko_tko_losses` | int | losses by KO/TKO | |
| 12 | `career_sub_losses` | int | losses by submission | |
| 13 | `career_title_fights` | int | count of prior title fights | |
| 14 | `career_title_wins` | int | count of prior title fight wins | |
| 15 | `career_minutes` | float | sum of fight time in minutes across prior bouts | From `finish_round`, `finish_time_seconds`, `scheduled_rounds` |
| 16 | `career_sig_strikes_landed_pm` | float | total sig strikes landed / `career_minutes` | NULL if `career_minutes = 0` |
| 17 | `career_sig_strikes_absorbed_pm` | float | total opponent sig strikes landed / `career_minutes` | NULL if `career_minutes = 0` |
| 18 | `career_sig_strike_accuracy` | float | total sig landed / total sig attempted | NULL if no attempts |
| 19 | `career_sig_strike_defense` | float | `1 - (opp_sig_landed / opp_sig_attempted)` | NULL if no opp attempts |
| 20 | `career_takedown_accuracy` | float | total TD landed / total TD attempted | NULL if no attempts |
| 21 | `career_takedown_defense` | float | `1 - (opp_td_landed / opp_td_attempted)` | NULL if no opp attempts |
| 22 | `career_sub_attempts_pm` | float | total sub attempts / `career_minutes` | NULL if `career_minutes = 0` |
| 23 | `career_control_rate` | float | total control seconds / total fight seconds | NULL if `career_minutes = 0` |
| 24 | `career_knockdowns_pm` | float | total knockdowns / `career_minutes` | NULL if `career_minutes = 0` |

---

## 2. Rolling Window Features

**Family:** `rolling`
**Source:** `fights`, `fight_stats_aggregate`, `events`
**Windows:** last 1 fight (`_last1`), last 3 fights (`_last3`), last 5 fights (`_last5`)
**Leakage rule:** Only fights with `event_date < cutoff_date`, ordered by `event_date DESC`, take first N.
**Null handling:** NULL if the fighter has fewer than N prior fights. No imputation — downstream
models (LightGBM) handle NULL natively. A `career_fights` feature already encodes sparsity.

Each metric below is computed for all three windows (×3), producing **45 features total**.

| # | Base Metric | Type | Formula | Notes |
|---|-------------|------|---------|-------|
| 1 | `win_rate_last{N}` | float | wins / N in window | |
| 2 | `finish_rate_last{N}` | float | finishes / wins in window | NULL if 0 wins in window |
| 3 | `sig_strikes_landed_pm_last{N}` | float | avg sig strikes landed per minute across window | |
| 4 | `sig_strikes_absorbed_pm_last{N}` | float | avg opponent sig strikes landed per minute | |
| 5 | `sig_strike_accuracy_last{N}` | float | pooled sig landed / sig attempted over window | |
| 6 | `sig_strike_defense_last{N}` | float | `1 - (opp_sig_landed / opp_sig_attempted)` pooled | |
| 7 | `takedown_landed_pm_last{N}` | float | avg TD landed per minute across window | |
| 8 | `takedown_accuracy_last{N}` | float | pooled TD landed / TD attempted over window | |
| 9 | `takedown_defense_last{N}` | float | `1 - (opp_td_landed / opp_td_attempted)` pooled | |
| 10 | `control_rate_last{N}` | float | total control seconds / total fight seconds in window | |
| 11 | `knockdowns_pm_last{N}` | float | avg knockdowns per minute across window | |
| 12 | `knockdowns_absorbed_pm_last{N}` | float | avg opponent knockdowns per minute | |
| 13 | `sub_attempts_pm_last{N}` | float | avg submission attempts per minute | |
| 14 | `avg_fight_time_last{N}` | float | mean fight duration (minutes) in window | |
| 15 | `streak_last{N}` | int | consecutive wins at end of window (0 if last fight is a loss) | Capped at N |

---

## 3. Exponentially Decayed Features

**Family:** `decay`
**Source:** `fights`, `fight_stats_aggregate`, `events`
**Decay factor:** `alpha = 0.85` (configurable). Weight for i-th most recent fight = `alpha^i`.
**Leakage rule:** Only fights with `event_date < cutoff_date`.
**Null handling:** NULL if the fighter has 0 prior fights.

| # | Feature | Type | Formula | Notes |
|---|---------|------|---------|-------|
| 1 | `sig_strikes_landed_pm_decay` | float | decay-weighted avg of per-fight sig strikes landed/min | |
| 2 | `sig_strikes_absorbed_pm_decay` | float | decay-weighted avg of per-fight opp sig strikes landed/min | |
| 3 | `sig_strike_accuracy_decay` | float | decay-weighted sig landed / sig attempted | |
| 4 | `sig_strike_defense_decay` | float | decay-weighted `1 - (opp_sig_landed / opp_sig_attempted)` | |
| 5 | `takedown_landed_pm_decay` | float | decay-weighted avg of per-fight TD landed/min | |
| 6 | `takedown_accuracy_decay` | float | decay-weighted TD landed / TD attempted | |
| 7 | `takedown_defense_decay` | float | decay-weighted `1 - (opp_td_landed / opp_td_attempted)` | |
| 8 | `control_rate_decay` | float | decay-weighted control seconds / fight seconds | |
| 9 | `knockdowns_pm_decay` | float | decay-weighted avg knockdowns per minute | |
| 10 | `win_rate_decay` | float | decay-weighted win indicator (1 for win, 0 for loss/draw/NC) | |

---

## 4. Physical, Demographic, and Activity Features

**Family:** `physical`
**Source:** `fighters`, `events`
**Leakage rule:** Physical attributes are static profile data — no leakage risk.
Activity features use `event_date` of prior fights.
**Null handling:** See per-feature column below.

| # | Feature | Type | Source | Null Handling | Notes |
|---|---------|------|--------|---------------|-------|
| 1 | `age` | float | `fighters.dob`, `events.event_date` | NULL + `age_missing` flag | Years as decimal |
| 2 | `age_squared` | float | `age^2` | NULL if `age` is NULL | Captures non-linear age effects |
| 3 | `height_cm` | float | `fighters.height_cm` | NULL + `height_reach_missing` flag | |
| 4 | `reach_cm` | float | `fighters.reach_cm` | NULL + `height_reach_missing` flag | |
| 5 | `reach_to_height` | float | `reach_cm / height_cm` | NULL if either missing | Ape index proxy |
| 6 | `is_orthodox` | bool | `fighters.stance = 'Orthodox'` | False if stance is NULL | |
| 7 | `is_southpaw` | bool | `fighters.stance = 'Southpaw'` | False if stance is NULL | |
| 8 | `days_since_last_fight` | int | `cutoff_date - most_recent_prior_event_date` | NULL if debut | |
| 9 | `is_long_layoff` | bool | `days_since_last_fight > 365` | False if debut | |
| 10 | `is_short_notice` | bool | `days_since_last_fight < 28` | False if debut | Proxy — actual notice not in data |
| 11 | `is_debut` | bool | `career_fights = 0` | Always non-NULL | |

### Missingness Indicator Flags

| Flag | Type | Condition |
|------|------|-----------|
| `age_missing` | bool | `fighters.dob IS NULL` |
| `height_reach_missing` | bool | `fighters.height_cm IS NULL OR fighters.reach_cm IS NULL` |

---

## 5. Elo and Opponent-Adjusted Features

**Family:** `elo` / `opponent`
**Source:** `fights`, `events`, computed Elo ratings
**Leakage rule:** Elo is updated sequentially — a fighter's rating before a fight reflects only
prior results. Opponent-adjusted metrics use only the opponent's pre-fight stats.
**Null handling:** Debuting fighters start with default Elo (1500). Opponent-adjusted metrics
are NULL if the opponent has no prior stats.

### 5a. Elo Rating

| # | Feature | Type | Formula | Notes |
|---|---------|------|---------|-------|
| 1 | `elo_rating` | float | Standard Elo with K=32, updated fight-by-fight | Starting value = 1500 |
| 2 | `elo_opponent` | float | Opponent's pre-fight Elo | |
| 3 | `elo_diff` | float | `elo_rating - elo_opponent` | Positive = fighter is favored |

**Elo update rule:**
```
expected = 1 / (1 + 10^((elo_opponent - elo_rating) / 400))
actual   = 1.0 (win), 0.5 (draw), 0.0 (loss)
new_elo  = elo_rating + K * (actual - expected)
```

### 5b. Opponent-Adjusted Metrics

| # | Feature | Type | Formula | Notes |
|---|---------|------|---------|-------|
| 4 | `opp_avg_elo` | float | Mean Elo of prior opponents at fight time | Strength of schedule |
| 5 | `opp_adj_sig_strike_accuracy` | float | Fighter's sig accuracy minus avg opponent sig defense | Positive = better than average against quality opponents |

---

## 6. Bout-Level Features

**Family:** `bout`
**Source:** `fighter_snapshots` for both fighters in a bout
**Leakage rule:** Inherits from snapshot — each snapshot is pre-fight only.
**Null handling:** NULL if either fighter's underlying snapshot feature is NULL.

Bout features combine the two fighter snapshots into a single model-ready row. The fight is
always oriented as **fighter_1** vs **fighter_2** (matching `fights.fighter_1_id` /
`fights.fighter_2_id`). Features are symmetric differences and ratios.

### 6a. Difference Features (`diff_` prefix = fighter_1 - fighter_2)

| # | Feature | Type | Source Features |
|---|---------|------|-----------------|
| 1 | `diff_elo` | float | `elo_rating` |
| 2 | `diff_career_wins` | int | `career_wins` |
| 3 | `diff_career_fights` | int | `career_fights` |
| 4 | `diff_career_win_rate` | float | `career_win_rate` |
| 5 | `diff_career_finish_rate` | float | `career_finish_rate` |
| 6 | `diff_career_sig_strikes_landed_pm` | float | `career_sig_strikes_landed_pm` |
| 7 | `diff_career_sig_strike_accuracy` | float | `career_sig_strike_accuracy` |
| 8 | `diff_career_takedown_accuracy` | float | `career_takedown_accuracy` |
| 9 | `diff_career_control_rate` | float | `career_control_rate` |
| 10 | `diff_age` | float | `age` |
| 11 | `diff_height_cm` | float | `height_cm` |
| 12 | `diff_reach_cm` | float | `reach_cm` |
| 13 | `diff_days_since_last_fight` | int | `days_since_last_fight` |
| 14 | `diff_win_rate_last3` | float | `win_rate_last3` |
| 15 | `diff_sig_strikes_landed_pm_last3` | float | `sig_strikes_landed_pm_last3` |
| 16 | `diff_takedown_accuracy_last3` | float | `takedown_accuracy_last3` |
| 17 | `diff_control_rate_last3` | float | `control_rate_last3` |
| 18 | `diff_sig_strikes_landed_pm_decay` | float | `sig_strikes_landed_pm_decay` |
| 19 | `diff_win_rate_decay` | float | `win_rate_decay` |
| 20 | `diff_opp_avg_elo` | float | `opp_avg_elo` |

### 6b. Ratio Features (`ratio_` prefix = fighter_1 / (fighter_1 + fighter_2))

| # | Feature | Type | Source Features | Notes |
|---|---------|------|-----------------|-------|
| 1 | `ratio_career_wins` | float | `career_wins` | Bounded [0, 1] |
| 2 | `ratio_career_fights` | float | `career_fights` | Experience share |
| 3 | `ratio_career_sig_strikes_landed_pm` | float | `career_sig_strikes_landed_pm` | |
| 4 | `ratio_career_control_rate` | float | `career_control_rate` | |
| 5 | `ratio_elo` | float | `elo_rating` | |

### 6c. Matchup and Metadata Features

| # | Feature | Type | Formula | Notes |
|---|---------|------|---------|-------|
| 1 | `is_title_fight` | bool | `fights.is_title_fight` | Pre-fight metadata |
| 2 | `scheduled_rounds` | int | `fights.scheduled_rounds` | 3 or 5 |
| 3 | `is_orthodox_vs_southpaw` | bool | One orthodox + one southpaw | Stance matchup |
| 4 | `both_debuting` | bool | Both fighters have `career_fights = 0` | |

---

## 7. Summary

| Family | Feature Count | Null Strategy |
|--------|--------------|---------------|
| Career aggregates | 24 | 0 for counts; NULL for rates with 0 denominator |
| Rolling windows (×3) | 45 | NULL if < N prior fights |
| Exponentially decayed | 10 | NULL if 0 prior fights |
| Physical / demographic / activity | 11 + 2 flags = 13 | NULL + `_missing` flag for profile gaps |
| Elo + opponent-adjusted | 5 | Default Elo (1500) for debuts; NULL for opp-adjusted on debut |
| Bout differences | 20 | NULL if either snapshot is NULL |
| Bout ratios | 5 | NULL if both fighters are 0 |
| Bout matchup/metadata | 4 | Always non-NULL |

**Total fighter snapshot features:** ~97 (24 + 45 + 10 + 13 + 5)
**Total bout features:** ~29 (20 + 5 + 4)
**Grand total per bout row:** ~126 features (97 × 2 snapshots compressed into 29 bout-level + raw snapshot columns as needed)

---

## 8. Leakage Prevention Rules

1. **Cutoff date:** `event_date` of the target fight.
2. **Exclusion:** The target fight itself is never included in any aggregate, window, or Elo update.
3. **Career/rolling/decay:** Only fights with `event_date < cutoff_date`.
4. **Elo:** Updated sequentially in chronological order; a fighter's rating before fight F
   reflects only results from fights before F.
5. **Physical features:** Static profile data — no leakage risk.
6. **Opponent-adjusted:** Uses the opponent's pre-fight Elo/stats, not post-fight.
7. **Validation:** `features/tests/test_leakage.py` will assert these rules (T3.5.1).
