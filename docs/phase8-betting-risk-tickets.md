# Phase 8: Betting Value & Risk Management System

Phases 1-7 delivered the UFC data warehouse, leakage-safe features, calibrated fight
winner probabilities, confidence tiers, saved pre-event prediction history, and
post-event prediction accuracy reports.

Phase 8 adds a clean betting-value and risk-management subsystem inside this repo.
It must not replace the production fight prediction model. The existing model answers
"who is more likely to win?" This phase answers "is the market price worth taking,
and how much should we risk?"

This document is a proposed implementation ticket plan only. Do not start code
implementation until this file has been reviewed and approved.

---

## Current Repository Context

Relevant existing pieces inspected before drafting this plan:

| Area | Existing Files / Tables | Notes |
|---|---|---|
| Prediction CLI | `predict.py`, `modeling/score_upcoming.py` | Scores upcoming fights, saves `models/predictions/<event_date>/predictions.csv`, and inserts prediction history into `predictions`. |
| Prediction history | `warehouse/sql/010_predictions.sql`, `warehouse/sql/012_prediction_dashboard_views.sql` | Stores multiple prediction runs by `(fight_id, scored_at)` and exposes latest/current/pre-event views. |
| Honest pre-event logs | `modeling/build_pre_event_prediction_log.py` | Builds `data/reports/pre_event_prediction_fights.csv` and `data/reports/pre_event_prediction_events.csv` from predictions made before event day. |
| Post-event review | `modeling/post_event_review.py` | Separates real saved predictions from catch-up/retroactive review. |
| Retro model backtests | `modeling/backtest_past_events.py` | Scores historical fights with the current production model; useful for accuracy, but not sufficient for betting P/L because odds timing matters. |
| Confidence tiers | `modeling/uncertainty.py` | Current tiers: toss-up is 40%-60%, high is <=30% or >=70%, medium is the remaining confident band. |
| Reports | `data/reports/` | Existing report destination for dashboard-friendly CSV outputs. |
| Commands | `Makefile`, `COMMANDS.md` | Make targets are grouped by warehouse, features, modeling, post-event refresh, and prediction pipeline. |
| Tests | `modeling/tests/`, `features/tests/`, `warehouse/tests/`, `tests/` | Unit tests are pure where possible; DB integration tests skip when unavailable. |

Design implication: betting profitability must be reported separately from prediction
accuracy. Betting backtests must use saved pre-event predictions and historical odds
available before the event, not retroactively scored probability rows unless explicitly
labeled as research-only.

---

## V1 Product Scope

### In Scope

- Store market odds per fight, fighter, bookmaker, timestamp, and line type.
- Support American odds, decimal odds, raw implied probability, and no-vig implied
  probability for two-way UFC winner markets.
- Generate value recommendations by comparing calibrated model probability against
  no-vig market probability.
- Use deterministic conservative risk rules with fractional Kelly staking and hard
  exposure caps.
- Produce current-card betting recommendation CSVs under `data/reports/`.
- Produce chronological betting backtests from historical pre-event predictions and
  historical odds.
- Add unit tests for odds conversion, no-vig, EV, Kelly staking, risk caps, and
  backtest P/L math.
- Document formulas, assumptions, limitations, and the difference between prediction
  accuracy and betting profitability.

### Out of Scope for V1

- Automated bet placement.
- Live odds scraping unless a separate approved odds-source ticket is added later.
- In-play betting.
- Parlays, props, method-of-victory markets, round betting, or totals.
- Training a second learned betting/risk model.
- Optimizing for bet volume.

### Guiding Principles

- Prefer pass decisions when odds are missing, stale, duplicated, mismatched, or
  otherwise ambiguous.
- Conservative bankroll protection is more important than maximizing number of bets.
- Keep the subsystem modular so a learned risk model can later replace or augment the
  deterministic staking policy.
- Keep all betting-specific code under a new `betting/` package and betting-specific
  migrations under `warehouse/sql/`.
- Avoid disturbing `modeling/` except for read-only imports or optional shared helpers.

---

## Proposed Data Contracts

### Odds Input CSV

V1 should support a manual/imported odds CSV as the canonical source, with loader logic
that can later be replaced by a sportsbook API import.

Proposed path:

- `data/odds/fight_odds.csv`

Proposed grain:

- One row per `(fight_id, fighter_id, bookmaker, odds_timestamp, market, line_type)`.

Required columns:

| Column | Description |
|---|---|
| `fight_id` | UUID from `fights.fight_id`. |
| `event_id` | UUID from `events.event_id`, included for auditability and easier validation. |
| `event_date` | Scheduled event date. |
| `fighter_id` | UUID from `fighters.fighter_id`. |
| `fighter_name` | Display name at import time, used only for review/debugging. |
| `opponent_fighter_id` | Opponent UUID for validation. |
| `bookmaker` | Sportsbook or odds source name. |
| `market` | V1 value: `moneyline`. |
| `line_type` | `opening`, `current`, `closing`, or `unknown`. |
| `odds_timestamp` | Timestamp when the price was observed or imported. |
| `american_odds` | American odds, nullable if decimal odds supplied. |
| `decimal_odds` | Decimal odds, nullable if American odds supplied. |
| `source` | Manual/import/API source label. |
| `source_url` | Optional URL or reference. |
| `imported_at` | Timestamp when row entered this project. |

Derived columns should be calculated by code or warehouse views, not hand-entered:

- `implied_probability`
- `no_vig_implied_probability`
- `overround`
- normalized decimal odds used for EV and staking

### Warehouse Tables

Proposed migration:

- `warehouse/sql/017_betting_odds.sql`

Proposed tables/views:

| Object | Purpose |
|---|---|
| `fight_odds` | Raw normalized odds observations. |
| `latest_fight_odds` | Latest valid current line per fight/fighter/bookmaker. |
| `fight_odds_no_vig` | Two-sided no-vig probabilities by fight/bookmaker/timestamp/line type. |
| `betting_recommendations` | Optional persisted recommendation history if v1 needs database-backed auditability. |

V1 can begin with CSV outputs and add `betting_recommendations` only if persistence is
needed for dashboard or audit workflows. Odds storage itself should be in the warehouse
so backtests can query historical lines safely.

### Betting Report Outputs

Recommended output paths:

| Report | Path |
|---|---|
| Current betting recommendations | `data/reports/betting_recommendations.csv` |
| Current event-level betting summary | `data/reports/betting_event_summary.csv` |
| Historical betting backtest fights | `data/reports/betting_backtest_fights.csv` |
| Historical betting backtest events | `data/reports/betting_backtest_events.csv` |
| Historical betting backtest summary | `data/reports/betting_backtest_summary.csv` |

---

## Formulas

### American Odds to Decimal Odds

For positive American odds:

```text
decimal_odds = 1 + american_odds / 100
```

For negative American odds:

```text
decimal_odds = 1 + 100 / abs(american_odds)
```

### Decimal Odds to Raw Implied Probability

```text
implied_probability = 1 / decimal_odds
```

### No-Vig Implied Probability

For a two-sided moneyline market:

```text
overround = implied_probability_f1 + implied_probability_f2
no_vig_probability_f1 = implied_probability_f1 / overround
no_vig_probability_f2 = implied_probability_f2 / overround
```

Rows should be invalid for no-vig calculation unless both fighters for the same
fight, bookmaker, market, line type, and timestamp bucket are present exactly once.

### Expected Value

Use calibrated model probability for the fighter being evaluated:

```text
net_decimal = decimal_odds - 1
ev_per_unit = model_probability * net_decimal - (1 - model_probability)
ev_percent = ev_per_unit
```

### Model Edge

```text
edge = model_probability - no_vig_market_probability
```

### Full Kelly Fraction

```text
b = decimal_odds - 1
p = model_probability
q = 1 - p
full_kelly = (b * p - q) / b
```

Final stake fraction:

```text
stake_fraction = max(0, full_kelly * kelly_fraction)
stake_fraction = min(stake_fraction, tier_cap, max_single_bet_cap, remaining_event_cap)
```

V1 defaults should be conservative and configurable:

| Setting | Proposed Default |
|---|---:|
| `kelly_fraction` | 0.25 |
| `min_edge` | 0.03 |
| `min_ev` | 0.01 |
| `max_single_bet_fraction` | 0.02 |
| `max_event_fraction` | 0.06 |
| `medium_tier_cap` | 0.01 |
| `high_tier_cap` | 0.02 |
| `toss_up_tier_cap` | 0.00 |
| `max_odds_age_hours_current` | 48 |
| `drawdown_protection_threshold` | optional, disabled by default |

---

## Tickets

## T8.1 - Betting Subsystem Skeleton and Configuration

#### T8.1.1 Create betting package and config
- **Description:** Add a new `betting/` package for odds math, market joins, value
  decisions, risk rules, and backtesting. Add deterministic configuration defaults
  that can be overridden by CLI arguments or a small config file.
- **Status:** DONE
- **Dependencies:** None
- **Acceptance Criteria:**
  - New package exists at `betting/` with focused modules, for example:
    - `betting/odds.py`
    - `betting/value.py`
    - `betting/risk.py`
    - `betting/recommend.py`
    - `betting/backtest.py`
    - `betting/config.py`
  - Public functions are pure where possible and easy to unit test.
  - Default risk config uses conservative caps from this spec.
  - No production model training/scoring behavior changes.
  - No imports from `betting/` are required by existing prediction workflows.
- **Test Coverage:**
  - Basic import smoke test for the package.
  - Config default test verifying conservative caps and toss-up cap equals zero.
- **Complexity:** S
- **Risk:** Low

#### T8.1.2 Define reason-code vocabulary
- **Description:** Establish standard reason codes for all bet/pass decisions so reports
  are machine-readable and auditable.
- **Status:** DONE
- **Dependencies:** T8.1.1
- **Acceptance Criteria:**
  - Reason codes are centralized in code or documented constants.
  - Required pass codes include:
    - `missing_odds`
    - `stale_odds`
    - `ambiguous_odds`
    - `invalid_odds`
    - `missing_prediction`
    - `toss_up_tier`
    - `edge_below_threshold`
    - `ev_below_threshold`
    - `kelly_non_positive`
    - `single_bet_cap_zero`
    - `event_exposure_cap_reached`
    - `drawdown_protection`
  - Required bet/cap codes include:
    - `positive_edge`
    - `positive_ev`
    - `fractional_kelly`
    - `tier_cap_applied`
    - `single_bet_cap_applied`
    - `event_cap_applied`
  - Reports include `reason_codes` as a pipe-delimited string.
- **Test Coverage:**
  - Unit tests assert key decision scenarios produce expected reason codes.
- **Complexity:** S
- **Risk:** Low

---

## T8.2 - Odds Storage and Loading

#### T8.2.1 Add odds warehouse migration
- **Description:** Add schema support for normalized fight moneyline odds while keeping
  all odds tied to existing `fight_id` and `fighter_id` values.
- **Status:** DONE
- **Dependencies:** T8.1.1
- **Acceptance Criteria:**
  - New migration `warehouse/sql/017_betting_odds.sql`.
  - `fight_odds` stores raw odds observations with:
    `fight_id`, `event_id`, `fighter_id`, `opponent_fighter_id`, `bookmaker`,
    `market`, `line_type`, `odds_timestamp`, `american_odds`, `decimal_odds`,
    `source`, `source_url`, `imported_at`.
  - Foreign keys reference `fights`, `events`, and `fighters`.
  - Constraints reject rows where neither American nor decimal odds are supplied.
  - Constraints restrict `market` to `moneyline` for v1.
  - Constraints restrict `line_type` to `opening`, `current`, `closing`, `unknown`.
  - Indexes support lookup by fight, event, bookmaker, line type, and timestamp.
  - No-vig view or query pattern requires exactly two sides before emitting no-vig
    probabilities.
- **Test Coverage:**
  - Warehouse migration applies cleanly in the existing migration runner.
  - DB tests can be skipped if Postgres is unavailable, matching current integration
    test style.
- **Complexity:** M
- **Risk:** Medium - schema must be flexible enough for later odds providers.

#### T8.2.2 Add odds CSV import/validation
- **Description:** Build an idempotent loader for `data/odds/fight_odds.csv` into
  `fight_odds`.
- **Status:** DONE
- **Dependencies:** T8.2.1
- **Acceptance Criteria:**
  - New script, for example `warehouse/load_fight_odds.py`.
  - Validates required columns and enum values before writing.
  - Confirms each `fight_id`, `event_id`, `fighter_id`, and `opponent_fighter_id`
    exists in warehouse.
  - Confirms each fight has exactly two fighters and imported fighter/opponent IDs
    match that fight.
  - Converts supplied American/decimal odds into normalized decimal odds and raw
    implied probability.
  - Upserts by a stable key such as
    `(fight_id, fighter_id, bookmaker, market, line_type, odds_timestamp)`.
  - Prints row counts for imported, skipped, and rejected rows.
  - Re-running the loader is idempotent.
- **Test Coverage:**
  - Unit tests for CSV validation using small in-memory fixtures.
  - DB integration test can skip without database.
- **Complexity:** M
- **Risk:** Medium - imported odds data may be messy.

#### T8.2.3 Add odds data dictionary docs
- **Description:** Extend documentation with odds table and CSV field definitions.
- **Status:** DONE
- **Dependencies:** T8.2.1, T8.2.2
- **Acceptance Criteria:**
  - `docs/data_dictionary.md` includes `fight_odds` and derived betting report columns.
  - `docs/betting.md` includes the input CSV contract and examples.
  - The docs state that odds timestamps are required for leakage-safe backtests.
- **Test Coverage:**
  - Documentation-only ticket; no automated tests required.
- **Complexity:** S
- **Risk:** Low

---

## T8.3 - Odds Math and No-Vig Probabilities

#### T8.3.1 Implement odds conversion helpers
- **Description:** Add pure functions for odds normalization and implied probability.
- **Status:** DONE
- **Dependencies:** T8.1.1
- **Acceptance Criteria:**
  - Supports American odds to decimal odds.
  - Supports decimal odds to American odds if useful for reports.
  - Supports decimal odds to implied probability.
  - Rejects invalid American odds values, including `0`.
  - Rejects decimal odds `<= 1.0`.
  - Handles numeric strings from CSV imports.
  - Rounds only at presentation/output boundaries, not in core math.
- **Test Coverage:**
  - Unit tests for:
    - `+150 -> 2.50`
    - `-200 -> 1.50`
    - `2.50 -> 0.40 implied`
    - invalid zero American odds
    - invalid decimal odds
- **Complexity:** S
- **Risk:** Low

#### T8.3.2 Implement no-vig market probability
- **Description:** Add no-vig probability calculation for two-sided winner markets.
- **Status:** DONE
- **Dependencies:** T8.3.1
- **Acceptance Criteria:**
  - Accepts exactly two fighters for a market group.
  - Computes raw implied probability, overround, and normalized no-vig probability.
  - Rejects incomplete, duplicated, or more-than-two-sided groups.
  - Returns explicit invalid status/reason instead of silently guessing.
  - Preserves `bookmaker`, `line_type`, and `odds_timestamp` in derived rows.
- **Test Coverage:**
  - Unit tests for balanced and overround markets.
  - Unit tests for missing side and duplicate side invalidation.
  - Unit test that no-vig probabilities sum to 1.0 within tolerance.
- **Complexity:** S
- **Risk:** Low

---

## T8.4 - Value and Bet/Pass Decisions

#### T8.4.1 Join predictions to odds
- **Description:** Build a recommendation input layer that joins current/latest
  predictions with valid odds rows.
- **Status:** DONE
- **Dependencies:** T8.2.2, T8.3.2
- **Acceptance Criteria:**
  - Upcoming recommendations read from `latest_predictions` or
    `current_event_predictions`.
  - Each fight produces one evaluation row per fighter per selected bookmaker and
    line type.
  - Fighter 1 uses `calibrated_prob_f1`; fighter 2 uses `1 - calibrated_prob_f1`.
  - Odds are excluded if stale beyond configured max age.
  - Odds are excluded or marked pass if a fight/fighter join is ambiguous.
  - Output includes prediction fields and market fields without overwriting model
    accuracy fields.
- **Test Coverage:**
  - Unit tests for fighter 1/fighter 2 probability assignment.
  - Unit tests for stale and ambiguous odds handling.
- **Complexity:** M
- **Risk:** Medium - ID consistency is critical.

#### T8.4.2 Compute edge and expected value
- **Description:** Compare calibrated model probability against no-vig market
  probability and offered odds.
- **Status:** DONE
- **Dependencies:** T8.4.1
- **Acceptance Criteria:**
  - Computes:
    - `model_probability`
    - `market_implied_probability`
    - `no_vig_market_probability`
    - `edge`
    - `ev_per_unit`
    - `ev_percent`
  - Uses no-vig market probability for edge.
  - Uses offered decimal odds for EV.
  - Bets require both `edge >= min_edge` and `ev_per_unit >= min_ev`.
  - Missing/invalid odds always produce pass.
- **Test Coverage:**
  - Unit tests for positive EV, negative EV, positive edge with insufficient EV,
    and sufficient EV with insufficient edge.
- **Complexity:** S
- **Risk:** Low

#### T8.4.3 Implement conservative bet/pass policy
- **Description:** Produce final `bet` or `pass` decisions before staking.
- **Status:** DONE
- **Dependencies:** T8.4.2, T8.1.2
- **Acceptance Criteria:**
  - Toss-up tier always passes, even with apparent positive EV.
  - Medium/high tiers can bet only if odds are valid and edge/EV thresholds pass.
  - Missing, stale, or ambiguous odds always pass.
  - Output includes:
    - `decision`
    - `recommended_fighter_id`
    - `recommended_fighter_name`
    - `reason_codes`
  - The system can evaluate both fighters but should not recommend both sides of
    the same fight for the same bookmaker/timestamp group.
- **Test Coverage:**
  - Unit tests for all major pass reasons.
  - Unit test preventing two bet recommendations on opposite sides of one market.
- **Complexity:** M
- **Risk:** Medium - clear reason codes prevent misleading output.

---

## T8.5 - Risk Management and Staking

#### T8.5.1 Implement fractional Kelly staking
- **Description:** Convert positive EV recommendations into stake sizes using
  fractional Kelly and bankroll-aware caps.
- **Status:** DONE
- **Dependencies:** T8.4.3
- **Acceptance Criteria:**
  - Full Kelly formula is implemented for decimal odds.
  - Negative or zero Kelly produces pass or zero stake.
  - Fractional Kelly multiplier defaults to 0.25.
  - Stake is expressed as both bankroll fraction and currency amount when bankroll
    is supplied.
  - Core staking function is pure and deterministic.
- **Test Coverage:**
  - Unit tests for full Kelly, fractional Kelly, zero Kelly, and negative Kelly.
- **Complexity:** S
- **Risk:** Low

#### T8.5.2 Apply confidence and exposure caps
- **Description:** Apply tier caps, max single bet exposure, and max event/card exposure
  after Kelly sizing.
- **Status:** DONE
- **Dependencies:** T8.5.1
- **Acceptance Criteria:**
  - Toss-up cap is zero and results in no bet.
  - Medium tier uses smaller max stake than high tier.
  - Max single bet exposure is enforced after tier cap.
  - Max event exposure is enforced cumulatively by event.
  - If event exposure is exhausted, later qualifying bets are passed or reduced to
    zero with `event_exposure_cap_reached`.
  - Recommended allocation order is deterministic, for example highest EV then
    highest edge then timestamp/fight ID.
  - Output includes uncapped Kelly fraction and final capped stake fraction.
- **Test Coverage:**
  - Unit tests for medium/high/toss-up caps.
  - Unit tests for single-bet cap.
  - Unit tests for cumulative event cap across multiple bets.
- **Complexity:** M
- **Risk:** Medium - cap ordering must be predictable.

#### T8.5.3 Optional drawdown protection
- **Description:** Add configurable drawdown protection for backtests and optionally
  current recommendations.
- **Status:** DONE
- **Dependencies:** T8.5.2
- **Acceptance Criteria:**
  - Disabled by default.
  - When enabled, reduces or disables staking after a configured bankroll drawdown.
  - Backtest reports show whether drawdown protection was enabled and when it fired.
  - Current recommendations can accept an optional current drawdown input, but should
    not infer live bankroll state from historical reports unless explicitly provided.
- **Test Coverage:**
  - Unit tests for disabled behavior.
  - Unit tests for threshold-triggered stake reduction/pass behavior.
- **Complexity:** M
- **Risk:** Medium - avoid false precision around bankroll state.

---

## T8.6 - Current Betting Recommendation Reports and Commands

#### T8.6.1 Add betting recommendation CLI
- **Description:** Add a command-line entry point that generates current-card betting
  recommendations from current predictions and latest valid odds.
- **Status:** DONE
- **Dependencies:** T8.2.2, T8.4.3, T8.5.2
- **Acceptance Criteria:**
  - New script, for example `betting/recommend.py`.
  - Supports filters:
    - all upcoming/current fights
    - `--event "Event Name"`
    - `--next`
    - `--bookmaker`
    - `--line-type current|opening|closing`
    - `--bankroll`
  - Writes `data/reports/betting_recommendations.csv`.
  - Writes `data/reports/betting_event_summary.csv`.
  - Prints concise card-level summary:
    - bets recommended
    - total stake
    - event exposure
    - top pass reasons
  - Does not call model scoring itself unless explicitly documented. Recommended
    workflow remains: generate predictions first, then generate betting value.
- **Test Coverage:**
  - CLI smoke test with fixture CSV/database mocks where practical.
  - Unit tests cover calculation-heavy behavior in lower-level modules.
- **Complexity:** M
- **Risk:** Medium - CLI must avoid implying odds are fresh when they are not.

#### T8.6.2 Add Makefile and COMMANDS entries
- **Description:** Expose betting workflows through existing command conventions.
- **Status:** DONE
- **Dependencies:** T8.6.1
- **Acceptance Criteria:**
  - Add Makefile targets:
    - `load_odds`
    - `betting_recommendations`
    - `betting_backtest`
    - `test_betting`
  - Add `COMMANDS.md` section for odds import, recommendations, and backtests.
  - Make targets do not disturb existing `predict`, `predict_pipeline`,
    `review_event`, or `backtest_past` behavior.
- **Test Coverage:**
  - Manual command smoke checks documented in the implementation PR.
- **Complexity:** S
- **Risk:** Low

#### T8.6.3 Add report schema documentation
- **Description:** Document all output report columns and decision semantics.
- **Status:** DONE
- **Dependencies:** T8.6.1
- **Acceptance Criteria:**
  - `docs/betting.md` documents recommendation report columns:
    - fight/event IDs
    - fighter IDs/names
    - bookmaker/line metadata
    - model probability
    - market/no-vig probabilities
    - edge
    - EV
    - Kelly fraction
    - capped stake fraction
    - stake amount
    - decision
    - reason codes
  - Includes a clear warning that positive model edge is not guaranteed profit.
  - Explains stale odds and ambiguous odds behavior.
- **Test Coverage:**
  - Documentation-only ticket; no automated tests required.
- **Complexity:** S
- **Risk:** Low

---

## T8.7 - Chronological Betting Backtest

#### T8.7.1 Build leakage-safe historical betting dataset
- **Description:** Join historical pre-event predictions, historical odds, and actual
  outcomes without using information unavailable before each event.
- **Status:** DONE
- **Dependencies:** T8.2.2, T8.3.2, T8.4.1
- **Acceptance Criteria:**
  - Primary prediction source is `pre_event_prediction_fights`, because it keeps
    latest saved predictions where `scored_at::date < event_date`.
  - Odds source is historical odds rows where `odds_timestamp < event_date` and
    preferably `odds_timestamp <= scored_at` when comparing to a specific saved
    prediction timestamp.
  - Backtest defaults to one line policy, configurable:
    - latest available pre-event current line
    - closing line
    - opening line
  - Rows without valid two-sided no-vig market are passed, not guessed.
  - Draws, no contests, unresolved fights, and fighter replacements are excluded
    or marked non-bet according to documented policy.
  - Output records the exact prediction timestamp and odds timestamp used.
- **Test Coverage:**
  - Unit tests with fixture rows proving future odds are excluded.
  - Unit tests proving future predictions are excluded.
  - Unit test for missing odds producing pass.
- **Complexity:** M
- **Risk:** High - leakage prevention is the most important part of the backtest.

#### T8.7.2 Simulate bet outcomes and bankroll path
- **Description:** Run chronological event-by-event staking and settlement.
- **Status:** DONE
- **Dependencies:** T8.7.1, T8.5.2
- **Acceptance Criteria:**
  - Backtest processes events in ascending `event_date`.
  - Stakes are based on bankroll available before the event.
  - All bets on one event settle after the event, so same-card wins do not increase
    stake capacity for later fights on that card.
  - Winning bet profit is `stake * (decimal_odds - 1)`.
  - Losing bet profit is `-stake`.
  - Push/void handling is explicit if ever encountered; v1 can exclude non-W/L rows.
  - Bankroll path, peak bankroll, drawdown, and max drawdown are computed.
- **Test Coverage:**
  - Unit tests for win/loss P/L.
  - Unit tests for event-level settlement order.
  - Unit tests for max drawdown calculation.
- **Complexity:** M
- **Risk:** Medium

#### T8.7.3 Produce betting backtest reports
- **Description:** Generate required betting profitability reports under
  `data/reports/`.
- **Status:** DONE
- **Dependencies:** T8.7.2
- **Acceptance Criteria:**
  - Fight-level report includes every evaluated side or every final recommendation
    decision, depending on selected detail mode.
  - Summary report includes:
    - total bets
    - total staked
    - profit/loss
    - ROI
    - hit rate
    - average odds
    - max drawdown
    - ROI by confidence tier
    - ROI by edge bucket
    - ROI by event
  - Event-level report includes:
    - event date/name
    - bets
    - staked
    - P/L
    - ROI
    - ending bankroll
    - drawdown after event
  - Edge bucket defaults are documented, for example:
    - `0-3%`
    - `3-5%`
    - `5-10%`
    - `10%+`
  - The report clearly labels odds policy and bankroll/risk configuration used.
- **Test Coverage:**
  - Unit tests for ROI by tier and edge bucket aggregation.
  - Snapshot-style test for expected summary columns.
- **Complexity:** M
- **Risk:** Medium

#### T8.7.4 Add betting backtest CLI
- **Description:** Add a CLI for generating backtest reports.
- **Status:** DONE
- **Dependencies:** T8.7.3
- **Acceptance Criteria:**
  - New script, for example `betting/backtest.py`.
  - CLI supports:
    - `--start-date`
    - `--end-date`
    - `--bookmaker`
    - `--line-type`
    - `--odds-policy latest-before-event|latest-before-prediction|opening|closing`
    - `--initial-bankroll`
    - `--kelly-fraction`
    - cap overrides
  - Writes all required CSV reports under `data/reports/`.
  - Prints concise summary metrics.
  - Default behavior is conservative and leakage-safe.
- **Test Coverage:**
  - CLI smoke test with local fixtures where practical.
  - Core math covered by unit tests.
- **Complexity:** M
- **Risk:** Medium

---

## T8.8 - Documentation and Limitations

#### T8.8.1 Create betting methodology document
- **Description:** Add a dedicated document for assumptions, formulas, commands, and
  limitations.
- **Status:** DONE
- **Dependencies:** T8.3.2, T8.5.2, T8.7.3
- **Acceptance Criteria:**
  - New `docs/betting.md` explains:
    - Prediction probability vs betting profitability.
    - American odds, decimal odds, implied probability, and no-vig probability.
    - EV and edge formulas.
    - Fractional Kelly and why it is capped.
    - Confidence-tier staking rules.
    - Stale/missing/ambiguous odds behavior.
    - Backtest leakage rules.
    - Known limitations.
  - Includes examples with simple numbers.
  - Explicitly states that recommendations are analytical outputs, not automated
    betting instructions.
- **Test Coverage:**
  - Documentation-only ticket; no automated tests required.
- **Complexity:** S
- **Risk:** Low

#### T8.8.2 Update README and runbook references
- **Description:** Add concise links from existing docs to the new betting subsystem.
- **Status:** DONE
- **Dependencies:** T8.8.1, T8.6.2
- **Acceptance Criteria:**
  - `README.md` adds a short section explaining that betting value is separate from
    winner prediction.
  - `docs/runbook.md` or `COMMANDS.md` links to `docs/betting.md`.
  - Existing model card remains focused on prediction model quality, not betting ROI.
- **Test Coverage:**
  - Documentation-only ticket; no automated tests required.
- **Complexity:** S
- **Risk:** Low

---

## T8.9 - Test Suite and Quality Gates

#### T8.9.1 Add betting unit test suite
- **Description:** Add focused deterministic tests for the new subsystem.
- **Status:** DONE
- **Dependencies:** T8.3.1, T8.3.2, T8.4.3, T8.5.2, T8.7.2
- **Acceptance Criteria:**
  - New tests live under `betting/tests/` or `tests/betting/`, matching project
    conventions.
  - Required tests:
    - odds conversion
    - no-vig probability calculation
    - EV calculation
    - Kelly staking
    - risk caps
    - backtest profit/loss math
  - Tests use small explicit fixture data.
  - Tests do not require live odds or network access.
  - DB-dependent tests skip cleanly if Postgres is unavailable.
- **Test Coverage:**
  - This is the umbrella test ticket.
- **Complexity:** M
- **Risk:** Low

#### T8.9.2 Add regression checks for leakage-sensitive betting backtest behavior
- **Description:** Add tests that specifically guard the betting backtest from using
  unavailable future predictions or odds.
- **Status:** DONE
- **Dependencies:** T8.7.1
- **Acceptance Criteria:**
  - Fixture includes multiple predictions for the same fight, one before and one
    after event date.
  - Fixture includes multiple odds rows, one before and one after event date.
  - Backtest selects only valid pre-event rows.
  - Backtest records selected timestamps in output.
  - If no valid pre-event odds exist, the fight is pass with `missing_odds` or
    `stale_odds`, not a bet.
- **Test Coverage:**
  - Unit-level fixture test without database if possible.
- **Complexity:** M
- **Risk:** Medium

---

## T8.10 - External Odds Source Adapters

#### T8.10.0 Acquire Kaggle odds source data
- **Description:** Obtain the raw Kaggle UFC/MMA daily odds dataset for use as the
  first external odds source.
- **Status:** DONE
- **Dependencies:** T8.10.1
- **Acceptance Criteria:**
  - Raw Kaggle odds file is downloaded manually or through the Kaggle API.
  - Raw file is stored under `data/odds/raw/` without normalization or manual edits.
  - Source metadata is recorded, including:
    - Kaggle dataset URL
    - download date
    - source file name
    - license
    - notes about the download method
  - Raw data is checked for fields needed by the adapter:
    - event date/name
    - fighter names
    - bookmaker/source
    - moneyline/head-to-head odds
    - odds collection timestamp
  - If required fields are missing or ambiguous, document the gap before writing
    adapter logic.
  - No raw Kaggle rows are loaded directly into `fight_odds`.
- **Test Coverage:**
  - Data acquisition/documentation ticket; no automated tests required.
- **Complexity:** S
- **Risk:** Low

#### T8.10.1 Add raw odds source folders and canonical artifacts
- **Description:** Establish a small file layout for raw external odds inputs,
  normalized odds outputs, and review artifacts.
- **Status:** DONE
- **Dependencies:** T8.2.2, T8.8.2
- **Acceptance Criteria:**
  - Add directories or documented paths for:
    - `data/odds/raw/`
    - `data/odds/sources/`
    - `data/odds/fight_odds.csv`
    - `data/odds/unmatched_odds.csv`
  - `data/odds/fight_odds.csv` remains the canonical V1 loader input.
  - Raw source files are never treated as loaded/validated odds until converted.
  - Documentation explains which files are generated and which are manual inputs.
  - Existing odds loader behavior does not change.
- **Test Coverage:**
  - Documentation/path-only ticket; no automated tests required unless helper code is added.
- **Complexity:** S
- **Risk:** Low

#### T8.10.2 Add Kaggle odds adapter
- **Description:** Convert the Kaggle UFC/MMA daily odds dataset into the canonical
  `data/odds/fight_odds.csv` contract.
- **Status:** DONE
- **Dependencies:** T8.10.0, T8.10.1, T8.2.2
- **Acceptance Criteria:**
  - New script, for example `warehouse/adapt_kaggle_odds.py`.
  - Reads a manually downloaded Kaggle odds CSV from `data/odds/raw/`.
  - Filters to head-to-head/moneyline rows for V1.
  - Preserves bookmaker/source metadata and collection timestamps.
  - Maps event/fighter names to existing warehouse `event_id`, `fight_id`,
    `fighter_id`, and `opponent_fighter_id`.
  - Writes matched rows to `data/odds/fight_odds.csv` or
    `data/odds/sources/kaggle_fight_odds.csv`.
  - Writes uncertain or unmatched rows to `data/odds/unmatched_odds.csv` with
    enough detail for manual review.
  - Does not guess when multiple fights or fighters could match.
  - Does not import Kaggle modeling features into training or scoring workflows.
- **Test Coverage:**
  - Unit tests with tiny Kaggle-like CSV fixtures.
  - Tests for exact match, unmatched fighter, duplicate/ambiguous match, and
    moneyline-only filtering.
  - DB-dependent mapping tests skip cleanly if Postgres is unavailable.
- **Complexity:** M
- **Risk:** Medium - name matching must be auditable and conservative.

#### T8.10.3 Add odds matching QA report
- **Description:** Produce a reviewable report showing how external odds rows mapped
  to warehouse fights and where manual aliases are needed.
- **Status:** DONE
- **Dependencies:** T8.10.2
- **Acceptance Criteria:**
  - Writes a QA report under `data/reports/`, for example
    `data/reports/odds_matching_qa.csv`.
  - Report includes counts for matched, unmatched, duplicate, ambiguous, and
    rejected rows.
  - Report includes source event/fighter names, candidate warehouse IDs/names,
    match reason, and rejection reason.
  - Manual review can identify fighter aliases needed for future imports.
  - QA report can be regenerated deterministically from the same source CSV.
- **Test Coverage:**
  - Unit tests for QA row generation and summary counts.
- **Complexity:** S
- **Risk:** Low

#### T8.10.4 Document BestFightOdds future scraper design
- **Description:** Add a design note for a future BestFightOdds or comparable
  historical odds source adapter while keeping V1 focused on normalized CSV imports.
- **Status:** DONE
- **Dependencies:** T8.10.1
- **Acceptance Criteria:**
  - Documentation identifies BestFightOdds as a potential deeper historical source.
  - Design states that scraped output must normalize into the same
    `data/odds/fight_odds.csv` contract.
  - Design covers rate limiting, source attribution, raw snapshot storage, and
    terms-of-use review before scraping.
  - Design keeps BestFightOdds data separate from model training/scoring unless a
    future ticket explicitly changes that.
  - No scraper is run or scheduled as part of this ticket.
- **Test Coverage:**
  - Documentation-only ticket; no automated tests required.
- **Complexity:** S
- **Risk:** Medium - scraping must be handled carefully and respectfully.

Design note:

BestFightOdds, or another reputable public sportsbook-odds archive with MMA
coverage, is a possible future source for deeper historical moneyline prices.
This should be treated as a separate adapter ticket after V1 CSV imports are
stable. The V1 odds loader contract does not change: any scraped or externally
collected output must normalize into the same `data/odds/fight_odds.csv` schema
before it can be loaded into `fight_odds`.

Future source adapter shape:

- Store untouched raw fetch artifacts under `data/odds/raw/<source>/`, grouped by
  fetch date/source event page, before any parsing or normalization.
- Write source-specific normalized rows under
  `data/odds/sources/<source>_fight_odds.csv`.
- Preserve source attribution on every row: source name, source URL, bookmaker,
  original event/fighter labels, observed odds timestamp, and project import
  timestamp.
- Write unmatched or ambiguous rows to a source-specific review artifact, for
  example `data/odds/sources/<source>_unmatched_odds.csv`, rather than guessing.
- Only publish reviewed rows into `data/odds/fight_odds.csv`, the canonical V1
  loader input.

Scraping guardrails:

- Review the site's terms of use, robots policy, and any available licensing or
  API options before writing or running a scraper.
- Prefer an official export/API/permissioned data path when one exists.
- Use conservative rate limiting, retries with backoff, request timeouts, clear
  user agent/contact metadata where appropriate, and resumable fetch manifests.
- Capture raw snapshots for auditability so parser changes do not require
  repeated requests.
- Do not run continuous/scheduled scraping until a future ticket explicitly
  approves cadence, limits, monitoring, and failure handling.

Modeling boundary:

BestFightOdds or comparable source data remains an odds/evaluation input only.
It must stay separate from production model training and scoring unless a future
ticket explicitly approves adding market-derived features. Until then, odds may
feed recommendation reports, leakage-safe betting backtests, and clearly labeled
research-only analyses, but not the fight-winner model itself.

---

## T8.11 - Scraping-Only Future Odds Source Selection

#### T8.11.0 Research future odds source candidates
- **Description:** Compare candidate UFC/MMA odds websites and choose the next
  scraping-only source path for deeper and cleaner odds coverage. Paid API routes
  are out of scope by product decision.
- **Status:** DONE
- **Dependencies:** T8.10.1, T8.10.4
- **Acceptance Criteria:**
  - Research compares website/archive options for UFC/MMA moneyline coverage.
  - Research identifies the preferred next scraping candidate.
  - Research records why paid API routes are not active implementation paths.
  - Research records why scraping-heavy options need terms, robots, permission,
    and rate-limit review before implementation.
  - Research keeps all future source data normalized into the canonical
    `data/odds/fight_odds.csv` contract.
  - Research does not run, schedule, or implement a scraper.
- **Research Summary:**
  - **Recommended first scraping candidate: BestFightOdds.** It is MMA-specific
    and its archive advertises all site odds stored historically, with thousands
    of matchups/fighter profiles dating back to 2007. It also exposes fighter
    history pages with open prices, closing ranges, movement, event labels, and
    fight dates. This is the best match for a no-paid-API approach.
  - **Main BestFightOdds unknowns:** exact source attribution per bookmaker,
    whether true observed timestamps exist beyond open/close labels, page
    stability, and terms/robots/permission posture for automated collection.
  - **Avoid as scraper target by default: OddsPortal.** OddsPortal has broad
    archived odds, but its terms restrict non-personal/commercial use, database
    extraction, automated requests, aggregation, scraping, and recreating content
    without consent. Do not build an OddsPortal scraper unless permission or a
    licensed data path is obtained.
  - **Paid APIs are not active paths.** The Odds API, SportsDataIO paid access,
    TheRundown, ParlayAPI, and similar services may be technically cleaner, but
    they are excluded from this roadmap while the project has a no-paid-API
    constraint.
  - **Existing free/keyed API work stays secondary.** BALLDONTLIE can continue to
    grow the honest forward sample if already available, but it is not the main
    historical backfill strategy.
- **Source Notes:**
  - The Odds API MMA page: `https://the-odds-api.com/sports/mma-ufc-odds.html`
  - The Odds API v4 docs: `https://the-odds-api.com/liveapi/guides/v4/`
  - BestFightOdds archive: `https://www.bestfightodds.com/archive`
  - BestFightOdds terms: `https://www.bestfightodds.com/terms`
  - OddsPortal terms: `https://www.oddsportal.com/terms/`
  - SportsDataIO API/free trial docs:
    `https://sportsdata.io/developers/apis`
  - SportsDataIO scrambled-data help:
    `https://sportsdata.io/help/scrambled-data`
- **Test Coverage:**
  - Documentation-only research ticket; no automated tests required.
- **Complexity:** S
- **Risk:** Low

#### T8.11.1 BestFightOdds manual/deep-history feasibility study
- **Description:** Assess BestFightOdds as the first scraping candidate without
  building or running an automated scraper yet.
- **Status:** DONE
- **Dependencies:** T8.11.0
- **Acceptance Criteria:**
  - Manually review several UFC archive/fighter pages for fields needed by
    `data/odds/fight_odds.csv`: event date, event name, fighters, open odds,
    close odds/ranges, bookmaker/source attribution, and timestamps if present.
  - Document whether the archive exposes true observed timestamps or only
    opening/closing labels.
  - Document terms-of-use, robots, rate-limit, and permission questions before
    any automated fetching.
  - Document whether a manual export/review workflow is possible before scraping.
  - No scraper is implemented, run, or scheduled.
- **Test Coverage:**
  - Documentation-only feasibility ticket; no automated tests required.
- **Complexity:** S
- **Risk:** Medium - deep history is valuable, but website archive extraction can
  be brittle and must be handled respectfully.

Feasibility study completed on 2026-08-19.

Manual pages reviewed:

| Page | URL | Relevant observations |
|---|---|---|
| Archive | `https://www.bestfightodds.com/archive` | Archive page has event/fighter search, recently completed events, and states that posted odds are stored in the archive back to the site's 2007 launch. Good discovery surface, but not itself a canonical export. |
| Recent event | `https://www.bestfightodds.com/events/ufc-330-4237` | Event page exposes event name/date, fight matchups, current displayed moneyline prices, bookmaker columns on the rendered odds table, and relative "Last change" text. The reviewed rendered text did not expose precise observed timestamps for each price. |
| Current/home odds table | `https://www.bestfightodds.com/` | Rendered odds table showed bookmaker/source columns such as Polymarket, Kalshi, FanDuel, Caesars, BetRivers, BetWay, Unibet, BetMGM, DraftKings, and Props on current event rows. This confirms bookmaker attribution is present for live/recent event rows, though future parser work must verify column stability in stored raw HTML. |
| Fighter history: Ian Machado Garry | `https://www.bestfightodds.com/fighters/ian-machado-garry-15690` | Fighter page exposes fight count, "with odds" count, event names/dates, both fighters, opening moneyline, closing range, and movement percentage. No precise odds observation timestamps were visible in the rendered history table. |
| Fighter history: Islam Makhachev | `https://www.bestfightodds.com/fighters/islam-makhachev-5541` | Same useful fighter-history shape across many fights, including duplicate/superseded matchups on the same event date that would require conservative event/fighter matching and manual review. |
| Fighter history: Georges St-Pierre | `https://www.bestfightodds.com/fighters/Georges-St-Pierre-80` | Confirms deep UFC history back to 2007-era events with opening odds, closing ranges, movement, event names, and event dates. This supports BestFightOdds as a strong deep-history candidate for open/close benchmarks. |
| Older event examples | `https://www.bestfightodds.com/events/ufc-100-137`, `https://www.bestfightodds.com/events/ufc-74-respect-7` | Older event pages expose event identity, fight matchups, and relative "Last change" text. The rendered text did not expose precise per-bookmaker observation timestamps. |

Field coverage against `data/odds/fight_odds.csv`:

| Needed field | Manual finding |
|---|---|
| `event_date` | Present on archive/fighter history pages as event dates; event pages show month/day and need year from archive/fighter page or source URL context. |
| `event name` / local `event_id` mapping support | Present on archive/fighter history pages and event pages. Matching still needs conservative normalization because event names can be generic, duplicated, or changed. |
| Fighters / local fight and fighter mapping support | Present on event and fighter history pages. Aliases and duplicate future/cancelled matchups must go through unmatched-review output. |
| `open odds` | Present on fighter history pages. |
| `close odds` / close ranges | Present as a closing range on fighter history pages. Best single close price selection must be explicitly defined before loading. |
| Bookmaker/source attribution | Present on current/recent event odds tables as sportsbook/source columns. Fighter history summaries appear aggregated and do not, by themselves, attribute each open/close value to a bookmaker. |
| True observed timestamps | Not confirmed. Reviewed pages exposed event dates and relative "Last change" labels, but not precise timestamped line observations suitable for point-in-time line movement. |
| `source` / `source_url` | Feasible to preserve as `bestfightodds_manual_review` plus exact archive/event/fighter URLs. |
| `odds_timestamp` | Not safely available from reviewed rendered pages. Do not invent one from event date or review date for leakage-safe backtests. |

Timestamp conclusion:

BestFightOdds looks feasible for historical opening/closing benchmark data, but
the manually reviewed archive/fighter/event pages did not expose true observed
timestamps for each odds value. They expose opening labels, closing ranges,
movement summaries, event dates, and relative "Last change" text. Until a future
bounded raw snapshot probe proves that timestamped observations exist in the page
payload or a permissioned export, BestFightOdds-derived rows should not be used
as precise point-in-time line-movement observations. If loaded at all, they need
an explicit non-claiming timestamp policy and should be excluded from
leakage-safe P/L claims that require trustworthy `odds_timestamp` values.

Terms, robots, rate-limit, and permission notes:

- Terms reviewed at `https://www.bestfightodds.com/terms`. The public terms page
  reviewed on 2026-08-19 is brief, gives an accuracy/availability disclaimer,
  and does not provide an explicit scraping, reuse, database extraction, or API
  license grant.
- Privacy/contact reviewed at `https://www.bestfightodds.com/privacy`. The page
  identifies site analytics/session tracking and provides a contact route.
- `https://www.bestfightodds.com/robots.txt` was checked on 2026-08-19 and
  returned `User-agent: *`, `Allow: /`, plus sitemap URLs.
- No public crawl-delay or rate-limit policy was found in the reviewed pages.
  Future network work should therefore start only with the already proposed
  single-URL bounded snapshot probe, conservative delay, clear user agent,
  timeout, raw snapshot metadata, and no broad crawl.
- Because the terms are sparse and the data is valuable archived sportsbook
  content, ask for permission or an official export/API path before any broad
  automated collection, redistribution, or recurring fetch job.

Manual export/review workflow:

Manual review is possible before scraping:

1. Use fighter history pages as the first manual source for event date, event
   name, fighters, opening odds, closing range, and movement.
2. Use event pages as the manual cross-check for fight card membership and
   bookmaker/source columns where visible.
3. Enter rows into a source-specific review spreadsheet or CSV with original
   labels, exact source URLs, chosen open/close value, reviewer notes, and a
   timestamp-quality flag such as `open_close_label_only`.
4. Map to warehouse `event_id`, `fight_id`, `fighter_id`, and
   `opponent_fighter_id` only after review; ambiguous event/fighter matches stay
   out of `data/odds/fight_odds.csv`.
5. Do not publish BestFightOdds rows into canonical `fight_odds.csv` unless the
   row has a defensible `odds_timestamp` policy. For now, treat manual BFO data
   as research/open-close benchmark material, not leakage-safe timed market data.

No scraper was implemented, run, or scheduled for this ticket.

#### T8.11.2 Add BestFightOdds raw snapshot probe
- **Description:** Add a tiny, explicitly bounded BestFightOdds snapshot probe to
  test fetch/parsing feasibility and raw artifact storage before any broad crawl.
- **Status:** DONE
- **Dependencies:** T8.11.1, T8.10.1
- **Acceptance Criteria:**
  - Script accepts one explicit URL or one explicit event/fighter identifier; no
    site-wide crawling.
  - Script has a hard default request cap of 1 page and refuses broader work
    unless a future ticket changes that.
  - Script uses a conservative user agent, timeout, retry/backoff settings, and
    at least a 2-second delay before any optional subsequent request.
  - Raw HTML snapshots are stored under `data/odds/raw/bestfightodds/` with
    fetch timestamp, URL, HTTP status, and content hash metadata.
  - Script can run in dry-run mode that reports intended fetches without network
    access.
  - No parsed rows are loaded into `fight_odds` in this ticket.
  - No scheduled or continuous scraping is added.
- **Test Coverage:**
  - Unit tests for URL allowlist/validation, request-cap enforcement, metadata
    naming, and dry-run behavior.
  - Network calls are not required for unit tests.
- **Complexity:** M
- **Risk:** Medium - even bounded scraping must be respectful and auditable.

Implementation note:

- Added `warehouse/probe_bestfightodds_snapshot.py` as a one-page raw HTML
  snapshot probe.
- Supported targets are exactly one of `--url`, `--event-id`, or
  `--fighter-id`. URL targets are allowlisted to rendered BestFightOdds home,
  archive, event, and fighter pages; hidden/admin/API paths are rejected.
- The probe enforces `--request-cap 1` and rejects lower-than-2-second request
  intervals. It includes a conservative user agent, timeout, retry, and backoff
  settings.
- `--dry-run` reports the planned target and raw output directory without making
  a network request or writing files.
- Non-dry runs write raw HTML and adjacent JSON metadata under
  `data/odds/raw/bestfightodds/`, including fetch timestamp, source URL, final
  URL, HTTP status, content SHA-256, byte count, request settings, and explicit
  flags that parsing and `fight_odds` loading were not performed.
- Added `data/odds/raw/bestfightodds/README.md` to document the raw snapshot
  guardrails.
- No parser, canonical odds output, warehouse load, scheduler, or continuous
  fetch path was added for this ticket.

#### T8.11.3 Add BestFightOdds parser and normalized adapter
- **Description:** Parse reviewed BestFightOdds raw snapshots and normalize them
  into the canonical V1 odds contract.
- **Status:** DONE
- **Dependencies:** T8.11.2, T8.2.2, T8.10.1
- **Acceptance Criteria:**
  - New parser reads stored raw snapshots only; it does not fetch network content.
  - Extracts only V1 moneyline/open/close fields that can be mapped auditably.
  - Writes source-specific normalized rows to
    `data/odds/sources/bestfightodds_fight_odds.csv`.
  - Writes unmatched/ambiguous rows to
    `data/odds/sources/bestfightodds_unmatched_odds.csv`.
  - Preserves source URL, raw snapshot path/hash, source event/fighter labels,
    odds type (`opening` or `closing` if available), bookmaker/source label,
    observed timestamp when present, and project import timestamp.
  - If BestFightOdds exposes only opening/closing labels without precise observed
    timestamps, those rows must be labeled accordingly and cannot be used as
    point-in-time line-movement data.
  - Maps events/fighters conservatively to warehouse IDs; ambiguous rows are not
    guessed.
  - Output can be loaded by `warehouse/load_fight_odds.py` without changing the
    canonical `data/odds/fight_odds.csv` schema.
- **Test Coverage:**
  - Unit tests with small stored HTML fixtures.
  - Tests for open/close parsing, missing odds, unmatched fighter, ambiguous event
    match, duplicate market sides, and moneyline-only filtering.
- **Complexity:** M
- **Risk:** Medium - parsing can be brittle and timestamp semantics may limit
  leakage-safe backtest use.

Implementation note:

- Added `warehouse/adapt_bestfightodds_snapshots.py` as an offline-only adapter
  for stored BestFightOdds HTML snapshots and adjacent metadata.
- The adapter reads `data/odds/raw/bestfightodds/*.html`; it does not perform
  network requests.
- Supported extraction is intentionally narrow: reviewed fighter-history tables
  with event/date, fighter, opponent, moneyline market, open odds, closing range,
  optional bookmaker, and optional open/close observed timestamps.
- Source-specific normalized rows are written to
  `data/odds/sources/bestfightodds_fight_odds.csv`.
- Review rows are written to
  `data/odds/sources/bestfightodds_unmatched_odds.csv`.
- The source output keeps the canonical `data/odds/fight_odds.csv` columns first
  and appends BestFightOdds audit columns for raw snapshot path/hash, source
  event/fighter labels, timestamp quality, and raw source line value. The
  existing loader accepts the canonical columns without schema changes.
- Rows with explicit observed timestamps can become canonical-compatible
  `opening` or `closing` moneyline rows. Rows with only BestFightOdds
  open/close labels are labeled `open_close_label_only` in the unmatched review
  output and are not loaded as point-in-time observations.
- Event/fighter matching is conservative by local event date plus fighter pair,
  with optional event-name filtering. Unknown or ambiguous matches are sent to
  unmatched output; they are not guessed.
- Duplicate stable market sides are skipped into unmatched review output rather
  than loaded.
- No scheduled fetch, live scrape, model feature use, or warehouse load was
  added for this ticket.

#### T8.11.4 Alternate odds archive permission/feasibility fallback
- **Description:** Evaluate one alternate website archive only if BestFightOdds is
  blocked by terms, missing timestamps, or brittle parsing.
- **Status:** DONE
- **Dependencies:** T8.11.0, T8.10.4
- **Acceptance Criteria:**
  - Candidate list starts with MMA-specific or public archive pages before broad
    multi-sport odds sites.
  - OddsPortal remains excluded unless explicit permission or licensed access is
    obtained because its terms restrict automated access/scraping/extraction.
  - Feasibility notes cover terms, robots, rate limits, raw snapshot storage,
    field coverage, timestamp quality, and mapping difficulty.
  - Any accepted fallback source must normalize into `data/odds/fight_odds.csv`.
  - No scraper is implemented, run, or scheduled.
- **Test Coverage:**
  - Documentation-only fallback ticket; no automated tests required.
- **Complexity:** S
- **Risk:** Medium - many odds archive sites restrict automated access.

Fallback feasibility review completed on 2026-08-19.

Why fallback review is justified:

BestFightOdds remains the first scraping candidate, but T8.11.1/T8.11.3 found
that ordinary BFO open/close history rows may be `open_close_label_only` rather
than true observed point-in-time timestamps. That timestamp limitation is enough
to document a fallback path before any broader scraping is considered.

Candidate priority:

1. **MMAOddsBreaker fight-odds articles** - MMA-specific public article archive
   with separate opening-odds and closing-odds categories. This is the reviewed
   fallback candidate for this ticket.
2. **FightOdds.io** - MMA-specific betting site with odds-comparison/line-movement
   language, but terms state the service is for personal, noncommercial use and
   do not provide an obvious public historical export path. Keep as permission
   question, not the first fallback.
3. **Broad multi-sport odds archives** - lower priority because they tend to have
   stronger database/scraping restrictions and harder sport/event mapping.
4. **OddsPortal** - explicitly excluded unless permission or licensed access is
   obtained.

Reviewed fallback: MMAOddsBreaker

| Area | Feasibility notes |
|---|---|
| Source pages | `https://www.mmaoddsbreaker.com/fight-odds/`, opening-odds category, closing-odds category, and representative UFC opening/closing articles. |
| Field coverage | Opening articles expose article title/event name, event date in article body, article publish timestamp, fighter names, and American moneyline odds. Older closing-odds/results articles expose fighter names, closing American odds, winners/results narrative, and article publish timestamp. |
| Bookmaker/source attribution | Often uses broad wording such as offshore sportsbooks or several bookmakers, not stable per-bookmaker attribution. Treat `bookmaker` as `MMAOddsBreaker article aggregate` unless an article explicitly names a source. |
| Timestamp quality | Article publish timestamps are present, but they are article publication times, not necessarily the exact sportsbook observation time. Opening articles say odds were recently opened; closing articles are usually post-event or near-event summaries. These rows should be labeled `article_publish_time`, not precise observed line movement. |
| Mapping difficulty | Medium/high. Article text is prose, not a normalized table. Fighter pairs are usually adjacent lines and event names are readable, but cancelled/rebooked bouts, changed cards, and article typos require unmatched review. |
| Raw snapshot storage | If used, follow the BFO pattern: one explicit URL per probe, store raw HTML plus metadata under `data/odds/raw/mmaoddsbreaker/`, then parse stored snapshots only. |
| Normalization path | Any accepted rows must normalize into the canonical `data/odds/fight_odds.csv` columns through a source-specific output such as `data/odds/sources/mmaoddsbreaker_fight_odds.csv`, with unmatched rows in `data/odds/sources/mmaoddsbreaker_unmatched_odds.csv`. |

Terms, robots, and permission notes:

- MMAOddsBreaker terms reviewed at
  `https://www.mmaoddsbreaker.com/terms-and-conditions/`. The terms allow use of
  the company service subject to restrictions, prohibit downloading,
  reproducing, redistributing, reselling, publicly displaying, or otherwise
  commercially exploiting any portion of the site, and prohibit unauthorized use
  of company materials except as expressly authorized.
- MMAOddsBreaker `robots.txt` was checked on 2026-08-19. It disallows specific
  WordPress/WooCommerce/admin paths, allows `wp-admin/admin-ajax.php`, includes a
  sitemap, and declares `Crawl-delay: 10`.
- No explicit public data export/API or bulk archive permission was found during
  this review. Before any broad collection, ask for permission or use an
  official export/licensed path.
- If a future bounded probe is approved, use stricter limits than BFO by default:
  one explicit article/category URL, raw snapshot metadata, no scheduler, and at
  least the documented 10-second crawl delay before any optional follow-up.

OddsPortal exclusion:

OddsPortal remains excluded as a fallback scraper target. Its terms identify MMA
as one of many covered sports, but restrict use to personal use, prohibit
commercial use without consent, protect database content, prohibit substantial
database extraction/exploitation without consent, prohibit automated requests
that burden servers, and prohibit embedding, aggregating, scraping, or recreating
content without express consent. Do not build an OddsPortal scraper unless
explicit permission or licensed access is obtained.

Decision:

MMAOddsBreaker is a possible fallback only for permissioned/manual article
review, especially for missing opening odds. It is not a superior replacement
for BestFightOdds for timestamp-safe line movement because article publish times
are not true sportsbook observation timestamps and bookmaker attribution is often
aggregate. If accepted later, rows must carry a timestamp-quality flag such as
`article_publish_time` and must not be used for leakage-safe point-in-time P/L
claims unless the article or source metadata provides a trustworthy observed odds
timestamp.

No scraper was implemented, run, or scheduled for this ticket.

#### T8.11.5 Document odds source decision matrix
- **Description:** Add a short maintained decision matrix to `docs/betting.md` so
  future work can compare source candidates without rediscovering the same tradeoffs.
- **Status:** DONE
- **Dependencies:** T8.11.0
- **Acceptance Criteria:**
  - Matrix includes The Odds API, BALLDONTLIE, SportsDataIO, BestFightOdds, and
    OddsPortal.
  - Matrix records source type, MMA/UFC coverage, historical suitability,
    implementation risk, terms/scraping risk, and current recommendation.
  - Matrix explicitly records that paid API routes are out of scope and
    BestFightOdds is the first scraping candidate.
  - Matrix reiterates that all odds source outputs must normalize into
    `data/odds/fight_odds.csv`.
- **Test Coverage:**
  - Documentation-only ticket; no automated tests required.
- **Complexity:** S
- **Risk:** Low

## T8.12 - BestFightOdds Sample Decision

#### T8.12.0 BestFightOdds sample snapshot review and go/no-go decision
- **Description:** Capture a tiny reviewed BestFightOdds sample, run the offline
  adapter, and decide whether BestFightOdds can feed canonical odds or remains
  research-only.
- **Status:** DONE
- **Dependencies:** T8.11.2, T8.11.3, T8.11.5
- **Acceptance Criteria:**
  - Capture no more than three explicit BestFightOdds pages; no discovery crawl,
    no future-event search, and no scheduled scraping.
  - Store raw HTML plus metadata under `data/odds/raw/bestfightodds/`.
  - Run the offline adapter against stored snapshots only.
  - Review whether snapshots expose trustworthy observed timestamps, only
    open/close labels, or brittle/unparseable markup.
  - Review matched, unmatched, and timestamp-quality outcomes.
  - Document a go/no-go decision:
    - `go_canonical` if trustworthy observed timestamps and conservative mapping
      are present.
    - `research_only` if rows are useful open/close benchmarks but timestamp weak.
    - `blocked` if terms/markup/mapping make the source unsuitable.
  - Do not load BestFightOdds rows into `data/odds/fight_odds.csv` or the
    warehouse as part of this ticket.
- **Test Coverage:**
  - Existing T8.11.2/T8.11.3 unit tests cover probe and offline adapter behavior.
  - This sample-review ticket requires no new automated tests unless code changes.
- **Complexity:** S
- **Risk:** Medium - this intentionally touches live pages, but only explicit
  single-page probes with raw snapshot storage.

**Implementation Notes (2026-08-19):**

Three explicit fighter-history pages were captured with
`warehouse/probe_bestfightodds_snapshot.py`. No archive discovery, future-event
search, crawler, scheduler, or continuous fetch was added.

| Page | HTTP | Raw Snapshot | SHA-256 |
|---|---:|---|---|
| `https://www.bestfightodds.com/fighters/ian-machado-garry-15690` | 200 | `data/odds/raw/bestfightodds/20260819T202409Z_fighter_ian-machado-garry-15690_946028307014.html` | `9460283070141f191fb3e004d1773ccb10b5c52d72cc9518ffb6c1c1a0a575d5` |
| `https://www.bestfightodds.com/fighters/islam-makhachev-5541` | 200 | `data/odds/raw/bestfightodds/20260819T202417Z_fighter_islam-makhachev-5541_711a642008b6.html` | `711a642008b60c8648577f81856d063934bc861d35b74aa23f70fd9a4414e132` |
| `https://www.bestfightodds.com/fighters/Georges-St-Pierre-80` | 200 | `data/odds/raw/bestfightodds/20260819T202512Z_fighter_Georges-St-Pierre-80_2b9b769abcf2.html` | `2b9b769abcf28c8862f5a1ae1ab6c153065e4a55cfece554cb3576e80f4a687f` |

The stored pages expose rendered fighter-history tables with matchup, open odds,
closing odds/ranges, movement, event labels, and event dates. The reviewed HTML
does not expose trustworthy sportsbook-observation timestamps for individual odds
points. Native table metadata includes line-movement payloads such as
`data-sparkline` and matchup-side identifiers, and the pages contain unrelated
news/article `<time>` elements, but no reviewed field can be treated as an
observed odds timestamp.

The offline adapter read only stored snapshots and produced:

| Metric | Count |
|---|---:|
| Snapshots read | 3 |
| Parsed source rows | 96 |
| Canonical-compatible rows | 0 |
| Unmatched/review rows | 96 |
| Duplicate source rows skipped | 0 |

All 96 parsed rows were labeled `open_close_label_only`. Review rejection reasons
were:

| Rejection Reason | Count |
|---|---:|
| `missing_opening_observed_timestamp_open_close_label_only` | 42 |
| `unknown_local_fight_pair` | 30 |
| `unknown_local_event_name` | 16 |
| `missing_source_event_date` | 8 |

Decision: `research_only`. BestFightOdds is useful for audited historical
open/close benchmarks and manual review, but the sampled pages should not feed
canonical point-in-time betting backtests unless a future permissioned export,
API, or page payload provides true observed odds timestamps. No rows were loaded
into `data/odds/fight_odds.csv` or the warehouse.

This rendered-page decision is superseded/qualified by T8.12.1 for rows whose
`data-li` values can be resolved to timestamped `/api/ggd` line-history payloads.

#### T8.12.1 BestFightOdds line-history payload timestamp probe
- **Description:** Inspect BestFightOdds chart/data payloads behind fighter/event
  rows to determine whether `data-li` and `data-sparkline` can be resolved into
  timestamped line movement.
- **Status:** DONE
- **Dependencies:** T8.12.0
- **Acceptance Criteria:**
  - Inspect BFO chart/data payloads behind fighter/event rows.
  - Use only one explicit fighter or event page and one explicitly discovered
    detail/history request.
  - Determine whether `data-li` / `data-sparkline` can be resolved to timestamped
    line movement.
  - Store raw responses with fetch metadata.
  - Do not add a crawl, scheduler, canonical load, or warehouse load.
  - If true observed timestamps exist, update the BFO decision from
    `research_only` toward `go_canonical`.
- **Test Coverage:**
  - Unit tests for payload URL validation, request-cap enforcement, metadata
    naming, and dry-run behavior.
  - Network calls are not required for unit tests.
- **Complexity:** S
- **Risk:** Medium - the probe is tiny, but hidden payloads can change and should
  not become an accidental broad scrape.

**Implementation Notes (2026-08-19):**

The explicit reviewed source page was the previously captured Ian Machado Garry
fighter-history snapshot:

`data/odds/raw/bestfightodds/20260819T202409Z_fighter_ian-machado-garry-15690_946028307014.html`

That page contains fighter-history chart cells with inline `data-sparkline`
summaries and `data-li` identifiers. The Ian Machado Garry vs. Islam Makhachev
row used for the probe had `data-li="[43741,1]"`.

Added `warehouse/probe_bestfightodds_line_history_payload.py` as a separate
payload-only probe. It allows the reviewed BFO JS asset and same-host `/api/`
payload URLs only, enforces `--request-cap 1`, supports dry-run mode, writes raw
payload bytes plus adjacent metadata, and never parses into or loads
`fight_odds`.

Raw payloads captured:

| Payload | HTTP | Raw Artifact | SHA-256 |
|---|---:|---|---|
| `https://www.bestfightodds.com/js/bfo.min.js?v=0.4.10` | 200 | `data/odds/raw/bestfightodds/payloads/20260819T204158Z_js_asset_bfo.min.js_ec84daec4aa4.js` | `ec84daec4aa42e54e835fa1204bbf5f950e0aaf398e4d7c49dc2fc25e77fac1c` |
| `https://www.bestfightodds.com/api/ggd?m=43741&p=1` | 200 | `data/odds/raw/bestfightodds/payloads/20260819T204227Z_detail_payload_api-ggd_00dd7bedfe94_1ba74396017b.html` | `1ba74396017b96087ef6230d1da81d3eca9eacf8efedd4d39057b9ddf949f9b5` |

JS inspection found that fighter-history sparklines call
`createMIChart(matchup_id, side)`, which fetches
`/api/ggd?m=<matchup_id>&p=<side>` and decodes the response with the site's
`notIn` function before `JSON.parse`. The decoded payload is a Highcharts series
with `xAxis.type = "datetime"` in the chart code.

The one reviewed `/api/ggd` response decoded to a single `Mean` series with 214
points. Every point had an `x` Unix-millisecond timestamp and a decimal-odds
`y` value. For the sampled row:

| Field | Value |
|---|---|
| Series name | `Mean` |
| Point count | 214 |
| First timestamp | `2026-06-17T23:00:06+00:00` |
| Last timestamp | `2026-08-16T04:16:31+00:00` |
| Decimal odds range | `3.44` to `9.05` |

Decision update: `timestamped_payload_found`; BestFightOdds should move from
rendered-page `research_only` toward `go_canonical_candidate`. The sampled
payload exposes true timestamped line movement, but the current reviewed
endpoint is a BFO `Mean` odds series rather than a normalized bookmaker-specific
row set. A future parser ticket must preserve that semantics, decode only stored
raw payloads, map rows conservatively, and decide whether `BestFightOdds Mean`
is acceptable as a canonical bookmaker/source label or whether event-page
bookmaker-specific `b=` payloads are required. No canonical or warehouse load was
performed for this ticket.

#### T8.12.2 Add BestFightOdds line-history decoder and normalized adapter
- **Description:** Decode stored BestFightOdds `/api/ggd` line-history payloads
  and normalize timestamped `Mean` moneyline points into the canonical V1 odds
  contract for source-specific review.
- **Status:** DONE
- **Dependencies:** T8.12.1, T8.11.3, T8.2.2
- **Acceptance Criteria:**
  - New adapter reads stored raw snapshots and stored raw payloads only; it does
    not fetch network content.
  - Decoder implements the reviewed BFO `notIn` payload transform and parses
    `/api/ggd?m=<matchup_id>&p=<side>` responses.
  - Adapter maps payload `(matchup_id, side)` back to stored fighter-history
    `data-li` context before local warehouse event/fight/fighter matching.
  - Supported V1 output is timestamped moneyline observations from BFO `Mean`
    odds series, labeled with bookmaker/source semantics as `BestFightOdds Mean`.
  - Writes source-specific normalized rows to
    `data/odds/sources/bestfightodds_line_history_fight_odds.csv`.
  - Writes missing, ambiguous, unsupported, or unmatched rows to
    `data/odds/sources/bestfightodds_line_history_unmatched_odds.csv`.
  - Preserves source URL, raw payload path/hash, source snapshot path/hash,
    matchup id, side, series name, source event/fighter labels, observed
    timestamp, decimal odds, and import timestamp.
  - No rows are merged into canonical `data/odds/fight_odds.csv`, and no
    warehouse load, crawl, scheduler, or continuous scraping is added.
- **Test Coverage:**
  - Unit tests for `notIn` decoding, stored payload parsing, `data-li` context
    extraction, matched loadable output, missing context, and unknown local fight
    pair handling.
- **Complexity:** M
- **Risk:** Medium - timestamped line movement is valuable, but the first
  supported payload is a BFO aggregate/mean series rather than
  bookmaker-specific observations.

**Implementation Notes (2026-08-19):**

Added `warehouse/adapt_bestfightodds_line_history_payloads.py`, an offline-only
adapter for stored BFO fighter-history pages and stored `/api/ggd` payloads.
The adapter supports the reviewed fighter-history `Mean` endpoint only:
`/api/ggd?m=<matchup_id>&p=<side>`. It intentionally does not fetch network
content and does not attempt bookmaker-specific `b=` payloads yet.

The stored sample payload produced:

| Metric | Count |
|---|---:|
| Payloads read | 1 |
| Timestamped source points read | 214 |
| Canonical-compatible source rows | 214 |
| Unmatched/review rows | 0 |
| Duplicate source rows skipped | 0 |

Output summary:

| Field | Value |
|---|---|
| Output CSV | `data/odds/sources/bestfightodds_line_history_fight_odds.csv` |
| Unmatched CSV | `data/odds/sources/bestfightodds_line_history_unmatched_odds.csv` |
| Source label | `bestfightodds_line_history_payload` |
| Bookmaker/source semantics | `BestFightOdds Mean` |
| Line type | `current` |
| Market | `moneyline` |
| Event/fight | `UFC 330`, Ian Machado Garry vs. Islam Makhachev |
| Timestamp range | `2026-06-17T23:00:06+00:00` to `2026-08-16T04:16:31+00:00` |
| Decimal odds range | `3.44` to `9.05` |

Decision update: BestFightOdds line-history payloads are now an implementable
canonical-source candidate for timestamped line movement, pending review of
whether `BestFightOdds Mean` is acceptable for the first betting backtest path
or whether a future ticket must first add bookmaker-specific `b=` payload
support. No rows were copied into `data/odds/fight_odds.csv`, and no warehouse
load was performed.

#### T8.12.3 Promote BestFightOdds Mean rows as first canonical market benchmark
- **Description:** Use reviewed timestamped BestFightOdds `Mean` line-history
  rows as the first canonical market-benchmark odds source while preserving the
  source semantics and avoiding sportsbook-specific execution claims.
- **Status:** DONE
- **Dependencies:** T8.12.2, T8.2.2
- **Acceptance Criteria:**
  - Capture at most the paired opposite-side payload for the already-reviewed
    matchup; do not add discovery, broad crawling, a scheduler, or continuous
    fetching.
  - Rerun the offline line-history adapter against stored snapshots and payloads
    only.
  - Require both fight sides before canonical promotion so no-vig market
    grouping is possible.
  - Add a validated, de-duplicating promotion helper that keeps only the
    canonical `data/odds/fight_odds.csv` columns.
  - Promote reviewed valid BFO `Mean` rows into `data/odds/fight_odds.csv`.
  - Label rows with `bookmaker = BestFightOdds Mean`,
    `source = bestfightodds_line_history_payload`, and `line_type = current`.
  - Document that these rows support market-benchmark backtests, not
    sportsbook-specific executable P/L claims.
  - Do not run a warehouse load as part of this ticket.
- **Test Coverage:**
  - Unit tests for canonical promotion, extra audit-column stripping,
    de-duplication, and validation failure handling.
  - Existing BFO probe/adapter tests remain green.
- **Complexity:** S
- **Risk:** Medium - this intentionally starts using BFO in the canonical odds
  file, but only as a clearly labeled market-benchmark source.

**Implementation Notes (2026-08-19):**

Captured one paired opposite-side payload for the already-reviewed matchup:

| Payload | HTTP | Raw Artifact | SHA-256 |
|---|---:|---|---|
| `https://www.bestfightodds.com/api/ggd?m=43741&p=2` | 200 | `data/odds/raw/bestfightodds/payloads/20260819T205948Z_detail_payload_api-ggd_7d2f15c00043_1e7bddb93e45.html` | `1e7bddb93e455ce27e3e071b0c33ab263789c7f4e6c1a39b97b6da2c72783fe6` |

Rerunning the offline adapter after the paired-side payload produced:

| Metric | Count |
|---|---:|
| Payloads read | 2 |
| Timestamped source points read | 428 |
| Canonical-compatible source rows | 428 |
| Unmatched/review rows | 0 |
| Duplicate source rows skipped | 0 |

Added `warehouse/promote_odds_source_to_canonical.py`, a generic reviewed-source
promotion helper. It validates source rows against local `events.csv`,
`fights.csv`, and `fighters.csv`, strips source-specific audit columns, skips
stable-key duplicates, and writes canonical `data/odds/fight_odds.csv` columns
only. It does not load the database.

Promotion result:

| Metric | Count |
|---|---:|
| Existing canonical rows before promotion | 149,894 |
| Source rows read | 428 |
| Source rows valid | 428 |
| Appended canonical rows | 428 |
| Duplicate rows skipped | 0 |

Canonical BFO row summary after promotion:

| Field | Value |
|---|---|
| Canonical row count | 428 |
| Fight/event | `UFC 330`, Ian Machado Garry vs. Islam Makhachev |
| Fighters represented | 214 Ian Machado Garry rows, 214 Islam Makhachev rows |
| Unique paired timestamps | 214 |
| Single-side timestamps | 0 |
| Timestamp range | `2026-06-17T23:00:06+00:00` to `2026-08-16T04:16:31+00:00` |
| Decimal odds range | `1.23753` to `9.05` |

Pure loader validation on the promoted BFO rows returned 428 valid rows, 0
skipped, and 0 rejected. A first-timestamp no-vig check was valid with two sides
and overround `1.032558139534883720930232558`.

Decision: `go_canonical_market_benchmark`. BestFightOdds `Mean` line-history
rows are acceptable as the first canonical odds source for market-benchmark
analysis. They must still be described as BFO aggregate/mean market lines, not
named-bookmaker executable prices. No warehouse load was run for this ticket.

#### T8.13.0 Run first BFO Mean market-benchmark backtest
- **Description:** Run the first betting backtest using canonical
  `BestFightOdds Mean` rows as a market benchmark.
- **Status:** DONE
- **Dependencies:** T8.12.3, T8.7.1
- **Acceptance Criteria:**
  - Use canonical `BestFightOdds Mean` rows from `data/odds/fight_odds.csv`.
  - Join to saved pre-event predictions only.
  - Select the latest BFO line before each prediction timestamp.
  - Compute no-vig market probability and model edge.
  - Produce betting backtest reports under `data/reports/`.
  - Label output as market-benchmark, not sportsbook-executable P/L.
- **Test Coverage:**
  - Unit tests cover BFO Mean source filtering, no-vig row construction,
    prediction enrichment from local CSV IDs, and latest-before-prediction
    selection.
- **Complexity:** S
- **Risk:** Medium - this uses real timestamped BFO aggregate market rows, but
  the result must not be described as sportsbook-executable P/L.

**Implementation Notes (2026-08-19):**

Added `betting/bfo_mean_market_backtest.py`, an offline CSV runner that reads
canonical BFO Mean rows from `data/odds/fight_odds.csv`, enriches saved
`data/reports/pre_event_prediction_fights.csv` rows with local fighter IDs from
`data/fights.csv` and `data/fighters.csv`, computes two-sided no-vig market
probabilities, and delegates betting decisions/settlement to the existing
historical backtest engine. It does not fetch network content, crawl, schedule,
or load the warehouse.

The first run wrote:

| Report | Path |
|---|---|
| Fight rows | `data/reports/bfo_mean_market_benchmark/bfo_mean_market_benchmark_fights.csv` |
| Event rows | `data/reports/bfo_mean_market_benchmark/bfo_mean_market_benchmark_events.csv` |
| Summary rows | `data/reports/bfo_mean_market_benchmark/bfo_mean_market_benchmark_summary.csv` |
| Benchmark notice | `data/reports/bfo_mean_market_benchmark/market_benchmark_notice.md` |

Run counters:

| Metric | Count |
|---|---:|
| Saved prediction rows read | 211 |
| BFO fights with no-vig odds | 1 |
| Canonical BFO Mean rows | 428 |
| BFO market groups | 214 |
| Valid no-vig odds rows | 428 |
| Prediction rows after BFO/date filters | 1 |
| Dataset rows | 2 |
| Dataset issues | 0 |

The joined fight was `UFC 330: Makhachev vs. Machado Garry`. The selected line
timestamp was `2026-08-09T03:00:03+00:00`, which is before the saved prediction
timestamp `2026-08-09T11:17:50.025384+00:00`.

Default-policy result:

| Side | Model Probability | BFO No-Vig Market Probability | Edge | EV/Unit | Decision |
|---|---:|---:|---:|---:|---|
| Ian Machado Garry | 0.2300 | 0.2461144779 | -0.0161144779 | -0.107600 | pass |
| Islam Makhachev | 0.7700 | 0.7538855221 | 0.0161144779 | -0.024664100 | pass |

Overall result: 0 bets, 0 total staked, 0 profit/loss, ending bankroll 1000.
This is expected under the default thresholds because both sides failed the
edge/EV gates. The report is explicitly labeled `market-benchmark, not
sportsbook-executable P/L`.

#### T8.13.1 Expand BestFightOdds Mean historical backfill
- **Description:** Expand the BestFightOdds Mean line-history capture from the
  one reviewed matchup to a bounded set of past completed fights so the
  market-benchmark backtest has meaningful sample size.
- **Status:** IN_PROGRESS
- **Dependencies:** T8.12.3, T8.13.0
- **Acceptance Criteria:**
  - Use `BestFightOdds Mean` as the accepted market-benchmark source.
  - Target past completed UFC fights/events that can join to saved pre-event
    predictions; prioritize fights already present in
    `data/reports/pre_event_prediction_fights.csv`.
  - Accept explicit event/fighter URLs or an explicit local fight/event ID list;
    no unbounded site-wide crawl.
  - Maintain a manifest of every intended and completed BFO request, including
    local fight/event target, URL, fetch timestamp, HTTP status, content hash,
    raw artifact path, and parse/load status.
  - Keep conservative request behavior: explicit request cap, dry-run support,
    timeout, retry/backoff, and at least a 2-second delay before any subsequent
    request.
  - Store raw HTML/payload artifacts under `data/odds/raw/bestfightodds/`.
  - Decode stored payloads offline and promote only two-sided timestamped
    `BestFightOdds Mean` moneyline rows into `data/odds/fight_odds.csv`.
  - Re-run the BFO Mean market-benchmark backtest after promotion and write
    reports under `data/reports/bfo_mean_market_benchmark/`.
  - Label all outputs as market-benchmark, not sportsbook-executable P/L.
  - Do not add a scheduler, continuous scraper, model-training feature, or
    named-bookmaker execution claim.
- **Test Coverage:**
  - Unit tests for manifest target parsing, request-cap enforcement, dry-run
    behavior, duplicate raw/payload handling, two-sided promotion gating, and
    backtest report generation on expanded fixtures.
- **Complexity:** M
- **Risk:** Medium - historical BFO depth is valuable, but hidden payload
  extraction must remain bounded, auditable, and respectful.

**Implementation Notes (2026-08-19):**

Added `warehouse/capture_bestfightodds_manifest.py`, a manifest-driven capture
runner for explicit BFO page/payload targets. It does not discover targets or
crawl the site. It validates a CSV manifest, enforces an explicit request cap
with a hard maximum of 25 targets per run, preserves dry-run mode, requires at
least a 2-second interval before subsequent live requests, and delegates actual
network fetches to the existing one-request snapshot/payload probes.

Added unit tests for manifest target parsing, request-cap enforcement, dry-run
behavior, injected capture probes, and inter-request delay handling.

Seed manifest:

`data/odds/raw/bestfightodds/manifests/bfo_mean_targets.seed.csv`

Dry-run result manifest:

`data/odds/raw/bestfightodds/manifests/bfo_mean_capture_results.dry_run.csv`

Live bounded result manifest:

`data/odds/raw/bestfightodds/manifests/bfo_mean_capture_results.live.csv`

The seed currently includes six explicit targets:

| Purpose | Target |
|---|---|
| Historical backfill | `https://www.bestfightodds.com/events/ufc-330-4237` |
| Historical backfill | `https://www.bestfightodds.com/api/ggd?m=43741&p=1` |
| Historical backfill | `https://www.bestfightodds.com/api/ggd?m=43741&p=2` |
| Upcoming snapshot | `https://www.bestfightodds.com/events/ufc-sacramento-4320` |
| Upcoming snapshot | `https://www.bestfightodds.com/fighters/anthony-hernandez-6092` |
| Upcoming snapshot | `https://www.bestfightodds.com/fighters/gregory-rodrigues-6917` |

Dry-run command executed:

```bash
python3 warehouse/capture_bestfightodds_manifest.py \
  --manifest data/odds/raw/bestfightodds/manifests/bfo_mean_targets.seed.csv \
  --output data/odds/raw/bestfightodds/manifests/bfo_mean_capture_results.dry_run.csv \
  --dry-run \
  --request-cap 6
```

A live run for the seed manifest was approved and completed with six explicit
requests. The historical portion intentionally recaptured the existing UFC 330
sample event and paired `/api/ggd` payloads; duplicate stable odds keys were
skipped during canonical promotion, so the completed-fight benchmark sample did
not expand beyond the first reviewed matchup yet.

After decoding stored payloads offline and promoting reviewed rows, canonical
`BestFightOdds Mean` coverage is 560 rows: 428 rows for the completed UFC 330
sample and 132 rows for the upcoming Hernandez/Rodrigues matchup. The completed
historical benchmark was rerun with `--end-date 2026-08-15`; it still joined one
completed fight and produced 0 bets under the default edge/EV gates.

Remaining T8.13.1 work is to add a larger explicit manifest of past completed
fights that join to saved pre-event predictions, capture those targets with the
same bounded workflow, promote only two-sided timestamped BFO Mean rows, and
rerun the historical benchmark on the expanded completed-fight sample.

#### T8.13.2 Add BestFightOdds Mean upcoming market snapshot path
- **Description:** Add a bounded BestFightOdds Mean path for upcoming UFC fights
  so current model predictions can be compared with the BFO aggregate market.
- **Status:** IN_PROGRESS
- **Dependencies:** T8.12.3, T8.6.2, T8.13.1
- **Acceptance Criteria:**
  - Use `BestFightOdds Mean` as a current/upcoming market-benchmark source, not
    as sportsbook-executable odds.
  - Accept only explicit upcoming event/fighter URLs or explicit local upcoming
    fight/event IDs; no future-event discovery crawl unless a later ticket
    approves it.
  - Preserve the same raw manifest and artifact requirements as T8.13.1.
  - Store timestamped BFO Mean rows in canonical `data/odds/fight_odds.csv` only
    after both moneyline sides are present and conservatively mapped.
  - Feed current-card recommendation reports as market-benchmark comparisons,
    with report labels clearly separating BFO Mean benchmark edges from
    sportsbook-executable bet placement.
  - Add stale-odds handling for upcoming rows using the existing betting config
    age limits.
  - Do not add a scheduler, background polling, or continuous live line monitor.
    Any recurring refresh workflow requires a separate approval ticket.
- **Test Coverage:**
  - Unit tests for explicit upcoming target validation, stale benchmark odds,
    current-card report labeling, unmatched fight handling, and no scheduler
    behavior.
- **Complexity:** M
- **Risk:** Medium - useful for live decision support, but easy to overstate if
  aggregate BFO Mean prices are treated like executable book lines.

**Implementation Notes (2026-08-19):**

The seed manifest captured explicit upcoming targets for
`UFC Sacramento` / `UFC Fight Night: Hernandez vs. Rodrigues`, including the
event page and both fighter pages. The fighter snapshots exposed matchup
`m=44429`, which was then captured through an additional explicit two-target
payload manifest:

`data/odds/raw/bestfightodds/manifests/bfo_mean_upcoming_payload_targets.csv`

Live upcoming payload result manifest:

`data/odds/raw/bestfightodds/manifests/bfo_mean_upcoming_payload_results.live.csv`

The offline adapter accepted the BFO event-name alias by using the unique
date+fighter-pair warehouse match, promoted 132 two-sided timestamped
`BestFightOdds Mean` rows for Anthony Hernandez vs. Gregory Rodrigues, and
left duplicate recaptured UFC 330 rows out of canonical storage.

Added `betting/bfo_mean_upcoming_snapshot.py`, which reads saved pre-event
predictions and canonical BFO Mean rows, selects the latest two-sided BFO group
at or before an explicit `--as-of` timestamp, computes no-vig market probability,
model edge, and EV/unit, and writes:

`data/reports/bfo_mean_market_benchmark/bfo_mean_upcoming_market_snapshot.csv`

The report is labeled
`market-benchmark-not-sportsbook-executable`. The first run as of
`2026-08-19T21:40:00+00:00` wrote two rows for Hernandez/Rodrigues. Anthony
Hernandez showed a +0.040135 no-vig model edge and +0.014727 EV/unit against
the BFO Mean benchmark; Gregory Rodrigues was the inverse side and remained a
pass benchmark comparison.

Remaining T8.13.2 work is to wire config-driven stale-odds age limits into the
upcoming report path and, if desired, surface the benchmark comparison inside
the broader current-card recommendation report. No scheduler, background
polling, or sportsbook-executable claim has been added.

#### T8.13.3 Build prioritized BFO historical target manifest
- **Description:** Create the first real completed-fight BestFightOdds target
  manifest from saved pre-event predictions so T8.13.1 can expand beyond the
  single UFC 330 sample without adding discovery crawling.
- **Status:** DONE
- **Dependencies:** T8.13.1
- **Acceptance Criteria:**
  - Read saved pre-event predictions and select a bounded set of past completed
    UFC fights that currently have no canonical `BestFightOdds Mean` rows.
  - Prioritize fights with resolved outcomes, saved prediction timestamps, and
    local event/fighter IDs available in the warehouse.
  - Produce an explicit review manifest under
    `data/odds/raw/bestfightodds/manifests/` with local event/fight IDs,
    fighter labels, event date/name, proposed BFO event/fighter URLs or manual
    lookup notes, and target status.
  - Require human-reviewed BFO URLs or matchup IDs before any live payload
    request is added to a capture manifest.
  - Keep the first expansion batch small, for example 5-10 completed fights or
    no more than 25 explicit BFO requests.
  - Do not fetch network content, decode payloads, promote canonical rows, or
    schedule scraping in this ticket.
- **Test Coverage:**
  - Unit tests for selecting candidate fights from saved predictions, excluding
    fights that already have BFO rows, deterministic prioritization, and manifest
    row validation.
- **Complexity:** S
- **Risk:** Low/Medium - this is metadata planning, but wrong target mapping
  would contaminate downstream odds joins.

**Implementation Notes (2026-08-20):**

Added `warehouse/build_bestfightodds_historical_manifest.py`, a local-only
review-manifest builder. It reads saved pre-event predictions, canonical odds,
and local fight rows; selects resolved past completed fights with saved
prediction timestamps and local fighter IDs; excludes fights that already have
canonical `BestFightOdds Mean` rows; and writes a review CSV for manual BFO
event/matchup lookup. It does not fetch BestFightOdds, decode payloads, promote
odds rows, or schedule scraping.

Review manifest written:

`data/odds/raw/bestfightodds/manifests/bfo_mean_historical_targets.review.csv`

Run command:

```bash
python3 warehouse/build_bestfightodds_historical_manifest.py \
  --max-fights 10 \
  --as-of-date 2026-08-20
```

Run counters:

| Metric | Count |
|---|---:|
| Prediction rows read | 211 |
| Candidate fights | 112 |
| Manifest rows written | 10 |
| Skipped existing BFO Mean | 2 |
| Skipped missing local fight | 20 |
| Skipped not past event | 64 |
| Skipped unresolved prediction | 13 |

The first review manifest prioritizes ten completed UFC 330 fights that do not
yet have canonical BFO Mean rows. Each row is marked
`needs_manual_bfo_lookup`, has `capture_manifest_ready = false`, and leaves BFO
event/matchup/payload URL fields blank until manual review supplies exact BFO
targets. No live capture manifest was produced.

#### T8.13.4 Capture first expanded BFO historical batch and rerun benchmark
- **Description:** Use the reviewed manifest from T8.13.3 to perform the first
  bounded completed-fight BFO Mean historical expansion and rerun the
  market-benchmark backtest.
- **Status:** DONE
- **Dependencies:** T8.13.3, T8.13.1
- **Acceptance Criteria:**
  - Run `warehouse/capture_bestfightodds_manifest.py` in dry-run mode first and
    store the dry-run result manifest.
  - With explicit approval, capture only the reviewed historical batch with a
    request cap no higher than the manifest size and no higher than the existing
    hard cap.
  - Store raw snapshots/payloads and metadata under
    `data/odds/raw/bestfightodds/`.
  - Decode stored payloads offline; no parser may fetch network content.
  - Promote only two-sided timestamped `BestFightOdds Mean` moneyline rows that
    conservatively map to local warehouse fight/fighter IDs.
  - Record skipped duplicates, unmatched rows, ambiguous mappings, and rejected
    one-sided markets in the source/unmatched review CSVs.
  - Re-run the completed-fight BFO Mean market-benchmark backtest and update
    reports under `data/reports/bfo_mean_market_benchmark/`.
  - Label all outputs as market-benchmark, not sportsbook-executable P/L.
  - Do not add broad crawling, a scheduler, background polling, or model-training
    features.
- **Test Coverage:**
  - Use existing adapter/promotion tests; add regression coverage for any new
    mapping edge case found in the first expanded batch.
- **Complexity:** M
- **Risk:** Medium - this is the first meaningful historical scrape batch, so
  target mapping and request discipline matter.

**Implementation Notes (2026-08-20):**

The reviewed UFC 330 event snapshot from the prior bounded run contained
matchup IDs for all ten T8.13.3 review rows, so no additional event/fighter page
fetch was needed to build the first capture manifest. Added event-page context
support to `warehouse/adapt_bestfightodds_line_history_payloads.py` so stored
BFO event snapshots can identify `matchup_id`, side, event date, fighter, and
opponent for `/api/ggd` payloads. This keeps the first expanded historical batch
to payload requests only.

Reviewed capture manifest:

`data/odds/raw/bestfightodds/manifests/bfo_mean_historical_payload_targets.csv`

Dry-run result manifest:

`data/odds/raw/bestfightodds/manifests/bfo_mean_historical_payload_results.dry_run.csv`

Live result manifest:

`data/odds/raw/bestfightodds/manifests/bfo_mean_historical_payload_results.live.csv`

The live manifest captured 20 explicit payload URLs for ten completed UFC 330
fights. Every target returned HTTP 200. The run used the manifest capture helper
with `--request-cap 20`; no site discovery, event crawl, scheduler, background
polling, or model-training feature was added.

Offline adapter result after the capture:

| Metric | Count |
|---|---:|
| Payloads read | 26 |
| Source points read | 6110 |
| Matched canonical-compatible rows | 5682 |
| Unmatched/review rows | 428 |
| Skipped duplicate source rows | 428 |

The remaining unmatched rows were duplicate stable odds keys from earlier
recaptured sample payloads.

Canonical promotion result:

| Metric | Count |
|---|---:|
| Existing canonical rows | 150454 |
| Source rows read | 5682 |
| Source rows valid | 5682 |
| Appended rows | 5122 |
| Duplicate rows skipped | 560 |

Canonical `BestFightOdds Mean` coverage is now 5682 rows: 5550 completed-fight
rows for 2026-08-15 and 132 upcoming rows for 2026-08-22.

Completed-fight BFO Mean market-benchmark rerun:

```bash
python3 betting/bfo_mean_market_backtest.py --end-date 2026-08-15
```

Run counters:

| Metric | Count |
|---|---:|
| BFO fights with no-vig odds | 11 |
| BFO market groups | 2841 |
| Canonical BFO Mean rows | 5682 |
| No-vig odds rows after filters | 5550 |
| Prediction rows after filters | 11 |
| Dataset rows | 22 |
| Dataset issues | 0 |

Default-policy result:

| Metric | Value |
|---|---:|
| Total bets | 4 |
| Wins | 2 |
| Losses | 2 |
| Total staked | 60.00 |
| Profit/Loss | 81.218000 |
| ROI | 1.353633333333333333333333333 |
| Hit rate | 0.5 |
| Ending bankroll | 1081.218000 |

This remains a small market-benchmark sample, not sportsbook-executable P/L.
The report outputs remain under
`data/reports/bfo_mean_market_benchmark/` and retain the
`market-benchmark, not sportsbook-executable P/L` label.

#### T8.13.5 Add stale-odds gate to BFO upcoming snapshot report
- **Description:** Add config-driven freshness handling to the upcoming BFO Mean
  market-benchmark report so current-card comparisons do not silently use stale
  market rows.
- **Status:** DONE
- **Dependencies:** T8.13.2, T8.6.2
- **Acceptance Criteria:**
  - Read the existing betting config age/freshness setting or add a clearly named
    BFO benchmark freshness setting if the existing config is not appropriate.
  - Mark upcoming benchmark rows with odds age at report `as_of` time.
  - Exclude or flag rows whose latest BFO Mean timestamp is older than the
    configured freshness limit.
  - Preserve the market-benchmark label and avoid sportsbook-executable language.
  - Write stale/missing benchmark reasons so current-card review can tell the
    difference between no BFO rows, one-sided BFO rows, future-only rows, and
    stale rows.
  - Do not add live polling or automatic refresh.
- **Test Coverage:**
  - Unit tests for fresh rows, stale rows, missing rows, one-sided rows,
    future-only rows, and config override behavior.
- **Complexity:** S
- **Risk:** Low/Medium - freshness gates are straightforward but important for
  not overtrusting old market data.

**Implementation Notes (2026-08-20):**

Updated `betting/bfo_mean_upcoming_snapshot.py` to use the existing betting
config freshness setting, `risk.max_odds_age_hours_current`, for BFO Mean
benchmark rows. The CLI now accepts `--config` and
`--max-odds-age-hours-current` like the recommendation path.

The upcoming report now includes:

- `benchmark_freshness_status`
- `benchmark_exclusion_reason`
- `max_odds_age_hours`
- `odds_age_hours`

Fresh and stale two-sided BFO groups keep their benchmark prices in the report,
but stale groups are flagged with `benchmark_freshness_status = stale` and
`benchmark_exclusion_reason = odds_age_exceeds_max`. Fights without usable BFO
benchmark rows are no longer silently dropped; the report writes placeholder
fighter-side rows with explicit reasons such as `missing_bfo_rows`,
`future_only_bfo_rows`, or `expected_two_bfo_sides_got_1`.

Regenerated upcoming report:

```bash
python3 betting/bfo_mean_upcoming_snapshot.py \
  --as-of 2026-08-20T10:30:00+00:00
```

Output:

`data/reports/bfo_mean_market_benchmark/bfo_mean_upcoming_market_snapshot.csv`

Report status breakdown:

| Status | Rows |
|---|---:|
| `fresh` | 2 |
| `missing` | 128 |

Reason breakdown:

| Reason | Rows |
|---|---:|
| blank/fresh | 2 |
| `missing_bfo_rows` | 128 |

The two fresh rows are Anthony Hernandez and Gregory Rodrigues, using the
`2026-08-19T21:36:06+00:00` BFO Mean timestamp. At the report as-of time, those
odds are 12.898333 hours old, inside the default 48-hour current-odds freshness
limit. No live polling, automatic refresh, scheduler, or sportsbook-executable
claim was added.

#### T8.13.6 Surface BFO benchmark edges in current-card reports
- **Description:** Add optional BFO Mean market-benchmark comparison columns to
  current-card recommendation outputs while keeping betting decisions separate
  from executable sportsbook odds.
- **Status:** DONE
- **Dependencies:** T8.13.2, T8.13.5, T8.6.2
- **Acceptance Criteria:**
  - Join current-card predictions/recommendations to latest fresh two-sided BFO
    Mean benchmark rows when available.
  - Add benchmark-only fields such as BFO timestamp, BFO decimal odds, BFO no-vig
    market probability, model edge versus BFO, EV/unit versus BFO, freshness
    status, and benchmark label.
  - Keep the actual recommendation/bet placement path unchanged unless executable
    sportsbook odds are provided separately.
  - Clearly distinguish BFO benchmark pass/bet-style diagnostics from actual
    sportsbook-executable recommendations in CSV and human-readable outputs.
  - Preserve rows for fights with missing/stale/unmatched BFO data and include
    reason codes.
  - Do not add a scheduler, background refresh, or BFO-driven automatic bet
    placement.
- **Test Coverage:**
  - Unit tests for successful BFO join, missing BFO data, stale BFO data,
    unmatched fight IDs, label propagation, and unchanged executable
    recommendation behavior.
- **Complexity:** M
- **Risk:** Medium - this is user-facing decision support, so labels and
  separation from executable odds must be unmistakable.

**Implementation Notes (2026-08-20):**

Updated `betting/recommend.py` so current-card recommendation reports can load
the BFO Mean upcoming benchmark snapshot CSV as report-only enrichment. The
default benchmark input is:

`data/reports/bfo_mean_market_benchmark/bfo_mean_upcoming_market_snapshot.csv`

The CLI now supports:

- `--bfo-benchmark-report`
- `--no-bfo-benchmark`

The actual executable odds join, value policy, staking, bet/pass decisions, and
bankroll exposure logic remain unchanged. BFO fields are attached only when
writing report rows after staking has already been calculated.

New recommendation CSV columns:

- `bfo_benchmark_label`
- `bfo_benchmark_freshness_status`
- `bfo_benchmark_exclusion_reason`
- `bfo_benchmark_odds_timestamp`
- `bfo_benchmark_odds_age_hours`
- `bfo_benchmark_decimal_odds`
- `bfo_benchmark_no_vig_market_probability`
- `bfo_benchmark_edge`
- `bfo_benchmark_ev_per_unit`

New event-summary CSV columns:

- `bfo_benchmark_fresh_rows`
- `bfo_benchmark_stale_rows`
- `bfo_benchmark_missing_rows`
- `bfo_benchmark_other_unusable_rows`

The console summary now prints aggregate BFO benchmark coverage counts. Missing
or disabled benchmark data is explicit via report-only reason strings such as
`missing_bfo_benchmark_row`, `bfo_benchmark_report_missing`, or
`bfo_benchmark_disabled`. These fields do not create sportsbook-executable
recommendations and do not trigger any BFO fetch, polling, scheduler, or
automatic refresh.

#### T8.13.7 Commit/QA BFO odds benchmark pipeline
- **Description:** Consolidate the BFO Mean market-benchmark pipeline before any
  additional scraping batch, including QA reporting, runbook documentation,
  artifact policy, and verification.
- **Status:** DONE
- **Dependencies:** T8.13.4, T8.13.5, T8.13.6
- **Acceptance Criteria:**
  - Add a short reproducible command runbook for the BFO flow: build historical
    review manifest, fill reviewed payload URLs, dry-run capture, live bounded
    capture, offline adapt, canonical promotion, historical benchmark, upcoming
    snapshot, and current-card recommendation report enrichment.
  - Add a small QA report summarizing canonical BFO rows by event/fight.
  - Confirm generated artifacts that should remain local/ignored versus source
    files that should be committed.
  - Run the full relevant BFO/recommendation verification suite.
  - Keep all BFO outputs labeled as market-benchmark, not sportsbook-executable
    P/L.
  - Do not add new scraping, scheduler, polling, or model-training features.
- **Test Coverage:**
  - Unit test for canonical BFO coverage QA grouping.
  - Re-run BFO adapter, manifest, recommendation, market-backtest, and upcoming
    snapshot tests.
- **Complexity:** S
- **Risk:** Low - consolidation only, but important for reproducibility before
  expanding scrape volume.

**Implementation Notes (2026-08-20):**

Added `warehouse/report_bestfightodds_canonical_coverage.py`, which reads
canonical `data/odds/fight_odds.csv`, filters `BestFightOdds Mean` rows from
`bestfightodds_line_history_payload`, and writes fight-level coverage QA to:

`data/reports/bfo_mean_market_benchmark/bfo_mean_canonical_coverage.csv`

The local QA run reported:

| Metric | Count |
|---|---:|
| Canonical BFO rows | 5682 |
| Covered events | 2 |
| Covered fights | 12 |
| Two-sided fights | 12 |

Added a BFO pipeline runbook to `data/odds/README.md` covering the full manual
review and bounded capture flow through reports. Updated `.gitignore` so raw BFO
HTML/JS captures and generated BFO benchmark reports follow the existing
generated-artifact policy. Raw captures, source CSVs, canonical odds CSVs, and
generated reports remain local/regenerable; source code, tests, docs, and small
fixtures are source-controlled.

Verification command:

```bash
pytest betting/tests/test_recommend_cli.py \
  betting/tests/test_recommend.py \
  betting/tests/test_bfo_mean_upcoming_snapshot.py \
  betting/tests/test_bfo_mean_market_backtest.py \
  warehouse/tests/test_adapt_bestfightodds_line_history_payloads.py \
  warehouse/tests/test_capture_bestfightodds_manifest.py \
  warehouse/tests/test_build_bestfightodds_historical_manifest.py \
  warehouse/tests/test_report_bestfightodds_canonical_coverage.py
```

Result: 40 tests passed. Python compile checks also passed for the BFO and
recommendation scripts. No network fetch, canonical promotion, scheduler,
polling, or model-training feature was added for this ticket.

---

## Dependency Graph

```text
T8.1.1
  |
  +-- T8.1.2
  |
  +-- T8.2.1 -- T8.2.2 -- T8.2.3
  |
  +-- T8.3.1 -- T8.3.2
                  |
                  +-- T8.4.1 -- T8.4.2 -- T8.4.3
                                            |
                                            +-- T8.5.1 -- T8.5.2 -- T8.5.3
                                                              |
                                                              +-- T8.6.1 -- T8.6.2 -- T8.6.3
                                                              |
                                                              +-- T8.7.1 -- T8.7.2 -- T8.7.3 -- T8.7.4

T8.8.1 depends on formulas, risk rules, and reports being finalized.
T8.8.2 depends on T8.8.1 and command names from T8.6.2.
T8.9.1 spans math/risk/backtest tickets.
T8.9.2 depends on T8.7.1.
T8.10.1 follows the V1 odds loader/docs, T8.10.0 acquires the raw Kaggle source,
T8.10.2 and T8.10.3 build the Kaggle odds adapter path, and T8.10.4 documents
the future BestFightOdds scraper.
T8.11.0 selects the scraping-only source direction. T8.11.1 verifies
BestFightOdds feasibility before T8.11.2 captures bounded raw snapshots and
T8.11.3 parses reviewed snapshots into normalized odds. T8.11.4 is the fallback
permission/feasibility review for alternate archives. T8.11.5 documents the
source decision matrix. T8.12.0 uses a tiny explicit BestFightOdds sample to
make the go/no-go decision before any canonical promotion. T8.12.1 probes the
single discovered BFO line-history payload path for timestamp quality. T8.12.2
decodes stored BFO line-history payloads into source-specific normalized rows.
T8.12.3 promotes reviewed BFO Mean rows as the first canonical market-benchmark
source. T8.13.0 runs the first saved-prediction market-benchmark backtest using
those canonical BFO Mean rows. T8.13.1 expands the bounded historical BFO Mean
backfill, and T8.13.2 adds an explicitly targeted upcoming-fight market
snapshot path. T8.13.3 builds the first prioritized completed-fight target
manifest, T8.13.4 captures and promotes that bounded historical batch, T8.13.5
adds stale-odds handling for upcoming BFO benchmark rows, and T8.13.6 surfaces
fresh BFO benchmark edges in current-card reports without changing executable
bet recommendations. T8.13.7 consolidates the BFO pipeline with QA reporting,
artifact policy, runbook documentation, verification, and a clean commit point.
```

---

## Suggested Execution Order

| Stage | Tickets | Outcome |
|---|---|---|
| 1 | T8.1.1, T8.1.2, T8.3.1, T8.3.2 | Pure betting math and reason-code foundation. |
| 2 | T8.2.1, T8.2.2, T8.2.3 | Odds can be stored, loaded, and audited. |
| 3 | T8.4.1, T8.4.2, T8.4.3 | Recommendations can determine bet/pass from predictions and odds. |
| 4 | T8.5.1, T8.5.2, T8.5.3 | Stakes are conservative and capped. |
| 5 | T8.6.1, T8.6.2, T8.6.3 | Current-card reports and commands are available. |
| 6 | T8.7.1, T8.7.2, T8.7.3, T8.7.4 | Historical betting profitability can be backtested leakage-safely. |
| 7 | T8.8.1, T8.8.2, T8.9.1, T8.9.2 | Documentation and quality gates complete. |
| 8 | T8.10.1, T8.10.0, T8.10.2, T8.10.3, T8.10.4 | External odds sources can feed the canonical odds contract. |
| 9 | T8.11.1, T8.11.2, T8.11.3, T8.11.4, T8.11.5, T8.12.0, T8.12.1, T8.12.2, T8.12.3 | Scraping-only BestFightOdds-first path is verified, probed, normalized, documented, go/no-go reviewed, timestamp-payload checked, decoded to source-specific review rows, and promoted as a canonical market benchmark. |
| 10 | T8.13.0 | First canonical BFO Mean market-benchmark backtest report is produced from saved pre-event predictions. |
| 11 | T8.13.1, T8.13.2 | BFO Mean market-benchmark coverage expands to bounded historical backfill and explicitly targeted upcoming-fight snapshots. |
| 12 | T8.13.3, T8.13.4 | Historical BFO Mean coverage expands through a reviewed completed-fight manifest and one bounded capture/promote/backtest batch. |
| 13 | T8.13.5, T8.13.6 | Upcoming BFO Mean benchmark data gets stale-odds handling and appears in current-card reports as benchmark-only decision support. |
| 14 | T8.13.7 | BFO Mean benchmark pipeline is documented, QA-reportable, artifact-scoped, verified, and ready for the next bounded source batch. |

---

## V1 Success Criteria

Phase 8 is successful when:

1. `make load_odds` imports idempotent odds data tied to existing fight and fighter IDs.
2. `make betting_recommendations` writes current-card betting recommendations under
   `data/reports/` with bet/pass decisions, stake sizes, EV, edge, and reason codes.
3. `make betting_backtest` writes chronological betting profitability reports under
   `data/reports/` using only pre-event predictions and pre-event odds.
4. Toss-up fights are never recommended as bets in the default policy.
5. Missing, stale, ambiguous, or invalid odds always result in pass decisions.
6. Risk caps prevent excessive single-fight and event-level exposure.
7. Unit tests cover odds conversion, no-vig, EV, Kelly staking, caps, and P/L math.
8. Documentation clearly separates prediction accuracy from betting profitability.

---

## Resolved V1 Policy Decisions

1. `data/odds/fight_odds.csv` is the canonical V1 historical odds source.
   OddsPortal-style exports, paid APIs, or additional providers can be normalized
   into that contract later.
2. Current recommendations evaluate all imported bookmakers independently by
   default. Use `--bookmaker` for a preferred-source review.
3. Historical backtests default to `latest-before-prediction`, which requires
   odds before both the event date and the saved prediction timestamp. Opening,
   closing, and latest-before-event remain comparison modes.
4. Current recommendations report stake fractions unless `--bankroll` is
   supplied. Backtests use a paper starting bankroll of `1000` unless
   `--initial-bankroll` is supplied.
5. Default caps stay at 1% medium, 2% high, 0% toss-up, 2% single bet, and 6%
   event exposure. First live paper-trial reviews should use tighter CLI/config
   caps, for example 0.5% medium, 1% high, 1% single bet, and 3% event exposure.
