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
- **Status:** PROPOSED
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

#### T8.11.2 Add BestFightOdds raw snapshot probe
- **Description:** Add a tiny, explicitly bounded BestFightOdds snapshot probe to
  test fetch/parsing feasibility and raw artifact storage before any broad crawl.
- **Status:** PROPOSED
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

#### T8.11.3 Add BestFightOdds parser and normalized adapter
- **Description:** Parse reviewed BestFightOdds raw snapshots and normalize them
  into the canonical V1 odds contract.
- **Status:** PROPOSED
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

#### T8.11.4 Alternate odds archive permission/feasibility fallback
- **Description:** Evaluate one alternate website archive only if BestFightOdds is
  blocked by terms, missing timestamps, or brittle parsing.
- **Status:** PROPOSED
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
source decision matrix.
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
| 9 | T8.11.1, T8.11.2, T8.11.3, T8.11.4, T8.11.5 | Scraping-only BestFightOdds-first path is verified, probed, normalized, and documented. |

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
