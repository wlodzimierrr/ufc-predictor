# Public prediction decision contract

Policy version: `probability_band_v1`.

Use the existing calibrated `p = P(fighter_1 wins)` at its original float64
precision. Inclusive `0.40 <= p <= 0.60` means **NO PICK**. Below 0.40 selects
fighter 2 (`pick_label = 0`); above 0.60 selects fighter 1 (`pick_label = 1`).
The policy never changes or inverts either model probability. Invalid or
nonfinite probabilities raise an error.

High confidence remains inclusive `p <= 0.30 or p >= 0.70`; toss-up remains
inclusive 0.40–0.60; medium covers the remaining values. Strongest 57 is a
separate ranking group and is not the high-confidence tier.

## Fields and serialization

| Field | Meaning |
|---|---|
| `decision_status` | `pick` or `no_pick` |
| `is_actionable` | Boolean winner-forecast decision; false for NO PICK |
| `pick_label` | Nullable integer: 1 for fighter 1, 0 for fighter 2, null for NO PICK |
| `pick_winner_name` | Selected fighter's name; null for NO PICK (also null if a name is unavailable) |
| `uncertainty_reasons` | `['probability_band']` for NO PICK, otherwise an empty list |
| `decision_policy_version` | `probability_band_v1` |
| `decision_origin` | `recorded_at_scoring`, `derived_at_review`, or `derived_from_legacy_probability` |
| `pick_correct` | Boolean only for a resolved actionable pick; null for NO PICK or unresolved results |

`predicted_label`, `predicted_winner_name`, `correct`, and `predicted_correct`
retain their latent statistical meaning: compare `p >= 0.5` with the actual
label. They must not be used as the public actionable winner.

Python DataFrames use nullable `Int64` labels and nullable boolean correctness.
JSON uses null and arrays, never NaN. CSV uses empty nullable cells and JSON
arrays in `uncertainty_reasons`; parse the list with `json.loads`. Database
labels/names/correctness use SQL NULL, and reasons use `text[]`.

JSON exposes both `calibrated_prob_f1` and complementary `calibrated_prob_f2`.
CSV retains the original raw/calibrated fighter-1 probabilities. Human output
shows both fighters' probabilities and explicitly prints NO PICK.

## Precision and database migration

Apply `warehouse/sql/019_prediction_decisions.sql` through the migration runner
before deploying the new prediction/review writers or database report reader.
Do this in a separately authorized deployment; this implementation session
does not migrate the live warehouse. With deployment connection settings
configured, run `python3 warehouse/migrate.py`; existing migration tracking
skips applied files. Validate the pending migration list first. No UPDATE or
historical backfill is part of migration 019.

The original `numeric(6,4)` columns and old view names, column types, and column
order remain intact. Additive `predicted_prob_f1_full` and
`calibrated_prob_f1_full` columns use PostgreSQL `double precision`, matching
Python float64 model outputs without rounding. Future decisions are validated
against the full calibrated value. JSON no longer rounds to four decimals.
CSV writes the round-trip value; readers use pandas
`float_precision='round_trip'`.

For example, 0.39999 is an actionable fighter-2 pick even though the historical
numeric projection reads 0.4000. Similarly 0.60001 selects fighter 1 while the
old projection reads 0.6000. Consumers of new decision views must read the
`*_full` probabilities alongside the decision fields. Do not recompute the
decision from rounded display percentages or the old numeric projection.

For old rows, full precision cannot be recovered from warehouse numerics.
Views derive the new policy from the available stored probability and identify
it as `derived_from_legacy_probability`; they do not claim the decision was
recorded at the original scoring time. Legacy CSVs are enriched in memory and
outputs without rewriting the source. Summary-only legacy event logs cannot
reconstruct decision counts; their new metrics/version remain null.

Additive views:

- `prediction_decisions`: decision metadata and precise probability inputs per scoring record.
- `latest_prediction_decisions`: existing latest-prediction columns plus the contract.
- `current_event_prediction_decisions`: existing upcoming columns plus the contract.
- `pre_event_prediction_fight_decisions`: existing review columns plus the contract, `pick_correct`, `latent_label_full`, and `latent_correct_full`.
- `pre_event_prediction_event_decisions`: metrics from available fight rows, grouped by event/model/provenance.
- `reviewed_prediction_event_decisions`: persisted review summaries, retaining total-card coverage when results are only partially resolved. Legacy summary-only decisions are explicitly unavailable.

Database report generation requires migration 019. Auto mode can fall back to
CSV when the new view is unavailable; explicit database mode surfaces the error.

## Reporting denominators

- `total_count`: all forecasts in the reporting population, including pending results.
- `resolved_count`: forecasts with a resolved binary 0/1 result.
- `latent_correct_count` / `latent_accuracy`: correct latent forecasts / resolved count.
- `actionable_count`: all forecasts outside the inclusive NO PICK band.
- `actionable_resolved_count`: actionable forecasts with resolved results.
- `actionable_correct_count` / `actionable_accuracy`: correct actionable forecasts / actionable resolved count.
- `actionable_coverage`: actionable count / total count.
- `no_pick_count` / `no_pick_share`: band count / total count.
- `threshold_high_count`: all forecasts with `p <= 0.30 or p >= 0.70`.
- `threshold_high_resolved_count`: high forecasts with resolved results.
- `threshold_high_correct_count` / `threshold_high_accuracy`: correct high forecasts / high resolved count.

Zero eligible results produce null accuracy. Empty populations produce null
coverage/share. Pending, draw, NC, or unresolved outcomes never count as losses.
Log loss and Brier use all resolved forecasts, including NO PICK; single-class
events are supported. Legacy `n_fights` / `n_predicted_fights` event summaries
continue to count resolved forecasts. Catch-up and retroactive provenance stays
visible; pre-event report groups retain their evidence separately. Mixed reviews
containing retroactive scoring are conservatively marked `retroactive_review`.
Old dashboard views retain their historical behavior; use the additive views
for the new denominators.

## Dashboard and betting integration

The separate `/home/wlodzimierrr/ufc-dashboard` frontend has not been changed and
may still display latent winners. Its next integration must:

- Read `current_event_prediction_decisions` and `pre_event_prediction_fight_decisions`, using `calibrated_prob_f1_full` and its complement for display.
- Adopt all six decision fields plus `decision_origin` and `pick_correct`.
- Render NO PICK whenever `decision_status == 'no_pick'`; use only nullable `pick_label` / `pick_winner_name` for actionable selections.
- Use the new event count/accuracy/coverage/share fields and the explicit resolved denominators. Prefer persisted reviewed-event summaries for full-card coverage on partially resolved catch-up reviews.
- Keep latent accuracy and proper scores available for evaluation and provenance visible.

Winner abstention is independent of market value. Existing model-versus-market
calculations, bet/pass rules, and staking limits are unchanged. Existing betting
warehouse readers continue to use the original compatible views; CSV consumers
ignore the additive decision fields.
