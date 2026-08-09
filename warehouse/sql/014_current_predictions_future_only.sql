-- Keep stale past-dated "upcoming" events out of current prediction views.
-- Some sources can lag and leave an event marked upcoming after the event date.

CREATE OR REPLACE VIEW current_event_predictions AS
SELECT
    e.event_id,
    e.event_name,
    e.event_date,
    e.city,
    e.state,
    e.country,
    e.event_status,
    f.fight_id,
    f.is_title_fight,
    f.is_interim_title,
    f.scheduled_rounds,
    lp.fighter_1_id,
    lp.fighter_2_id,
    lp.fighter_1_name,
    lp.fighter_2_name,
    lp.weight_class,
    lp.predicted_prob_f1,
    lp.calibrated_prob_f1,
    lp.predicted_label,
    lp.predicted_winner_name,
    lp.confidence_tier,
    lp.is_uncertain,
    lp.model_name,
    lp.model_artifact,
    lp.scored_at
FROM latest_predictions lp
JOIN fights f ON f.fight_id = lp.fight_id
JOIN events e ON e.event_id = f.event_id
WHERE e.event_status = 'upcoming'
  AND e.event_date >= CURRENT_DATE;
