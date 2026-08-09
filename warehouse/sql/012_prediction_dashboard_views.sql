-- Dashboard-facing prediction views.
-- These views keep raw prediction history in ``predictions`` and derive
-- current/upcoming predictions plus honest post-event performance from it.

CREATE OR REPLACE VIEW latest_predictions AS
SELECT DISTINCT ON (p.fight_id)
    p.fight_id,
    p.event_date,
    p.fighter_1_id,
    p.fighter_2_id,
    p.fighter_1_name,
    p.fighter_2_name,
    p.weight_class,
    p.predicted_prob_f1,
    p.calibrated_prob_f1,
    CASE
        WHEN p.calibrated_prob_f1 >= 0.5 THEN 1
        ELSE 0
    END AS predicted_label,
    CASE
        WHEN p.calibrated_prob_f1 >= 0.5 THEN p.fighter_1_name
        ELSE p.fighter_2_name
    END AS predicted_winner_name,
    p.confidence_tier,
    p.is_uncertain,
    p.model_name,
    p.model_artifact,
    p.scored_at
FROM predictions p
ORDER BY p.fight_id, p.scored_at DESC;


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


CREATE OR REPLACE VIEW pre_event_prediction_fights AS
WITH pre_event_predictions AS (
    SELECT DISTINCT ON (p.fight_id)
        p.*
    FROM predictions p
    WHERE p.scored_at::date < p.event_date
    ORDER BY p.fight_id, p.scored_at DESC
)
SELECT
    e.event_id,
    e.event_name,
    e.event_date,
    f.fight_id,
    p.fighter_1_id,
    p.fighter_2_id,
    p.fighter_1_name,
    p.fighter_2_name,
    p.weight_class,
    f.is_title_fight,
    f.is_interim_title,
    f.scheduled_rounds,
    p.scored_at,
    'database_scored_at_before_event'::text AS pre_event_evidence,
    p.predicted_prob_f1,
    p.calibrated_prob_f1,
    CASE
        WHEN p.calibrated_prob_f1 >= 0.5 THEN 1
        ELSE 0
    END AS predicted_label,
    CASE
        WHEN p.calibrated_prob_f1 >= 0.5 THEN p.fighter_1_name
        ELSE p.fighter_2_name
    END AS predicted_winner_name,
    p.confidence_tier,
    p.is_uncertain,
    CASE
        WHEN f.result_type = 'win' AND f.winner_fighter_id = p.fighter_1_id THEN 1
        WHEN f.result_type = 'win' AND f.winner_fighter_id = p.fighter_2_id THEN 0
        ELSE NULL
    END AS actual_label,
    CASE
        WHEN f.result_type = 'win' AND f.winner_fighter_id = p.fighter_1_id THEN p.fighter_1_name
        WHEN f.result_type = 'win' AND f.winner_fighter_id = p.fighter_2_id THEN p.fighter_2_name
        WHEN repl.fight_id IS NOT NULL THEN concat(
            'Fighter changed: ',
            repl.fighter_1_name,
            ' vs ',
            repl.fighter_2_name
        )
        ELSE 'Pending / no W-L result'
    END AS actual_winner_name,
    f.result_type,
    f.finish_method,
    f.finish_round,
    f.finish_time_seconds,
    (
        f.result_type = 'win'
        AND f.winner_fighter_id IN (p.fighter_1_id, p.fighter_2_id)
    ) AS resolved,
    CASE
        WHEN f.result_type = 'win' AND f.winner_fighter_id = p.fighter_1_id
            THEN p.calibrated_prob_f1 >= 0.5
        WHEN f.result_type = 'win' AND f.winner_fighter_id = p.fighter_2_id
            THEN p.calibrated_prob_f1 < 0.5
        ELSE NULL
    END AS correct,
    p.model_name,
    p.model_artifact
FROM pre_event_predictions p
JOIN fights f ON f.fight_id = p.fight_id
JOIN events e ON e.event_id = f.event_id
LEFT JOIN LATERAL (
    SELECT
        af.fight_id,
        af1.full_name AS fighter_1_name,
        af2.full_name AS fighter_2_name
    FROM fights af
    JOIN fighters af1 ON af1.fighter_id = af.fighter_1_id
    JOIN fighters af2 ON af2.fighter_id = af.fighter_2_id
    WHERE af.event_id = f.event_id
      AND af.fight_id <> f.fight_id
      AND af.result_type = 'win'
      AND (
          af.fighter_1_id IN (p.fighter_1_id, p.fighter_2_id)
          OR af.fighter_2_id IN (p.fighter_1_id, p.fighter_2_id)
      )
    ORDER BY
        (
            CASE WHEN af.fighter_1_id IN (p.fighter_1_id, p.fighter_2_id) THEN 1 ELSE 0 END
            + CASE WHEN af.fighter_2_id IN (p.fighter_1_id, p.fighter_2_id) THEN 1 ELSE 0 END
        ) DESC,
        af.scraped_at DESC NULLS LAST
    LIMIT 1
) repl ON TRUE;


CREATE OR REPLACE VIEW pre_event_prediction_events AS
SELECT
    event_id,
    event_name,
    event_date,
    model_name,
    pre_event_evidence,
    count(*) AS n_predicted_fights,
    count(*) FILTER (WHERE correct) AS correct,
    avg(CASE WHEN correct THEN 1.0 ELSE 0.0 END) AS accuracy,
    avg(
        -(
            actual_label * ln(LEAST(GREATEST(calibrated_prob_f1::double precision, 1e-15), 1 - 1e-15))
            + (1 - actual_label) * ln(1 - LEAST(GREATEST(calibrated_prob_f1::double precision, 1e-15), 1 - 1e-15))
        )
    ) AS log_loss,
    avg(power(calibrated_prob_f1::double precision - actual_label, 2)) AS brier_score,
    min(scored_at) AS first_scored_at,
    max(scored_at) AS last_scored_at,
    count(*) FILTER (WHERE confidence_tier = 'high') AS high_count,
    count(*) FILTER (WHERE confidence_tier = 'medium') AS medium_count,
    count(*) FILTER (WHERE confidence_tier = 'toss-up') AS toss_up_count,
    avg(CASE WHEN correct THEN 1.0 ELSE 0.0 END) FILTER (WHERE confidence_tier = 'high') AS high_accuracy,
    avg(CASE WHEN correct THEN 1.0 ELSE 0.0 END) FILTER (WHERE confidence_tier = 'medium') AS medium_accuracy,
    avg(CASE WHEN correct THEN 1.0 ELSE 0.0 END) FILTER (WHERE confidence_tier = 'toss-up') AS toss_up_accuracy
FROM pre_event_prediction_fights
WHERE resolved
GROUP BY
    event_id,
    event_name,
    event_date,
    model_name,
    pre_event_evidence;
