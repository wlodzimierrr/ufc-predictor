-- Compatibility aliases for the dashboard repository.
-- Keeps canonical warehouse names (for example ``correct``) while also exposing
-- the names currently used by ufc-dashboard queries.

CREATE OR REPLACE VIEW pre_event_prediction_fights AS
WITH pre_event_predictions AS (
    SELECT DISTINCT ON (p.fight_id)
        p.*
    FROM predictions p
    WHERE p.scored_at::date < p.event_date
    ORDER BY p.fight_id, p.scored_at DESC
),
reviewed_fights AS (
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
    ) repl ON TRUE
)
SELECT
    *,
    correct AS is_correct
FROM reviewed_fights;


CREATE OR REPLACE VIEW pre_event_prediction_events AS
SELECT
    pf.event_id,
    pf.event_name,
    pf.event_date,
    pf.model_name,
    pf.pre_event_evidence,
    count(*) AS n_predicted_fights,
    count(*) FILTER (WHERE pf.correct) AS correct,
    avg(CASE WHEN pf.correct THEN 1.0 ELSE 0.0 END) AS accuracy,
    avg(
        -(
            pf.actual_label * ln(LEAST(GREATEST(pf.calibrated_prob_f1::double precision, 1e-15), 1 - 1e-15))
            + (1 - pf.actual_label) * ln(1 - LEAST(GREATEST(pf.calibrated_prob_f1::double precision, 1e-15), 1 - 1e-15))
        )
    ) AS log_loss,
    avg(power(pf.calibrated_prob_f1::double precision - pf.actual_label, 2)) AS brier_score,
    min(pf.scored_at) AS first_scored_at,
    max(pf.scored_at) AS last_scored_at,
    count(*) FILTER (WHERE pf.confidence_tier = 'high') AS high_count,
    count(*) FILTER (WHERE pf.confidence_tier = 'medium') AS medium_count,
    count(*) FILTER (WHERE pf.confidence_tier = 'toss-up') AS toss_up_count,
    avg(CASE WHEN pf.correct THEN 1.0 ELSE 0.0 END) FILTER (WHERE pf.confidence_tier = 'high') AS high_accuracy,
    avg(CASE WHEN pf.correct THEN 1.0 ELSE 0.0 END) FILTER (WHERE pf.confidence_tier = 'medium') AS medium_accuracy,
    avg(CASE WHEN pf.correct THEN 1.0 ELSE 0.0 END) FILTER (WHERE pf.confidence_tier = 'toss-up') AS toss_up_accuracy,
    e.event_status,
    concat_ws(', ', e.city, e.state, e.country) AS location,
    concat_ws(', ', e.city, e.state, e.country) AS event_location
FROM pre_event_prediction_fights pf
JOIN events e ON e.event_id = pf.event_id
WHERE pf.resolved
GROUP BY
    pf.event_id,
    pf.event_name,
    pf.event_date,
    pf.model_name,
    pf.pre_event_evidence,
    e.event_status,
    e.city,
    e.state,
    e.country;
