-- Fight-level reviewed catch-up rows for dashboards that summarize from
-- pre_event_prediction_fights instead of the event-level rollup.

CREATE TABLE IF NOT EXISTS reviewed_prediction_fights (
    event_id             uuid,
    event_name           text        NOT NULL,
    event_date           date        NOT NULL,
    review_type          text        NOT NULL DEFAULT 'catchup_scored_before_result_load',
    fight_id             uuid        NOT NULL,
    actual_fight_id      uuid,
    fighter_1_id         uuid,
    fighter_2_id         uuid,
    fighter_1_name       text,
    fighter_2_name       text,
    weight_class         text,
    is_title_fight       boolean,
    is_interim_title     boolean,
    scheduled_rounds     smallint,
    scored_at            timestamptz,
    predicted_prob_f1    numeric(6,4),
    calibrated_prob_f1   numeric(6,4),
    predicted_label      integer,
    predicted_winner_name text,
    confidence_tier      text,
    is_uncertain         boolean,
    actual_label         integer,
    actual_winner_name   text,
    result_type          text,
    finish_method        text,
    finish_round         smallint,
    finish_time_seconds  smallint,
    correct              boolean,
    model_name           text,
    model_artifact       text,
    reviewed_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_date, review_type, fight_id)
);

CREATE OR REPLACE VIEW pre_event_prediction_fights AS
WITH pre_event_predictions AS (
    SELECT DISTINCT ON (p.fight_id)
        p.*
    FROM predictions p
    WHERE p.scored_at::date < p.event_date
    ORDER BY p.fight_id, p.scored_at DESC
),
strict_fights AS (
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
),
catchup_fights AS (
    SELECT
        r.event_id,
        r.event_name,
        r.event_date,
        r.fight_id,
        r.fighter_1_id,
        r.fighter_2_id,
        r.fighter_1_name,
        r.fighter_2_name,
        r.weight_class,
        r.is_title_fight,
        r.is_interim_title,
        r.scheduled_rounds,
        r.scored_at,
        r.review_type AS pre_event_evidence,
        r.predicted_prob_f1,
        r.calibrated_prob_f1,
        r.predicted_label,
        r.predicted_winner_name,
        r.confidence_tier,
        r.is_uncertain,
        r.actual_label,
        r.actual_winner_name,
        r.result_type,
        r.finish_method,
        r.finish_round,
        r.finish_time_seconds,
        true AS resolved,
        r.correct,
        r.model_name,
        r.model_artifact
    FROM reviewed_prediction_fights r
    WHERE NOT EXISTS (
        SELECT 1
        FROM strict_fights sf
        WHERE sf.event_date = r.event_date
          AND lower(sf.event_name) = lower(r.event_name)
          AND sf.fight_id = r.fight_id
    )
)
SELECT
    *,
    correct AS is_correct
FROM strict_fights
UNION ALL
SELECT
    *,
    correct AS is_correct
FROM catchup_fights;

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
LEFT JOIN events e ON e.event_id = pf.event_id
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
