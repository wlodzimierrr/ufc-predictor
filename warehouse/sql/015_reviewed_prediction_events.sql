-- Event-level reviewed prediction summaries for dashboard history.
--
-- True pre-event predictions still come from predictions.scored_at < event_date
-- via pre_event_prediction_fights. This table lets us surface explicitly
-- reviewed catch-up cards whose saved predictions were generated before
-- loading results, but after the event date had already passed.

CREATE TABLE IF NOT EXISTS reviewed_prediction_events (
    event_name          text        NOT NULL,
    event_date          date        NOT NULL,
    review_type         text        NOT NULL DEFAULT 'catchup_scored_before_result_load',
    model_name          text,
    n_predicted_fights  integer     NOT NULL,
    correct             integer,
    accuracy            numeric,
    log_loss            numeric,
    brier_score         numeric,
    first_scored_at     timestamptz,
    last_scored_at      timestamptz,
    high_count          integer,
    medium_count        integer,
    toss_up_count       integer,
    high_accuracy       numeric,
    medium_accuracy     numeric,
    toss_up_accuracy    numeric,
    reviewed_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_name, event_date, review_type)
);

CREATE OR REPLACE VIEW pre_event_prediction_events AS
WITH strict_events AS (
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
        e.country
),
reviewed_events AS (
    SELECT
        e.event_id,
        r.event_name,
        r.event_date,
        r.model_name,
        r.review_type AS pre_event_evidence,
        r.n_predicted_fights::bigint AS n_predicted_fights,
        r.correct::bigint AS correct,
        r.accuracy,
        r.log_loss,
        r.brier_score,
        r.first_scored_at,
        r.last_scored_at,
        r.high_count::bigint AS high_count,
        r.medium_count::bigint AS medium_count,
        r.toss_up_count::bigint AS toss_up_count,
        r.high_accuracy,
        r.medium_accuracy,
        r.toss_up_accuracy,
        e.event_status,
        concat_ws(', ', e.city, e.state, e.country) AS location,
        concat_ws(', ', e.city, e.state, e.country) AS event_location
    FROM reviewed_prediction_events r
    LEFT JOIN events e
      ON e.event_date = r.event_date
     AND lower(e.event_name) = lower(r.event_name)
    WHERE NOT EXISTS (
        SELECT 1
        FROM strict_events se
        WHERE se.event_date = r.event_date
          AND lower(se.event_name) = lower(r.event_name)
    )
)
SELECT * FROM strict_events
UNION ALL
SELECT * FROM reviewed_events;
