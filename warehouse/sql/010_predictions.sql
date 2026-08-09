-- Predictions table
-- Stores model predictions for upcoming fights.
-- Each row is one prediction for one fight from a specific model run.
-- Re-scoring the same fight inserts another row when scored_at is different,
-- preserving prediction history for post-event audits.

CREATE TABLE IF NOT EXISTS predictions (
    fight_id            uuid        NOT NULL REFERENCES fights (fight_id),
    event_date          date,
    fighter_1_id        uuid        NOT NULL REFERENCES fighters (fighter_id),
    fighter_2_id        uuid        NOT NULL REFERENCES fighters (fighter_id),
    fighter_1_name      text,
    fighter_2_name      text,
    weight_class        text,
    predicted_prob_f1   numeric(6,4),   -- raw model output
    calibrated_prob_f1  numeric(6,4),   -- after Platt calibration
    confidence_tier     text,           -- 'high' | 'medium' | 'toss-up'
    is_uncertain        boolean,
    model_name          text        NOT NULL,
    model_artifact      text,
    scored_at           timestamptz NOT NULL,
    PRIMARY KEY (fight_id, scored_at)
);

CREATE INDEX IF NOT EXISTS idx_predictions_event_date ON predictions (event_date);
CREATE INDEX IF NOT EXISTS idx_predictions_scored_at ON predictions (scored_at DESC);
