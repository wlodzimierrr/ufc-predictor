-- Additive NO PICK contract. No UPDATE/backfill; old columns/views retain their
-- types, order and latent semantics. float8 preserves the Python model's float64
-- outputs; numeric(6,4) remains a compatibility projection, never policy input
-- when the full-precision value exists.

CREATE FUNCTION probability_band_v1(p double precision, f1 text, f2 text)
RETURNS TABLE (
    decision_status text, is_actionable boolean, pick_label integer,
    pick_winner_name text, uncertainty_reasons text[], decision_policy_version text
)
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF p IS NULL OR NOT (p >= 0 AND p <= 1) THEN
        RAISE EXCEPTION 'Probability must be finite and within [0, 1]';
    END IF;
    RETURN QUERY SELECT
        CASE WHEN p BETWEEN 0.40 AND 0.60 THEN 'no_pick' ELSE 'pick' END,
        NOT (p BETWEEN 0.40 AND 0.60),
        CASE WHEN p < 0.40 THEN 0 WHEN p > 0.60 THEN 1 ELSE NULL END,
        CASE WHEN p < 0.40 THEN f2 WHEN p > 0.60 THEN f1 ELSE NULL END,
        CASE WHEN p BETWEEN 0.40 AND 0.60 THEN ARRAY['probability_band'] ELSE ARRAY[]::text[] END,
        'probability_band_v1'::text;
END;
$$;

CREATE FUNCTION prediction_decision_matches(
    p double precision, f1 text, f2 text, status text, actionable boolean,
    label integer, winner text, reasons text[], version text, origin text
) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE d record;
BEGIN
    IF version IS NULL THEN
        RETURN status IS NULL AND actionable IS NULL AND label IS NULL
            AND winner IS NULL AND reasons IS NULL AND origin IS NULL AND p IS NULL;
    END IF;
    SELECT * INTO d FROM probability_band_v1(p, f1, f2);
    RETURN version = 'probability_band_v1'
        AND origin IS NOT NULL
        AND origin IN ('recorded_at_scoring', 'derived_at_review', 'derived_from_legacy_probability')
        AND ROW(status, actionable, label, winner, reasons)
            IS NOT DISTINCT FROM ROW(d.decision_status, d.is_actionable, d.pick_label,
                                     d.pick_winner_name, d.uncertainty_reasons);
END;
$$;

ALTER TABLE predictions
    ADD COLUMN predicted_prob_f1_full double precision,
    ADD COLUMN calibrated_prob_f1_full double precision,
    ADD COLUMN decision_status text,
    ADD COLUMN is_actionable boolean,
    ADD COLUMN pick_label integer,
    ADD COLUMN pick_winner_name text,
    ADD COLUMN uncertainty_reasons text[],
    ADD COLUMN decision_policy_version text,
    ADD COLUMN decision_origin text,
    ADD CONSTRAINT predictions_full_raw_probability CHECK (
        predicted_prob_f1_full IS NULL OR predicted_prob_f1_full BETWEEN 0 AND 1
    ),
    ADD CONSTRAINT predictions_decision_contract CHECK (prediction_decision_matches(
        calibrated_prob_f1_full, fighter_1_name, fighter_2_name, decision_status,
        is_actionable, pick_label, pick_winner_name, uncertainty_reasons,
        decision_policy_version, decision_origin
    ));

ALTER TABLE reviewed_prediction_fights
    ADD COLUMN predicted_prob_f1_full double precision,
    ADD COLUMN calibrated_prob_f1_full double precision,
    ADD COLUMN decision_status text,
    ADD COLUMN is_actionable boolean,
    ADD COLUMN pick_label integer,
    ADD COLUMN pick_winner_name text,
    ADD COLUMN uncertainty_reasons text[],
    ADD COLUMN decision_policy_version text,
    ADD COLUMN decision_origin text,
    ADD COLUMN pick_correct boolean,
    ADD CONSTRAINT reviewed_full_raw_probability CHECK (
        predicted_prob_f1_full IS NULL OR predicted_prob_f1_full BETWEEN 0 AND 1
    ),
    ADD CONSTRAINT reviewed_decision_contract CHECK (prediction_decision_matches(
        calibrated_prob_f1_full, fighter_1_name, fighter_2_name, decision_status,
        is_actionable, pick_label, pick_winner_name, uncertainty_reasons,
        decision_policy_version, decision_origin
    )),
    ADD CONSTRAINT reviewed_pick_correct CHECK (
        CASE WHEN decision_policy_version IS NULL THEN pick_correct IS NULL
             WHEN NOT is_actionable OR actual_label IS NULL THEN pick_correct IS NULL
             ELSE pick_correct IS NOT DISTINCT FROM (pick_label = actual_label) END
    );

ALTER TABLE reviewed_prediction_events
    ADD COLUMN decision_policy_version text,
    ADD COLUMN total_count integer,
    ADD COLUMN resolved_count integer,
    ADD COLUMN latent_correct_count integer,
    ADD COLUMN latent_accuracy numeric,
    ADD COLUMN actionable_count integer,
    ADD COLUMN actionable_resolved_count integer,
    ADD COLUMN actionable_correct_count integer,
    ADD COLUMN actionable_accuracy numeric,
    ADD COLUMN actionable_coverage numeric,
    ADD COLUMN no_pick_count integer,
    ADD COLUMN no_pick_share numeric,
    ADD COLUMN threshold_high_count integer,
    ADD COLUMN threshold_high_resolved_count integer,
    ADD COLUMN threshold_high_correct_count integer,
    ADD COLUMN threshold_high_accuracy numeric;

-- Retain complete coverage denominators from reviews of partially resolved
-- cards. Old fight views only include persisted resolved review rows; old
-- summary-only records cannot reconstruct decision counts and stay NULL.
CREATE VIEW reviewed_prediction_event_decisions AS
SELECT r.*,
    CASE WHEN decision_policy_version IS NOT NULL THEN 'derived_at_review'
         ELSE 'legacy_summary_without_decision_metadata' END AS decision_origin
FROM reviewed_prediction_events r;

CREATE VIEW prediction_decisions AS
SELECT p.fight_id, p.scored_at,
    COALESCE(p.predicted_prob_f1_full, p.predicted_prob_f1::double precision) AS predicted_prob_f1_full,
    COALESCE(p.calibrated_prob_f1_full, p.calibrated_prob_f1::double precision) AS calibrated_prob_f1_full,
    d.*,
    COALESCE(p.decision_origin, 'derived_from_legacy_probability') AS decision_origin
FROM predictions p
CROSS JOIN LATERAL probability_band_v1(
    COALESCE(p.calibrated_prob_f1_full, p.calibrated_prob_f1::double precision),
    p.fighter_1_name, p.fighter_2_name
) d;

CREATE VIEW latest_prediction_decisions AS
SELECT lp.*, d.predicted_prob_f1_full, d.calibrated_prob_f1_full,
    d.decision_status, d.is_actionable, d.pick_label, d.pick_winner_name,
    d.uncertainty_reasons, d.decision_policy_version, d.decision_origin
FROM latest_predictions lp JOIN prediction_decisions d USING (fight_id, scored_at);

CREATE VIEW current_event_prediction_decisions AS
SELECT cp.*, d.predicted_prob_f1_full, d.calibrated_prob_f1_full,
    d.decision_status, d.is_actionable, d.pick_label, d.pick_winner_name,
    d.uncertainty_reasons, d.decision_policy_version, d.decision_origin
FROM current_event_predictions cp JOIN prediction_decisions d USING (fight_id, scored_at);

CREATE VIEW pre_event_prediction_fight_decisions AS
SELECT pf.*, precision.predicted_prob_f1_full, precision.calibrated_prob_f1_full,
    d.*,
    COALESCE(CASE WHEN pf.pre_event_evidence = 'database_scored_at_before_event'
        THEN p.decision_origin ELSE r.decision_origin END,
        'derived_from_legacy_probability') AS decision_origin,
    CASE WHEN pf.actual_label IN (0, 1) AND d.is_actionable
        THEN d.pick_label = pf.actual_label ELSE NULL END AS pick_correct,
    (precision.calibrated_prob_f1_full >= 0.5)::integer AS latent_label_full,
    CASE WHEN pf.actual_label IN (0, 1)
        THEN (precision.calibrated_prob_f1_full >= 0.5)::integer = pf.actual_label
        ELSE NULL END AS latent_correct_full
FROM pre_event_prediction_fights pf
LEFT JOIN predictions p ON pf.pre_event_evidence = 'database_scored_at_before_event'
    AND p.fight_id = pf.fight_id AND p.scored_at = pf.scored_at
LEFT JOIN reviewed_prediction_fights r ON pf.pre_event_evidence <> 'database_scored_at_before_event'
    AND r.fight_id = pf.fight_id AND r.event_date = pf.event_date
    AND r.review_type = pf.pre_event_evidence
CROSS JOIN LATERAL (
    SELECT COALESCE(p.predicted_prob_f1_full, r.predicted_prob_f1_full,
                   pf.predicted_prob_f1::double precision) AS predicted_prob_f1_full,
           COALESCE(p.calibrated_prob_f1_full, r.calibrated_prob_f1_full,
                   pf.calibrated_prob_f1::double precision) AS calibrated_prob_f1_full
) precision
CROSS JOIN LATERAL probability_band_v1(
    precision.calibrated_prob_f1_full, pf.fighter_1_name, pf.fighter_2_name
) d;

-- All rows contribute coverage. Only labels 0/1 contribute accuracies and proper
-- scores. Keeping evidence in GROUP BY separates strict/catch-up/retroactive rows.
CREATE VIEW pre_event_prediction_event_decisions AS
SELECT event_id, event_name, event_date, model_name, pre_event_evidence,
    'probability_band_v1'::text AS decision_policy_version,
    count(*) AS total_count,
    count(actual_label) AS resolved_count,
    count(*) FILTER (WHERE latent_correct_full) AS latent_correct_count,
    avg(latent_correct_full::integer) AS latent_accuracy,
    count(*) FILTER (WHERE is_actionable) AS actionable_count,
    count(actual_label) FILTER (WHERE is_actionable) AS actionable_resolved_count,
    count(*) FILTER (WHERE pick_correct) AS actionable_correct_count,
    avg(pick_correct::integer) AS actionable_accuracy,
    avg(is_actionable::integer) AS actionable_coverage,
    count(*) FILTER (WHERE NOT is_actionable) AS no_pick_count,
    avg((NOT is_actionable)::integer) AS no_pick_share,
    count(*) FILTER (WHERE calibrated_prob_f1_full <= 0.30 OR calibrated_prob_f1_full >= 0.70) AS threshold_high_count,
    count(actual_label) FILTER (WHERE calibrated_prob_f1_full <= 0.30 OR calibrated_prob_f1_full >= 0.70) AS threshold_high_resolved_count,
    count(*) FILTER (WHERE latent_correct_full AND (calibrated_prob_f1_full <= 0.30 OR calibrated_prob_f1_full >= 0.70)) AS threshold_high_correct_count,
    avg(latent_correct_full::integer) FILTER (WHERE calibrated_prob_f1_full <= 0.30 OR calibrated_prob_f1_full >= 0.70) AS threshold_high_accuracy,
    avg(-actual_label * ln(LEAST(GREATEST(calibrated_prob_f1_full, 1e-15), 1 - 1e-15))
        - (1 - actual_label) * ln(1 - LEAST(GREATEST(calibrated_prob_f1_full, 1e-15), 1 - 1e-15))) AS log_loss,
    avg(power(calibrated_prob_f1_full - actual_label, 2)) AS brier_score
FROM pre_event_prediction_fight_decisions
GROUP BY event_id, event_name, event_date, model_name, pre_event_evidence;
