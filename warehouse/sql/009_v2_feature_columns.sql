-- Phase 5 T5.1.1: Add v2 feature columns to fighter_snapshots and bout_features.
-- These columns support the expanded feature set (feature_version=2).

-- ── fighter_snapshots: trend features and activity ──────────────────────────

ALTER TABLE fighter_snapshots
    ADD COLUMN IF NOT EXISTS slope_sig_strikes_last5   numeric(8,4),
    ADD COLUMN IF NOT EXISTS slope_td_accuracy_last5   numeric(8,4),
    ADD COLUMN IF NOT EXISTS slope_control_rate_last5   numeric(8,4),
    ADD COLUMN IF NOT EXISTS std_sig_strikes_last5     numeric(8,4),
    ADD COLUMN IF NOT EXISTS std_td_accuracy_last5     numeric(8,4),
    ADD COLUMN IF NOT EXISTS fights_per_year_last3     numeric(6,2);


-- ── bout_features: wired diffs, trend diffs, stance detail, weight rank ────

ALTER TABLE bout_features
    -- Wired from existing career.py / physical.py fields
    ADD COLUMN IF NOT EXISTS diff_career_ko_rate               numeric(6,4),
    ADD COLUMN IF NOT EXISTS diff_career_sub_rate              numeric(6,4),
    ADD COLUMN IF NOT EXISTS diff_career_decision_rate         numeric(6,4),
    ADD COLUMN IF NOT EXISTS diff_career_sig_strikes_absorbed_pm numeric(8,4),
    ADD COLUMN IF NOT EXISTS diff_career_sig_strike_defense    numeric(6,4),
    ADD COLUMN IF NOT EXISTS diff_career_takedown_defense      numeric(6,4),
    ADD COLUMN IF NOT EXISTS diff_title_fight_count            smallint,
    ADD COLUMN IF NOT EXISTS diff_five_round_fights            smallint,
    ADD COLUMN IF NOT EXISTS diff_reach_height_ratio           numeric(6,4),
    ADD COLUMN IF NOT EXISTS diff_fights_per_year_last3        numeric(6,2),

    -- Trend diffs (slope and std over last 5 fights)
    ADD COLUMN IF NOT EXISTS diff_slope_sig_strikes_last5      numeric(8,4),
    ADD COLUMN IF NOT EXISTS diff_slope_td_accuracy_last5      numeric(8,4),
    ADD COLUMN IF NOT EXISTS diff_slope_control_rate_last5     numeric(8,4),
    ADD COLUMN IF NOT EXISTS diff_std_sig_strikes_last5        numeric(8,4),
    ADD COLUMN IF NOT EXISTS diff_std_td_accuracy_last5        numeric(8,4),

    -- Stance detail flags
    ADD COLUMN IF NOT EXISTS f1_is_southpaw                    boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS f2_is_southpaw                    boolean NOT NULL DEFAULT false,

    -- Weight class ordinal encoding
    ADD COLUMN IF NOT EXISTS weight_class_rank                 smallint;
