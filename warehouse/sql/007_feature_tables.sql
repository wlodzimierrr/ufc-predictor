-- Feature tables for Phase 3 Feature Engineering.
-- fighter_snapshots: one row per fighter per fight they participate in.
-- bout_features:     one row per fight, model-ready with difference/ratio/matchup features.
-- Column list derived from docs/feature-catalog.md.

-- ── fighter_snapshots ────────────────────────────────────────────────────────
-- One row per (fighter, fight) pair, computed with cutoff = that fight's event_date.
-- Contains all pre-fight features for one fighter only; the bout pipeline joins two rows.

CREATE TABLE IF NOT EXISTS fighter_snapshots (
    fighter_id                          uuid        NOT NULL REFERENCES fighters (fighter_id),
    fight_id                            uuid        NOT NULL REFERENCES fights (fight_id),
    as_of_date                          date        NOT NULL,   -- event_date of the fight (cutoff)
    feature_version                     smallint    NOT NULL DEFAULT 1,

    -- ── Career aggregates ────────────────────────────────────────────────────
    career_fights                       smallint    NOT NULL DEFAULT 0,
    career_wins                         smallint    NOT NULL DEFAULT 0,
    career_losses                       smallint    NOT NULL DEFAULT 0,
    career_draws                        smallint    NOT NULL DEFAULT 0,
    career_nc                           smallint    NOT NULL DEFAULT 0,
    career_win_rate                     numeric(6,4),
    career_finish_rate                  numeric(6,4),
    career_ko_tko_wins                  smallint    NOT NULL DEFAULT 0,
    career_sub_wins                     smallint    NOT NULL DEFAULT 0,
    career_dec_wins                     smallint    NOT NULL DEFAULT 0,
    career_ko_tko_losses                smallint    NOT NULL DEFAULT 0,
    career_sub_losses                   smallint    NOT NULL DEFAULT 0,
    career_title_fights                 smallint    NOT NULL DEFAULT 0,
    career_title_wins                   smallint    NOT NULL DEFAULT 0,
    career_minutes                      numeric(8,2) NOT NULL DEFAULT 0,
    career_sig_strikes_landed_pm        numeric(8,4),
    career_sig_strikes_absorbed_pm      numeric(8,4),
    career_sig_strike_accuracy          numeric(6,4),
    career_sig_strike_defense           numeric(6,4),
    career_takedown_accuracy            numeric(6,4),
    career_takedown_defense             numeric(6,4),
    career_sub_attempts_pm              numeric(8,4),
    career_control_rate                 numeric(6,4),
    career_knockdowns_pm                numeric(8,4),

    -- ── Rolling window — last 1 fight ────────────────────────────────────────
    win_rate_last1                      numeric(6,4),
    finish_rate_last1                   numeric(6,4),
    sig_strikes_landed_pm_last1         numeric(8,4),
    sig_strikes_absorbed_pm_last1       numeric(8,4),
    sig_strike_accuracy_last1           numeric(6,4),
    sig_strike_defense_last1            numeric(6,4),
    takedown_landed_pm_last1            numeric(8,4),
    takedown_accuracy_last1             numeric(6,4),
    takedown_defense_last1              numeric(6,4),
    control_rate_last1                  numeric(6,4),
    knockdowns_pm_last1                 numeric(8,4),
    knockdowns_absorbed_pm_last1        numeric(8,4),
    sub_attempts_pm_last1               numeric(8,4),
    avg_fight_time_last1                numeric(6,2),
    streak_last1                        smallint,

    -- ── Rolling window — last 3 fights ───────────────────────────────────────
    win_rate_last3                      numeric(6,4),
    finish_rate_last3                   numeric(6,4),
    sig_strikes_landed_pm_last3         numeric(8,4),
    sig_strikes_absorbed_pm_last3       numeric(8,4),
    sig_strike_accuracy_last3           numeric(6,4),
    sig_strike_defense_last3            numeric(6,4),
    takedown_landed_pm_last3            numeric(8,4),
    takedown_accuracy_last3             numeric(6,4),
    takedown_defense_last3              numeric(6,4),
    control_rate_last3                  numeric(6,4),
    knockdowns_pm_last3                 numeric(8,4),
    knockdowns_absorbed_pm_last3        numeric(8,4),
    sub_attempts_pm_last3               numeric(8,4),
    avg_fight_time_last3                numeric(6,2),
    streak_last3                        smallint,

    -- ── Rolling window — last 5 fights ───────────────────────────────────────
    win_rate_last5                      numeric(6,4),
    finish_rate_last5                   numeric(6,4),
    sig_strikes_landed_pm_last5         numeric(8,4),
    sig_strikes_absorbed_pm_last5       numeric(8,4),
    sig_strike_accuracy_last5           numeric(6,4),
    sig_strike_defense_last5            numeric(6,4),
    takedown_landed_pm_last5            numeric(8,4),
    takedown_accuracy_last5             numeric(6,4),
    takedown_defense_last5              numeric(6,4),
    control_rate_last5                  numeric(6,4),
    knockdowns_pm_last5                 numeric(8,4),
    knockdowns_absorbed_pm_last5        numeric(8,4),
    sub_attempts_pm_last5               numeric(8,4),
    avg_fight_time_last5                numeric(6,2),
    streak_last5                        smallint,

    -- ── Exponentially decayed metrics (alpha = 0.85) ─────────────────────────
    sig_strikes_landed_pm_decay         numeric(8,4),
    sig_strikes_absorbed_pm_decay       numeric(8,4),
    sig_strike_accuracy_decay           numeric(6,4),
    sig_strike_defense_decay            numeric(6,4),
    takedown_landed_pm_decay            numeric(8,4),
    takedown_accuracy_decay             numeric(6,4),
    takedown_defense_decay              numeric(6,4),
    control_rate_decay                  numeric(6,4),
    knockdowns_pm_decay                 numeric(8,4),
    win_rate_decay                      numeric(6,4),

    -- ── Physical / demographic / activity ────────────────────────────────────
    age                                 numeric(5,2),
    age_squared                         numeric(8,2),
    height_cm                           numeric(5,2),
    reach_cm                            numeric(5,2),
    reach_to_height                     numeric(6,4),
    is_orthodox                         boolean     NOT NULL DEFAULT false,
    is_southpaw                         boolean     NOT NULL DEFAULT false,
    days_since_last_fight               smallint,
    is_long_layoff                      boolean     NOT NULL DEFAULT false,
    is_short_notice                     boolean     NOT NULL DEFAULT false,
    is_debut                            boolean     NOT NULL DEFAULT false,

    -- Missingness indicator flags
    age_missing                         boolean     NOT NULL DEFAULT false,
    height_reach_missing                boolean     NOT NULL DEFAULT false,

    -- ── Elo and opponent-adjusted ─────────────────────────────────────────────
    elo_rating                          numeric(8,2) NOT NULL DEFAULT 1500,
    elo_opponent                        numeric(8,2),
    elo_diff                            numeric(8,2),
    opp_avg_elo                         numeric(8,2),
    opp_adj_sig_strike_accuracy         numeric(8,4),

    computed_at                         timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (fighter_id, fight_id)
);

CREATE INDEX IF NOT EXISTS idx_fighter_snapshots_fighter_id
    ON fighter_snapshots (fighter_id);

CREATE INDEX IF NOT EXISTS idx_fighter_snapshots_fight_id
    ON fighter_snapshots (fight_id);


-- ── bout_features ─────────────────────────────────────────────────────────────
-- One row per fight. Model-ready: difference and ratio features from two snapshots,
-- plus fight metadata and the training label.

CREATE TABLE IF NOT EXISTS bout_features (
    fight_id                            uuid        PRIMARY KEY REFERENCES fights (fight_id),
    fighter_1_id                        uuid        NOT NULL REFERENCES fighters (fighter_id),
    fighter_2_id                        uuid        NOT NULL REFERENCES fighters (fighter_id),
    event_date                          date        NOT NULL,
    weight_class                        text,
    is_title_fight                      boolean     NOT NULL DEFAULT false,
    scheduled_rounds                    smallint,

    -- label: 1 = fighter_1 wins, 0 = fighter_2 wins, NULL = draw or NC
    label                               smallint,

    feature_version                     smallint    NOT NULL DEFAULT 1,

    -- ── Difference features (fighter_1 - fighter_2) ───────────────────────────
    diff_elo                            numeric(8,2),
    diff_career_wins                    smallint,
    diff_career_fights                  smallint,
    diff_career_win_rate                numeric(6,4),
    diff_career_finish_rate             numeric(6,4),
    diff_career_sig_strikes_landed_pm   numeric(8,4),
    diff_career_sig_strike_accuracy     numeric(6,4),
    diff_career_takedown_accuracy       numeric(6,4),
    diff_career_control_rate            numeric(6,4),
    diff_age                            numeric(5,2),
    diff_height_cm                      numeric(5,2),
    diff_reach_cm                       numeric(5,2),
    diff_days_since_last_fight          smallint,
    diff_win_rate_last3                 numeric(6,4),
    diff_sig_strikes_landed_pm_last3    numeric(8,4),
    diff_takedown_accuracy_last3        numeric(6,4),
    diff_control_rate_last3             numeric(6,4),
    diff_sig_strikes_landed_pm_decay    numeric(8,4),
    diff_win_rate_decay                 numeric(6,4),
    diff_opp_avg_elo                    numeric(8,2),

    -- ── Ratio features (fighter_1 / (fighter_1 + fighter_2)) ─────────────────
    ratio_career_wins                   numeric(6,4),
    ratio_career_fights                 numeric(6,4),
    ratio_career_sig_strikes_landed_pm  numeric(6,4),
    ratio_career_control_rate           numeric(6,4),
    ratio_elo                           numeric(6,4),

    -- ── Matchup / metadata ────────────────────────────────────────────────────
    is_orthodox_vs_southpaw             boolean     NOT NULL DEFAULT false,
    both_debuting                       boolean     NOT NULL DEFAULT false,

    computed_at                         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bout_features_event_date
    ON bout_features (event_date);

CREATE INDEX IF NOT EXISTS idx_bout_features_weight_class
    ON bout_features (weight_class);
