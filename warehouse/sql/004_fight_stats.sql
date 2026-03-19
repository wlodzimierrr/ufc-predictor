-- Fight stats tables
-- fight_stats_aggregate: one row per fighter per fight (2 rows per fight).
-- fight_stats_by_round:  one row per fighter per round per fight.
-- Both tables share the same stat columns; by_round adds `round`.
-- Source: data/fight_stats.csv, data/fight_stats_by_round.csv
-- Normalization rules: docs/normalization-rules.md

CREATE TABLE IF NOT EXISTS fight_stats_aggregate (
    fight_stat_id                   uuid        PRIMARY KEY,    -- from CSV
    fight_id                        uuid        NOT NULL REFERENCES fights (fight_id),
    fighter_id                      uuid        NOT NULL REFERENCES fighters (fighter_id),
    knockdowns                      smallint    NOT NULL DEFAULT 0,
    total_strikes_landed            smallint    NOT NULL DEFAULT 0,
    total_strikes_attempted         smallint    NOT NULL DEFAULT 0,
    sig_strikes_landed              smallint    NOT NULL DEFAULT 0,
    sig_strikes_attempted           smallint    NOT NULL DEFAULT 0,
    sig_strikes_head_landed         smallint    NOT NULL DEFAULT 0,
    sig_strikes_head_attempted      smallint    NOT NULL DEFAULT 0,
    sig_strikes_body_landed         smallint    NOT NULL DEFAULT 0,
    sig_strikes_body_attempted      smallint    NOT NULL DEFAULT 0,
    sig_strikes_leg_landed          smallint    NOT NULL DEFAULT 0,
    sig_strikes_leg_attempted       smallint    NOT NULL DEFAULT 0,
    sig_strikes_distance_landed     smallint    NOT NULL DEFAULT 0,
    sig_strikes_distance_attempted  smallint    NOT NULL DEFAULT 0,
    sig_strikes_clinch_landed       smallint    NOT NULL DEFAULT 0,
    sig_strikes_clinch_attempted    smallint    NOT NULL DEFAULT 0,
    sig_strikes_ground_landed       smallint    NOT NULL DEFAULT 0,
    sig_strikes_ground_attempted    smallint    NOT NULL DEFAULT 0,
    takedowns_landed                smallint    NOT NULL DEFAULT 0,
    takedowns_attempted             smallint    NOT NULL DEFAULT 0,
    control_time_seconds            smallint    NOT NULL DEFAULT 0, -- control_time_minutes*60 + control_time_seconds
    submissions_attempted           smallint    NOT NULL DEFAULT 0,
    reversals                       smallint    NOT NULL DEFAULT 0,
    source_url                      text,
    scraped_at                      timestamptz
);


CREATE TABLE IF NOT EXISTS fight_stats_by_round (
    fight_stat_by_round_id          uuid        PRIMARY KEY,    -- from CSV
    fight_id                        uuid        NOT NULL REFERENCES fights (fight_id),
    fighter_id                      uuid        NOT NULL REFERENCES fighters (fighter_id),
    round                           smallint    NOT NULL,
    knockdowns                      smallint    NOT NULL DEFAULT 0,
    total_strikes_landed            smallint    NOT NULL DEFAULT 0,
    total_strikes_attempted         smallint    NOT NULL DEFAULT 0,
    sig_strikes_landed              smallint    NOT NULL DEFAULT 0,
    sig_strikes_attempted           smallint    NOT NULL DEFAULT 0,
    sig_strikes_head_landed         smallint    NOT NULL DEFAULT 0,
    sig_strikes_head_attempted      smallint    NOT NULL DEFAULT 0,
    sig_strikes_body_landed         smallint    NOT NULL DEFAULT 0,
    sig_strikes_body_attempted      smallint    NOT NULL DEFAULT 0,
    sig_strikes_leg_landed          smallint    NOT NULL DEFAULT 0,
    sig_strikes_leg_attempted       smallint    NOT NULL DEFAULT 0,
    sig_strikes_distance_landed     smallint    NOT NULL DEFAULT 0,
    sig_strikes_distance_attempted  smallint    NOT NULL DEFAULT 0,
    sig_strikes_clinch_landed       smallint    NOT NULL DEFAULT 0,
    sig_strikes_clinch_attempted    smallint    NOT NULL DEFAULT 0,
    sig_strikes_ground_landed       smallint    NOT NULL DEFAULT 0,
    sig_strikes_ground_attempted    smallint    NOT NULL DEFAULT 0,
    takedowns_landed                smallint    NOT NULL DEFAULT 0,
    takedowns_attempted             smallint    NOT NULL DEFAULT 0,
    control_time_seconds            smallint    NOT NULL DEFAULT 0, -- control_time_minutes*60 + control_time_seconds
    submissions_attempted           smallint    NOT NULL DEFAULT 0,
    reversals                       smallint    NOT NULL DEFAULT 0,
    source_url                      text,
    scraped_at                      timestamptz
);
