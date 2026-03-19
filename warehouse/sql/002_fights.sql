-- Fights table
-- One row per bout. fighter_1/fighter_2 FKs to fighters added in 005_constraints_and_indexes.sql
-- once the fighters table exists.
-- Source: data/fights.csv
-- Normalization rules: docs/normalization-rules.md

CREATE TABLE IF NOT EXISTS fights (
    fight_id            uuid        PRIMARY KEY,
    event_id            uuid        NOT NULL REFERENCES events (event_id),
    fighter_1_id        uuid        NOT NULL,           -- FK added in 005_constraints_and_indexes.sql
    fighter_2_id        uuid        NOT NULL,           -- FK added in 005_constraints_and_indexes.sql
    winner_fighter_id   uuid,                           -- NULL for draws and NC
    result_type         text        NOT NULL,           -- 'win' | 'draw' | 'nc'
    weight_class        text,                           -- NULL for ~20 early UFC tournament bouts
    is_title_fight      boolean     NOT NULL DEFAULT false,
    is_interim_title    boolean     NOT NULL DEFAULT false,
    scheduled_rounds    smallint,
    finish_method       text,                           -- 'decision' | 'ko_tko' | 'submission' |
                                                        -- 'doctor_stoppage' | 'overturned' |
                                                        -- 'could_not_continue' | 'dq' | 'other'
    finish_detail       text,                           -- secondary_finish_method, free text
    finish_round        smallint,
    finish_time_seconds smallint,                       -- finish_time_minute * 60 + finish_time_second
    referee             text,
    source_url          text        NOT NULL,
    scraped_at          timestamptz
);
