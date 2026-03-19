-- Fighters table
-- One row per fighter. Career record columns (wins/losses/draws/no_contests) are
-- intentionally excluded — they are current totals at scrape time and will be
-- computed per-fight in Phase 3 feature engineering.
-- Source: data/fighters.csv
-- Normalization rules: docs/normalization-rules.md

CREATE TABLE IF NOT EXISTS fighters (
    fighter_id  uuid            PRIMARY KEY,
    full_name   text            NOT NULL,
    first_name  text,
    last_name   text,                       -- from last_names in CSV
    nickname    text,
    height_cm   numeric(5, 2),
    weight_lbs  numeric(5, 1),
    reach_cm    numeric(5, 2),
    stance      text,
    dob         date,                       -- from dob_formatted in CSV
    source_url  text            NOT NULL,
    scraped_at  timestamptz
);
