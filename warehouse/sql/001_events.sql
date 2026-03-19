-- Events table
-- One row per UFC event (completed or upcoming).
-- Source: data/events.csv + data/manifests/events_manifest.csv
-- Normalization rules: docs/normalization-rules.md

CREATE TABLE IF NOT EXISTS events (
    event_id    uuid        PRIMARY KEY,
    event_name  text        NOT NULL,
    event_date  date        NOT NULL,
    city        text,
    state       text,
    country     text,
    event_status text,               -- 'completed' | 'upcoming'
    source_url  text        NOT NULL,
    scraped_at  timestamptz
);
