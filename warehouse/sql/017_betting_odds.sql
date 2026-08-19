-- Betting odds storage and derived moneyline market views.
-- Raw observations are stored once per fight/fighter/bookmaker/line timestamp.
-- Derived probabilities are exposed through views so imported odds stay auditable.

DO $$ BEGIN
    ALTER TABLE fights ADD CONSTRAINT uq_fights_fight_event
        UNIQUE (fight_id, event_id);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


CREATE TABLE IF NOT EXISTS fight_odds (
    fight_id              uuid        NOT NULL REFERENCES fights (fight_id),
    event_id              uuid        NOT NULL REFERENCES events (event_id),
    fighter_id            uuid        NOT NULL REFERENCES fighters (fighter_id),
    opponent_fighter_id   uuid        NOT NULL REFERENCES fighters (fighter_id),
    bookmaker             text        NOT NULL,
    market                text        NOT NULL DEFAULT 'moneyline',
    line_type             text        NOT NULL DEFAULT 'unknown',
    odds_timestamp        timestamptz NOT NULL,
    american_odds         integer,
    decimal_odds          numeric(10,4),
    source                text        NOT NULL DEFAULT 'manual',
    source_url            text,
    imported_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        fight_id,
        fighter_id,
        bookmaker,
        market,
        line_type,
        odds_timestamp
    ),
    CONSTRAINT ck_fight_odds_any_odds
        CHECK (american_odds IS NOT NULL OR decimal_odds IS NOT NULL),
    CONSTRAINT ck_fight_odds_american_non_zero
        CHECK (american_odds IS NULL OR american_odds <> 0),
    CONSTRAINT ck_fight_odds_decimal_valid
        CHECK (decimal_odds IS NULL OR decimal_odds > 1.0),
    CONSTRAINT ck_fight_odds_market_moneyline
        CHECK (market = 'moneyline'),
    CONSTRAINT ck_fight_odds_line_type
        CHECK (line_type IN ('opening', 'current', 'closing', 'unknown')),
    CONSTRAINT ck_fight_odds_distinct_fighters
        CHECK (fighter_id <> opponent_fighter_id),
    CONSTRAINT ck_fight_odds_bookmaker_nonblank
        CHECK (length(btrim(bookmaker)) > 0),
    CONSTRAINT ck_fight_odds_source_nonblank
        CHECK (length(btrim(source)) > 0),
    CONSTRAINT fk_fight_odds_fight_event
        FOREIGN KEY (fight_id, event_id) REFERENCES fights (fight_id, event_id)
);


CREATE INDEX IF NOT EXISTS idx_fight_odds_fight_id
    ON fight_odds (fight_id);

CREATE INDEX IF NOT EXISTS idx_fight_odds_event_id
    ON fight_odds (event_id);

CREATE INDEX IF NOT EXISTS idx_fight_odds_fighter_id
    ON fight_odds (fighter_id);

CREATE INDEX IF NOT EXISTS idx_fight_odds_bookmaker_line_timestamp
    ON fight_odds (bookmaker, line_type, odds_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_fight_odds_market_group
    ON fight_odds (fight_id, bookmaker, market, line_type, odds_timestamp);

CREATE INDEX IF NOT EXISTS idx_fight_odds_current_lookup
    ON fight_odds (fight_id, fighter_id, bookmaker, odds_timestamp DESC)
    WHERE line_type = 'current';


CREATE OR REPLACE VIEW fight_odds_normalized AS
SELECT
    fo.fight_id,
    fo.event_id,
    fo.fighter_id,
    fo.opponent_fighter_id,
    fo.bookmaker,
    fo.market,
    fo.line_type,
    fo.odds_timestamp,
    fo.american_odds,
    fo.decimal_odds,
    CASE
        WHEN fo.decimal_odds IS NOT NULL THEN fo.decimal_odds
        WHEN fo.american_odds > 0 THEN 1 + (fo.american_odds::numeric / 100)
        ELSE 1 + (100::numeric / abs(fo.american_odds))
    END AS normalized_decimal_odds,
    1 / (
        CASE
            WHEN fo.decimal_odds IS NOT NULL THEN fo.decimal_odds
            WHEN fo.american_odds > 0 THEN 1 + (fo.american_odds::numeric / 100)
            ELSE 1 + (100::numeric / abs(fo.american_odds))
        END
    ) AS implied_probability,
    fo.source,
    fo.source_url,
    fo.imported_at
FROM fight_odds fo;


CREATE OR REPLACE VIEW latest_fight_odds AS
SELECT DISTINCT ON (fight_id, fighter_id, bookmaker, market)
    fight_id,
    event_id,
    fighter_id,
    opponent_fighter_id,
    bookmaker,
    market,
    line_type,
    odds_timestamp,
    american_odds,
    decimal_odds,
    normalized_decimal_odds,
    implied_probability,
    source,
    source_url,
    imported_at
FROM fight_odds_normalized
WHERE line_type = 'current'
ORDER BY
    fight_id,
    fighter_id,
    bookmaker,
    market,
    odds_timestamp DESC,
    imported_at DESC;


CREATE OR REPLACE VIEW fight_odds_no_vig AS
WITH market_groups AS (
    SELECT
        fight_id,
        event_id,
        bookmaker,
        market,
        line_type,
        odds_timestamp,
        count(*) AS side_count,
        count(DISTINCT fighter_id) AS distinct_fighter_count,
        sum(implied_probability) AS overround
    FROM fight_odds_normalized
    GROUP BY
        fight_id,
        event_id,
        bookmaker,
        market,
        line_type,
        odds_timestamp
    HAVING count(*) = 2
       AND count(DISTINCT fighter_id) = 2
)
SELECT
    fo.fight_id,
    fo.event_id,
    fo.fighter_id,
    fo.opponent_fighter_id,
    fo.bookmaker,
    fo.market,
    fo.line_type,
    fo.odds_timestamp,
    fo.american_odds,
    fo.decimal_odds,
    fo.normalized_decimal_odds,
    fo.implied_probability,
    mg.overround,
    fo.implied_probability / mg.overround AS no_vig_implied_probability,
    fo.source,
    fo.source_url,
    fo.imported_at
FROM fight_odds_normalized fo
JOIN market_groups mg
  ON mg.fight_id = fo.fight_id
 AND mg.event_id = fo.event_id
 AND mg.bookmaker = fo.bookmaker
 AND mg.market = fo.market
 AND mg.line_type = fo.line_type
 AND mg.odds_timestamp = fo.odds_timestamp
WHERE EXISTS (
    SELECT 1
    FROM fight_odds_normalized opp
    WHERE opp.fight_id = fo.fight_id
      AND opp.event_id = fo.event_id
      AND opp.bookmaker = fo.bookmaker
      AND opp.market = fo.market
      AND opp.line_type = fo.line_type
      AND opp.odds_timestamp = fo.odds_timestamp
      AND opp.fighter_id = fo.opponent_fighter_id
      AND opp.opponent_fighter_id = fo.fighter_id
);
