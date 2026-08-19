-- Betting report storage for dashboard consumption.
--
-- Backtest CSVs are generated artifacts in the data repo. These tables persist
-- the latest selected report outputs so deployed dashboard containers can read
-- them from Postgres instead of relying on a local filesystem mount.

CREATE TABLE IF NOT EXISTS betting_report_summaries (
    report_key                      text        NOT NULL,
    label                           text        NOT NULL,
    source                          text        NOT NULL,
    summary_type                    text        NOT NULL,
    group_name                      text        NOT NULL,
    total_bets                      integer     NOT NULL DEFAULT 0,
    wins                            integer     NOT NULL DEFAULT 0,
    losses                          integer     NOT NULL DEFAULT 0,
    pushes                          integer     NOT NULL DEFAULT 0,
    total_staked                    numeric     NOT NULL DEFAULT 0,
    profit_loss                     numeric     NOT NULL DEFAULT 0,
    roi                             numeric,
    hit_rate                        numeric,
    average_odds                    numeric,
    max_drawdown                    numeric     NOT NULL DEFAULT 0,
    starting_bankroll               numeric     NOT NULL DEFAULT 0,
    ending_bankroll                 numeric     NOT NULL DEFAULT 0,
    odds_policy                     text        NOT NULL DEFAULT '',
    require_odds_before_prediction  boolean     NOT NULL DEFAULT true,
    max_one_bet_per_fight           boolean     NOT NULL DEFAULT true,
    kelly_fraction                  numeric     NOT NULL DEFAULT 0,
    min_edge                        numeric     NOT NULL DEFAULT 0,
    min_ev                          numeric     NOT NULL DEFAULT 0,
    max_single_bet_fraction         numeric     NOT NULL DEFAULT 0,
    max_event_fraction              numeric     NOT NULL DEFAULT 0,
    medium_tier_cap                 numeric,
    high_tier_cap                   numeric,
    toss_up_tier_cap                numeric,
    drawdown_protection_threshold   numeric,
    report_generated_at             timestamptz NOT NULL DEFAULT now(),
    imported_at                     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_betting_report_summaries
        PRIMARY KEY (report_key, summary_type, group_name),
    CONSTRAINT ck_betting_report_summary_source
        CHECK (source IN ('honest', 'research'))
);

CREATE TABLE IF NOT EXISTS betting_report_fights (
    report_key                      text        NOT NULL,
    label                           text        NOT NULL,
    source                          text        NOT NULL,
    row_number                      integer     NOT NULL,
    event_id                        text        NOT NULL DEFAULT '',
    event_name                      text        NOT NULL DEFAULT '',
    event_date                      date,
    fight_id                        text        NOT NULL DEFAULT '',
    fighter_id                      text        NOT NULL DEFAULT '',
    fighter_name                    text        NOT NULL DEFAULT '',
    opponent_fighter_id             text        NOT NULL DEFAULT '',
    opponent_fighter_name           text        NOT NULL DEFAULT '',
    bookmaker                       text        NOT NULL DEFAULT '',
    market                          text        NOT NULL DEFAULT '',
    line_type                       text        NOT NULL DEFAULT '',
    odds_timestamp                  timestamptz,
    scored_at                       timestamptz,
    model_probability               numeric     NOT NULL DEFAULT 0,
    market_implied_probability      numeric     NOT NULL DEFAULT 0,
    no_vig_market_probability       numeric     NOT NULL DEFAULT 0,
    edge                            numeric     NOT NULL DEFAULT 0,
    edge_bucket                     text        NOT NULL DEFAULT '',
    ev_per_unit                     numeric     NOT NULL DEFAULT 0,
    offered_decimal_odds            numeric     NOT NULL DEFAULT 0,
    decision                        text        NOT NULL DEFAULT '',
    recommended_fighter_id          text        NOT NULL DEFAULT '',
    recommended_fighter_name        text        NOT NULL DEFAULT '',
    confidence_tier                 text        NOT NULL DEFAULT '',
    reason_codes                    text        NOT NULL DEFAULT '',
    full_kelly_fraction             numeric     NOT NULL DEFAULT 0,
    fractional_kelly_fraction       numeric     NOT NULL DEFAULT 0,
    final_stake_fraction            numeric     NOT NULL DEFAULT 0,
    stake_amount                    numeric     NOT NULL DEFAULT 0,
    bet_result                      text        NOT NULL DEFAULT '',
    profit_loss_amount              numeric     NOT NULL DEFAULT 0,
    bankroll_before_event           numeric     NOT NULL DEFAULT 0,
    bankroll_after_event            numeric     NOT NULL DEFAULT 0,
    peak_bankroll                   numeric     NOT NULL DEFAULT 0,
    drawdown                        numeric     NOT NULL DEFAULT 0,
    max_drawdown                    numeric     NOT NULL DEFAULT 0,
    actual_winner_fighter_id        text        NOT NULL DEFAULT '',
    actual_winner_name              text        NOT NULL DEFAULT '',
    result_type                     text        NOT NULL DEFAULT '',
    resolved                        boolean     NOT NULL DEFAULT false,
    detail_mode                     text        NOT NULL DEFAULT '',
    odds_policy                     text        NOT NULL DEFAULT '',
    require_odds_before_prediction  boolean     NOT NULL DEFAULT true,
    max_one_bet_per_fight           boolean     NOT NULL DEFAULT true,
    starting_bankroll               numeric     NOT NULL DEFAULT 0,
    ending_bankroll                 numeric     NOT NULL DEFAULT 0,
    kelly_fraction                  numeric     NOT NULL DEFAULT 0,
    min_edge                        numeric     NOT NULL DEFAULT 0,
    min_ev                          numeric     NOT NULL DEFAULT 0,
    max_single_bet_fraction         numeric     NOT NULL DEFAULT 0,
    max_event_fraction              numeric     NOT NULL DEFAULT 0,
    medium_tier_cap                 numeric,
    high_tier_cap                   numeric,
    toss_up_tier_cap                numeric,
    drawdown_protection_threshold   numeric,
    report_generated_at             timestamptz NOT NULL DEFAULT now(),
    imported_at                     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_betting_report_fights
        PRIMARY KEY (report_key, row_number),
    CONSTRAINT ck_betting_report_fight_source
        CHECK (source IN ('honest', 'research'))
);

CREATE INDEX IF NOT EXISTS idx_betting_report_fights_report_decision
    ON betting_report_fights (report_key, decision);

CREATE INDEX IF NOT EXISTS idx_betting_report_fights_event_date
    ON betting_report_fights (event_date DESC);

CREATE INDEX IF NOT EXISTS idx_betting_report_fights_open
    ON betting_report_fights (report_key, event_date)
    WHERE resolved = false OR bet_result NOT IN ('win', 'loss', 'push');
