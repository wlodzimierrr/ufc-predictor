-- Fighter career summary view for dashboards (Power BI, etc.)
-- Uses the LAST fighter_snapshot per fighter (post-fight, including the result
-- of the most recent fight) joined with fighter bio from the fighters table.

CREATE OR REPLACE VIEW fighter_career_summary AS
WITH latest_snapshot AS (
    SELECT DISTINCT ON (fs.fighter_id)
        fs.fighter_id,
        fs.fight_id,
        fs.as_of_date,
        fs.career_fights,
        fs.career_wins,
        fs.career_losses,
        fs.career_draws,
        fs.career_nc,
        fs.career_win_rate,
        fs.career_finish_rate,
        fs.career_ko_tko_wins,
        fs.career_sub_wins,
        fs.career_dec_wins,
        fs.career_ko_tko_losses,
        fs.career_sub_losses,
        fs.career_title_fights,
        fs.career_title_wins,
        fs.career_minutes,
        fs.career_sig_strikes_landed_pm,
        fs.career_sig_strikes_absorbed_pm,
        fs.career_sig_strike_accuracy,
        fs.career_sig_strike_defense,
        fs.career_takedown_accuracy,
        fs.career_takedown_defense,
        fs.career_sub_attempts_pm,
        fs.career_control_rate,
        fs.career_knockdowns_pm,
        fs.elo_rating,
        fs.win_rate_last3,
        fs.win_rate_last5,
        fs.sig_strikes_landed_pm_last3,
        fs.sig_strikes_landed_pm_last5,
        fs.takedown_accuracy_last3,
        fs.takedown_accuracy_last5,
        fs.control_rate_last3,
        fs.control_rate_last5,
        fs.streak_last5,
        fs.age,
        fs.is_orthodox,
        fs.is_southpaw,
        fs.days_since_last_fight
    FROM fighter_snapshots fs
    ORDER BY fs.fighter_id, fs.as_of_date DESC
),
-- Add the result of the latest fight to get true post-fight stats
latest_with_result AS (
    SELECT
        ls.*,
        -- Increment career stats with latest fight result
        ls.career_fights + 1                                         AS total_fights,
        ls.career_wins + CASE WHEN f.winner_fighter_id = ls.fighter_id
                               THEN 1 ELSE 0 END                    AS total_wins,
        ls.career_losses + CASE WHEN f.result_type = 'win'
                                 AND f.winner_fighter_id != ls.fighter_id
                               THEN 1 ELSE 0 END                    AS total_losses,
        ls.career_draws + CASE WHEN f.result_type = 'draw'
                               THEN 1 ELSE 0 END                    AS total_draws,
        -- Last fight info
        f.result_type                                                AS last_fight_result_type,
        CASE WHEN f.winner_fighter_id = ls.fighter_id THEN 'win'
             WHEN f.result_type = 'win' THEN 'loss'
             ELSE f.result_type END                                  AS last_fight_outcome,
        f.finish_method                                              AS last_fight_method,
        e.event_name                                                 AS last_event_name,
        e.event_date                                                 AS last_fight_date
    FROM latest_snapshot ls
    JOIN fights f ON f.fight_id = ls.fight_id
    JOIN events e ON e.event_id = f.event_id
)
SELECT
    lr.fighter_id,
    ft.full_name,
    ft.first_name,
    ft.last_name,
    ft.nickname,
    ft.height_cm,
    ft.weight_lbs,
    ft.reach_cm,
    ft.stance,
    ft.dob,
    lr.total_fights,
    lr.total_wins,
    lr.total_losses,
    lr.total_draws,
    lr.career_nc,
    CASE WHEN lr.total_fights > 0
         THEN round(lr.total_wins::numeric / lr.total_fights, 4)
         ELSE NULL END                                               AS win_rate,
    lr.career_finish_rate                                            AS finish_rate,
    lr.career_ko_tko_wins,
    lr.career_sub_wins,
    lr.career_dec_wins,
    lr.career_ko_tko_losses,
    lr.career_sub_losses,
    lr.career_title_fights,
    lr.career_title_wins,
    round(lr.career_minutes::numeric, 1)                             AS total_minutes,
    lr.career_sig_strikes_landed_pm                                  AS sig_strikes_landed_pm,
    lr.career_sig_strikes_absorbed_pm                                AS sig_strikes_absorbed_pm,
    lr.career_sig_strike_accuracy                                    AS sig_strike_accuracy,
    lr.career_sig_strike_defense                                     AS sig_strike_defense,
    lr.career_takedown_accuracy                                      AS takedown_accuracy,
    lr.career_takedown_defense                                       AS takedown_defense,
    lr.career_control_rate                                           AS control_rate,
    lr.career_knockdowns_pm                                          AS knockdowns_pm,
    lr.elo_rating,
    lr.win_rate_last3,
    lr.win_rate_last5,
    lr.sig_strikes_landed_pm_last3,
    lr.sig_strikes_landed_pm_last5,
    lr.streak_last5,
    lr.age,
    lr.days_since_last_fight,
    lr.last_fight_outcome,
    lr.last_fight_method,
    lr.last_event_name,
    lr.last_fight_date
FROM latest_with_result lr
JOIN fighters ft ON ft.fighter_id = lr.fighter_id;
