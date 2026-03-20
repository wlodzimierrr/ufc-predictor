-- Widen numeric columns in feature tables that can exceed numeric(6,4) range.
-- control_rate columns store control_time_per_fight (seconds, up to 300+).
-- career_knockdowns_pm stores knockdowns_per_fight (small but could exceed 99).

-- fighter_snapshots
ALTER TABLE fighter_snapshots
    ALTER COLUMN career_control_rate TYPE numeric(8,4),
    ALTER COLUMN career_knockdowns_pm TYPE numeric(8,4),
    ALTER COLUMN control_rate_last1 TYPE numeric(8,4),
    ALTER COLUMN control_rate_last3 TYPE numeric(8,4),
    ALTER COLUMN control_rate_last5 TYPE numeric(8,4),
    ALTER COLUMN control_rate_decay TYPE numeric(8,4);

-- bout_features
ALTER TABLE bout_features
    ALTER COLUMN diff_career_control_rate TYPE numeric(8,4),
    ALTER COLUMN diff_control_rate_last3 TYPE numeric(8,4);
