-- Foreign key constraints and indexes
-- FKs from fights to fighters are added here (deferred from 002_fights.sql
-- because fighters table did not exist yet at that point).
-- Each ADD CONSTRAINT is wrapped in a DO block so re-running is a no-op.

-- Foreign keys: fights → fighters
DO $$ BEGIN
    ALTER TABLE fights ADD CONSTRAINT fk_fights_fighter_1
        FOREIGN KEY (fighter_1_id) REFERENCES fighters (fighter_id);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE fights ADD CONSTRAINT fk_fights_fighter_2
        FOREIGN KEY (fighter_2_id) REFERENCES fighters (fighter_id);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE fights ADD CONSTRAINT fk_fights_winner
        FOREIGN KEY (winner_fighter_id) REFERENCES fighters (fighter_id)
        DEFERRABLE INITIALLY DEFERRED;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Indexes: fights
CREATE INDEX IF NOT EXISTS idx_fights_event_id      ON fights (event_id);
CREATE INDEX IF NOT EXISTS idx_fights_fighter_1_id  ON fights (fighter_1_id);
CREATE INDEX IF NOT EXISTS idx_fights_fighter_2_id  ON fights (fighter_2_id);

-- Indexes: fight_stats_aggregate
CREATE INDEX IF NOT EXISTS idx_fsa_fight_id    ON fight_stats_aggregate (fight_id);
CREATE INDEX IF NOT EXISTS idx_fsa_fighter_id  ON fight_stats_aggregate (fighter_id);

-- Indexes: fight_stats_by_round
CREATE INDEX IF NOT EXISTS idx_fsbr_fight_fighter_round
    ON fight_stats_by_round (fight_id, fighter_id, round);
