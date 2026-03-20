-- Make fighter FK constraints on fights DEFERRABLE INITIALLY DEFERRED.
-- This allows loading fights before fighters within the same transaction,
-- and lets loaders run in any order without FK violations mid-batch.
-- winner_fighter_id was already deferrable from 005; this aligns the others.

ALTER TABLE fights
    DROP CONSTRAINT IF EXISTS fk_fights_fighter_1,
    DROP CONSTRAINT IF EXISTS fk_fights_fighter_2;

ALTER TABLE fights
    ADD CONSTRAINT fk_fights_fighter_1
        FOREIGN KEY (fighter_1_id) REFERENCES fighters (fighter_id)
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT fk_fights_fighter_2
        FOREIGN KEY (fighter_2_id) REFERENCES fighters (fighter_id)
        DEFERRABLE INITIALLY DEFERRED;
