-- Operator-applied migration. The scheduled refresh never applies migrations.
CREATE TABLE IF NOT EXISTS automation_refresh_runs (
    run_id uuid PRIMARY KEY,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    image_revision text NOT NULL,
    git_revision text NOT NULL,
    command_version text NOT NULL,
    exit_code integer,
    counts jsonb NOT NULL DEFAULT '{}'::jsonb,
    errors jsonb NOT NULL DEFAULT '[]'::jsonb,
    disabled_scrapers jsonb NOT NULL DEFAULT '[]'::jsonb,
    CHECK ((status = 'running' AND finished_at IS NULL AND exit_code IS NULL)
        OR (status <> 'running' AND finished_at IS NOT NULL AND exit_code IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_automation_refresh_runs_started_at
    ON automation_refresh_runs (started_at DESC);
