-- Migration state schema for mssql-pg-migrate
-- Stores migration run history and per-table state for resume and incremental sync

-- Migration runs (overall state)
CREATE TABLE IF NOT EXISTS _mssql_pg_migrate.migration_runs (
    run_id TEXT PRIMARY KEY,
    config_hash TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Per-table state for each migration run
CREATE TABLE IF NOT EXISTS _mssql_pg_migrate.table_state (
    run_id TEXT NOT NULL REFERENCES _mssql_pg_migrate.migration_runs(run_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
    rows_total BIGINT NOT NULL DEFAULT 0,
    rows_transferred BIGINT NOT NULL DEFAULT 0,
    rows_skipped BIGINT NOT NULL DEFAULT 0,
    last_pk BIGINT,
    last_sync_timestamp TIMESTAMPTZ,  -- For date-based incremental sync
    completed_at TIMESTAMPTZ,
    error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, table_name)
);

-- Index for finding latest successful sync per table (for incremental sync)
CREATE INDEX IF NOT EXISTS idx_table_state_incremental_sync
    ON _mssql_pg_migrate.table_state(table_name, status, last_sync_timestamp)
    WHERE status = 'completed' AND last_sync_timestamp IS NOT NULL;

-- Index for finding latest run
CREATE INDEX IF NOT EXISTS idx_migration_runs_latest
    ON _mssql_pg_migrate.migration_runs(started_at DESC);

COMMENT ON TABLE _mssql_pg_migrate.migration_runs IS
    'Migration run history for mssql-pg-migrate. Each row represents one migration run.';

COMMENT ON TABLE _mssql_pg_migrate.table_state IS
    'Per-table state for each migration run. Used for crash recovery and incremental sync.';

COMMENT ON COLUMN _mssql_pg_migrate.table_state.last_sync_timestamp IS
    'High-water mark timestamp for date-based incremental sync. Set to run start time after successful completion.';
