/**
 * Ordered schema migrations for the controller's Postgres database.
 * Applied versions are tracked in d2v_schema_migrations; never edit an entry
 * after it has shipped — append a new one instead.
 */
export const MIGRATIONS: string[] = [
    // 1: initial schema
    `
    CREATE TABLE IF NOT EXISTS d2v_configs (
        id             SERIAL PRIMARY KEY,
        path           TEXT NOT NULL UNIQUE,
        name           TEXT NOT NULL,
        content        TEXT NOT NULL,
        content_hash   TEXT NOT NULL,
        schedule       TEXT,
        source_summary JSONB NOT NULL DEFAULT '[]',
        parse_error    TEXT,
        enabled        BOOLEAN NOT NULL DEFAULT TRUE,
        deleted_at     TIMESTAMPTZ,
        created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS d2v_runs (
        id          SERIAL PRIMARY KEY,
        config_id   INT NOT NULL REFERENCES d2v_configs(id),
        config_hash TEXT NOT NULL,
        trigger     TEXT NOT NULL CHECK (trigger IN ('scheduled','manual')),
        status      TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed','skipped','canceled')),
        pid         INT,
        exit_code   INT,
        error       TEXT,
        stats       JSONB NOT NULL DEFAULT '{}',
        queued_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        started_at  TIMESTAMPTZ,
        finished_at TIMESTAMPTZ
    );

    CREATE INDEX IF NOT EXISTS d2v_runs_config_queued_idx ON d2v_runs (config_id, queued_at DESC);
    CREATE INDEX IF NOT EXISTS d2v_runs_status_idx ON d2v_runs (status);

    CREATE TABLE IF NOT EXISTS d2v_run_logs (
        run_id  INT NOT NULL REFERENCES d2v_runs(id) ON DELETE CASCADE,
        seq     BIGINT NOT NULL,
        ts      TIMESTAMPTZ NOT NULL,
        level   TEXT NOT NULL,
        module  TEXT,
        message TEXT NOT NULL,
        PRIMARY KEY (run_id, seq)
    );
    `,
];
