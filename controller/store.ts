import { Pool } from 'pg';
import { Logger } from '../logger';
import { MIGRATIONS } from './migrations';
import {
    ConfigRecord,
    LogRow,
    RequestedSource,
    RunRecord,
    RunStatsPayload,
    RunStatus,
    RunTrigger,
    SourceSummary
} from './types';

export interface LogFilter {
    /** Only these levels; empty/undefined means every level. */
    levels?: string[];
    /** Case-insensitive substring match on message or module. */
    keyword?: string;
}

/** Neutralise LIKE wildcards so a keyword search stays a literal substring match. */
function escapeLike(value: string): string {
    return value.replace(/[\\%_]/g, ch => `\\${ch}`);
}

export interface UpsertConfigInput {
    path: string;
    name: string;
    content: string;
    contentHash: string;
    schedule: string | null;
    sourceSummary: SourceSummary[];
    parseError: string | null;
}

/**
 * All Postgres access for controller mode: configs, runs, and run logs.
 */
export class ControllerStore {
    private pool: Pool;
    private logger: Logger;

    constructor(databaseUrl: string, logger: Logger) {
        this.pool = new Pool({ connectionString: databaseUrl, max: 10 });
        this.logger = logger;
    }

    async init(): Promise<void> {
        await this.migrate();
        await this.markOrphanedRuns();
    }

    async close(): Promise<void> {
        await this.pool.end();
    }

    private async migrate(): Promise<void> {
        const client = await this.pool.connect();
        try {
            await client.query(`CREATE TABLE IF NOT EXISTS d2v_schema_migrations (
                version INT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )`);
            const { rows } = await client.query('SELECT COALESCE(MAX(version), 0) AS version FROM d2v_schema_migrations');
            const current = Number(rows[0].version);
            for (let i = current; i < MIGRATIONS.length; i++) {
                const version = i + 1;
                this.logger.info(`Applying migration ${version}`);
                await client.query('BEGIN');
                try {
                    await client.query(MIGRATIONS[i]);
                    await client.query('INSERT INTO d2v_schema_migrations (version) VALUES ($1)', [version]);
                    await client.query('COMMIT');
                } catch (err) {
                    await client.query('ROLLBACK');
                    throw err;
                }
            }
        } finally {
            client.release();
        }
    }

    /** Runs left 'queued'/'running' by a previous controller process can never finish — fail them. */
    private async markOrphanedRuns(): Promise<void> {
        const { rowCount } = await this.pool.query(
            `UPDATE d2v_runs
             SET status = 'failed', error = 'controller restarted while run was in progress', finished_at = NOW()
             WHERE status IN ('queued', 'running')`
        );
        if (rowCount) {
            this.logger.warn(`Marked ${rowCount} orphaned run(s) from a previous controller process as failed`);
        }
    }

    // ------------------------------------------------------------------ configs

    async upsertConfig(input: UpsertConfigInput): Promise<ConfigRecord> {
        const { rows } = await this.pool.query(
            `INSERT INTO d2v_configs (path, name, content, content_hash, schedule, source_summary, parse_error)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (path) DO UPDATE SET
                name = EXCLUDED.name,
                content = EXCLUDED.content,
                content_hash = EXCLUDED.content_hash,
                schedule = EXCLUDED.schedule,
                source_summary = EXCLUDED.source_summary,
                parse_error = EXCLUDED.parse_error,
                deleted_at = NULL,
                updated_at = NOW()
             RETURNING *`,
            [input.path, input.name, input.content, input.contentHash, input.schedule,
             JSON.stringify(input.sourceSummary), input.parseError]
        );
        return rows[0];
    }

    async markConfigDeleted(path: string): Promise<void> {
        await this.pool.query('UPDATE d2v_configs SET deleted_at = NOW(), updated_at = NOW() WHERE path = $1', [path]);
    }

    async listConfigs(includeDeleted = false): Promise<ConfigRecord[]> {
        const { rows } = await this.pool.query(
            `SELECT * FROM d2v_configs ${includeDeleted ? '' : 'WHERE deleted_at IS NULL'} ORDER BY name`
        );
        return rows;
    }

    async getConfig(id: number): Promise<ConfigRecord | null> {
        const { rows } = await this.pool.query('SELECT * FROM d2v_configs WHERE id = $1', [id]);
        return rows[0] ?? null;
    }

    // ------------------------------------------------------------------ runs

    async createRun(configId: number, configHash: string, trigger: RunTrigger, status: RunStatus, error?: string, requestedSources?: RequestedSource[]): Promise<RunRecord> {
        const terminal = status === 'skipped';
        const { rows } = await this.pool.query(
            `INSERT INTO d2v_runs (config_id, config_hash, trigger, status, error, requested_sources, finished_at)
             VALUES ($1, $2, $3, $4, $5, $6, ${terminal ? 'NOW()' : 'NULL'})
             RETURNING *`,
            [configId, configHash, trigger, status, error ?? null, requestedSources?.length ? JSON.stringify(requestedSources) : null]
        );
        return rows[0];
    }

    async markRunStarted(runId: number, pid: number | undefined): Promise<RunRecord> {
        const { rows } = await this.pool.query(
            `UPDATE d2v_runs SET status = 'running', pid = $2, started_at = NOW() WHERE id = $1 RETURNING *`,
            [runId, pid ?? null]
        );
        return rows[0];
    }

    async finishRun(runId: number, outcome: { status: RunStatus; exitCode?: number | null; error?: string | null; stats?: RunStatsPayload }): Promise<RunRecord> {
        const { rows } = await this.pool.query(
            `UPDATE d2v_runs
             SET status = $2, exit_code = $3, error = $4, stats = $5, finished_at = NOW()
             WHERE id = $1 RETURNING *`,
            [runId, outcome.status, outcome.exitCode ?? null, outcome.error ?? null, JSON.stringify(outcome.stats ?? {})]
        );
        return rows[0];
    }

    async getRun(runId: number): Promise<RunRecord | null> {
        const { rows } = await this.pool.query('SELECT * FROM d2v_runs WHERE id = $1', [runId]);
        return rows[0] ?? null;
    }

    async listRuns(filter: { configId?: number; status?: RunStatus; limit?: number; before?: number } = {}): Promise<RunRecord[]> {
        const conditions: string[] = [];
        const params: any[] = [];
        if (filter.configId !== undefined) {
            params.push(filter.configId);
            conditions.push(`config_id = $${params.length}`);
        }
        if (filter.status !== undefined) {
            params.push(filter.status);
            conditions.push(`status = $${params.length}`);
        }
        if (filter.before !== undefined) {
            params.push(filter.before);
            conditions.push(`id < $${params.length}`);
        }
        params.push(Math.min(filter.limit ?? 50, 500));
        const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
        const { rows } = await this.pool.query(
            `SELECT * FROM d2v_runs ${where} ORDER BY id DESC LIMIT $${params.length}`,
            params
        );
        return rows;
    }

    /** Latest run per config, for the dashboard list. */
    async getLastRuns(): Promise<Map<number, RunRecord>> {
        const { rows } = await this.pool.query(
            `SELECT DISTINCT ON (config_id) * FROM d2v_runs ORDER BY config_id, id DESC`
        );
        return new Map(rows.map((r: RunRecord) => [r.config_id, r]));
    }

    // ------------------------------------------------------------------ logs

    async insertLogs(runId: number, rows: LogRow[]): Promise<void> {
        if (rows.length === 0) return;
        const values: any[] = [];
        const placeholders = rows.map((row, i) => {
            const base = i * 6;
            values.push(runId, row.seq, row.ts, row.level, row.module, row.message);
            return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6})`;
        });
        await this.pool.query(
            `INSERT INTO d2v_run_logs (run_id, seq, ts, level, module, message) VALUES ${placeholders.join(', ')}
             ON CONFLICT DO NOTHING`,
            values
        );
    }

    /**
     * Page through a run's logs. Filtering happens here rather than in the
     * browser so that a search covers the whole run: the UI only holds a
     * trailing window of a long run's output in memory.
     */
    async getLogs(
        runId: number,
        afterSeq = 0,
        limit = 1000,
        filter: LogFilter = {}
    ): Promise<LogRow[]> {
        const values: any[] = [runId, afterSeq];
        let where = 'run_id = $1 AND seq > $2';
        if (filter.levels?.length) {
            values.push(filter.levels);
            where += ` AND level = ANY($${values.length})`;
        }
        if (filter.keyword) {
            values.push(`%${escapeLike(filter.keyword)}%`);
            where += ` AND (message ILIKE $${values.length} ESCAPE '\\' OR module ILIKE $${values.length} ESCAPE '\\')`;
        }
        values.push(Math.min(limit, 5000));
        const { rows } = await this.pool.query(
            `SELECT seq, ts, level, module, message FROM d2v_run_logs
             WHERE ${where} ORDER BY seq LIMIT $${values.length}`,
            values
        );
        return rows.map((r: any) => ({ ...r, seq: Number(r.seq) }));
    }

    /**
     * Seq to replay from so that only the last `tail` lines follow, i.e. the
     * seq of the (tail + 1)-th newest line — 0 when the run is shorter.
     */
    async getTailStartSeq(runId: number, tail: number): Promise<number> {
        const { rows } = await this.pool.query(
            `SELECT seq FROM d2v_run_logs WHERE run_id = $1 ORDER BY seq DESC OFFSET $2 LIMIT 1`,
            [runId, Math.max(0, tail)]
        );
        return rows.length > 0 ? Number(rows[0].seq) : 0;
    }

    /** Per-level line totals for a whole run, for the log viewer's filter chips. */
    async countLogsByLevel(runId: number): Promise<Record<string, number>> {
        const { rows } = await this.pool.query(
            `SELECT level, COUNT(*)::bigint AS count FROM d2v_run_logs
             WHERE run_id = $1 GROUP BY level`,
            [runId]
        );
        const counts: Record<string, number> = {};
        for (const row of rows) counts[row.level] = Number(row.count);
        return counts;
    }

    async pruneOldLogs(retentionDays: number): Promise<number> {
        const { rowCount } = await this.pool.query(
            `DELETE FROM d2v_run_logs USING d2v_runs
             WHERE d2v_run_logs.run_id = d2v_runs.id
               AND d2v_runs.finished_at < NOW() - make_interval(days => $1)`,
            [retentionDays]
        );
        return rowCount ?? 0;
    }

    // ------------------------------------------------------------------ stats

    /** Daily run counts by status + duration stats, for the config stats page. */
    async getConfigStats(configId: number, days: number): Promise<{
        daily: Array<{ day: string; status: string; count: number; avg_duration_s: number | null }>;
        recentDurations: Array<{ id: number; status: string; started_at: string; duration_s: number }>;
        totals: { total: number; succeeded: number; failed: number; skipped: number; canceled: number };
    }> {
        const boundedDays = Math.min(Math.max(days, 1), 365);
        const [daily, recent, totals] = await Promise.all([
            this.pool.query(
                `SELECT date_trunc('day', queued_at)::date::text AS day, status, COUNT(*)::int AS count,
                        AVG(EXTRACT(EPOCH FROM (finished_at - started_at)))::float AS avg_duration_s
                 FROM d2v_runs
                 WHERE config_id = $1 AND queued_at > NOW() - make_interval(days => $2)
                 GROUP BY 1, 2 ORDER BY 1`,
                [configId, boundedDays]
            ),
            this.pool.query(
                `SELECT id, status, started_at, EXTRACT(EPOCH FROM (finished_at - started_at))::float AS duration_s
                 FROM d2v_runs
                 WHERE config_id = $1 AND status IN ('succeeded', 'failed') AND started_at IS NOT NULL AND finished_at IS NOT NULL
                 ORDER BY id DESC LIMIT 50`,
                [configId]
            ),
            this.pool.query(
                `SELECT COUNT(*)::int AS total,
                        COUNT(*) FILTER (WHERE status = 'succeeded')::int AS succeeded,
                        COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
                        COUNT(*) FILTER (WHERE status = 'skipped')::int AS skipped,
                        COUNT(*) FILTER (WHERE status = 'canceled')::int AS canceled
                 FROM d2v_runs
                 WHERE config_id = $1 AND queued_at > NOW() - make_interval(days => $2)`,
                [configId, boundedDays]
            ),
        ]);
        return {
            daily: daily.rows,
            recentDurations: recent.rows.reverse(),
            totals: totals.rows[0],
        };
    }
}
