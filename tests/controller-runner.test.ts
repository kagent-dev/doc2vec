import { describe, it, expect, beforeEach, vi } from 'vitest';
import { JobRunner } from '../controller/job-runner';
import { ConflictError, ConfigRecord, LogRow, RunRecord } from '../controller/types';

function makeLogger(): any {
    const l: any = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn(), section: vi.fn(), event: vi.fn() };
    l.child = vi.fn(() => makeLogger());
    return l;
}

function makeConfig(id: number, overrides: Partial<ConfigRecord> = {}): ConfigRecord {
    return {
        id,
        path: `/x/config-${id}.yaml`,
        name: `config-${id}`,
        content: '',
        content_hash: `hash-${id}`,
        schedule: null,
        source_summary: [],
        parse_error: null,
        enabled: true,
        deleted_at: null,
        created_at: '',
        updated_at: '',
        ...overrides,
    };
}

/** Minimal in-memory stand-in for ControllerStore. */
function makeStore() {
    let nextRunId = 1;
    const runs = new Map<number, RunRecord>();
    const logs = new Map<number, LogRow[]>();
    return {
        runs,
        logs,
        createRun: vi.fn(async (configId: number, configHash: string, trigger: any, status: any, error?: string) => {
            const run: RunRecord = {
                id: nextRunId++,
                config_id: configId,
                config_hash: configHash,
                trigger,
                status,
                pid: null,
                exit_code: null,
                error: error ?? null,
                stats: {},
                queued_at: new Date().toISOString(),
                started_at: null,
                finished_at: status === 'skipped' ? new Date().toISOString() : null,
            };
            runs.set(run.id, run);
            return run;
        }),
        // Like the real store, return fresh row objects instead of mutating shared ones
        markRunStarted: vi.fn(async (runId: number, pid: number) => {
            const run = { ...runs.get(runId)!, status: 'running' as const, pid, started_at: new Date().toISOString() };
            runs.set(runId, run);
            return run;
        }),
        finishRun: vi.fn(async (runId: number, outcome: any) => {
            const run = {
                ...runs.get(runId)!,
                status: outcome.status,
                exit_code: outcome.exitCode ?? null,
                error: outcome.error ?? null,
                stats: outcome.stats ?? {},
                finished_at: new Date().toISOString(),
            };
            runs.set(runId, run);
            return run;
        }),
        getRun: vi.fn(async (runId: number) => runs.get(runId) ?? null),
        insertLogs: vi.fn(async (runId: number, rows: LogRow[]) => {
            logs.set(runId, [...(logs.get(runId) ?? []), ...rows]);
        }),
    };
}

function makeEvents(): any {
    return { emitRunUpdate: vi.fn(), emitRunLogs: vi.fn(), emitConfigUpdate: vi.fn() };
}

/** Waits until the run reaches a terminal status or the timeout hits. */
async function waitForRun(store: any, runId: number, timeoutMs = 10_000): Promise<RunRecord> {
    const terminal = ['succeeded', 'failed', 'canceled', 'skipped'];
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const run = store.runs.get(runId);
        if (run && terminal.includes(run.status)) return run;
        await new Promise(resolve => setTimeout(resolve, 50));
    }
    throw new Error(`run ${runId} did not finish in time (status: ${store.runs.get(runId)?.status})`);
}

/** commandFor stub: runs an inline node script instead of a real sync. */
function inlineScript(script: string) {
    return () => ({ cmd: process.execPath, args: ['-e', script] });
}

describe('JobRunner', () => {
    let store: ReturnType<typeof makeStore>;
    let events: any;

    beforeEach(() => {
        store = makeStore();
        events = makeEvents();
    });

    function makeRunner(script: string, maxParallel = 1) {
        return new JobRunner(store as any, events, { maxParallel, commandFor: inlineScript(script) }, makeLogger());
    }

    it('runs a job to success and captures structured logs + run-summary stats', async () => {
        const runner = makeRunner(`
            console.log(JSON.stringify({ ts: new Date().toISOString(), level: 'info', module: 'test', msg: 'hello' }));
            console.log(JSON.stringify({ event: 'run-summary', sources: [{ product_name: 'p', type: 'website', version: '1', duration_ms: 5, ok: true }] }));
        `);
        const run = await runner.enqueue(makeConfig(1), 'manual');
        expect(run.status).toBe('queued');

        const finished = await waitForRun(store, run.id);
        expect(finished.status).toBe('succeeded');
        expect(finished.exit_code).toBe(0);
        expect(finished.stats.sources).toHaveLength(1);

        const logs = store.logs.get(run.id) ?? [];
        expect(logs.some(l => l.message === 'hello' && l.level === 'info' && l.module === 'test')).toBe(true);
        // run-summary is stats, not a log line
        expect(logs.some(l => l.message.includes('run-summary'))).toBe(false);
    });

    it('marks non-zero exits as failed and names failing sources', async () => {
        const runner = makeRunner(`
            console.log(JSON.stringify({ event: 'run-summary', sources: [{ product_name: 'bad-src', type: 'website', version: '1', duration_ms: 5, ok: false, error: 'boom' }] }));
            process.exit(1);
        `);
        const run = await runner.enqueue(makeConfig(1), 'manual');
        const finished = await waitForRun(store, run.id);
        expect(finished.status).toBe('failed');
        expect(finished.exit_code).toBe(1);
        expect(finished.error).toContain('bad-src');
    });

    it('stores non-JSON output as plain log lines', async () => {
        const runner = makeRunner(`
            console.log('plain stdout noise');
            console.error('plain stderr noise');
        `);
        const run = await runner.enqueue(makeConfig(1), 'manual');
        await waitForRun(store, run.id);
        const logs = store.logs.get(run.id) ?? [];
        expect(logs.find(l => l.message === 'plain stdout noise')?.level).toBe('info');
        expect(logs.find(l => l.message === 'plain stderr noise')?.level).toBe('warn');
    });

    it('rejects a manual run while the config is busy, and skips an overlapping scheduled run', async () => {
        const runner = makeRunner(`setTimeout(() => {}, 2000);`);
        const config = makeConfig(1);
        const first = await runner.enqueue(config, 'manual');

        await expect(runner.enqueue(config, 'manual')).rejects.toThrow(ConflictError);

        const skipped = await runner.enqueue(config, 'scheduled');
        expect(skipped.status).toBe('skipped');

        await runner.cancel(first.id);
        await waitForRun(store, first.id);
    });

    it('honors maxParallel with a FIFO queue across different configs', async () => {
        const runner = makeRunner(`setTimeout(() => {}, 300);`, 1);
        const run1 = await runner.enqueue(makeConfig(1), 'manual');
        const run2 = await runner.enqueue(makeConfig(2), 'manual');

        // Only one may be running at a time
        await new Promise(resolve => setTimeout(resolve, 100));
        expect(store.runs.get(run1.id)!.status).toBe('running');
        expect(store.runs.get(run2.id)!.status).toBe('queued');

        await waitForRun(store, run1.id);
        const second = await waitForRun(store, run2.id);
        expect(second.status).toBe('succeeded');
    });

    it('cancel terminates a running job and marks it canceled', async () => {
        const runner = makeRunner(`setTimeout(() => {}, 30000);`);
        const run = await runner.enqueue(makeConfig(1), 'manual');
        await new Promise(resolve => setTimeout(resolve, 200));

        await runner.cancel(run.id);
        const finished = await waitForRun(store, run.id);
        expect(finished.status).toBe('canceled');
    });

    it('cancel removes a queued job without spawning it', async () => {
        const runner = makeRunner(`setTimeout(() => {}, 2000);`, 1);
        const running = await runner.enqueue(makeConfig(1), 'manual');
        const queued = await runner.enqueue(makeConfig(2), 'manual');

        const canceled = await runner.cancel(queued.id);
        expect(canceled.status).toBe('canceled');

        await runner.cancel(running.id);
        await waitForRun(store, running.id);
    });

    it('shutdown cancels queued runs and fails running ones', async () => {
        const runner = makeRunner(`setTimeout(() => {}, 30000);`, 1);
        const running = await runner.enqueue(makeConfig(1), 'manual');
        const queued = await runner.enqueue(makeConfig(2), 'manual');
        await new Promise(resolve => setTimeout(resolve, 200));

        await runner.shutdown();

        expect(store.runs.get(queued.id)!.status).toBe('canceled');
        const finishedRunning = store.runs.get(running.id)!;
        expect(finishedRunning.status).toBe('failed');
        expect(finishedRunning.error).toBe('controller shutdown');

        await expect(runner.enqueue(makeConfig(3), 'manual')).rejects.toThrow(ConflictError);
    });

    it('rejects runs for configs with parse errors', async () => {
        const runner = makeRunner('');
        await expect(runner.enqueue(makeConfig(1, { parse_error: 'bad yaml' }), 'manual')).rejects.toThrow(/invalid/);
    });
});
