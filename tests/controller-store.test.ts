import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { ControllerStore } from '../controller/store';

// These tests need a real Postgres. Run them with e.g.:
//   TEST_DATABASE_URL=postgres://postgres:test@localhost:5433/doc2vec npm test
const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL;

function makeLogger(): any {
    const l: any = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn(), section: vi.fn(), event: vi.fn() };
    l.child = vi.fn(() => makeLogger());
    return l;
}

describe.skipIf(!TEST_DATABASE_URL)('ControllerStore (Postgres)', () => {
    let store: ControllerStore;

    beforeAll(async () => {
        store = new ControllerStore(TEST_DATABASE_URL!, makeLogger());
        await store.init();
    });

    afterAll(async () => {
        await store?.close();
    });

    it('applies migrations idempotently', async () => {
        const again = new ControllerStore(TEST_DATABASE_URL!, makeLogger());
        await again.init();
        await again.close();
    });

    it('upserts configs by path and round-trips fields', async () => {
        const created = await store.upsertConfig({
            path: '/test/store-a.yaml',
            name: 'store-a',
            content: 'sources: []',
            contentHash: 'h1',
            schedule: '0 2 * * *',
            sourceSummary: [{ type: 'website', product_name: 'p' }],
            parseError: null,
        });
        expect(created.id).toBeGreaterThan(0);
        expect(created.schedule).toBe('0 2 * * *');
        expect(created.source_summary).toEqual([{ type: 'website', product_name: 'p' }]);

        const updated = await store.upsertConfig({
            path: '/test/store-a.yaml',
            name: 'store-a2',
            content: 'sources: [] # v2',
            contentHash: 'h2',
            schedule: null,
            sourceSummary: [],
            parseError: 'oops',
        });
        expect(updated.id).toBe(created.id);
        expect(updated.name).toBe('store-a2');
        expect(updated.parse_error).toBe('oops');
    });

    it('soft-deletes and revives configs', async () => {
        const config = await store.upsertConfig({
            path: '/test/store-b.yaml', name: 'store-b', content: 'x', contentHash: 'h',
            schedule: null, sourceSummary: [], parseError: null,
        });
        await store.markConfigDeleted(config.path);
        expect((await store.listConfigs()).find(c => c.id === config.id)).toBeUndefined();
        expect((await store.listConfigs(true)).find(c => c.id === config.id)).toBeDefined();

        // Re-upserting the same path revives it
        await store.upsertConfig({
            path: '/test/store-b.yaml', name: 'store-b', content: 'x', contentHash: 'h',
            schedule: null, sourceSummary: [], parseError: null,
        });
        expect((await store.listConfigs()).find(c => c.id === config.id)).toBeDefined();
    });

    it('covers the run lifecycle and log paging', async () => {
        const config = await store.upsertConfig({
            path: '/test/store-c.yaml', name: 'store-c', content: 'x', contentHash: 'h',
            schedule: null, sourceSummary: [], parseError: null,
        });

        const run = await store.createRun(config.id, 'h', 'manual', 'queued');
        expect(run.status).toBe('queued');

        const started = await store.markRunStarted(run.id, 12345);
        expect(started.status).toBe('running');
        expect(started.pid).toBe(12345);

        await store.insertLogs(run.id, [
            { seq: 1, ts: new Date().toISOString(), level: 'info', module: 'm', message: 'one' },
            { seq: 2, ts: new Date().toISOString(), level: 'warn', module: null, message: 'two' },
            { seq: 3, ts: new Date().toISOString(), level: 'error', module: 'm', message: 'three' },
        ]);
        // Duplicate seq inserts are ignored (flush retry safety)
        await store.insertLogs(run.id, [
            { seq: 3, ts: new Date().toISOString(), level: 'error', module: 'm', message: 'dupe' },
        ]);

        const page1 = await store.getLogs(run.id, 0, 2);
        expect(page1.map(l => l.seq)).toEqual([1, 2]);
        const page2 = await store.getLogs(run.id, 2, 10);
        expect(page2.map(l => l.seq)).toEqual([3]);
        expect(page2[0].message).toBe('three');

        const finished = await store.finishRun(run.id, {
            status: 'succeeded',
            exitCode: 0,
            stats: { sources: [{ product_name: 'p', type: 'website', version: '1', duration_ms: 10, ok: true }], warn_count: 1, error_count: 1 },
        });
        expect(finished.status).toBe('succeeded');
        expect(finished.stats.warn_count).toBe(1);

        const runs = await store.listRuns({ configId: config.id });
        expect(runs[0].id).toBe(run.id);

        const lastRuns = await store.getLastRuns();
        expect(lastRuns.get(config.id)?.id).toBe(run.id);

        const stats = await store.getConfigStats(config.id, 30);
        expect(stats.totals.total).toBeGreaterThanOrEqual(1);
        expect(stats.totals.succeeded).toBeGreaterThanOrEqual(1);
    });

    it('marks orphaned runs failed on init', async () => {
        const config = await store.upsertConfig({
            path: '/test/store-d.yaml', name: 'store-d', content: 'x', contentHash: 'h',
            schedule: null, sourceSummary: [], parseError: null,
        });
        const orphan = await store.createRun(config.id, 'h', 'scheduled', 'queued');
        await store.markRunStarted(orphan.id, 999);

        const second = new ControllerStore(TEST_DATABASE_URL!, makeLogger());
        await second.init();
        const after = await second.getRun(orphan.id);
        await second.close();

        expect(after?.status).toBe('failed');
        expect(after?.error).toMatch(/controller restarted/);
    });
});
