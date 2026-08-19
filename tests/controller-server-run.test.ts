import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from 'vitest';
import * as http from 'http';
import { createServer } from '../controller/server';
import { Logger, LogLevel } from '../logger';

const testLogger = new Logger('server-run-test', { level: LogLevel.NONE });

let server: http.Server;
let baseUrl: string;
let enqueue: ReturnType<typeof vi.fn>;

const CONFIG = {
    id: 1,
    name: 'cfg',
    path: '/x/cfg.yaml',
    content: '',
    content_hash: 'hash-v1',
    schedule: null,
    parse_error: null,
    enabled: true,
    // 'istio' appears twice (two source entries sharing a product name)
    source_summary: [
        { type: 'website', product_name: 'istio', version: 'latest' },
        { type: 'code', product_name: 'istio', version: 'master' },
        { type: 'website', product_name: 'argo', version: 'latest' },
        { type: 'github', product_name: 'cilium', version: 'latest' },
    ],
};

function makeDeps(): any {
    enqueue = vi.fn(async (_config: any, trigger: string, sources?: any[]) => ({
        id: 42,
        config_id: 1,
        trigger,
        status: 'queued',
        requested_sources: sources ?? null,
    }));
    return {
        store: { getLastRuns: async () => new Map() },
        registry: { get: (id: number) => (id === 1 ? CONFIG : undefined), list: () => [CONFIG] },
        scheduler: { nextRun: () => null },
        runner: { isBusy: () => false, enqueue },
        events: { on: () => {}, off: () => {} },
        readWrite: false,
        logger: testLogger,
    };
}

beforeAll(async () => {
    const app = createServer(makeDeps());
    server = http.createServer(app);
    await new Promise<void>(resolve => server.listen(0, resolve));
    const address = server.address() as { port: number };
    baseUrl = `http://127.0.0.1:${address.port}`;
});

afterAll(async () => {
    await new Promise<void>((resolve, reject) => server.close(err => (err ? reject(err) : resolve())));
});

beforeEach(() => {
    enqueue.mockClear();
});

async function triggerRun(body?: unknown): Promise<Response> {
    return fetch(`${baseUrl}/api/configs/1/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        ...(body !== undefined && { body: JSON.stringify(body) }),
    });
}

describe('POST /api/configs/:id/run', () => {
    it('enqueues a full run when no sources are given', async () => {
        const res = await triggerRun();
        expect(res.status).toBe(202);
        expect(enqueue).toHaveBeenCalledWith(CONFIG, 'manual', undefined);
    });

    it('enqueues a partial run resolving indices to source entries, deduplicated and in config order', async () => {
        const res = await triggerRun({ sources: [2, 1, 2], baseHash: 'hash-v1' });
        expect(res.status).toBe(202);
        expect(enqueue).toHaveBeenCalledWith(CONFIG, 'manual', [
            { index: 1, type: 'code', product_name: 'istio', version: 'master' },
            { index: 2, type: 'website', product_name: 'argo', version: 'latest' },
        ]);
        const run = await res.json();
        expect(run.requested_sources.map((s: any) => s.index)).toEqual([1, 2]);
    });

    it('distinguishes entries that share a product name', async () => {
        const res = await triggerRun({ sources: [1] });
        expect(res.status).toBe(202);
        const [, , sources] = enqueue.mock.calls[0];
        expect(sources).toEqual([{ index: 1, type: 'code', product_name: 'istio', version: 'master' }]);
    });

    it('treats selecting every source as a full run', async () => {
        const res = await triggerRun({ sources: [0, 1, 2, 3] });
        expect(res.status).toBe(202);
        expect(enqueue).toHaveBeenCalledWith(CONFIG, 'manual', undefined);
    });

    it('treats an empty selection as a full run', async () => {
        const res = await triggerRun({ sources: [] });
        expect(res.status).toBe(202);
        expect(enqueue).toHaveBeenCalledWith(CONFIG, 'manual', undefined);
    });

    it('409s when the config changed since the client loaded the source list', async () => {
        const res = await triggerRun({ sources: [0], baseHash: 'stale-hash' });
        expect(res.status).toBe(409);
        expect(enqueue).not.toHaveBeenCalled();
    });

    it('400s on out-of-range indices', async () => {
        const res = await triggerRun({ sources: [0, 9] });
        expect(res.status).toBe(400);
        const body = await res.json();
        expect(body.error).toContain('9');
        expect(enqueue).not.toHaveBeenCalled();
    });

    it('400s when sources is not an array of integers', async () => {
        for (const sources of ['istio', ['istio'], [1.5]]) {
            const res = await triggerRun({ sources });
            expect(res.status).toBe(400);
        }
        expect(enqueue).not.toHaveBeenCalled();
    });

    it('404s for an unknown config', async () => {
        const res = await fetch(`${baseUrl}/api/configs/99/run`, { method: 'POST' });
        expect(res.status).toBe(404);
    });
});
