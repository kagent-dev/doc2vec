import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from 'vitest';
import * as http from 'http';
import { createServer } from '../controller/server';
import { Logger, LogLevel } from '../logger';
import { LogRow } from '../controller/types';

const testLogger = new Logger('server-logs-test', { level: LogLevel.NONE });

let server: http.Server;
let baseUrl: string;
let getLogs: ReturnType<typeof vi.fn>;
let countLogsByLevel: ReturnType<typeof vi.fn>;
let getTailStartSeq: ReturnType<typeof vi.fn>;

// A run whose output is long enough that the viewer can only hold a window of
// it: errors sit near the start, so they must be reachable server-side.
const ALL_ROWS: LogRow[] = Array.from({ length: 25 }, (_, i) => ({
    seq: i + 1,
    ts: `2026-07-27T01:30:${String(i % 60).padStart(2, '0')}.000Z`,
    level: i < 2 ? 'error' : i < 6 ? 'warn' : 'info',
    module: i % 2 === 0 ? 'Doc2Vec:GitHub' : null,
    message: i < 2 ? `boom ${i}` : `line ${i}`,
}));

function applyFilter(rows: LogRow[], levels: string[], q: string): LogRow[] {
    return rows.filter(row => {
        if (levels.length > 0 && !levels.includes(row.level)) return false;
        if (q) {
            const needle = q.toLowerCase();
            return row.message.toLowerCase().includes(needle) || (row.module ?? '').toLowerCase().includes(needle);
        }
        return true;
    });
}

function makeDeps(): any {
    getLogs = vi.fn(async (_runId: number, afterSeq = 0, limit = 1000, filter: any = {}) =>
        applyFilter(ALL_ROWS, filter.levels ?? [], filter.keyword ?? '')
            .filter(row => row.seq > afterSeq)
            .slice(0, limit)
    );
    countLogsByLevel = vi.fn(async () => ({ error: 2, warn: 4, info: 19 }));
    getTailStartSeq = vi.fn(async (_runId: number, tail: number) => Math.max(0, ALL_ROWS.length - tail));
    return {
        store: {
            getRun: async (id: number) => (id === 1 ? { id: 1, status: 'succeeded' } : null),
            getLogs,
            countLogsByLevel,
            getTailStartSeq,
        },
        registry: { get: () => undefined, list: () => [] },
        scheduler: { nextRun: () => null },
        runner: { isBusy: () => false },
        events: { on: () => {}, off: () => {} },
        readWrite: false,
        logger: testLogger,
    };
}

beforeAll(async () => {
    const app = createServer(makeDeps());
    server = http.createServer(app);
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    baseUrl = `http://127.0.0.1:${(server.address() as any).port}`;
});

afterAll(async () => {
    await new Promise<void>((resolve, reject) => server.close(err => (err ? reject(err) : resolve())));
});

beforeEach(() => {
    getLogs.mockClear();
    countLogsByLevel.mockClear();
    getTailStartSeq.mockClear();
});

describe('GET /api/runs/:id/logs', () => {
    it('filters by level across the whole run', async () => {
        const res = await fetch(`${baseUrl}/api/runs/1/logs?levels=error`);
        const body = await res.json();

        expect(res.status).toBe(200);
        expect(getLogs).toHaveBeenCalledWith(1, 0, 1000, { levels: ['error'], keyword: undefined });
        expect(body.map((r: LogRow) => r.seq)).toEqual([1, 2]);
    });

    it('accepts several levels and a keyword together', async () => {
        const res = await fetch(`${baseUrl}/api/runs/1/logs?levels=error,warn&q=boom`);
        const body = await res.json();

        expect(getLogs).toHaveBeenCalledWith(1, 0, 1000, { levels: ['error', 'warn'], keyword: 'boom' });
        expect(body).toHaveLength(2);
    });

    it('ignores unknown level names rather than returning nothing', async () => {
        await fetch(`${baseUrl}/api/runs/1/logs?levels=error,bogus`);
        expect(getLogs).toHaveBeenCalledWith(1, 0, 1000, { levels: ['error'], keyword: undefined });
    });

    it('pages with afterSeq', async () => {
        const res = await fetch(`${baseUrl}/api/runs/1/logs?afterSeq=1&limit=1&levels=error`);
        const body = await res.json();
        expect(body.map((r: LogRow) => r.seq)).toEqual([2]);
    });

    it('404s for an unknown run', async () => {
        const res = await fetch(`${baseUrl}/api/runs/99/logs`);
        expect(res.status).toBe(404);
    });
});

describe('GET /api/runs/:id/logs/counts', () => {
    it('returns whole-run totals per level', async () => {
        const res = await fetch(`${baseUrl}/api/runs/1/logs/counts`);
        expect(await res.json()).toEqual({ error: 2, warn: 4, info: 19 });
    });

    it('404s for an unknown run', async () => {
        expect((await fetch(`${baseUrl}/api/runs/99/logs/counts`)).status).toBe(404);
    });
});

describe('GET /api/runs/:id/logs/download', () => {
    it('streams the full log as plain text', async () => {
        const res = await fetch(`${baseUrl}/api/runs/1/logs/download`);
        const text = await res.text();

        expect(res.headers.get('content-type')).toContain('text/plain');
        expect(res.headers.get('content-disposition')).toContain('run-1.log');
        expect(text.trim().split('\n')).toHaveLength(ALL_ROWS.length);
        expect(text.split('\n')[0]).toBe('2026-07-27T01:30:00.000Z ERROR [Doc2Vec:GitHub] boom 0');
    });

    it('honours the same filters as the log endpoint', async () => {
        const res = await fetch(`${baseUrl}/api/runs/1/logs/download?levels=error`);
        const text = await res.text();
        expect(text.trim().split('\n')).toHaveLength(2);
    });

    it('404s for an unknown run', async () => {
        expect((await fetch(`${baseUrl}/api/runs/99/logs/download`)).status).toBe(404);
    });
});
