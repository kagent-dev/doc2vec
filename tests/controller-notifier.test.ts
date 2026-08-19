import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { EventEmitter } from 'events';
import { buildRunMessage, SlackNotifier } from '../controller/notifier';
import { RunRecord } from '../controller/types';

function makeLogger(): any {
    const l: any = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn(), section: vi.fn(), event: vi.fn() };
    l.child = vi.fn(() => makeLogger());
    return l;
}

function makeRun(overrides: Partial<RunRecord> = {}): RunRecord {
    return {
        id: 42,
        config_id: 7,
        config_hash: 'h',
        trigger: 'scheduled',
        status: 'succeeded',
        pid: null,
        exit_code: 0,
        error: null,
        stats: {
            sources: [
                { product_name: 'istio', type: 'website', version: 'latest', duration_ms: 60000, ok: true },
                { product_name: 'helm', type: 'website', version: 'latest', duration_ms: 30000, ok: true },
            ],
            warn_count: 0,
            error_count: 0,
        },
        queued_at: '2026-07-08T00:30:00Z',
        started_at: '2026-07-08T00:30:01Z',
        finished_at: '2026-07-08T01:12:31Z',
        ...overrides,
    };
}

/** Flushes the promise chain kicked off by an event handler. */
const settle = () => new Promise(resolve => setTimeout(resolve, 0));

describe('buildRunMessage', () => {
    it('describes a successful run with duration and source count', () => {
        const message = buildRunMessage(makeRun(), 'qdrant-sync', 'https://doc2vec.is.solo.io');
        const text = JSON.stringify(message);
        expect(message.text).toBe('doc2vec sync qdrant-sync succeeded');
        expect(text).toContain('✅');
        expect(text).toContain('2/2 sources ok');
        expect(text).toContain('42m 30s');
        expect(text).toContain('https://doc2vec.is.solo.io/runs/42');
    });

    it('lists failed sources with their errors', () => {
        const run = makeRun({
            status: 'failed',
            exit_code: 1,
            error: '1 source(s) failed: istio',
            stats: {
                sources: [
                    { product_name: 'istio', type: 'website', version: 'latest', duration_ms: 5000, ok: false, error: 'browser failed to launch twice' },
                    { product_name: 'helm', type: 'website', version: 'latest', duration_ms: 30000, ok: true },
                ],
                warn_count: 3,
                error_count: 94,
            },
        });
        const text = JSON.stringify(buildRunMessage(run, 'qdrant-sync'));
        expect(text).toContain('❌');
        expect(text).toContain('1/2 sources ok');
        expect(text).toContain('*istio*: browser failed to launch twice');
        expect(text).toContain('errors: 94');
    });

    it('truncates long failed-source lists', () => {
        const sources = Array.from({ length: 8 }, (_, i) => ({
            product_name: `src-${i}`, type: 'website', version: '1', duration_ms: 1, ok: false, error: 'boom',
        }));
        const text = JSON.stringify(buildRunMessage(makeRun({ status: 'failed', stats: { sources } }), 'c'));
        expect(text).toContain('…and 3 more');
    });

    it('falls back to run id when no public URL is configured', () => {
        const text = JSON.stringify(buildRunMessage(makeRun(), 'c'));
        expect(text).toContain('run #42');
        expect(text).not.toContain('/runs/42|');
    });
});

describe('SlackNotifier', () => {
    let events: any;
    let store: any;
    let fetchMock: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        events = new EventEmitter();
        store = { getConfig: vi.fn(async () => ({ id: 7, name: 'qdrant-sync' })) };
        fetchMock = vi.fn(async () => ({ ok: true, status: 200, text: async () => '' }));
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    function makeNotifier(notify: 'all' | 'failures' = 'all') {
        const notifier = new SlackNotifier(
            { webhookUrl: 'https://hooks.slack.com/services/T/B/X', notify },
            store,
            makeLogger()
        );
        notifier.attach(events);
        return notifier;
    }

    it('posts to the webhook when a run succeeds', async () => {
        makeNotifier();
        events.emit('run:update', makeRun());
        await settle();
        expect(fetchMock).toHaveBeenCalledTimes(1);
        const [url, init] = fetchMock.mock.calls[0];
        expect(url).toBe('https://hooks.slack.com/services/T/B/X');
        expect(JSON.parse(init.body)).toMatchObject({ text: 'doc2vec sync qdrant-sync succeeded' });
    });

    it('ignores non-terminal and skipped statuses', async () => {
        makeNotifier();
        for (const status of ['queued', 'running', 'skipped'] as const) {
            events.emit('run:update', makeRun({ status }));
        }
        await settle();
        expect(fetchMock).not.toHaveBeenCalled();
    });

    it('failures mode skips successes but reports failed and canceled runs', async () => {
        makeNotifier('failures');
        events.emit('run:update', makeRun({ status: 'succeeded' }));
        events.emit('run:update', makeRun({ status: 'failed' }));
        events.emit('run:update', makeRun({ status: 'canceled' }));
        await settle();
        expect(fetchMock).toHaveBeenCalledTimes(2);
    });

    it('survives webhook failures without throwing', async () => {
        fetchMock.mockRejectedValueOnce(new Error('network down'));
        makeNotifier();
        events.emit('run:update', makeRun());
        await settle();
        // A second run still notifies
        events.emit('run:update', makeRun({ id: 43 }));
        await settle();
        expect(fetchMock).toHaveBeenCalledTimes(2);
    });
});
