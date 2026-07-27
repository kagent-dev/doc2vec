import express, { NextFunction, Request, Response } from 'express';
import * as fs from 'fs';
import * as path from 'path';
import { Logger } from '../logger';
import { lookupChunks } from './chunk-inspector';
import { ConfigRegistry, isValidCron, parseConfigMeta } from './config-registry';
import { ControllerEvents } from './events';
import { JobRunner } from './job-runner';
import { Scheduler } from './scheduler';
import { ControllerStore, LogFilter } from './store';
import { ConflictError, LogRow, NotFoundError, RunRecord, RunStatus, ValidationError } from './types';

const SSE_HEARTBEAT_MS = 15_000;
const LOG_PAGE_SIZE = 5000;
const LOG_LEVELS = new Set(['error', 'warn', 'info', 'debug']);

/** Shared parsing for the `levels` (csv) and `q` log query parameters. */
function parseLogFilter(req: Request): LogFilter {
    const levels = typeof req.query.levels === 'string'
        ? req.query.levels.split(',').map(l => l.trim().toLowerCase()).filter(l => LOG_LEVELS.has(l))
        : [];
    const keyword = typeof req.query.q === 'string' ? req.query.q.trim() : '';
    return { levels, keyword: keyword || undefined };
}

export interface ServerDeps {
    store: ControllerStore;
    registry: ConfigRegistry;
    scheduler: Scheduler;
    runner: JobRunner;
    events: ControllerEvents;
    readWrite: boolean;
    logger: Logger;
}

function getVersion(): string {
    try {
        return require('../../package.json').version;
    } catch {
        return 'unknown';
    }
}

function sseInit(res: Response): void {
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no',
    });
    res.write(':ok\n\n');
}

function sseSend(res: Response, event: string, data: unknown): void {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

export function createServer(deps: ServerDeps): express.Express {
    const { store, registry, scheduler, runner, events, readWrite, logger } = deps;
    const app = express();
    app.use(express.json({ limit: '2mb' }));

    const requireReadWrite = (_req: Request, res: Response, next: NextFunction) => {
        if (!readWrite) {
            res.status(403).json({ error: 'controller is running in read-only mode' });
            return;
        }
        next();
    };

    const parseId = (raw: string): number => {
        const id = Number(raw);
        if (!Number.isInteger(id) || id <= 0) throw new ValidationError('invalid id');
        return id;
    };

    const enrichConfig = (config: any, lastRuns: Map<number, RunRecord>) => ({
        ...config,
        next_run: scheduler.nextRun(config.id),
        last_run: lastRuns.get(config.id) ?? null,
        busy: runner.isBusy(config.id),
    });

    // ------------------------------------------------------------------ meta

    app.get('/api/health', (_req, res) => {
        res.json({ status: 'ok', mode: readWrite ? 'rw' : 'ro', version: getVersion() });
    });

    // ------------------------------------------------------------------ configs

    app.get('/api/configs', async (_req, res) => {
        const lastRuns = await store.getLastRuns();
        res.json(registry.list().map(c => enrichConfig(c, lastRuns)));
    });

    app.get('/api/configs/:id', async (req, res) => {
        const config = registry.get(parseId(String(req.params.id)));
        if (!config) throw new NotFoundError('config not found');
        const lastRuns = await store.getLastRuns();
        res.json(enrichConfig(config, lastRuns));
    });

    app.post('/api/configs', requireReadWrite, async (req, res) => {
        const { filename, content } = req.body ?? {};
        if (typeof filename !== 'string' || typeof content !== 'string') {
            throw new ValidationError('body must contain filename and content strings');
        }
        const record = await registry.create(filename, content);
        res.status(201).json(record);
    });

    app.put('/api/configs/:id', requireReadWrite, async (req, res) => {
        const { content, baseHash } = req.body ?? {};
        if (typeof content !== 'string' || typeof baseHash !== 'string') {
            throw new ValidationError('body must contain content and baseHash strings');
        }
        const record = await registry.update(parseId(String(req.params.id)), content, baseHash);
        res.json(record);
    });

    app.delete('/api/configs/:id', requireReadWrite, async (req, res) => {
        await registry.delete(parseId(String(req.params.id)));
        res.status(204).end();
    });

    app.post('/api/configs/validate', (req, res) => {
        const { content } = req.body ?? {};
        if (typeof content !== 'string') {
            throw new ValidationError('body must contain a content string');
        }
        const meta = parseConfigMeta(content, 'config.yaml');
        res.json({
            valid: !meta.parseError,
            error: meta.parseError,
            name: meta.name,
            schedule: meta.schedule,
            schedule_valid: meta.schedule ? isValidCron(meta.schedule) : null,
            sources: meta.sourceSummary,
        });
    });

    app.post('/api/configs/:id/run', async (req, res) => {
        const config = registry.get(parseId(String(req.params.id)));
        if (!config) throw new NotFoundError('config not found');
        const run = await runner.enqueue(config, 'manual');
        res.status(202).json(run);
    });

    app.get('/api/configs/:id/stats', async (req, res) => {
        const id = parseId(String(req.params.id));
        if (!registry.get(id)) throw new NotFoundError('config not found');
        const days = Number(req.query.days) || 30;
        res.json(await store.getConfigStats(id, days));
    });

    // Inspect the chunks currently stored for a URL in one source's vector store.
    // This reads the live store, so results reflect the present state, not a
    // historical run.
    app.get('/api/configs/:id/chunks', async (req, res) => {
        const config = registry.get(parseId(String(req.params.id)));
        if (!config) throw new NotFoundError('config not found');
        const url = typeof req.query.url === 'string' ? req.query.url.trim() : '';
        const productName = typeof req.query.product_name === 'string' ? req.query.product_name : '';
        if (!url) throw new ValidationError('url query parameter is required');
        if (!productName) throw new ValidationError('product_name query parameter is required');

        try {
            res.json(await lookupChunks(config.content, {
                product_name: productName,
                type: typeof req.query.type === 'string' && req.query.type ? req.query.type : undefined,
                version: typeof req.query.version === 'string' && req.query.version ? req.query.version : undefined,
            }, url));
        } catch (err) {
            if (err instanceof NotFoundError || err instanceof ValidationError) throw err;
            // Vector store unreachable (e.g. Qdrant down) — surface the reason
            logger.error('Chunk lookup failed:', err);
            res.status(502).json({ error: `chunk lookup failed: ${err instanceof Error ? err.message : String(err)}` });
        }
    });

    // ------------------------------------------------------------------ runs

    app.get('/api/runs', async (req, res) => {
        res.json(await store.listRuns({
            configId: req.query.configId !== undefined ? parseId(String(req.query.configId)) : undefined,
            status: req.query.status !== undefined ? String(req.query.status) as RunStatus : undefined,
            limit: req.query.limit !== undefined ? Number(req.query.limit) : undefined,
            before: req.query.before !== undefined ? Number(req.query.before) : undefined,
        }));
    });

    app.get('/api/runs/:id', async (req, res) => {
        const run = await store.getRun(parseId(String(req.params.id)));
        if (!run) throw new NotFoundError('run not found');
        const config = registry.get(run.config_id) ?? await store.getConfig(run.config_id);
        res.json({ ...run, config_name: config?.name ?? null });
    });

    app.get('/api/runs/:id/logs', async (req, res) => {
        const id = parseId(String(req.params.id));
        if (!await store.getRun(id)) throw new NotFoundError('run not found');
        const afterSeq = Number(req.query.afterSeq) || 0;
        const limit = Number(req.query.limit) || 1000;
        res.json(await store.getLogs(id, afterSeq, limit, parseLogFilter(req)));
    });

    /** Whole-run line totals per level — the UI can only hold a trailing window. */
    app.get('/api/runs/:id/logs/counts', async (req, res) => {
        const id = parseId(String(req.params.id));
        if (!await store.getRun(id)) throw new NotFoundError('run not found');
        res.json(await store.countLogsByLevel(id));
    });

    // Full log as a plain-text download: the escape hatch when the viewer's
    // in-memory window or filters aren't enough (grep it locally, attach it).
    app.get('/api/runs/:id/logs/download', async (req, res) => {
        const id = parseId(String(req.params.id));
        if (!await store.getRun(id)) throw new NotFoundError('run not found');
        const filter = parseLogFilter(req);
        res.setHeader('Content-Type', 'text/plain; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="run-${id}.log"`);

        let afterSeq = 0;
        let batch: LogRow[];
        do {
            batch = await store.getLogs(id, afterSeq, LOG_PAGE_SIZE, filter);
            for (const row of batch) {
                const module = row.module ? ` [${row.module}]` : '';
                // pg hands back TIMESTAMPTZ as a Date; JSON responses serialise
                // it for us, a text body has to do it explicitly
                const ts = new Date(row.ts).toISOString();
                res.write(`${ts} ${row.level.toUpperCase().padEnd(5)}${module} ${row.message}\n`);
            }
            if (batch.length > 0) afterSeq = batch[batch.length - 1].seq;
        } while (batch.length === LOG_PAGE_SIZE);
        res.end();
    });

    app.post('/api/runs/:id/cancel', async (req, res) => {
        res.json(await runner.cancel(parseId(String(req.params.id))));
    });

    // ------------------------------------------------------------------ SSE

    app.get('/api/events', (req, res) => {
        sseInit(res);
        const onRunUpdate = (run: RunRecord) => sseSend(res, 'run:update', run);
        const onConfigUpdate = () => sseSend(res, 'config:update', {});
        events.on('run:update', onRunUpdate);
        events.on('config:update', onConfigUpdate);
        const heartbeat = setInterval(() => res.write(':hb\n\n'), SSE_HEARTBEAT_MS);
        req.on('close', () => {
            clearInterval(heartbeat);
            events.off('run:update', onRunUpdate);
            events.off('config:update', onConfigUpdate);
        });
    });

    app.get('/api/runs/:id/logs/stream', async (req, res) => {
        const id = parseId(String(req.params.id));
        const run = await store.getRun(id);
        if (!run) throw new NotFoundError('run not found');

        sseInit(res);
        // `tail` keeps the replay to the trailing window the viewer can hold —
        // older lines stay reachable through the filter/download endpoints.
        const tail = Number(req.query.tail) || 0;
        let lastSeq = Number(req.query.afterSeq) || 0;
        if (!lastSeq && tail > 0) lastSeq = await store.getTailStartSeq(id, tail);
        let replayDone = false;
        const pending: LogRow[] = [];

        const sendRow = (row: LogRow) => {
            if (row.seq > lastSeq) {
                lastSeq = row.seq;
                sseSend(res, 'log', row);
            }
        };
        const onLog = (payload: { runId: number; lines: LogRow[] }) => {
            if (payload.runId !== id) return;
            if (!replayDone) {
                pending.push(...payload.lines);
                return;
            }
            payload.lines.forEach(sendRow);
        };
        const onRunUpdate = (updated: RunRecord) => {
            if (updated.id !== id) return;
            sseSend(res, 'run:update', updated);
            if (['succeeded', 'failed', 'canceled', 'skipped'].includes(updated.status)) {
                sseSend(res, 'end', { status: updated.status });
            }
        };
        events.on('run:log', onLog);
        events.on('run:update', onRunUpdate);

        // Replay history from the DB, then flush anything that streamed in meanwhile
        try {
            let batch: LogRow[];
            do {
                batch = await store.getLogs(id, lastSeq, LOG_PAGE_SIZE);
                batch.forEach(sendRow);
            } while (batch.length === LOG_PAGE_SIZE);
        } finally {
            replayDone = true;
            pending.forEach(sendRow);
            pending.length = 0;
        }

        const current = await store.getRun(id);
        if (current && ['succeeded', 'failed', 'canceled', 'skipped'].includes(current.status)) {
            sseSend(res, 'end', { status: current.status });
        }

        const heartbeat = setInterval(() => res.write(':hb\n\n'), SSE_HEARTBEAT_MS);
        req.on('close', () => {
            clearInterval(heartbeat);
            events.off('run:log', onLog);
            events.off('run:update', onRunUpdate);
        });
    });

    // ------------------------------------------------------------------ static UI

    const uiDir = path.resolve(__dirname, '..', 'ui');
    if (fs.existsSync(path.join(uiDir, 'index.html'))) {
        app.use(express.static(uiDir));
        // SPA fallback for client-side routes (anything that's not /api)
        app.use((req, res, next) => {
            if (req.method === 'GET' && !req.path.startsWith('/api')) {
                res.sendFile(path.join(uiDir, 'index.html'));
                return;
            }
            next();
        });
    } else {
        logger.warn(`UI assets not found at ${uiDir} — API only (build them with 'npm run build:ui')`);
    }

    // ------------------------------------------------------------------ errors

    app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
        if (res.headersSent) return;
        if (err instanceof ValidationError) {
            res.status(400).json({ error: err.message });
        } else if (err instanceof NotFoundError) {
            res.status(404).json({ error: err.message });
        } else if (err instanceof ConflictError) {
            res.status(409).json({ error: err.message });
        } else {
            logger.error('Unhandled API error:', err);
            res.status(500).json({ error: 'internal error' });
        }
    });

    return app;
}
