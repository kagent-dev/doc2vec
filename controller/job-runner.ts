import { ChildProcess, spawn } from 'child_process';
import * as path from 'path';
import * as readline from 'readline';
import { Logger } from '../logger';
import { ControllerEvents } from './events';
import { ControllerStore } from './store';
import { ConfigRecord, ConflictError, LogRow, RunRecord, RunTrigger } from './types';

const MAX_LOG_MESSAGE_BYTES = 8192;
const LOG_FLUSH_LINES = 100;
const LOG_FLUSH_MS = 500;
const CANCEL_KILL_GRACE_MS = 10_000;
const SHUTDOWN_GRACE_MS = 25_000;

interface QueuedJob {
    runId: number;
    configId: number;
    configPath: string;
}

interface ActiveJob {
    runId: number;
    configId: number;
    child: ChildProcess;
    seq: number;
    buffer: LogRow[];
    flushTimer: ReturnType<typeof setTimeout> | null;
    warnCount: number;
    errorCount: number;
    sources: any[] | null;
    cancelReason: 'user' | 'shutdown' | null;
    killTimer: ReturnType<typeof setTimeout> | null;
}

export interface JobRunnerOptions {
    maxParallel: number;
    /**
     * Command used to run a sync job for a config file. Defaults to spawning this
     * same build's doc2vec.js with the `run` subcommand; overridable for tests.
     */
    commandFor?: (configPath: string) => { cmd: string; args: string[] };
}

/**
 * Runs sync jobs as child processes with a global concurrency cap and a
 * per-config lock (a config never has two simultaneous runs). Captures the
 * child's structured log stream into Postgres and fans it out over SSE.
 */
export class JobRunner {
    private queue: QueuedJob[] = [];
    private active = new Map<number, ActiveJob>();       // by runId
    private queuedConfigs = new Set<number>();
    private activeConfigs = new Set<number>();
    private shuttingDown = false;

    constructor(
        private store: ControllerStore,
        private events: ControllerEvents,
        private opts: JobRunnerOptions,
        private logger: Logger
    ) {}

    isBusy(configId: number): boolean {
        return this.queuedConfigs.has(configId) || this.activeConfigs.has(configId);
    }

    runningCount(): number {
        return this.active.size;
    }

    async enqueue(config: ConfigRecord, trigger: RunTrigger): Promise<RunRecord> {
        if (this.shuttingDown) {
            throw new ConflictError('controller is shutting down');
        }
        if (this.isBusy(config.id)) {
            if (trigger === 'scheduled') {
                // Overlapping scheduled run: record it as skipped so the history shows why nothing happened
                const run = await this.store.createRun(config.id, config.content_hash, trigger, 'skipped', 'previous run still queued or running');
                this.events.emitRunUpdate(run);
                this.logger.warn(`Skipped scheduled run for '${config.name}': previous run still in progress`);
                return run;
            }
            throw new ConflictError('a run for this config is already queued or running');
        }
        if (config.parse_error) {
            throw new ConflictError(`config is invalid: ${config.parse_error}`);
        }

        const run = await this.store.createRun(config.id, config.content_hash, trigger, 'queued');
        this.queuedConfigs.add(config.id);
        this.queue.push({ runId: run.id, configId: config.id, configPath: config.path });
        this.events.emitRunUpdate(run);
        this.pump();
        return run;
    }

    private pump(): void {
        while (!this.shuttingDown && this.active.size < this.opts.maxParallel && this.queue.length > 0) {
            const job = this.queue.shift()!;
            this.queuedConfigs.delete(job.configId);
            this.activeConfigs.add(job.configId);
            this.start(job).catch(async err => {
                this.logger.error(`Failed to start run ${job.runId}:`, err);
                this.activeConfigs.delete(job.configId);
                this.active.delete(job.runId);
                const run = await this.store.finishRun(job.runId, {
                    status: 'failed',
                    error: `failed to spawn job: ${err instanceof Error ? err.message : String(err)}`,
                });
                this.events.emitRunUpdate(run);
                this.pump();
            });
        }
    }

    private commandFor(configPath: string): { cmd: string; args: string[] } {
        if (this.opts.commandFor) return this.opts.commandFor(configPath);
        // dist/controller/job-runner.js → dist/doc2vec.js
        const scriptPath = path.resolve(__dirname, '..', 'doc2vec.js');
        return { cmd: process.execPath, args: [scriptPath, 'run', configPath] };
    }

    private async start(job: QueuedJob): Promise<void> {
        const { cmd, args } = this.commandFor(job.configPath);
        this.logger.info(`Starting run ${job.runId}: ${cmd} ${args.join(' ')}`);

        const child = spawn(cmd, args, {
            env: { ...process.env, DOC2VEC_STRUCTURED_LOGS: '1' },
            stdio: ['ignore', 'pipe', 'pipe'],
        });

        const activeJob: ActiveJob = {
            runId: job.runId,
            configId: job.configId,
            child,
            seq: 0,
            buffer: [],
            flushTimer: null,
            warnCount: 0,
            errorCount: 0,
            sources: null,
            cancelReason: null,
            killTimer: null,
        };
        this.active.set(job.runId, activeJob);

        const run = await this.store.markRunStarted(job.runId, child.pid);
        this.events.emitRunUpdate(run);

        readline.createInterface({ input: child.stdout! }).on('line', line => this.handleLine(activeJob, line, false));
        readline.createInterface({ input: child.stderr! }).on('line', line => this.handleLine(activeJob, line, true));

        child.on('error', err => {
            this.appendLog(activeJob, 'error', null, `spawn error: ${err.message}`);
        });

        child.on('close', (code, signal) => {
            this.finish(activeJob, code, signal).catch(err =>
                this.logger.error(`Failed to finalize run ${job.runId}:`, err)
            );
        });
    }

    private handleLine(aj: ActiveJob, raw: string, fromStderr: boolean): void {
        if (!raw.trim()) return;
        let level = fromStderr ? 'warn' : 'info';
        let module: string | null = null;
        let message = raw;
        let ts = new Date().toISOString();

        try {
            const obj = JSON.parse(raw);
            if (obj && typeof obj === 'object') {
                ts = obj.ts || ts;
                module = obj.module ?? null;
                if (obj.event === 'run-summary') {
                    aj.sources = Array.isArray(obj.sources) ? obj.sources : null;
                    return;
                } else if (obj.event === 'progress') {
                    message = `${obj.title}: ${obj.current}/${obj.total}${obj.message ? ` — ${obj.message}` : ''}`;
                    level = 'info';
                } else if (obj.event === 'section') {
                    message = `═══ ${obj.title} ═══`;
                    level = 'info';
                } else if (obj.msg !== undefined) {
                    message = String(obj.msg);
                    level = String(obj.level || level);
                }
            }
        } catch {
            // Not JSON (e.g. Chromium noise on stderr) — keep the raw line
        }

        if (level === 'warn') aj.warnCount++;
        if (level === 'error') aj.errorCount++;
        this.appendLog(aj, level, module, message, ts);
    }

    private appendLog(aj: ActiveJob, level: string, module: string | null, message: string, ts = new Date().toISOString()): void {
        aj.buffer.push({
            seq: ++aj.seq,
            ts,
            level,
            module,
            message: Buffer.byteLength(message) > MAX_LOG_MESSAGE_BYTES
                ? message.slice(0, MAX_LOG_MESSAGE_BYTES) + '… [truncated]'
                : message,
        });
        if (aj.buffer.length >= LOG_FLUSH_LINES) {
            void this.flush(aj);
        } else if (!aj.flushTimer) {
            aj.flushTimer = setTimeout(() => void this.flush(aj), LOG_FLUSH_MS);
        }
    }

    private async flush(aj: ActiveJob): Promise<void> {
        if (aj.flushTimer) {
            clearTimeout(aj.flushTimer);
            aj.flushTimer = null;
        }
        if (aj.buffer.length === 0) return;
        const rows = aj.buffer.splice(0, aj.buffer.length);
        this.events.emitRunLogs(aj.runId, rows);
        try {
            await this.store.insertLogs(aj.runId, rows);
        } catch (err) {
            this.logger.error(`Failed to persist ${rows.length} log line(s) for run ${aj.runId}:`, err);
        }
    }

    private async finish(aj: ActiveJob, code: number | null, signal: NodeJS.Signals | null): Promise<void> {
        if (aj.killTimer) clearTimeout(aj.killTimer);
        await this.flush(aj);

        let status: RunRecord['status'];
        let error: string | null = null;
        if (aj.cancelReason === 'user') {
            status = 'canceled';
        } else if (aj.cancelReason === 'shutdown') {
            status = 'failed';
            error = 'controller shutdown';
        } else if (code === 0) {
            status = 'succeeded';
        } else {
            status = 'failed';
            error = signal ? `terminated by signal ${signal}` : `exited with code ${code}`;
            const failedSources = aj.sources?.filter(s => !s.ok) ?? [];
            if (failedSources.length > 0) {
                error = `${failedSources.length} source(s) failed: ${failedSources.map(s => s.product_name).join(', ')}`;
            }
        }

        const run = await this.store.finishRun(aj.runId, {
            status,
            exitCode: code,
            error,
            stats: {
                ...(aj.sources && { sources: aj.sources }),
                warn_count: aj.warnCount,
                error_count: aj.errorCount,
            },
        });

        this.active.delete(aj.runId);
        this.activeConfigs.delete(aj.configId);
        this.logger.info(`Run ${aj.runId} finished: ${status}${error ? ` (${error})` : ''}`);
        this.events.emitRunUpdate(run);
        this.pump();
    }

    async cancel(runId: number): Promise<RunRecord> {
        const queuedIndex = this.queue.findIndex(job => job.runId === runId);
        if (queuedIndex >= 0) {
            const [job] = this.queue.splice(queuedIndex, 1);
            this.queuedConfigs.delete(job.configId);
            const run = await this.store.finishRun(runId, { status: 'canceled', error: 'canceled while queued' });
            this.events.emitRunUpdate(run);
            return run;
        }

        const aj = this.active.get(runId);
        if (!aj) {
            throw new ConflictError('run is not queued or running');
        }
        aj.cancelReason = 'user';
        aj.child.kill('SIGTERM');
        aj.killTimer = setTimeout(() => {
            if (this.active.has(runId)) aj.child.kill('SIGKILL');
        }, CANCEL_KILL_GRACE_MS);
        aj.killTimer.unref();
        const run = await this.store.getRun(runId);
        return run!;
    }

    async shutdown(): Promise<void> {
        this.shuttingDown = true;

        const queued = this.queue.splice(0, this.queue.length);
        this.queuedConfigs.clear();
        for (const job of queued) {
            const run = await this.store.finishRun(job.runId, { status: 'canceled', error: 'controller shutdown' });
            this.events.emitRunUpdate(run);
        }

        if (this.active.size === 0) return;
        this.logger.info(`Waiting for ${this.active.size} running job(s) to terminate...`);
        for (const aj of this.active.values()) {
            aj.cancelReason = aj.cancelReason ?? 'shutdown';
            aj.child.kill('SIGTERM');
        }

        const deadline = Date.now() + SHUTDOWN_GRACE_MS;
        while (this.active.size > 0 && Date.now() < deadline) {
            await new Promise(resolve => setTimeout(resolve, 250));
        }
        for (const aj of this.active.values()) {
            this.logger.warn(`Run ${aj.runId} did not exit in time, sending SIGKILL`);
            aj.child.kill('SIGKILL');
        }
        const killDeadline = Date.now() + 3000;
        while (this.active.size > 0 && Date.now() < killDeadline) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }
}
