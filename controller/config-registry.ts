import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'js-yaml';
import { Cron } from 'croner';
import { Logger } from '../logger';
import { ControllerEvents } from './events';
import { ControllerStore } from './store';
import { ConfigRecord, ConflictError, NotFoundError, SourceSummary, ValidationError } from './types';

export interface ParsedConfigMeta {
    name: string;
    schedule: string | null;
    sourceSummary: SourceSummary[];
    parseError: string | null;
}

/**
 * Parse raw config YAML WITHOUT ${ENV} substitution (secrets stay as placeholders)
 * and without Doc2Vec.loadConfig()'s process.exit() behavior — invalid files are
 * reported, never fatal.
 */
export function parseConfigMeta(content: string, filePath: string): ParsedConfigMeta {
    const fallbackName = path.basename(filePath).replace(/\.ya?ml$/i, '');
    try {
        const parsed = yaml.load(content) as any;
        if (!parsed || typeof parsed !== 'object') {
            return { name: fallbackName, schedule: null, sourceSummary: [], parseError: 'config is empty or not a YAML mapping' };
        }
        if (!Array.isArray(parsed.sources) || parsed.sources.length === 0) {
            return { name: parsed.name || fallbackName, schedule: null, sourceSummary: [], parseError: 'config has no sources' };
        }
        const sourceSummary: SourceSummary[] = parsed.sources.map((s: any) => ({
            type: String(s?.type ?? 'unknown'),
            product_name: String(s?.product_name ?? 'unknown'),
            ...(s?.version !== undefined && { version: String(s.version) }),
        }));
        let schedule: string | null = null;
        let parseError: string | null = null;
        if (parsed.schedule !== undefined && parsed.schedule !== null) {
            schedule = String(parsed.schedule);
            if (!isValidCron(schedule)) {
                parseError = `invalid cron expression: ${schedule}`;
                schedule = null;
            }
        }
        return { name: String(parsed.name || fallbackName), schedule, sourceSummary, parseError };
    } catch (err) {
        return {
            name: fallbackName,
            schedule: null,
            sourceSummary: [],
            parseError: err instanceof Error ? err.message : String(err),
        };
    }
}

export function isValidCron(expr: string): boolean {
    try {
        new Cron(expr, { paused: true }).stop();
        return true;
    } catch {
        return false;
    }
}

export function hashContent(content: string): string {
    return crypto.createHash('sha256').update(content).digest('hex');
}

export interface ConfigRegistryOptions {
    configArgs: string[];
    configDir?: string;
    readWrite: boolean;
    reloadIntervalSec: number;
}

/**
 * Watches the config files/directories passed on the CLI (plus --config-dir in
 * read-write mode), keeps the d2v_configs table in sync, and — in read-write mode —
 * persists UI edits back to YAML files on disk. Disk is the source of truth.
 *
 * Change detection is content-hash polling rather than fs.watch: Kubernetes
 * ConfigMap updates arrive as atomic symlink swaps that fs.watch misses.
 */
export class ConfigRegistry {
    private byPath = new Map<string, ConfigRecord>();
    private timer: ReturnType<typeof setInterval> | null = null;
    private syncing = false;

    constructor(
        private opts: ConfigRegistryOptions,
        private store: ControllerStore,
        private events: ControllerEvents,
        private logger: Logger
    ) {}

    async start(): Promise<void> {
        await this.sync();
        this.timer = setInterval(() => {
            this.sync().catch(err => this.logger.error('Config re-sync failed:', err));
        }, Math.max(this.opts.reloadIntervalSec, 5) * 1000);
        this.timer.unref();
    }

    stop(): void {
        if (this.timer) clearInterval(this.timer);
        this.timer = null;
    }

    list(): ConfigRecord[] {
        return [...this.byPath.values()].sort((a, b) => a.name.localeCompare(b.name));
    }

    get(id: number): ConfigRecord | undefined {
        return [...this.byPath.values()].find(c => c.id === id);
    }

    /** Expand CLI args (files and directories) into the current set of config files. */
    private discoverFiles(): string[] {
        const files = new Set<string>();
        const addDir = (dir: string) => {
            for (const entry of fs.readdirSync(dir)) {
                if (/\.ya?ml$/i.test(entry) && !entry.startsWith('.')) {
                    const full = path.join(dir, entry);
                    if (fs.statSync(full).isFile()) files.add(full);
                }
            }
        };
        for (const arg of this.opts.configArgs) {
            const abs = path.resolve(arg);
            if (!fs.existsSync(abs)) {
                this.logger.warn(`Config path does not exist: ${abs}`);
                continue;
            }
            if (fs.statSync(abs).isDirectory()) {
                addDir(abs);
            } else {
                files.add(abs);
            }
        }
        if (this.opts.readWrite && this.opts.configDir && fs.existsSync(this.opts.configDir)) {
            addDir(path.resolve(this.opts.configDir));
        }
        return [...files];
    }

    async sync(): Promise<void> {
        if (this.syncing) return;
        this.syncing = true;
        try {
            const files = this.discoverFiles();
            const seen = new Set<string>();
            let changed = false;

            for (const file of files) {
                seen.add(file);
                let content: string;
                try {
                    content = fs.readFileSync(file, 'utf8');
                } catch (err) {
                    this.logger.warn(`Failed to read config ${file}:`, err);
                    continue;
                }
                const contentHash = hashContent(content);
                const existing = this.byPath.get(file);
                if (existing && existing.content_hash === contentHash) continue;

                const meta = parseConfigMeta(content, file);
                if (meta.parseError) {
                    this.logger.warn(`Config ${file} has a problem: ${meta.parseError}`);
                }
                const record = await this.store.upsertConfig({
                    path: file,
                    name: meta.name,
                    content,
                    contentHash,
                    schedule: meta.schedule,
                    sourceSummary: meta.sourceSummary,
                    parseError: meta.parseError,
                });
                this.byPath.set(file, record);
                changed = true;
                this.logger.info(`${existing ? 'Reloaded' : 'Loaded'} config '${record.name}' from ${file}` +
                    (record.schedule ? ` (schedule: ${record.schedule})` : ' (manual runs only)'));
            }

            for (const [file] of this.byPath) {
                if (!seen.has(file)) {
                    this.logger.info(`Config file removed: ${file}`);
                    await this.store.markConfigDeleted(file);
                    this.byPath.delete(file);
                    changed = true;
                }
            }

            if (changed) this.events.emitConfigUpdate();
        } finally {
            this.syncing = false;
        }
    }

    // ------------------------------------------------------------- read-write mode

    /** Validate content for create/update: must parse and declare sources; cron must be valid. */
    private validateForWrite(content: string, filePath: string): ParsedConfigMeta {
        const meta = parseConfigMeta(content, filePath);
        if (meta.parseError) {
            throw new ValidationError(meta.parseError);
        }
        return meta;
    }

    private atomicWrite(target: string, content: string): void {
        const tmp = `${target}.tmp-${process.pid}`;
        fs.writeFileSync(tmp, content, 'utf8');
        fs.renameSync(tmp, target);
    }

    async create(filename: string, content: string): Promise<ConfigRecord> {
        if (!this.opts.configDir) {
            throw new ValidationError('no --config-dir configured');
        }
        if (!/^[A-Za-z0-9][A-Za-z0-9._-]*\.ya?ml$/.test(filename)) {
            throw new ValidationError('filename must be a plain name ending in .yaml or .yml');
        }
        const target = path.join(path.resolve(this.opts.configDir), filename);
        if (fs.existsSync(target)) {
            throw new ConflictError(`config file ${filename} already exists`);
        }
        this.validateForWrite(content, target);
        this.atomicWrite(target, content);
        await this.sync();
        const record = this.byPath.get(target);
        if (!record) throw new Error('config was written but failed to load');
        return record;
    }

    async update(id: number, content: string, baseHash: string): Promise<ConfigRecord> {
        const existing = this.get(id);
        if (!existing) throw new NotFoundError(`config ${id} not found`);
        if (existing.content_hash !== baseHash) {
            throw new ConflictError('config changed on disk since you loaded it — reload and reapply your edits');
        }
        this.validateForWrite(content, existing.path);
        this.atomicWrite(existing.path, content);
        await this.sync();
        const record = this.byPath.get(existing.path);
        if (!record) throw new Error('config was written but failed to load');
        return record;
    }

    async delete(id: number): Promise<void> {
        const existing = this.get(id);
        if (!existing) throw new NotFoundError(`config ${id} not found`);
        fs.unlinkSync(existing.path);
        await this.store.markConfigDeleted(existing.path);
        this.byPath.delete(existing.path);
        this.events.emitConfigUpdate();
    }
}
