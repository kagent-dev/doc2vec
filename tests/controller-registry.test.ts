import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { ConfigRegistry, hashContent, isValidCron, parseConfigMeta } from '../controller/config-registry';
import { ConflictError, ValidationError } from '../controller/types';

const VALID_YAML = `
name: my-config
schedule: "0 2 * * *"
sources:
  - type: website
    product_name: demo
    version: latest
`;

function makeLogger(): any {
    const l: any = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn(), section: vi.fn(), event: vi.fn() };
    l.child = vi.fn(() => makeLogger());
    return l;
}

function makeStore(): any {
    let nextId = 1;
    const byPath = new Map<string, any>();
    return {
        upsertConfig: vi.fn(async (input: any) => {
            const existing = byPath.get(input.path);
            const record = {
                id: existing?.id ?? nextId++,
                path: input.path,
                name: input.name,
                content: input.content,
                content_hash: input.contentHash,
                schedule: input.schedule,
                source_summary: input.sourceSummary,
                parse_error: input.parseError,
                enabled: true,
                deleted_at: null,
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString(),
            };
            byPath.set(input.path, record);
            return record;
        }),
        markConfigDeleted: vi.fn(async (p: string) => { byPath.delete(p); }),
    };
}

function makeEvents(): any {
    return { emitConfigUpdate: vi.fn(), emitRunUpdate: vi.fn(), emitRunLogs: vi.fn() };
}

describe('parseConfigMeta', () => {
    it('extracts name, schedule and source summary', () => {
        const meta = parseConfigMeta(VALID_YAML, '/x/my.yaml');
        expect(meta.name).toBe('my-config');
        expect(meta.schedule).toBe('0 2 * * *');
        expect(meta.sourceSummary).toEqual([{ type: 'website', product_name: 'demo', version: 'latest' }]);
        expect(meta.parseError).toBeNull();
    });

    it('falls back to the file basename when name is missing', () => {
        const meta = parseConfigMeta('sources:\n  - type: website\n    product_name: p\n', '/x/istio-docs.yaml');
        expect(meta.name).toBe('istio-docs');
    });

    it('reports invalid YAML instead of throwing', () => {
        const meta = parseConfigMeta('sources: [unclosed', '/x/bad.yaml');
        expect(meta.parseError).toBeTruthy();
    });

    it('reports a missing sources array', () => {
        const meta = parseConfigMeta('name: nope\n', '/x/no-sources.yaml');
        expect(meta.parseError).toMatch(/no sources/);
    });

    it('rejects an invalid cron schedule', () => {
        const meta = parseConfigMeta('schedule: "not a cron"\nsources:\n  - type: website\n    product_name: p\n', '/x/bad-cron.yaml');
        expect(meta.parseError).toMatch(/invalid cron/);
        expect(meta.schedule).toBeNull();
    });

    it('never substitutes ${ENV} placeholders', () => {
        process.env.REGISTRY_TEST_SECRET = 'supersecret';
        const yamlWithSecret = 'sources:\n  - type: zendesk\n    product_name: z\n    api_token: ${REGISTRY_TEST_SECRET}\n';
        const meta = parseConfigMeta(yamlWithSecret, '/x/secret.yaml');
        expect(meta.parseError).toBeNull();
        // The raw content is what gets stored/displayed; parseConfigMeta must not resolve env vars
        expect(yamlWithSecret).toContain('${REGISTRY_TEST_SECRET}');
        expect(JSON.stringify(meta)).not.toContain('supersecret');
        delete process.env.REGISTRY_TEST_SECRET;
    });
});

describe('isValidCron', () => {
    it('accepts standard cron expressions', () => {
        expect(isValidCron('*/5 * * * *')).toBe(true);
        expect(isValidCron('0 2 * * *')).toBe(true);
    });
    it('rejects garbage', () => {
        expect(isValidCron('not a cron')).toBe(false);
    });
});

describe('ConfigRegistry', () => {
    let dir: string;
    let store: any;
    let events: any;
    let registry: ConfigRegistry;

    beforeEach(() => {
        dir = fs.mkdtempSync(path.join(os.tmpdir(), 'd2v-registry-'));
        store = makeStore();
        events = makeEvents();
    });

    afterEach(() => {
        registry?.stop();
        fs.rmSync(dir, { recursive: true, force: true });
    });

    function makeRegistry(args: string[], readWrite = false, configDir?: string) {
        registry = new ConfigRegistry(
            { configArgs: args, configDir, readWrite, reloadIntervalSec: 3600 },
            store,
            events,
            makeLogger()
        );
        return registry;
    }

    it('loads config files and directories on start', async () => {
        fs.writeFileSync(path.join(dir, 'a.yaml'), VALID_YAML);
        fs.writeFileSync(path.join(dir, 'b.yml'), VALID_YAML.replace('my-config', 'other'));
        fs.writeFileSync(path.join(dir, 'ignored.txt'), 'nope');
        await makeRegistry([dir]).start();

        expect(registry.list()).toHaveLength(2);
        expect(registry.list().map(c => c.name).sort()).toEqual(['my-config', 'other']);
        expect(events.emitConfigUpdate).toHaveBeenCalledTimes(1);
    });

    it('detects content changes by hash on re-sync', async () => {
        const file = path.join(dir, 'a.yaml');
        fs.writeFileSync(file, VALID_YAML);
        await makeRegistry([file]).start();
        expect(store.upsertConfig).toHaveBeenCalledTimes(1);

        await registry.sync();
        expect(store.upsertConfig).toHaveBeenCalledTimes(1); // unchanged → no upsert

        fs.writeFileSync(file, VALID_YAML.replace('0 2 * * *', '0 3 * * *'));
        await registry.sync();
        expect(store.upsertConfig).toHaveBeenCalledTimes(2);
        expect(registry.list()[0].schedule).toBe('0 3 * * *');
    });

    it('soft-deletes configs whose file disappears', async () => {
        const file = path.join(dir, 'a.yaml');
        fs.writeFileSync(file, VALID_YAML);
        await makeRegistry([dir]).start();
        expect(registry.list()).toHaveLength(1);

        fs.unlinkSync(file);
        await registry.sync();
        expect(registry.list()).toHaveLength(0);
        expect(store.markConfigDeleted).toHaveBeenCalledWith(file);
    });

    it('keeps invalid configs visible with a parse_error', async () => {
        fs.writeFileSync(path.join(dir, 'broken.yaml'), 'sources: [unclosed');
        await makeRegistry([dir]).start();
        expect(registry.list()).toHaveLength(1);
        expect(registry.list()[0].parse_error).toBeTruthy();
    });

    describe('read-write mode', () => {
        it('creates a new config file in the config dir', async () => {
            await makeRegistry([], true, dir).start();
            const record = await registry.create('new.yaml', VALID_YAML);
            expect(record.name).toBe('my-config');
            expect(fs.readFileSync(path.join(dir, 'new.yaml'), 'utf8')).toBe(VALID_YAML);
        });

        it('rejects path-traversal filenames', async () => {
            await makeRegistry([], true, dir).start();
            await expect(registry.create('../evil.yaml', VALID_YAML)).rejects.toThrow(ValidationError);
            await expect(registry.create('no-extension', VALID_YAML)).rejects.toThrow(ValidationError);
        });

        it('rejects invalid content on create', async () => {
            await makeRegistry([], true, dir).start();
            await expect(registry.create('bad.yaml', 'nope: true')).rejects.toThrow(ValidationError);
        });

        it('updates with optimistic concurrency on the content hash', async () => {
            fs.writeFileSync(path.join(dir, 'a.yaml'), VALID_YAML);
            await makeRegistry([], true, dir).start();
            const config = registry.list()[0];

            const newContent = VALID_YAML.replace('my-config', 'renamed');
            const updated = await registry.update(config.id, newContent, config.content_hash);
            expect(updated.name).toBe('renamed');

            // Stale base hash → conflict
            await expect(registry.update(config.id, VALID_YAML, config.content_hash)).rejects.toThrow(ConflictError);
        });

        it('update round-trips through the hash returned by the previous update', async () => {
            fs.writeFileSync(path.join(dir, 'a.yaml'), VALID_YAML);
            await makeRegistry([], true, dir).start();
            const config = registry.list()[0];
            const v2 = await registry.update(config.id, VALID_YAML.replace('my-config', 'v2'), config.content_hash);
            const v3 = await registry.update(v2.id, VALID_YAML.replace('my-config', 'v3'), v2.content_hash);
            expect(v3.name).toBe('v3');
        });

        it('deletes the file and the registry entry', async () => {
            fs.writeFileSync(path.join(dir, 'a.yaml'), VALID_YAML);
            await makeRegistry([], true, dir).start();
            const config = registry.list()[0];
            await registry.delete(config.id);
            expect(fs.existsSync(config.path)).toBe(false);
            expect(registry.list()).toHaveLength(0);
        });
    });
});

describe('hashContent', () => {
    it('is deterministic and content-sensitive', () => {
        expect(hashContent('abc')).toBe(hashContent('abc'));
        expect(hashContent('abc')).not.toBe(hashContent('abd'));
    });
});
