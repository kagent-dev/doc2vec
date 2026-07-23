import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import * as fs from 'fs';
import * as http from 'http';
import * as os from 'os';
import * as path from 'path';
import BetterSqlite3 from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';
import { createServer } from '../controller/server';
import { DatabaseManager } from '../database';
import { Logger, LogLevel } from '../logger';

const testLogger = new Logger('server-chunks-test', { level: LogLevel.NONE });

let tmpDir: string;
let dbPath: string;
let server: http.Server;
let baseUrl: string;

function configContent(): string {
    return `
sources:
  - type: website
    product_name: TestSite
    version: '1.0'
    max_size: 1000
    url: https://example.com
    database_config:
      type: sqlite
      params:
        db_path: ${dbPath}
`;
}

// Minimal stand-ins for the controller wiring — the route under test only
// touches registry.get() and the config content.
function makeDeps(): any {
    return {
        store: {},
        registry: {
            get: (id: number) => (id === 1 ? { id: 1, name: 'test', content: configContent() } : undefined),
            list: () => [],
        },
        scheduler: { nextRun: () => null },
        runner: { isBusy: () => false },
        events: { on: () => {}, off: () => {} },
        readWrite: false,
        logger: testLogger,
    };
}

async function get(pathname: string): Promise<{ status: number; body: any }> {
    const res = await fetch(`${baseUrl}${pathname}`);
    return { status: res.status, body: await res.json().catch(() => null) };
}

beforeAll(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'server-chunks-'));
    dbPath = path.join(tmpDir, 'test.db');

    const db = new BetterSqlite3(dbPath, { allowExtension: true } as any);
    sqliteVec.load(db);
    db.exec(`
        CREATE VIRTUAL TABLE IF NOT EXISTS vec_items USING vec0(
            embedding FLOAT[4],
            product_name TEXT,
            version TEXT,
            branch TEXT,
            repo TEXT,
            heading_hierarchy TEXT,
            section TEXT,
            chunk_id TEXT UNIQUE,
            content TEXT,
            url TEXT,
            hash TEXT,
            chunk_index INTEGER,
            total_chunks INTEGER
        );
    `);
    DatabaseManager.insertVectorsSQLite(db, {
        content: 'Hello chunk',
        metadata: {
            product_name: 'TestSite',
            version: '1.0',
            branch: '',
            repo: '',
            heading_hierarchy: ['Docs'],
            section: 'Docs',
            chunk_id: 'chunk-1',
            url: 'https://example.com/page',
            chunk_index: 0,
            total_chunks: 1,
        },
    }, [0.1, 0.2, 0.3, 0.4], testLogger);
    db.close();

    const app = createServer(makeDeps());
    server = http.createServer(app);
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    const address = server.address() as any;
    baseUrl = `http://127.0.0.1:${address.port}`;
});

afterAll(async () => {
    await new Promise<void>((resolve, reject) => server.close(err => (err ? reject(err) : resolve())));
    fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('GET /api/configs/:id/chunks', () => {
    it('returns the stored chunks for a URL', async () => {
        const { status, body } = await get(
            `/api/configs/1/chunks?product_name=TestSite&url=${encodeURIComponent('https://example.com/page')}`
        );

        expect(status).toBe(200);
        expect(body.source).toEqual({ product_name: 'TestSite', type: 'website', version: '1.0' });
        expect(body.chunks).toHaveLength(1);
        expect(body.chunks[0].content).toBe('Hello chunk');
        expect(body.chunks[0].created_at).toBeTruthy();
    });

    it('400s without a url parameter', async () => {
        const { status } = await get('/api/configs/1/chunks?product_name=TestSite');
        expect(status).toBe(400);
    });

    it('400s without a product_name parameter', async () => {
        const { status } = await get(`/api/configs/1/chunks?url=${encodeURIComponent('https://example.com/page')}`);
        expect(status).toBe(400);
    });

    it('404s for an unknown config', async () => {
        const { status } = await get(
            `/api/configs/99/chunks?product_name=TestSite&url=${encodeURIComponent('https://example.com/page')}`
        );
        expect(status).toBe(404);
    });

    it('404s for a source that is not in the config', async () => {
        const { status, body } = await get(
            `/api/configs/1/chunks?product_name=Nope&url=${encodeURIComponent('https://example.com/page')}`
        );
        expect(status).toBe(404);
        expect(body.error).toContain('Nope');
    });
});
