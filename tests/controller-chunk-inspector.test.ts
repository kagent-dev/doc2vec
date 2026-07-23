import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import BetterSqlite3 from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';
import { lookupChunks } from '../controller/chunk-inspector';
import { NotFoundError } from '../controller/types';
import { DatabaseManager } from '../database';
import { DocumentChunk } from '../types';
import { Logger, LogLevel } from '../logger';

const testLogger = new Logger('chunk-inspector-test', { level: LogLevel.NONE });
const DIMENSION = 4;

let tmpDir: string;
let dbPath: string;

function createDbFile(withDatesTable = true): BetterSqlite3.Database {
    const db = new BetterSqlite3(dbPath, { allowExtension: true } as any);
    sqliteVec.load(db);
    db.exec(`
        CREATE VIRTUAL TABLE IF NOT EXISTS vec_items USING vec0(
            embedding FLOAT[${DIMENSION}],
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
    if (!withDatesTable) {
        // Simulate a database created before chunk creation dates existed
        db.exec(`DROP TABLE IF EXISTS vec_chunk_dates;`);
    }
    return db;
}

function makeChunk(overrides: Partial<DocumentChunk['metadata']> & { content?: string } = {}): DocumentChunk {
    const { content, ...meta } = overrides;
    return {
        content: content ?? 'Chunk content',
        metadata: {
            product_name: 'TestSite',
            version: '1.0',
            branch: '',
            repo: '',
            heading_hierarchy: ['Guide', 'Install'],
            section: 'Install',
            chunk_id: 'chunk-1',
            url: 'https://example.com/page',
            chunk_index: 0,
            total_chunks: 1,
            ...meta,
        },
    };
}

function configYaml(params: string): string {
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
        ${params}
`;
}

beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'chunk-inspector-'));
    dbPath = path.join(tmpDir, 'test.db');
});

afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
    delete process.env.CHUNK_INSPECTOR_TEST_DB;
});

describe('lookupChunks (sqlite)', () => {
    it('returns the chunks stored for a URL in page order, with creation dates and content', async () => {
        const db = createDbFile();
        const embedding = [0.1, 0.2, 0.3, 0.4];
        // Insert out of page order to prove the result is sorted by position
        DatabaseManager.insertVectorsSQLite(db, makeChunk({ chunk_id: 'chunk-second', content: 'Second chunk', chunk_index: 1, total_chunks: 2 }), embedding, testLogger);
        DatabaseManager.insertVectorsSQLite(db, makeChunk({ chunk_id: 'chunk-first', content: 'First chunk', chunk_index: 0, total_chunks: 2 }), embedding, testLogger);
        db.prepare(`UPDATE vec_chunk_dates SET created_at = ? WHERE chunk_id = ?`).run('2026-01-01T00:00:00.000Z', 'chunk-first');
        db.prepare(`UPDATE vec_chunk_dates SET created_at = ? WHERE chunk_id = ?`).run('2026-06-01T00:00:00.000Z', 'chunk-second');
        db.close();

        const result = await lookupChunks(
            configYaml(`db_path: ${dbPath}`),
            { product_name: 'TestSite' },
            'https://example.com/page'
        );

        expect(result.database).toEqual({ type: 'sqlite', path: dbPath });
        expect(result.source).toEqual({ product_name: 'TestSite', type: 'website', version: '1.0' });
        expect(result.chunks).toHaveLength(2);
        expect(result.chunks[0].chunk_id).toBe('chunk-first');
        expect(result.chunks[0].created_at).toBe('2026-01-01T00:00:00.000Z');
        expect(result.chunks[0].content).toBe('First chunk');
        expect(result.chunks[0].heading_hierarchy).toEqual(['Guide', 'Install']);
        expect(result.chunks[1].chunk_id).toBe('chunk-second');
        expect(result.chunks[1].created_at).toBe('2026-06-01T00:00:00.000Z');
    });

    it('returns an empty list for a URL with no chunks', async () => {
        createDbFile().close();

        const result = await lookupChunks(
            configYaml(`db_path: ${dbPath}`),
            { product_name: 'TestSite' },
            'https://example.com/nope'
        );

        expect(result.chunks).toEqual([]);
    });

    it('returns chunks with a null created_at for databases predating the dates table', async () => {
        const db = createDbFile();
        DatabaseManager.insertVectorsSQLite(db, makeChunk(), [0.1, 0.2, 0.3, 0.4], testLogger);
        db.exec(`DROP TABLE vec_chunk_dates;`);
        db.close();

        const result = await lookupChunks(
            configYaml(`db_path: ${dbPath}`),
            { product_name: 'TestSite' },
            'https://example.com/page'
        );

        expect(result.chunks).toHaveLength(1);
        expect(result.chunks[0].created_at).toBeNull();
    });

    it('substitutes ${ENV_VAR} placeholders in the config like a sync job would', async () => {
        const db = createDbFile();
        DatabaseManager.insertVectorsSQLite(db, makeChunk(), [0.1, 0.2, 0.3, 0.4], testLogger);
        db.close();
        process.env.CHUNK_INSPECTOR_TEST_DB = dbPath;

        const result = await lookupChunks(
            // eslint-disable-next-line no-template-curly-in-string
            configYaml('db_path: ${CHUNK_INSPECTOR_TEST_DB}'),
            { product_name: 'TestSite' },
            'https://example.com/page'
        );

        expect(result.chunks).toHaveLength(1);
    });

    it('throws NotFoundError for a source that is not in the config', async () => {
        await expect(lookupChunks(
            configYaml(`db_path: ${dbPath}`),
            { product_name: 'Unknown' },
            'https://example.com/page'
        )).rejects.toBeInstanceOf(NotFoundError);
    });

    it('throws NotFoundError when the database file does not exist yet', async () => {
        await expect(lookupChunks(
            configYaml(`db_path: ${path.join(tmpDir, 'missing.db')}`),
            { product_name: 'TestSite' },
            'https://example.com/page'
        )).rejects.toBeInstanceOf(NotFoundError);
    });

    it('matches a source by version when several share a product name', async () => {
        const db = createDbFile();
        DatabaseManager.insertVectorsSQLite(db, makeChunk(), [0.1, 0.2, 0.3, 0.4], testLogger);
        db.close();
        const otherDbPath = path.join(tmpDir, 'other.db');
        const content = `
sources:
  - type: website
    product_name: TestSite
    version: '2.0'
    max_size: 1000
    url: https://example.com/v2
    database_config:
      type: sqlite
      params:
        db_path: ${otherDbPath}
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

        const result = await lookupChunks(content, { product_name: 'TestSite', version: '1.0' }, 'https://example.com/page');

        expect(result.database).toEqual({ type: 'sqlite', path: dbPath });
        expect(result.chunks).toHaveLength(1);
    });
});
