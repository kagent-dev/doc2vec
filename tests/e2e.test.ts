import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as http from 'http';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import BetterSqlite3 from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';
import { QdrantClient } from '@qdrant/js-client-rest';
import { ContentProcessor } from '../content-processor';
import { DatabaseManager } from '../database';
import { Utils } from '../utils';
import { Logger, LogLevel } from '../logger';
import {
    DocumentChunk,
    SqliteDB,
    QdrantDB,
    LocalDirectorySourceConfig,
    CodeSourceConfig,
    WebsiteSourceConfig,
} from '../types';

// ─── Shared Helpers ─────────────────────────────────────────────────────────

const testLogger = new Logger('e2e', { level: LogLevel.NONE });

/**
 * Creates an in-memory SQLite database with the full vec_items schema.
 */
function createE2EDatabase(): { db: BetterSqlite3.Database; dbConnection: SqliteDB } {
    const db = new BetterSqlite3(':memory:', { allowExtension: true } as any);
    sqliteVec.load(db);

    db.exec(`
        CREATE VIRTUAL TABLE IF NOT EXISTS vec_items USING vec0(
            embedding FLOAT[3072],
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

    return { db, dbConnection: { db, type: 'sqlite' } };
}

/**
 * Creates an embedding tracker that returns deterministic embeddings
 * and records which URLs have been embedded.
 */
function createEmbeddingTracker() {
    const embeddedUrls = new Set<string>();
    let embedCallCount = 0;

    const embedding = new Array(3072).fill(0.1);

    return {
        embedding,
        embeddedUrls,
        get embedCallCount() { return embedCallCount; },
        embed(url: string): number[] {
            embeddedUrls.add(url);
            embedCallCount++;
            return embedding;
        },
        reset() {
            embeddedUrls.clear();
            embedCallCount = 0;
        },
    };
}

/**
 * Replicates Doc2Vec.processChunksForUrl using public APIs.
 * Compares chunk hashes per URL: skip if unchanged, delete+re-insert if changed.
 * Returns the number of chunks that were embedded.
 */
async function processChunksForUrl(
    chunks: DocumentChunk[],
    url: string,
    db: BetterSqlite3.Database,
    embedFn: (url: string) => number[],
    logger: Logger
): Promise<number> {
    if (chunks.length === 0) return 0;

    // Compute hashes for all new chunks
    const newHashes = chunks.map(c => Utils.generateHash(c.content));
    const newHashesSorted = newHashes.slice().sort();

    // Fetch existing hashes for this URL
    const existingHashesSorted = DatabaseManager.getChunkHashesByUrlSQLite(db, url);

    // Compare: if identical sorted arrays, content is unchanged
    const unchanged = newHashesSorted.length === existingHashesSorted.length &&
        newHashesSorted.every((h, i) => h === existingHashesSorted[i]);

    if (unchanged) {
        return 0;
    }

    // Changed: delete all existing chunks for this URL
    if (existingHashesSorted.length > 0) {
        DatabaseManager.removeChunksByUrlSQLite(db, url, logger);
    }

    // Embed and insert all new chunks
    let embeddedCount = 0;
    for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        const chunkHash = newHashes[i];
        // Ensure branch/repo are non-null for vec0 TEXT columns
        chunk.metadata.branch = chunk.metadata.branch || '';
        chunk.metadata.repo = chunk.metadata.repo || '';
        const embedding = embedFn(url);
        DatabaseManager.insertVectorsSQLite(db, chunk, embedding, logger, chunkHash);
        embeddedCount++;
    }
    return embeddedCount;
}

/**
 * Returns all chunks for a URL, sorted by chunk_index.
 */
function getChunksForUrl(db: BetterSqlite3.Database, url: string) {
    const stmt = db.prepare(
        'SELECT chunk_id, content, hash, chunk_index, total_chunks FROM vec_items WHERE url = ? ORDER BY chunk_index'
    );
    return stmt.all(url) as {
        chunk_id: string;
        content: string;
        hash: string;
        chunk_index: number;
        total_chunks: number;
    }[];
}

/**
 * Returns the count of chunks for a URL.
 */
function countChunksForUrl(db: BetterSqlite3.Database, url: string): number {
    const stmt = db.prepare('SELECT COUNT(*) as count FROM vec_items WHERE url = ?');
    const row = stmt.get(url) as { count: number };
    return Number(row.count);
}

/**
 * Returns the total count of all chunks in the database.
 */
function countAllChunks(db: BetterSqlite3.Database): number {
    const stmt = db.prepare('SELECT COUNT(*) as count FROM vec_items');
    const row = stmt.get() as { count: number };
    return Number(row.count);
}

// ─── Local Directory (Markdown) E2E ─────────────────────────────────────────

describe('E2E: Local Directory (Markdown)', () => {
    let tempDir: string;
    let db: BetterSqlite3.Database;
    let dbConnection: SqliteDB;
    let tracker: ReturnType<typeof createEmbeddingTracker>;
    let processor: ContentProcessor;

    const config: LocalDirectorySourceConfig = {
        type: 'local_directory',
        product_name: 'e2e-test',
        version: '1.0',
        path: '', // set in beforeEach
        max_size: 1048576,
        include_extensions: ['.md'],
        recursive: true,
        database_config: { type: 'sqlite', params: {} },
    };

    beforeEach(() => {
        tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'e2e-md-'));
        config.path = tempDir;

        fs.writeFileSync(path.join(tempDir, 'doc1.md'), [
            '# Document One',
            '',
            'This is the first document covering installation procedures.',
            '',
            'Follow the steps below to get started with the platform.',
        ].join('\n'));

        fs.writeFileSync(path.join(tempDir, 'doc2.md'), [
            '# Document Two',
            '',
            'This is the second document about configuration options.',
            '',
            'You can customize the behavior using environment variables.',
        ].join('\n'));

        fs.writeFileSync(path.join(tempDir, 'doc3.md'), [
            '# Document Three',
            '',
            'This is the third document describing the API reference.',
            '',
            'All endpoints require authentication via bearer tokens.',
        ].join('\n'));

        const created = createE2EDatabase();
        db = created.db;
        dbConnection = created.dbConnection;
        tracker = createEmbeddingTracker();
        processor = new ContentProcessor(testLogger);
    });

    afterEach(() => {
        db.close();
        fs.rmSync(tempDir, { recursive: true, force: true });
    });

    it('should embed all files on first run and only modified file on second run', async () => {
        // ── Run 1: Process all 3 files ──────────────────────────────
        const files = fs.readdirSync(tempDir).filter(f => f.endsWith('.md')).sort();
        expect(files).toHaveLength(3);

        for (const file of files) {
            const filePath = path.join(tempDir, file);
            const content = fs.readFileSync(filePath, 'utf-8');
            const fileUrl = `file://${filePath}`;
            const chunks = await processor.chunkMarkdown(content, config, fileUrl);
            expect(chunks.length).toBeGreaterThan(0);
            await processChunksForUrl(chunks, fileUrl, db, (url) => tracker.embed(url), testLogger);
        }

        // All 3 files should have been embedded
        expect(tracker.embeddedUrls.size).toBe(3);
        const totalChunksRun1 = countAllChunks(db);
        expect(totalChunksRun1).toBeGreaterThan(0);
        const run1EmbedCount = tracker.embedCallCount;

        // Verify chunk_index / total_chunks consistency for each file
        for (const file of files) {
            const fileUrl = `file://${path.join(tempDir, file)}`;
            const chunks = getChunksForUrl(db, fileUrl);
            expect(chunks.length).toBeGreaterThan(0);
            for (let i = 0; i < chunks.length; i++) {
                expect(Number(chunks[i].chunk_index)).toBe(i);
                expect(Number(chunks[i].total_chunks)).toBe(chunks.length);
            }
        }

        // Save doc1 and doc3 chunk data for comparison after Run 2
        const doc1UrlBefore = `file://${path.join(tempDir, 'doc1.md')}`;
        const doc3UrlBefore = `file://${path.join(tempDir, 'doc3.md')}`;
        const doc1ChunksBefore = getChunksForUrl(db, doc1UrlBefore);
        const doc3ChunksBefore = getChunksForUrl(db, doc3UrlBefore);

        // ── Modify doc2.md ──────────────────────────────────────────
        tracker.reset();
        const doc2Path = path.join(tempDir, 'doc2.md');
        const doc2Content = fs.readFileSync(doc2Path, 'utf-8');
        fs.writeFileSync(doc2Path, doc2Content + '\n\nThis is a newly added paragraph about advanced configuration patterns.\n');

        // ── Run 2: Re-process all 3 files ───────────────────────────
        for (const file of files) {
            const filePath = path.join(tempDir, file);
            const content = fs.readFileSync(filePath, 'utf-8');
            const fileUrl = `file://${filePath}`;
            const chunks = await processor.chunkMarkdown(content, config, fileUrl);
            await processChunksForUrl(chunks, fileUrl, db, (url) => tracker.embed(url), testLogger);
        }

        // Only doc2 should have been re-embedded
        const doc2Url = `file://${doc2Path}`;
        expect(tracker.embeddedUrls.size).toBe(1);
        expect(tracker.embeddedUrls.has(doc2Url)).toBe(true);

        // doc1 and doc3 should NOT have been re-embedded
        expect(tracker.embeddedUrls.has(doc1UrlBefore)).toBe(false);
        expect(tracker.embeddedUrls.has(doc3UrlBefore)).toBe(false);

        // doc1 and doc3 chunks should be completely unchanged
        const doc1ChunksAfter = getChunksForUrl(db, doc1UrlBefore);
        const doc3ChunksAfter = getChunksForUrl(db, doc3UrlBefore);
        expect(doc1ChunksAfter).toEqual(doc1ChunksBefore);
        expect(doc3ChunksAfter).toEqual(doc3ChunksBefore);

        // doc2 chunks should have correct chunk_index/total_chunks
        const doc2Chunks = getChunksForUrl(db, doc2Url);
        expect(doc2Chunks.length).toBeGreaterThan(0);
        for (let i = 0; i < doc2Chunks.length; i++) {
            expect(Number(doc2Chunks[i].chunk_index)).toBe(i);
            expect(Number(doc2Chunks[i].total_chunks)).toBe(doc2Chunks.length);
        }

        // No orphaned chunks: total DB count should be consistent
        const totalChunksRun2 = countAllChunks(db);
        // The total might differ from run1 (doc2 may now produce more or fewer chunks)
        // but it should equal the sum of chunks across all 3 URLs
        const expectedTotal = countChunksForUrl(db, doc1UrlBefore) +
            countChunksForUrl(db, doc2Url) +
            countChunksForUrl(db, doc3UrlBefore);
        expect(totalChunksRun2).toBe(expectedTotal);
    });
});

// ─── Code Source (Local Directory) E2E ──────────────────────────────────────

describe('E2E: Code Source (Local Directory)', () => {
    let tempDir: string;
    let db: BetterSqlite3.Database;
    let dbConnection: SqliteDB;
    let tracker: ReturnType<typeof createEmbeddingTracker>;
    let processor: ContentProcessor;

    const config: CodeSourceConfig = {
        type: 'code',
        source: 'local_directory',
        product_name: 'e2e-code',
        version: '1.0',
        path: '', // set in beforeEach
        max_size: 1048576,
        include_extensions: ['.ts'],
        recursive: true,
        database_config: { type: 'sqlite', params: {} },
    };

    beforeEach(() => {
        tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'e2e-code-'));
        config.path = tempDir;

        fs.writeFileSync(path.join(tempDir, 'file1.ts'), [
            'export function greet(name: string): string {',
            '    return `Hello, ${name}! Welcome to the platform.`;',
            '}',
            '',
            'export function farewell(name: string): string {',
            '    return `Goodbye, ${name}! See you next time.`;',
            '}',
        ].join('\n'));

        fs.writeFileSync(path.join(tempDir, 'file2.ts'), [
            'export class Calculator {',
            '    add(a: number, b: number): number {',
            '        return a + b;',
            '    }',
            '',
            '    multiply(a: number, b: number): number {',
            '        return a * b;',
            '    }',
            '}',
        ].join('\n'));

        fs.writeFileSync(path.join(tempDir, 'file3.ts'), [
            'export async function fetchData(url: string): Promise<Response> {',
            '    const response = await fetch(url);',
            '    if (!response.ok) {',
            '        throw new Error(`HTTP error: ${response.status}`);',
            '    }',
            '    return response;',
            '}',
        ].join('\n'));

        const created = createE2EDatabase();
        db = created.db;
        dbConnection = created.dbConnection;
        tracker = createEmbeddingTracker();
        processor = new ContentProcessor(testLogger);
    });

    afterEach(() => {
        db.close();
        fs.rmSync(tempDir, { recursive: true, force: true });
    });

    it('should embed all code files on first run and only modified file on second run', async () => {
        const files = fs.readdirSync(tempDir).filter(f => f.endsWith('.ts')).sort();
        expect(files).toHaveLength(3);

        // ── Run 1: Process all 3 files ──────────────────────────────
        for (const file of files) {
            const filePath = path.join(tempDir, file);
            const content = fs.readFileSync(filePath, 'utf-8');
            const relativePath = file;
            const fileUrl = `file://${filePath}`;
            const chunks = await processor.chunkCode(content, config, fileUrl, relativePath);
            expect(chunks.length).toBeGreaterThan(0);
            await processChunksForUrl(chunks, fileUrl, db, (url) => tracker.embed(url), testLogger);
        }

        // All 3 files should have been embedded
        expect(tracker.embeddedUrls.size).toBe(3);
        const totalChunksRun1 = countAllChunks(db);
        expect(totalChunksRun1).toBeGreaterThan(0);

        // Verify chunk_index / total_chunks consistency
        for (const file of files) {
            const fileUrl = `file://${path.join(tempDir, file)}`;
            const chunks = getChunksForUrl(db, fileUrl);
            for (let i = 0; i < chunks.length; i++) {
                expect(Number(chunks[i].chunk_index)).toBe(i);
                expect(Number(chunks[i].total_chunks)).toBe(chunks.length);
            }
        }

        // Save file1 and file3 chunk data
        const file1Url = `file://${path.join(tempDir, 'file1.ts')}`;
        const file3Url = `file://${path.join(tempDir, 'file3.ts')}`;
        const file1ChunksBefore = getChunksForUrl(db, file1Url);
        const file3ChunksBefore = getChunksForUrl(db, file3Url);

        // ── Modify file2.ts ─────────────────────────────────────────
        tracker.reset();
        const file2Path = path.join(tempDir, 'file2.ts');
        fs.writeFileSync(file2Path, [
            'export class Calculator {',
            '    add(a: number, b: number): number {',
            '        return a + b;',
            '    }',
            '',
            '    subtract(a: number, b: number): number {',
            '        return a - b;',
            '    }',
            '',
            '    multiply(a: number, b: number): number {',
            '        return a * b;',
            '    }',
            '',
            '    divide(a: number, b: number): number {',
            '        if (b === 0) throw new Error("Division by zero");',
            '        return a / b;',
            '    }',
            '}',
        ].join('\n'));

        // ── Run 2: Re-process all 3 files ───────────────────────────
        for (const file of files) {
            const filePath = path.join(tempDir, file);
            const content = fs.readFileSync(filePath, 'utf-8');
            const relativePath = file;
            const fileUrl = `file://${filePath}`;
            const chunks = await processor.chunkCode(content, config, fileUrl, relativePath);
            await processChunksForUrl(chunks, fileUrl, db, (url) => tracker.embed(url), testLogger);
        }

        // Only file2 should have been re-embedded
        const file2Url = `file://${file2Path}`;
        expect(tracker.embeddedUrls.size).toBe(1);
        expect(tracker.embeddedUrls.has(file2Url)).toBe(true);
        expect(tracker.embeddedUrls.has(file1Url)).toBe(false);
        expect(tracker.embeddedUrls.has(file3Url)).toBe(false);

        // file1 and file3 chunks should be unchanged
        const file1ChunksAfter = getChunksForUrl(db, file1Url);
        const file3ChunksAfter = getChunksForUrl(db, file3Url);
        expect(file1ChunksAfter).toEqual(file1ChunksBefore);
        expect(file3ChunksAfter).toEqual(file3ChunksBefore);

        // file2 chunks should have correct chunk_index/total_chunks
        const file2Chunks = getChunksForUrl(db, file2Url);
        expect(file2Chunks.length).toBeGreaterThan(0);
        for (let i = 0; i < file2Chunks.length; i++) {
            expect(Number(file2Chunks[i].chunk_index)).toBe(i);
            expect(Number(file2Chunks[i].total_chunks)).toBe(file2Chunks.length);
        }

        // No orphaned chunks
        const totalChunksRun2 = countAllChunks(db);
        const expectedTotal = countChunksForUrl(db, file1Url) +
            countChunksForUrl(db, file2Url) +
            countChunksForUrl(db, file3Url);
        expect(totalChunksRun2).toBe(expectedTotal);
    });
});

// ─── Website Source E2E ─────────────────────────────────────────────────────

describe('E2E: Website Source', () => {
    let server: http.Server;
    let baseUrl: string;
    let db: BetterSqlite3.Database;
    let dbConnection: SqliteDB;
    let tracker: ReturnType<typeof createEmbeddingTracker>;
    let processor: ContentProcessor;

    // Mutable page content map — allows modifying page content between runs
    const pageContent: Record<string, string> = {};

    beforeEach(async () => {
        // Set up page content
        pageContent['/page1'] = `
            <html><head><title>Page 1</title></head><body>
            <article>
                <h1>Page One</h1>
                <p>This is the first page covering deployment strategies for production environments.</p>
                <p>It includes details about rolling updates and blue-green deployments.</p>
            </article>
            </body></html>
        `;
        pageContent['/page2'] = `
            <html><head><title>Page 2</title></head><body>
            <article>
                <h1>Page Two</h1>
                <p>This is the second page about monitoring and observability best practices.</p>
                <p>Learn how to set up dashboards and configure alerting rules.</p>
            </article>
            </body></html>
        `;
        pageContent['/page3'] = `
            <html><head><title>Page 3</title></head><body>
            <article>
                <h1>Page Three</h1>
                <p>This is the third page describing security hardening procedures.</p>
                <p>Covers TLS configuration, network policies, and access control.</p>
            </article>
            </body></html>
        `;

        // Start local HTTP server
        server = http.createServer((req, res) => {
            const content = pageContent[req.url || ''];
            if (content) {
                res.writeHead(200, { 'Content-Type': 'text/html' });
                res.end(content);
            } else {
                res.writeHead(404);
                res.end('Not found');
            }
        });

        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        if (!address || typeof address === 'string') {
            throw new Error('Failed to start test server');
        }
        baseUrl = `http://127.0.0.1:${address.port}`;

        const created = createE2EDatabase();
        db = created.db;
        dbConnection = created.dbConnection;
        tracker = createEmbeddingTracker();
        processor = new ContentProcessor(testLogger);
    });

    afterEach(() => {
        db.close();
        server.close();
    });

    it('should embed all pages on first run and only modified page on second run', async () => {
        const pages = ['/page1', '/page2', '/page3'];
        const config: WebsiteSourceConfig = {
            type: 'website',
            product_name: 'e2e-website',
            version: '1.0',
            url: baseUrl,
            max_size: 1048576,
            database_config: { type: 'sqlite', params: {} },
        };

        // ── Run 1: Process all 3 pages via Puppeteer ────────────────
        for (const pagePath of pages) {
            const url = `${baseUrl}${pagePath}`;
            const result = await processor.processPage(url, config);

            expect(result.content).not.toBeNull();
            const chunks = await processor.chunkMarkdown(result.content!, config, url);
            expect(chunks.length).toBeGreaterThan(0);
            await processChunksForUrl(chunks, url, db, (u) => tracker.embed(u), testLogger);
        }

        // All 3 pages should have been embedded
        expect(tracker.embeddedUrls.size).toBe(3);
        const totalChunksRun1 = countAllChunks(db);
        expect(totalChunksRun1).toBeGreaterThan(0);

        // Verify chunk_index / total_chunks consistency
        for (const pagePath of pages) {
            const url = `${baseUrl}${pagePath}`;
            const chunks = getChunksForUrl(db, url);
            expect(chunks.length).toBeGreaterThan(0);
            for (let i = 0; i < chunks.length; i++) {
                expect(Number(chunks[i].chunk_index)).toBe(i);
                expect(Number(chunks[i].total_chunks)).toBe(chunks.length);
            }
        }

        // Save page1 and page3 chunk data
        const page1Url = `${baseUrl}/page1`;
        const page3Url = `${baseUrl}/page3`;
        const page1ChunksBefore = getChunksForUrl(db, page1Url);
        const page3ChunksBefore = getChunksForUrl(db, page3Url);

        // ── Modify page 2 content ───────────────────────────────────
        tracker.reset();
        pageContent['/page2'] = `
            <html><head><title>Page 2</title></head><body>
            <article>
                <h1>Page Two</h1>
                <p>This is the second page about monitoring and observability best practices.</p>
                <p>This paragraph was added between runs to test change detection.</p>
                <p>Learn how to set up dashboards and configure alerting rules.</p>
            </article>
            </body></html>
        `;

        // ── Run 2: Re-process all 3 pages ───────────────────────────
        for (const pagePath of pages) {
            const url = `${baseUrl}${pagePath}`;
            const result = await processor.processPage(url, config);

            expect(result.content).not.toBeNull();
            const chunks = await processor.chunkMarkdown(result.content!, config, url);
            await processChunksForUrl(chunks, url, db, (u) => tracker.embed(u), testLogger);
        }

        // Only page2 should have been re-embedded
        const page2Url = `${baseUrl}/page2`;
        expect(tracker.embeddedUrls.size).toBe(1);
        expect(tracker.embeddedUrls.has(page2Url)).toBe(true);
        expect(tracker.embeddedUrls.has(page1Url)).toBe(false);
        expect(tracker.embeddedUrls.has(page3Url)).toBe(false);

        // page1 and page3 chunks should be unchanged
        const page1ChunksAfter = getChunksForUrl(db, page1Url);
        const page3ChunksAfter = getChunksForUrl(db, page3Url);
        expect(page1ChunksAfter).toEqual(page1ChunksBefore);
        expect(page3ChunksAfter).toEqual(page3ChunksBefore);

        // page2 chunks should have correct chunk_index/total_chunks
        const page2Chunks = getChunksForUrl(db, page2Url);
        expect(page2Chunks.length).toBeGreaterThan(0);
        for (let i = 0; i < page2Chunks.length; i++) {
            expect(Number(page2Chunks[i].chunk_index)).toBe(i);
            expect(Number(page2Chunks[i].total_chunks)).toBe(page2Chunks.length);
        }

        // No orphaned chunks
        const totalChunksRun2 = countAllChunks(db);
        const expectedTotal = countChunksForUrl(db, page1Url) +
            countChunksForUrl(db, page2Url) +
            countChunksForUrl(db, page3Url);
        expect(totalChunksRun2).toBe(expectedTotal);
    }, 60000); // 60s timeout for Puppeteer-based test
});

// ─── Qdrant Website Source E2E (conditional) ────────────────────────────────

const QDRANT_API_KEY = process.env.QDRANT_API_KEY;
const QDRANT_URL = process.env.QDRANT_URL;
const QDRANT_PORT = process.env.QDRANT_PORT;
const QDRANT_TEST_COLLECTION = process.env.QDRANT_TEST_COLLECTION;

const canRunQdrantTests = !!(QDRANT_API_KEY && QDRANT_URL && QDRANT_PORT && QDRANT_TEST_COLLECTION);

/**
 * Replicates Doc2Vec.processChunksForUrl for Qdrant.
 * Compares chunk hashes per URL: skip if unchanged, delete+re-insert if changed.
 */
async function processChunksForUrlQdrant(
    chunks: DocumentChunk[],
    url: string,
    qdrantDb: QdrantDB,
    embedFn: (url: string) => number[],
    logger: Logger
): Promise<number> {
    if (chunks.length === 0) return 0;

    const newHashes = chunks.map(c => Utils.generateHash(c.content));
    const newHashesSorted = newHashes.slice().sort();

    const existingHashesSorted = await DatabaseManager.getChunkHashesByUrlQdrant(qdrantDb, url);

    const unchanged = newHashesSorted.length === existingHashesSorted.length &&
        newHashesSorted.every((h, i) => h === existingHashesSorted[i]);

    if (unchanged) {
        return 0;
    }

    if (existingHashesSorted.length > 0) {
        await DatabaseManager.removeChunksByUrlQdrant(qdrantDb, url, logger);
    }

    let embeddedCount = 0;
    for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        const chunkHash = newHashes[i];
        chunk.metadata.branch = chunk.metadata.branch || '';
        chunk.metadata.repo = chunk.metadata.repo || '';
        const embedding = embedFn(url);
        await DatabaseManager.storeChunkInQdrant(qdrantDb, chunk, embedding, chunkHash);
        embeddedCount++;
    }
    return embeddedCount;
}

/**
 * Returns chunks for a URL from Qdrant, sorted by chunk_index.
 */
async function getChunksForUrlQdrant(qdrantDb: QdrantDB, url: string) {
    const { client, collectionName } = qdrantDb;
    const response = await client.scroll(collectionName, {
        limit: 10000,
        with_payload: true,
        with_vector: false,
        filter: {
            must: [{ key: 'url', match: { value: url } }],
            must_not: [{ key: 'is_metadata', match: { value: true } }],
        },
    });
    return response.points
        .map((p: any) => ({
            chunk_id: p.payload?.original_chunk_id as string,
            content: p.payload?.content as string,
            hash: p.payload?.hash as string,
            chunk_index: p.payload?.chunk_index as number,
            total_chunks: p.payload?.total_chunks as number,
        }))
        .sort((a: any, b: any) => a.chunk_index - b.chunk_index);
}

/**
 * Returns the count of non-metadata points in the collection.
 */
async function countAllChunksQdrant(qdrantDb: QdrantDB): Promise<number> {
    const { client, collectionName } = qdrantDb;
    const response = await client.scroll(collectionName, {
        limit: 10000,
        with_payload: false,
        with_vector: false,
        filter: {
            must_not: [{ key: 'is_metadata', match: { value: true } }],
        },
    });
    return response.points.length;
}

// Use describe.skipIf to conditionally skip when env vars are not set
describe.skipIf(!canRunQdrantTests)('E2E: Qdrant Website Source', () => {
    let server: http.Server;
    let baseUrl: string;
    let qdrantClient: QdrantClient;
    let qdrantDb: QdrantDB;
    let tracker: ReturnType<typeof createEmbeddingTracker>;
    let processor: ContentProcessor;

    const pageContent: Record<string, string> = {};

    beforeEach(async () => {
        // Set up page content
        pageContent['/page1'] = `
            <html><head><title>Page 1</title></head><body>
            <article>
                <h1>Page One</h1>
                <p>This is the first page covering deployment strategies for production environments.</p>
                <p>It includes details about rolling updates and blue-green deployments.</p>
            </article>
            </body></html>
        `;
        pageContent['/page2'] = `
            <html><head><title>Page 2</title></head><body>
            <article>
                <h1>Page Two</h1>
                <p>This is the second page about monitoring and observability best practices.</p>
                <p>Learn how to set up dashboards and configure alerting rules.</p>
            </article>
            </body></html>
        `;
        pageContent['/page3'] = `
            <html><head><title>Page 3</title></head><body>
            <article>
                <h1>Page Three</h1>
                <p>This is the third page describing security hardening procedures.</p>
                <p>Covers TLS configuration, network policies, and access control.</p>
            </article>
            </body></html>
        `;

        // Start local HTTP server
        server = http.createServer((req, res) => {
            const content = pageContent[req.url || ''];
            if (content) {
                res.writeHead(200, { 'Content-Type': 'text/html' });
                res.end(content);
            } else {
                res.writeHead(404);
                res.end('Not found');
            }
        });

        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        if (!address || typeof address === 'string') {
            throw new Error('Failed to start test server');
        }
        baseUrl = `http://127.0.0.1:${address.port}`;

        // Connect to Qdrant and delete the test collection if it exists
        qdrantClient = new QdrantClient({
            url: QDRANT_URL!,
            apiKey: QDRANT_API_KEY!,
            port: parseInt(QDRANT_PORT!, 10),
        });

        try {
            await qdrantClient.deleteCollection(QDRANT_TEST_COLLECTION!);
        } catch {
            // Collection may not exist yet — that's fine
        }

        // Create the collection
        await qdrantClient.createCollection(QDRANT_TEST_COLLECTION!, {
            vectors: { size: 3072, distance: 'Cosine' },
        });

        qdrantDb = {
            client: qdrantClient,
            collectionName: QDRANT_TEST_COLLECTION!,
            type: 'qdrant',
        };

        tracker = createEmbeddingTracker();
        processor = new ContentProcessor(testLogger);
    });

    afterEach(async () => {
        server.close();
        try {
            await qdrantClient.deleteCollection(QDRANT_TEST_COLLECTION!);
        } catch {
            // Ignore cleanup errors
        }
    });

    it('should embed all pages on first run and only modified page on second run', async () => {
        const pages = ['/page1', '/page2', '/page3'];
        const config: WebsiteSourceConfig = {
            type: 'website',
            product_name: 'e2e-qdrant',
            version: '1.0',
            url: baseUrl,
            max_size: 1048576,
            database_config: {
                type: 'qdrant',
                params: {
                    qdrant_url: QDRANT_URL!,
                    qdrant_port: parseInt(QDRANT_PORT!, 10),
                    collection_name: QDRANT_TEST_COLLECTION!,
                },
            },
        };

        // ── Run 1: Process all 3 pages via Puppeteer ────────────────
        for (const pagePath of pages) {
            const url = `${baseUrl}${pagePath}`;
            const result = await processor.processPage(url, config);

            expect(result.content).not.toBeNull();
            const chunks = await processor.chunkMarkdown(result.content!, config, url);
            expect(chunks.length).toBeGreaterThan(0);
            await processChunksForUrlQdrant(chunks, url, qdrantDb, (u) => tracker.embed(u), testLogger);
        }

        // All 3 pages should have been embedded
        expect(tracker.embeddedUrls.size).toBe(3);
        const totalChunksRun1 = await countAllChunksQdrant(qdrantDb);
        expect(totalChunksRun1).toBeGreaterThan(0);

        // Verify chunk_index / total_chunks consistency
        for (const pagePath of pages) {
            const url = `${baseUrl}${pagePath}`;
            const chunks = await getChunksForUrlQdrant(qdrantDb, url);
            expect(chunks.length).toBeGreaterThan(0);
            for (let i = 0; i < chunks.length; i++) {
                expect(Number(chunks[i].chunk_index)).toBe(i);
                expect(Number(chunks[i].total_chunks)).toBe(chunks.length);
            }
        }

        // Save page1 and page3 chunk data
        const page1Url = `${baseUrl}/page1`;
        const page3Url = `${baseUrl}/page3`;
        const page1ChunksBefore = await getChunksForUrlQdrant(qdrantDb, page1Url);
        const page3ChunksBefore = await getChunksForUrlQdrant(qdrantDb, page3Url);

        // ── Modify page 2 content ───────────────────────────────────
        tracker.reset();
        pageContent['/page2'] = `
            <html><head><title>Page 2</title></head><body>
            <article>
                <h1>Page Two</h1>
                <p>This is the second page about monitoring and observability best practices.</p>
                <p>This paragraph was added between runs to test change detection.</p>
                <p>Learn how to set up dashboards and configure alerting rules.</p>
            </article>
            </body></html>
        `;

        // ── Run 2: Re-process all 3 pages ───────────────────────────
        for (const pagePath of pages) {
            const url = `${baseUrl}${pagePath}`;
            const result = await processor.processPage(url, config);

            expect(result.content).not.toBeNull();
            const chunks = await processor.chunkMarkdown(result.content!, config, url);
            await processChunksForUrlQdrant(chunks, url, qdrantDb, (u) => tracker.embed(u), testLogger);
        }

        // Only page2 should have been re-embedded
        const page2Url = `${baseUrl}/page2`;
        expect(tracker.embeddedUrls.size).toBe(1);
        expect(tracker.embeddedUrls.has(page2Url)).toBe(true);
        expect(tracker.embeddedUrls.has(page1Url)).toBe(false);
        expect(tracker.embeddedUrls.has(page3Url)).toBe(false);

        // page1 and page3 chunks should be unchanged
        const page1ChunksAfter = await getChunksForUrlQdrant(qdrantDb, page1Url);
        const page3ChunksAfter = await getChunksForUrlQdrant(qdrantDb, page3Url);
        expect(page1ChunksAfter).toEqual(page1ChunksBefore);
        expect(page3ChunksAfter).toEqual(page3ChunksBefore);

        // page2 chunks should have correct chunk_index/total_chunks
        const page2Chunks = await getChunksForUrlQdrant(qdrantDb, page2Url);
        expect(page2Chunks.length).toBeGreaterThan(0);
        for (let i = 0; i < page2Chunks.length; i++) {
            expect(Number(page2Chunks[i].chunk_index)).toBe(i);
            expect(Number(page2Chunks[i].total_chunks)).toBe(page2Chunks.length);
        }

        // No orphaned chunks
        const totalChunksRun2 = await countAllChunksQdrant(qdrantDb);
        const page1Count = (await getChunksForUrlQdrant(qdrantDb, page1Url)).length;
        const page2Count = (await getChunksForUrlQdrant(qdrantDb, page2Url)).length;
        const page3Count = (await getChunksForUrlQdrant(qdrantDb, page3Url)).length;
        expect(totalChunksRun2).toBe(page1Count + page2Count + page3Count);
    }, 60000);
});
