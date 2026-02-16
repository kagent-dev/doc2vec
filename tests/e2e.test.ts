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

// ─── Website Multi-Sync E2E (full pipeline with all change detection layers) ─

/**
 * Helper: run one "sync" of the full website pipeline through crawlWebsite.
 * Mimics Doc2Vec.processWebsite — crawlWebsite drives the crawl, processPage
 * loads pages via Puppeteer, and the callback does chunking + embedding.
 *
 * Returns stats about what happened during this sync.
 */
async function runWebsiteSync(
    processor: ContentProcessor,
    config: WebsiteSourceConfig,
    dbConnection: SqliteDB,
    tracker: ReturnType<typeof createEmbeddingTracker>,
    options?: { failUrls?: Set<string>; forceFullSync?: boolean }
): Promise<{
    visitedUrls: Set<string>;
    processedUrls: Set<string>;
    embeddedUrls: Set<string>;
    embedCallCount: number;
    totalChunks: number;
}> {
    const visitedUrls = new Set<string>();
    const processedUrls = new Set<string>();
    const urlPrefix = Utils.getUrlPrefix(config.url);

    // Pre-load known URLs from DB
    const storedUrls = DatabaseManager.getStoredUrlsByPrefixSQLite(dbConnection.db, urlPrefix);
    const knownUrls = storedUrls.length > 0 ? new Set(storedUrls) : undefined;

    // ETag store
    const etagStore = {
        get: async (url: string) => DatabaseManager.getMetadataValue(dbConnection, `etag:${url}`, undefined, testLogger),
        set: async (url: string, etag: string) => DatabaseManager.setMetadataValue(dbConnection, `etag:${url}`, etag, testLogger, 3072),
    };

    // Lastmod store
    const lastmodStore = {
        get: async (url: string) => DatabaseManager.getMetadataValue(dbConnection, `lastmod:${url}`, undefined, testLogger),
        set: async (url: string, lastmod: string) => DatabaseManager.setMetadataValue(dbConnection, `lastmod:${url}`, lastmod, testLogger, 3072),
    };

    tracker.reset();

    await processor.crawlWebsite(
        config.url,
        config,
        async (url, content) => {
            processedUrls.add(url);

            // Simulate processing failure for specific URLs
            if (options?.failUrls?.has(url)) {
                return false;
            }

            const chunks = await processor.chunkMarkdown(content, config, url);
            const newHashes = chunks.map(c => Utils.generateHash(c.content));
            const newHashesSorted = newHashes.slice().sort();
            const existingHashesSorted = DatabaseManager.getChunkHashesByUrlSQLite(dbConnection.db, url);

            const unchanged = newHashesSorted.length === existingHashesSorted.length &&
                newHashesSorted.every((h, i) => h === existingHashesSorted[i]);

            if (unchanged) {
                return true;
            }

            // Changed — delete old, insert new
            if (existingHashesSorted.length > 0) {
                DatabaseManager.removeChunksByUrlSQLite(dbConnection.db, url, testLogger);
            }

            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];
                chunk.metadata.branch = chunk.metadata.branch || '';
                chunk.metadata.repo = chunk.metadata.repo || '';
                const embedding = tracker.embed(url);
                DatabaseManager.insertVectorsSQLite(dbConnection.db, chunk, embedding, testLogger, newHashes[i]);
            }

            return true;
        },
        testLogger,
        visitedUrls,
        { knownUrls, etagStore, lastmodStore, forceFullSync: options?.forceFullSync }
    );

    return {
        visitedUrls,
        processedUrls,
        embeddedUrls: new Set(tracker.embeddedUrls),
        embedCallCount: tracker.embedCallCount,
        totalChunks: countAllChunks(dbConnection.db),
    };
}

describe('E2E: Website Multi-Sync Change Detection', () => {
    let server: http.Server;
    let baseUrl: string;
    let db: BetterSqlite3.Database;
    let dbConnection: SqliteDB;
    let tracker: ReturnType<typeof createEmbeddingTracker>;
    let processor: ContentProcessor;

    // Mutable server state — controls what the HTTP server returns
    const pageContent: Record<string, string> = {};
    const pageEtags: Record<string, string> = {};
    let sitemapXml = '';
    let headRequestCount = 0;

    beforeEach(async () => {
        headRequestCount = 0;

        // Initial page content
        pageContent['/'] = `
            <html><head><title>Home</title></head><body>
            <article>
                <h1>Home Page</h1>
                <p>Welcome to the documentation site.</p>
                <a href="/page1">Page 1</a>
                <a href="/page2">Page 2</a>
                <a href="/page3">Page 3</a>
            </article>
            </body></html>
        `;
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

        // ETags per page
        pageEtags['/'] = '"etag-home-v1"';
        pageEtags['/page1'] = '"etag-page1-v1"';
        pageEtags['/page2'] = '"etag-page2-v1"';
        pageEtags['/page3'] = '"etag-page3-v1"';

        // Sitemap with lastmod for all pages
        sitemapXml = `<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>BASE_URL/</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page1</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page2</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page3</loc><lastmod>2026-01-01</lastmod></url>
            </urlset>`;

        // Start HTTP server with ETag and sitemap support
        server = http.createServer((req, res) => {
            const urlPath = req.url || '';

            // Serve sitemap
            if (urlPath === '/sitemap.xml') {
                res.writeHead(200, { 'Content-Type': 'application/xml' });
                res.end(sitemapXml.replace(/BASE_URL/g, baseUrl));
                return;
            }

            const content = pageContent[urlPath];
            if (content) {
                const etag = pageEtags[urlPath];
                const headers: Record<string, string> = { 'Content-Type': 'text/html' };
                if (etag) headers['ETag'] = etag;

                if (req.method === 'HEAD') {
                    headRequestCount++;
                    res.writeHead(200, headers);
                    res.end();
                } else {
                    res.writeHead(200, headers);
                    res.end(content);
                }
            } else {
                res.writeHead(404);
                res.end('Not found');
            }
        });

        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        if (!address || typeof address === 'string') throw new Error('Failed to start test server');
        baseUrl = `http://127.0.0.1:${(address as any).port}`;

        const created = createE2EDatabase();
        db = created.db;
        dbConnection = created.dbConnection;
        // Initialize metadata table
        await DatabaseManager.initDatabaseMetadata(dbConnection, testLogger);
        tracker = createEmbeddingTracker();
        processor = new ContentProcessor(testLogger);
    });

    afterEach(() => {
        db.close();
        server.close();
    });

    it('should use all 4 change detection layers across multiple syncs', async () => {
        const config: WebsiteSourceConfig = {
            type: 'website',
            product_name: 'e2e-multisync',
            version: '1.0',
            url: baseUrl + '/',
            max_size: 1048576,
            sitemap_url: baseUrl + '/sitemap.xml',
            database_config: { type: 'sqlite', params: {} },
        };

        // ═══════════════════════════════════════════════════════════════
        // RUN 1: Initial sync — all pages are new, everything embedded
        // ═══════════════════════════════════════════════════════════════
        const run1 = await runWebsiteSync(processor, config, dbConnection, tracker);

        // All 4 pages should be processed (home + page1-3)
        expect(run1.processedUrls.size).toBe(4);
        // All should be embedded (no prior data)
        expect(run1.embeddedUrls.size).toBe(4);
        expect(run1.totalChunks).toBeGreaterThan(0);
        const run1TotalChunks = run1.totalChunks;

        // Verify chunks exist for each page
        for (const pagePath of ['/', '/page1', '/page2', '/page3']) {
            const url = `${baseUrl}${pagePath}`;
            const chunks = getChunksForUrl(db, url);
            expect(chunks.length).toBeGreaterThan(0);
        }

        // ═══════════════════════════════════════════════════════════════
        // RUN 2: No changes — lastmod unchanged → all skipped at layer 1
        // No HEAD requests, no Puppeteer, no embedding
        // ═══════════════════════════════════════════════════════════════
        headRequestCount = 0;
        const run2 = await runWebsiteSync(processor, config, dbConnection, tracker);

        // No pages should be processed (all skipped via lastmod)
        expect(run2.processedUrls.size).toBe(0);
        // No HEAD requests made (lastmod bypasses ETag check)
        expect(headRequestCount).toBe(0);
        // No embeddings
        expect(run2.embedCallCount).toBe(0);
        // Chunks unchanged
        expect(run2.totalChunks).toBe(run1TotalChunks);

        // ═══════════════════════════════════════════════════════════════
        // RUN 3: page2 lastmod changes + content changes → re-embedded
        // page1 & page3 unchanged via lastmod → skipped
        // ═══════════════════════════════════════════════════════════════
        sitemapXml = `<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>BASE_URL/</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page1</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page2</loc><lastmod>2026-02-15</lastmod></url>
                <url><loc>BASE_URL/page3</loc><lastmod>2026-01-01</lastmod></url>
            </urlset>`;

        pageContent['/page2'] = `
            <html><head><title>Page 2</title></head><body>
            <article>
                <h1>Page Two — Updated</h1>
                <p>This page has been updated with new monitoring practices for cloud-native environments.</p>
                <p>Includes Prometheus, Grafana, and OpenTelemetry integration guides.</p>
            </article>
            </body></html>
        `;
        pageEtags['/page2'] = '"etag-page2-v2"';

        // Save page1 and page3 chunks for comparison
        const page1ChunksBefore = getChunksForUrl(db, `${baseUrl}/page1`);
        const page3ChunksBefore = getChunksForUrl(db, `${baseUrl}/page3`);

        headRequestCount = 0;
        const run3 = await runWebsiteSync(processor, config, dbConnection, tracker);

        // Only page2 and home should be visited by Puppeteer
        // (home has unchanged lastmod but page2 has changed)
        expect(run3.processedUrls.has(`${baseUrl}/page2`)).toBe(true);
        // page1 and page3 should NOT be processed
        expect(run3.processedUrls.has(`${baseUrl}/page1`)).toBe(false);
        expect(run3.processedUrls.has(`${baseUrl}/page3`)).toBe(false);
        // page2 content changed → re-embedded
        expect(run3.embeddedUrls.has(`${baseUrl}/page2`)).toBe(true);
        // page1 and page3 chunks should be completely unchanged
        expect(getChunksForUrl(db, `${baseUrl}/page1`)).toEqual(page1ChunksBefore);
        expect(getChunksForUrl(db, `${baseUrl}/page3`)).toEqual(page3ChunksBefore);
        // No HEAD requests for pages in sitemap (they all have lastmod)
        expect(headRequestCount).toBe(0);

        // ═══════════════════════════════════════════════════════════════
        // RUN 4: New page4 discovered via links (not in sitemap)
        // page1 updated to link to page4, with new lastmod.
        // page4 has no sitemap lastmod → falls through to ETag check
        // ═══════════════════════════════════════════════════════════════
        pageContent['/page1'] = `
            <html><head><title>Page 1</title></head><body>
            <article>
                <h1>Page One</h1>
                <p>This is the first page covering deployment strategies for production environments.</p>
                <p>It includes details about rolling updates and blue-green deployments.</p>
                <a href="/page4">See also: Advanced Deployment</a>
            </article>
            </body></html>
        `;
        pageEtags['/page1'] = '"etag-page1-v2"';

        pageContent['/page4'] = `
            <html><head><title>Page 4</title></head><body>
            <article>
                <h1>Page Four — Advanced Deployment</h1>
                <p>This page covers advanced deployment patterns including canary releases.</p>
                <p>Learn about traffic splitting and progressive delivery strategies.</p>
            </article>
            </body></html>
        `;
        pageEtags['/page4'] = '"etag-page4-v1"';

        // Update sitemap: page1 has new lastmod, page4 NOT in sitemap
        sitemapXml = `<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>BASE_URL/</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page1</loc><lastmod>2026-02-16</lastmod></url>
                <url><loc>BASE_URL/page2</loc><lastmod>2026-02-15</lastmod></url>
                <url><loc>BASE_URL/page3</loc><lastmod>2026-01-01</lastmod></url>
            </urlset>`;

        headRequestCount = 0;
        const page2ChunksBeforeRun4 = getChunksForUrl(db, `${baseUrl}/page2`);
        const page3ChunksBeforeRun4 = getChunksForUrl(db, `${baseUrl}/page3`);
        const run4 = await runWebsiteSync(processor, config, dbConnection, tracker);

        // page1 should be processed (lastmod changed)
        expect(run4.processedUrls.has(`${baseUrl}/page1`)).toBe(true);
        // page4 should be processed (discovered via link, no lastmod → ETag fallback)
        expect(run4.processedUrls.has(`${baseUrl}/page4`)).toBe(true);
        // page4 is new → embedded
        expect(run4.embeddedUrls.has(`${baseUrl}/page4`)).toBe(true);
        // page2 and page3 should be skipped (lastmod unchanged)
        expect(run4.processedUrls.has(`${baseUrl}/page2`)).toBe(false);
        expect(run4.processedUrls.has(`${baseUrl}/page3`)).toBe(false);
        expect(getChunksForUrl(db, `${baseUrl}/page2`)).toEqual(page2ChunksBeforeRun4);
        expect(getChunksForUrl(db, `${baseUrl}/page3`)).toEqual(page3ChunksBeforeRun4);
        // HEAD request should be made for page4 (not in sitemap) but not for others
        expect(headRequestCount).toBeGreaterThan(0);
        // page4 chunks should exist now
        const page4Chunks = getChunksForUrl(db, `${baseUrl}/page4`);
        expect(page4Chunks.length).toBeGreaterThan(0);

        // ═══════════════════════════════════════════════════════════════
        // RUN 5: Processing failure → lastmod/etag should NOT be stored
        // Change page3 lastmod so it gets processed, but simulate failure.
        // On run 6 (hypothetical), page3 should be reprocessed.
        // ═══════════════════════════════════════════════════════════════
        sitemapXml = `<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>BASE_URL/</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page1</loc><lastmod>2026-02-16</lastmod></url>
                <url><loc>BASE_URL/page2</loc><lastmod>2026-02-15</lastmod></url>
                <url><loc>BASE_URL/page3</loc><lastmod>2026-03-01</lastmod></url>
            </urlset>`;

        pageContent['/page3'] = `
            <html><head><title>Page 3</title></head><body>
            <article>
                <h1>Page Three — Updated</h1>
                <p>Updated security hardening with zero-trust architecture patterns.</p>
                <p>New section on supply chain security and SBOM requirements.</p>
            </article>
            </body></html>
        `;
        pageEtags['/page3'] = '"etag-page3-v2"';

        const run5 = await runWebsiteSync(processor, config, dbConnection, tracker, {
            failUrls: new Set([`${baseUrl}/page3`]),
        });

        // page3 should be processed (lastmod changed) but fail
        expect(run5.processedUrls.has(`${baseUrl}/page3`)).toBe(true);
        // page3 should NOT be embedded (processing failed)
        expect(run5.embeddedUrls.has(`${baseUrl}/page3`)).toBe(false);
        // page3 chunks should still have the OLD content (from run 1)
        // because the failure prevented new chunks from being written
        expect(getChunksForUrl(db, `${baseUrl}/page3`)).toEqual(page3ChunksBefore);

        // ═══════════════════════════════════════════════════════════════
        // RUN 6: Retry after failure — page3 should be reprocessed
        // because lastmod was NOT stored on run 5 (processing failed)
        // ═══════════════════════════════════════════════════════════════
        const run6 = await runWebsiteSync(processor, config, dbConnection, tracker);

        // page3 should be processed again (lastmod wasn't stored)
        expect(run6.processedUrls.has(`${baseUrl}/page3`)).toBe(true);
        // This time it succeeds → new content should be embedded
        expect(run6.embeddedUrls.has(`${baseUrl}/page3`)).toBe(true);
        // page3 chunks should now have the UPDATED content
        const page3ChunksAfterRetry = getChunksForUrl(db, `${baseUrl}/page3`);
        expect(page3ChunksAfterRetry.length).toBeGreaterThan(0);
        expect(page3ChunksAfterRetry).not.toEqual(page3ChunksBefore);

        // ═══════════════════════════════════════════════════════════════
        // Final verification: all chunks accounted for, no orphans
        // ═══════════════════════════════════════════════════════════════
        const finalTotal = countAllChunks(db);
        const expectedTotal =
            countChunksForUrl(db, `${baseUrl}/`) +
            countChunksForUrl(db, `${baseUrl}/page1`) +
            countChunksForUrl(db, `${baseUrl}/page2`) +
            countChunksForUrl(db, `${baseUrl}/page3`) +
            countChunksForUrl(db, `${baseUrl}/page4`);
        expect(finalTotal).toBe(expectedTotal);
        expect(finalTotal).toBeGreaterThan(0);
    }, 120000); // 2 min timeout for 6 Puppeteer-based sync runs
});

// ─── Incomplete Sync Recovery E2E ───────────────────────────────────────────

describe('E2E: Website Incomplete Sync Recovery', () => {
    let server: http.Server;
    let baseUrl: string;
    let db: BetterSqlite3.Database;
    let dbConnection: SqliteDB;
    let tracker: ReturnType<typeof createEmbeddingTracker>;
    let processor: ContentProcessor;

    // Mutable server state
    const pageContent: Record<string, string> = {};
    const pageEtags: Record<string, string> = {};
    let sitemapXml = '';
    let headRequestCount = 0;

    beforeEach(async () => {
        headRequestCount = 0;

        pageContent['/'] = `
            <html><head><title>Home</title></head><body>
            <article>
                <h1>Home Page</h1>
                <p>Welcome to the documentation site.</p>
                <a href="/page1">Page 1</a>
                <a href="/page2">Page 2</a>
                <a href="/page3">Page 3</a>
            </article>
            </body></html>
        `;
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

        pageEtags['/'] = '"etag-home-v1"';
        pageEtags['/page1'] = '"etag-page1-v1"';
        pageEtags['/page2'] = '"etag-page2-v1"';
        pageEtags['/page3'] = '"etag-page3-v1"';

        sitemapXml = `<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>BASE_URL/</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page1</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page2</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page3</loc><lastmod>2026-01-01</lastmod></url>
            </urlset>`;

        server = http.createServer((req, res) => {
            const urlPath = req.url || '';
            if (urlPath === '/sitemap.xml') {
                res.writeHead(200, { 'Content-Type': 'application/xml' });
                res.end(sitemapXml.replace(/BASE_URL/g, baseUrl));
                return;
            }
            const content = pageContent[urlPath];
            if (content) {
                const etag = pageEtags[urlPath];
                const headers: Record<string, string> = { 'Content-Type': 'text/html' };
                if (etag) headers['ETag'] = etag;
                if (req.method === 'HEAD') {
                    headRequestCount++;
                    res.writeHead(200, headers);
                    res.end();
                } else {
                    res.writeHead(200, headers);
                    res.end(content);
                }
            } else {
                res.writeHead(404);
                res.end('Not found');
            }
        });

        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        if (!address || typeof address === 'string') throw new Error('Failed to start test server');
        baseUrl = `http://127.0.0.1:${(address as any).port}`;

        const created = createE2EDatabase();
        db = created.db;
        dbConnection = created.dbConnection;
        await DatabaseManager.initDatabaseMetadata(dbConnection, testLogger);
        tracker = createEmbeddingTracker();
        processor = new ContentProcessor(testLogger);
    });

    afterEach(() => {
        db.close();
        server.close();
    });

    it('should force-process all pages when forceFullSync is true (with sitemap lastmod)', async () => {
        const config: WebsiteSourceConfig = {
            type: 'website',
            product_name: 'e2e-incomplete',
            version: '1.0',
            url: baseUrl + '/',
            max_size: 1048576,
            sitemap_url: baseUrl + '/sitemap.xml',
            database_config: { type: 'sqlite', params: {} },
        };

        // ═══════════════════════════════════════════════════════════════
        // RUN 1: Normal first sync with forceFullSync=true — all pages
        // are processed. This simulates the initial sync.
        // ═══════════════════════════════════════════════════════════════
        const run1 = await runWebsiteSync(processor, config, dbConnection, tracker, {
            forceFullSync: true,
        });

        expect(run1.processedUrls.size).toBe(4);
        expect(run1.embeddedUrls.size).toBe(4);
        expect(run1.totalChunks).toBeGreaterThan(0);

        // ═══════════════════════════════════════════════════════════════
        // RUN 2: Simulate an interrupted sync recovery.
        // All pages now have stored lastmod values from Run 1, but
        // sync_complete was never set (process was killed).
        // With forceFullSync=true, ALL pages should be force-processed
        // despite having unchanged lastmod.
        // ═══════════════════════════════════════════════════════════════
        headRequestCount = 0;
        const run2 = await runWebsiteSync(processor, config, dbConnection, tracker, {
            forceFullSync: true,
        });

        // All 4 pages should be processed (force-processing bypasses lastmod skip)
        expect(run2.processedUrls.size).toBe(4);
        // No HEAD requests (pages have lastmod from sitemap, so ETag path is not used)
        expect(headRequestCount).toBe(0);
        // Content hasn't changed, so no re-embedding
        expect(run2.embedCallCount).toBe(0);

        // ═══════════════════════════════════════════════════════════════
        // RUN 3: Sync completed successfully, forceFullSync=false.
        // Normal skip behavior should apply — all pages skipped via lastmod.
        // ═══════════════════════════════════════════════════════════════
        headRequestCount = 0;
        const run3 = await runWebsiteSync(processor, config, dbConnection, tracker, {
            forceFullSync: false,
        });

        expect(run3.processedUrls.size).toBe(0);
        expect(headRequestCount).toBe(0);
        expect(run3.embedCallCount).toBe(0);
    }, 120000);

    it('should force-process all pages when forceFullSync is true (with ETag, no sitemap)', async () => {
        const config: WebsiteSourceConfig = {
            type: 'website',
            product_name: 'e2e-incomplete-etag',
            version: '1.0',
            url: baseUrl + '/',
            max_size: 1048576,
            // No sitemap — pages use ETag-based change detection
            database_config: { type: 'sqlite', params: {} },
        };

        // ═══════════════════════════════════════════════════════════════
        // RUN 1: Normal first sync — all pages processed, ETags stored.
        // ═══════════════════════════════════════════════════════════════
        const run1 = await runWebsiteSync(processor, config, dbConnection, tracker, {
            forceFullSync: true,
        });

        expect(run1.processedUrls.size).toBe(4);
        expect(run1.embeddedUrls.size).toBe(4);

        // ═══════════════════════════════════════════════════════════════
        // RUN 2: Simulate interrupted sync recovery. ETags are stored
        // from Run 1 but sync_complete was never set.
        // With forceFullSync=true, pages should be force-processed
        // despite ETag matching.
        // ═══════════════════════════════════════════════════════════════
        headRequestCount = 0;
        const run2 = await runWebsiteSync(processor, config, dbConnection, tracker, {
            forceFullSync: true,
        });

        // All 4 pages should be processed (force-processing bypasses ETag skip)
        expect(run2.processedUrls.size).toBe(4);
        // HEAD requests still happen (needed to get the ETag), but the
        // skip decision is overridden by forceFullSync
        expect(headRequestCount).toBeGreaterThan(0);
        // Content hasn't changed, so no re-embedding
        expect(run2.embedCallCount).toBe(0);

        // ═══════════════════════════════════════════════════════════════
        // RUN 3: Sync completed successfully, forceFullSync=false.
        // Normal ETag skip should apply — all pages skipped.
        // ═══════════════════════════════════════════════════════════════
        headRequestCount = 0;
        const run3 = await runWebsiteSync(processor, config, dbConnection, tracker, {
            forceFullSync: false,
        });

        expect(run3.processedUrls.size).toBe(0);
        expect(run3.embedCallCount).toBe(0);
    }, 120000);
});

// ─── In-memory Markdown Store (simulates Postgres MarkdownStore) ────────────

/**
 * In-memory implementation of the MarkdownStore interface for testing.
 * Tracks all operations (upserts and deletes) so tests can assert on behavior.
 */
class InMemoryMarkdownStore {
    private data = new Map<string, { productName: string; markdown: string }>();
    upsertCount = 0;
    deleteCount = 0;
    deletedUrls = new Set<string>();

    async getUrlsWithMarkdown(urlPrefix: string): Promise<Set<string>> {
        const urls = new Set<string>();
        for (const url of this.data.keys()) {
            if (url.startsWith(urlPrefix)) {
                urls.add(url);
            }
        }
        return urls;
    }

    async upsertMarkdown(url: string, productName: string, markdown: string): Promise<void> {
        this.data.set(url, { productName, markdown });
        this.upsertCount++;
    }

    async deleteMarkdown(url: string): Promise<void> {
        this.data.delete(url);
        this.deletedUrls.add(url);
        this.deleteCount++;
    }

    has(url: string): boolean {
        return this.data.has(url);
    }

    getMarkdown(url: string): string | undefined {
        return this.data.get(url)?.markdown;
    }

    get size(): number {
        return this.data.size;
    }

    resetCounters(): void {
        this.upsertCount = 0;
        this.deleteCount = 0;
        this.deletedUrls.clear();
    }
}

// ─── Markdown Store Multi-Sync E2E ──────────────────────────────────────────

/**
 * Helper: run one website sync with markdown store integration.
 * Mirrors runWebsiteSync but adds markdownStoreUrls to crawlWebsite options
 * and calls markdownStore.upsertMarkdown on successful page processing.
 */
async function runWebsiteSyncWithMarkdownStore(
    processor: ContentProcessor,
    config: WebsiteSourceConfig,
    dbConnection: SqliteDB,
    tracker: ReturnType<typeof createEmbeddingTracker>,
    markdownStore: InMemoryMarkdownStore,
    options?: { failUrls?: Set<string> }
): Promise<{
    visitedUrls: Set<string>;
    processedUrls: Set<string>;
    embeddedUrls: Set<string>;
    embedCallCount: number;
    totalChunks: number;
    notFoundUrls: Set<string>;
}> {
    const visitedUrls = new Set<string>();
    const processedUrls = new Set<string>();
    const urlPrefix = Utils.getUrlPrefix(config.url);

    // Pre-load known URLs from DB
    const storedUrls = DatabaseManager.getStoredUrlsByPrefixSQLite(dbConnection.db, urlPrefix);
    const knownUrls = storedUrls.length > 0 ? new Set(storedUrls) : undefined;

    // ETag store
    const etagStore = {
        get: async (url: string) => DatabaseManager.getMetadataValue(dbConnection, `etag:${url}`, undefined, testLogger),
        set: async (url: string, etag: string) => DatabaseManager.setMetadataValue(dbConnection, `etag:${url}`, etag, testLogger, 3072),
    };

    // Lastmod store
    const lastmodStore = {
        get: async (url: string) => DatabaseManager.getMetadataValue(dbConnection, `lastmod:${url}`, undefined, testLogger),
        set: async (url: string, lastmod: string) => DatabaseManager.setMetadataValue(dbConnection, `lastmod:${url}`, lastmod, testLogger, 3072),
    };

    // Markdown store URLs — load from the in-memory store
    const markdownStoreUrls = await markdownStore.getUrlsWithMarkdown(urlPrefix);

    tracker.reset();
    markdownStore.resetCounters();

    const crawlResult = await processor.crawlWebsite(
        config.url,
        config,
        async (url, content) => {
            processedUrls.add(url);

            // Simulate processing failure for specific URLs
            if (options?.failUrls?.has(url)) {
                return false;
            }

            const chunks = await processor.chunkMarkdown(content, config, url);
            const newHashes = chunks.map(c => Utils.generateHash(c.content));
            const newHashesSorted = newHashes.slice().sort();
            const existingHashesSorted = DatabaseManager.getChunkHashesByUrlSQLite(dbConnection.db, url);

            const unchanged = newHashesSorted.length === existingHashesSorted.length &&
                newHashesSorted.every((h, i) => h === existingHashesSorted[i]);

            if (!unchanged) {
                // Changed — delete old, insert new
                if (existingHashesSorted.length > 0) {
                    DatabaseManager.removeChunksByUrlSQLite(dbConnection.db, url, testLogger);
                }

                for (let i = 0; i < chunks.length; i++) {
                    const chunk = chunks[i];
                    chunk.metadata.branch = chunk.metadata.branch || '';
                    chunk.metadata.repo = chunk.metadata.repo || '';
                    const embedding = tracker.embed(url);
                    DatabaseManager.insertVectorsSQLite(dbConnection.db, chunk, embedding, testLogger, newHashes[i]);
                }
            }

            // Store markdown in the markdown store (mirrors Doc2Vec.processWebsite behavior)
            await markdownStore.upsertMarkdown(url, config.product_name, content);

            return true;
        },
        testLogger,
        visitedUrls,
        { knownUrls, etagStore, lastmodStore, markdownStoreUrls }
    );

    // Clean up 404 URLs from the markdown store
    for (const url of crawlResult.notFoundUrls) {
        await markdownStore.deleteMarkdown(url);
    }

    return {
        visitedUrls,
        processedUrls,
        embeddedUrls: new Set(tracker.embeddedUrls),
        embedCallCount: tracker.embedCallCount,
        totalChunks: countAllChunks(dbConnection.db),
        notFoundUrls: crawlResult.notFoundUrls,
    };
}

describe('E2E: Website Markdown Store Multi-Sync', () => {
    let server: http.Server;
    let baseUrl: string;
    let db: BetterSqlite3.Database;
    let dbConnection: SqliteDB;
    let tracker: ReturnType<typeof createEmbeddingTracker>;
    let processor: ContentProcessor;
    let markdownStore: InMemoryMarkdownStore;

    // Mutable server state
    const pageContent: Record<string, string> = {};
    const pageEtags: Record<string, string> = {};
    // Track which pages should return 404 on HEAD
    const notFoundPages: Set<string> = new Set();
    let sitemapXml = '';
    let headRequestCount = 0;

    beforeEach(async () => {
        headRequestCount = 0;
        notFoundPages.clear();

        // Initial page content
        pageContent['/'] = `
            <html><head><title>Home</title></head><body>
            <article>
                <h1>Home Page</h1>
                <p>Welcome to the documentation site.</p>
                <a href="/page1">Page 1</a>
                <a href="/page2">Page 2</a>
                <a href="/page3">Page 3</a>
            </article>
            </body></html>
        `;
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

        // ETags per page
        pageEtags['/'] = '"etag-home-v1"';
        pageEtags['/page1'] = '"etag-page1-v1"';
        pageEtags['/page2'] = '"etag-page2-v1"';
        pageEtags['/page3'] = '"etag-page3-v1"';

        // Sitemap with lastmod for all pages
        sitemapXml = `<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>BASE_URL/</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page1</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page2</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page3</loc><lastmod>2026-01-01</lastmod></url>
            </urlset>`;

        // Start HTTP server
        server = http.createServer((req, res) => {
            const urlPath = req.url || '';

            if (urlPath === '/sitemap.xml') {
                res.writeHead(200, { 'Content-Type': 'application/xml' });
                res.end(sitemapXml.replace(/BASE_URL/g, baseUrl));
                return;
            }

            // Handle 404 for specific pages on HEAD requests
            if (req.method === 'HEAD' && notFoundPages.has(urlPath)) {
                headRequestCount++;
                res.writeHead(404);
                res.end();
                return;
            }

            const content = pageContent[urlPath];
            if (content) {
                const etag = pageEtags[urlPath];
                const headers: Record<string, string> = { 'Content-Type': 'text/html' };
                if (etag) headers['ETag'] = etag;

                if (req.method === 'HEAD') {
                    headRequestCount++;
                    res.writeHead(200, headers);
                    res.end();
                } else {
                    res.writeHead(200, headers);
                    res.end(content);
                }
            } else {
                if (req.method === 'HEAD') headRequestCount++;
                res.writeHead(404);
                res.end('Not found');
            }
        });

        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        if (!address || typeof address === 'string') throw new Error('Failed to start test server');
        baseUrl = `http://127.0.0.1:${(address as any).port}`;

        const created = createE2EDatabase();
        db = created.db;
        dbConnection = created.dbConnection;
        await DatabaseManager.initDatabaseMetadata(dbConnection, testLogger);
        tracker = createEmbeddingTracker();
        processor = new ContentProcessor(testLogger);
        markdownStore = new InMemoryMarkdownStore();
    });

    afterEach(() => {
        db.close();
        server.close();
    });

    it('should force-process pages not in markdown store, then skip on subsequent syncs', async () => {
        const config: WebsiteSourceConfig = {
            type: 'website',
            product_name: 'e2e-mdstore',
            version: '1.0',
            url: baseUrl + '/',
            max_size: 1048576,
            sitemap_url: baseUrl + '/sitemap.xml',
            markdown_store: true,
            database_config: { type: 'sqlite', params: {} },
        };

        // ═══════════════════════════════════════════════════════════════
        // RUN 1: First sync — markdown store is empty, all pages should
        // be force-processed even though lastmod would normally skip them
        // on a second run (they're "new" to both the vector DB and the
        // markdown store, so no skip logic applies anyway).
        // ═══════════════════════════════════════════════════════════════
        const run1 = await runWebsiteSyncWithMarkdownStore(
            processor, config, dbConnection, tracker, markdownStore
        );

        // All 4 pages should be processed and embedded
        expect(run1.processedUrls.size).toBe(4);
        expect(run1.embeddedUrls.size).toBe(4);
        expect(run1.totalChunks).toBeGreaterThan(0);

        // All 4 pages should have markdown stored
        expect(markdownStore.size).toBe(4);
        expect(markdownStore.has(`${baseUrl}/`)).toBe(true);
        expect(markdownStore.has(`${baseUrl}/page1`)).toBe(true);
        expect(markdownStore.has(`${baseUrl}/page2`)).toBe(true);
        expect(markdownStore.has(`${baseUrl}/page3`)).toBe(true);
        expect(markdownStore.upsertCount).toBe(4);

        // ═══════════════════════════════════════════════════════════════
        // RUN 2: No changes — all pages now in markdown store AND lastmod
        // unchanged → normal skip behavior applies, no pages processed
        // ═══════════════════════════════════════════════════════════════
        headRequestCount = 0;
        const run2 = await runWebsiteSyncWithMarkdownStore(
            processor, config, dbConnection, tracker, markdownStore
        );

        // No pages should be processed (all skipped via lastmod, all in markdown store)
        expect(run2.processedUrls.size).toBe(0);
        expect(headRequestCount).toBe(0);
        expect(run2.embedCallCount).toBe(0);
        // Markdown store should have zero new upserts
        expect(markdownStore.upsertCount).toBe(0);
        // Store still has all 4 pages
        expect(markdownStore.size).toBe(4);

        // ═══════════════════════════════════════════════════════════════
        // RUN 3: page2 content + lastmod changes → re-processed, markdown updated
        // Other pages skipped normally
        // ═══════════════════════════════════════════════════════════════
        sitemapXml = `<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>BASE_URL/</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page1</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page2</loc><lastmod>2026-02-15</lastmod></url>
                <url><loc>BASE_URL/page3</loc><lastmod>2026-01-01</lastmod></url>
            </urlset>`;

        pageContent['/page2'] = `
            <html><head><title>Page 2</title></head><body>
            <article>
                <h1>Page Two — Updated</h1>
                <p>This page has been updated with new monitoring practices.</p>
                <p>Includes Prometheus, Grafana, and OpenTelemetry integration guides.</p>
            </article>
            </body></html>
        `;
        pageEtags['/page2'] = '"etag-page2-v2"';

        const page1MarkdownBefore = markdownStore.getMarkdown(`${baseUrl}/page1`);

        const run3 = await runWebsiteSyncWithMarkdownStore(
            processor, config, dbConnection, tracker, markdownStore
        );

        // Only page2 (and possibly home via lastmod change) should be processed
        expect(run3.processedUrls.has(`${baseUrl}/page2`)).toBe(true);
        expect(run3.processedUrls.has(`${baseUrl}/page1`)).toBe(false);
        expect(run3.processedUrls.has(`${baseUrl}/page3`)).toBe(false);

        // page2 markdown should be updated in the store
        expect(markdownStore.has(`${baseUrl}/page2`)).toBe(true);
        const page2MarkdownAfter = markdownStore.getMarkdown(`${baseUrl}/page2`);
        expect(page2MarkdownAfter).toContain('Updated');

        // page1 markdown should be unchanged
        expect(markdownStore.getMarkdown(`${baseUrl}/page1`)).toBe(page1MarkdownBefore);

        // Store still has all 4 pages
        expect(markdownStore.size).toBe(4);
    }, 120000);

    it('should force-process pages missing from markdown store even when lastmod is unchanged', async () => {
        const config: WebsiteSourceConfig = {
            type: 'website',
            product_name: 'e2e-mdstore-force',
            version: '1.0',
            url: baseUrl + '/',
            max_size: 1048576,
            sitemap_url: baseUrl + '/sitemap.xml',
            markdown_store: true,
            database_config: { type: 'sqlite', params: {} },
        };

        // ═══════════════════════════════════════════════════════════════
        // RUN 1: Normal first sync — populate everything
        // ═══════════════════════════════════════════════════════════════
        const run1 = await runWebsiteSyncWithMarkdownStore(
            processor, config, dbConnection, tracker, markdownStore
        );
        expect(run1.processedUrls.size).toBe(4);
        expect(markdownStore.size).toBe(4);

        // ═══════════════════════════════════════════════════════════════
        // RUN 2: Verify normal skip — no changes, nothing processed
        // ═══════════════════════════════════════════════════════════════
        const run2 = await runWebsiteSyncWithMarkdownStore(
            processor, config, dbConnection, tracker, markdownStore
        );
        expect(run2.processedUrls.size).toBe(0);

        // ═══════════════════════════════════════════════════════════════
        // SIMULATE: Remove page1 and page3 from the markdown store
        // (as if the Postgres table was partially cleared or this is a
        // new table being populated from an existing vector DB)
        // ═══════════════════════════════════════════════════════════════
        await markdownStore.deleteMarkdown(`${baseUrl}/page1`);
        await markdownStore.deleteMarkdown(`${baseUrl}/page3`);
        expect(markdownStore.size).toBe(2); // Only home and page2 remain

        // ═══════════════════════════════════════════════════════════════
        // RUN 3: page1 and page3 are NOT in the markdown store, so they
        // should be force-processed even though lastmod is unchanged.
        // Home and page2 are in the store, so they should be skipped.
        // ═══════════════════════════════════════════════════════════════
        headRequestCount = 0;
        const run3 = await runWebsiteSyncWithMarkdownStore(
            processor, config, dbConnection, tracker, markdownStore
        );

        // page1 and page3 should be force-processed (not in markdown store)
        expect(run3.processedUrls.has(`${baseUrl}/page1`)).toBe(true);
        expect(run3.processedUrls.has(`${baseUrl}/page3`)).toBe(true);

        // Home and page2 should be skipped (in markdown store + lastmod unchanged)
        expect(run3.processedUrls.has(`${baseUrl}/`)).toBe(false);
        expect(run3.processedUrls.has(`${baseUrl}/page2`)).toBe(false);

        // page1 and page3 should now be back in the markdown store
        expect(markdownStore.has(`${baseUrl}/page1`)).toBe(true);
        expect(markdownStore.has(`${baseUrl}/page3`)).toBe(true);
        expect(markdownStore.size).toBe(4);

        // Content hashes haven't changed, so no re-embedding should occur
        // (force-processing means fetching the page, but processChunksForUrl
        // will detect unchanged content hashes and skip embedding)
        expect(run3.embedCallCount).toBe(0);
    }, 120000);

    it('should remove 404 pages from markdown store', async () => {
        const config: WebsiteSourceConfig = {
            type: 'website',
            product_name: 'e2e-mdstore-404',
            version: '1.0',
            url: baseUrl + '/',
            max_size: 1048576,
            // No sitemap — use ETag-based change detection so HEAD requests
            // happen and we can test the 404 cleanup path
            database_config: { type: 'sqlite', params: {} },
        };

        // ═══════════════════════════════════════════════════════════════
        // RUN 1: Initial sync — all 4 pages processed
        // ═══════════════════════════════════════════════════════════════
        const run1 = await runWebsiteSyncWithMarkdownStore(
            processor, config, dbConnection, tracker, markdownStore
        );
        expect(run1.processedUrls.size).toBe(4);
        expect(markdownStore.size).toBe(4);
        expect(markdownStore.has(`${baseUrl}/page2`)).toBe(true);

        // ═══════════════════════════════════════════════════════════════
        // RUN 2: page2 returns 404 on HEAD → should be removed from store
        // ═══════════════════════════════════════════════════════════════
        notFoundPages.add('/page2');

        const run2 = await runWebsiteSyncWithMarkdownStore(
            processor, config, dbConnection, tracker, markdownStore
        );

        // page2 should be in notFoundUrls
        expect(run2.notFoundUrls.has(`${baseUrl}/page2`)).toBe(true);

        // page2 should be removed from the markdown store
        expect(markdownStore.has(`${baseUrl}/page2`)).toBe(false);
        expect(markdownStore.deletedUrls.has(`${baseUrl}/page2`)).toBe(true);

        // Other pages should still be in the store
        expect(markdownStore.has(`${baseUrl}/`)).toBe(true);
        expect(markdownStore.has(`${baseUrl}/page1`)).toBe(true);
        expect(markdownStore.has(`${baseUrl}/page3`)).toBe(true);
        expect(markdownStore.size).toBe(3);
    }, 120000);
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

// ─── Qdrant Multi-Sync E2E (conditional) ────────────────────────────────────

/**
 * Helper: run one "sync" of the full website pipeline through crawlWebsite
 * using Qdrant as the backend. Mirrors runWebsiteSync for SQLite.
 */
async function runWebsiteSyncQdrant(
    processor: ContentProcessor,
    config: WebsiteSourceConfig,
    qdrantDb: QdrantDB,
    tracker: ReturnType<typeof createEmbeddingTracker>,
    options?: { failUrls?: Set<string> }
): Promise<{
    visitedUrls: Set<string>;
    processedUrls: Set<string>;
    embeddedUrls: Set<string>;
    embedCallCount: number;
    totalChunks: number;
}> {
    const visitedUrls = new Set<string>();
    const processedUrls = new Set<string>();
    const urlPrefix = Utils.getUrlPrefix(config.url);

    const storedUrls = await DatabaseManager.getStoredUrlsByPrefixQdrant(qdrantDb, urlPrefix);
    const knownUrls = storedUrls.length > 0 ? new Set(storedUrls) : undefined;

    const dbConnection: QdrantDB = qdrantDb;
    const etagStore = {
        get: async (url: string) => DatabaseManager.getMetadataValue(dbConnection, `etag:${url}`, undefined, testLogger),
        set: async (url: string, etag: string) => DatabaseManager.setMetadataValue(dbConnection, `etag:${url}`, etag, testLogger, 3072),
    };
    const lastmodStore = {
        get: async (url: string) => DatabaseManager.getMetadataValue(dbConnection, `lastmod:${url}`, undefined, testLogger),
        set: async (url: string, lastmod: string) => DatabaseManager.setMetadataValue(dbConnection, `lastmod:${url}`, lastmod, testLogger, 3072),
    };

    tracker.reset();

    await processor.crawlWebsite(
        config.url,
        config,
        async (url, content) => {
            processedUrls.add(url);

            if (options?.failUrls?.has(url)) {
                return false;
            }

            const chunks = await processor.chunkMarkdown(content, config, url);
            const newHashes = chunks.map(c => Utils.generateHash(c.content));
            const newHashesSorted = newHashes.slice().sort();
            const existingHashesSorted = await DatabaseManager.getChunkHashesByUrlQdrant(qdrantDb, url);

            const unchanged = newHashesSorted.length === existingHashesSorted.length &&
                newHashesSorted.every((h, i) => h === existingHashesSorted[i]);

            if (unchanged) {
                return true;
            }

            if (existingHashesSorted.length > 0) {
                await DatabaseManager.removeChunksByUrlQdrant(qdrantDb, url, testLogger);
            }

            for (let i = 0; i < chunks.length; i++) {
                const chunk = chunks[i];
                chunk.metadata.branch = chunk.metadata.branch || '';
                chunk.metadata.repo = chunk.metadata.repo || '';
                const embedding = tracker.embed(url);
                await DatabaseManager.storeChunkInQdrant(qdrantDb, chunk, embedding, newHashes[i]);
            }

            return true;
        },
        testLogger,
        visitedUrls,
        { knownUrls, etagStore, lastmodStore }
    );

    return {
        visitedUrls,
        processedUrls,
        embeddedUrls: new Set(tracker.embeddedUrls),
        embedCallCount: tracker.embedCallCount,
        totalChunks: await countAllChunksQdrant(qdrantDb),
    };
}

describe.skipIf(!canRunQdrantTests)('E2E: Qdrant Multi-Sync Change Detection', () => {
    let server: http.Server;
    let baseUrl: string;
    let qdrantClient: QdrantClient;
    let qdrantDb: QdrantDB;
    let tracker: ReturnType<typeof createEmbeddingTracker>;
    let processor: ContentProcessor;

    const pageContent: Record<string, string> = {};
    const pageEtags: Record<string, string> = {};
    let sitemapXml = '';
    let headRequestCount = 0;

    beforeEach(async () => {
        headRequestCount = 0;

        pageContent['/'] = `
            <html><head><title>Home</title></head><body>
            <article>
                <h1>Home Page</h1>
                <p>Welcome to the documentation site.</p>
                <a href="/page1">Page 1</a>
                <a href="/page2">Page 2</a>
                <a href="/page3">Page 3</a>
            </article>
            </body></html>
        `;
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

        pageEtags['/'] = '"etag-home-v1"';
        pageEtags['/page1'] = '"etag-page1-v1"';
        pageEtags['/page2'] = '"etag-page2-v1"';
        pageEtags['/page3'] = '"etag-page3-v1"';

        sitemapXml = `<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>BASE_URL/</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page1</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page2</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page3</loc><lastmod>2026-01-01</lastmod></url>
            </urlset>`;

        server = http.createServer((req, res) => {
            const urlPath = req.url || '';
            if (urlPath === '/sitemap.xml') {
                res.writeHead(200, { 'Content-Type': 'application/xml' });
                res.end(sitemapXml.replace(/BASE_URL/g, baseUrl));
                return;
            }
            const content = pageContent[urlPath];
            if (content) {
                const etag = pageEtags[urlPath];
                const headers: Record<string, string> = { 'Content-Type': 'text/html' };
                if (etag) headers['ETag'] = etag;
                if (req.method === 'HEAD') {
                    headRequestCount++;
                    res.writeHead(200, headers);
                    res.end();
                } else {
                    res.writeHead(200, headers);
                    res.end(content);
                }
            } else {
                res.writeHead(404);
                res.end('Not found');
            }
        });

        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        if (!address || typeof address === 'string') throw new Error('Failed to start test server');
        baseUrl = `http://127.0.0.1:${(address as any).port}`;

        qdrantClient = new QdrantClient({
            url: QDRANT_URL!,
            apiKey: QDRANT_API_KEY!,
            port: parseInt(QDRANT_PORT!, 10),
        });

        try { await qdrantClient.deleteCollection(QDRANT_TEST_COLLECTION!); } catch { /* may not exist */ }
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
        try { await qdrantClient.deleteCollection(QDRANT_TEST_COLLECTION!); } catch { /* ignore */ }
    });

    it('should use all 4 change detection layers across multiple syncs', async () => {
        const config: WebsiteSourceConfig = {
            type: 'website',
            product_name: 'e2e-qdrant-multisync',
            version: '1.0',
            url: baseUrl + '/',
            max_size: 1048576,
            sitemap_url: baseUrl + '/sitemap.xml',
            database_config: {
                type: 'qdrant',
                params: {
                    qdrant_url: QDRANT_URL!,
                    qdrant_port: parseInt(QDRANT_PORT!, 10),
                    collection_name: QDRANT_TEST_COLLECTION!,
                },
            },
        };

        // ═══════════════════════════════════════════════════════════════
        // RUN 1: Initial sync — all pages are new, everything embedded
        // ═══════════════════════════════════════════════════════════════
        const run1 = await runWebsiteSyncQdrant(processor, config, qdrantDb, tracker);

        expect(run1.processedUrls.size).toBe(4);
        expect(run1.embeddedUrls.size).toBe(4);
        expect(run1.totalChunks).toBeGreaterThan(0);
        const run1TotalChunks = run1.totalChunks;

        for (const pagePath of ['/', '/page1', '/page2', '/page3']) {
            const chunks = await getChunksForUrlQdrant(qdrantDb, `${baseUrl}${pagePath}`);
            expect(chunks.length).toBeGreaterThan(0);
        }

        // ═══════════════════════════════════════════════════════════════
        // RUN 2: No changes — all skipped via lastmod
        // ═══════════════════════════════════════════════════════════════
        headRequestCount = 0;
        const run2 = await runWebsiteSyncQdrant(processor, config, qdrantDb, tracker);

        expect(run2.processedUrls.size).toBe(0);
        expect(headRequestCount).toBe(0);
        expect(run2.embedCallCount).toBe(0);
        expect(run2.totalChunks).toBe(run1TotalChunks);

        // ═══════════════════════════════════════════════════════════════
        // RUN 3: page2 lastmod + content change → re-embedded
        // ═══════════════════════════════════════════════════════════════
        sitemapXml = `<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>BASE_URL/</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page1</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page2</loc><lastmod>2026-02-15</lastmod></url>
                <url><loc>BASE_URL/page3</loc><lastmod>2026-01-01</lastmod></url>
            </urlset>`;

        pageContent['/page2'] = `
            <html><head><title>Page 2</title></head><body>
            <article>
                <h1>Page Two — Updated</h1>
                <p>This page has been updated with new monitoring practices for cloud-native environments.</p>
                <p>Includes Prometheus, Grafana, and OpenTelemetry integration guides.</p>
            </article>
            </body></html>
        `;
        pageEtags['/page2'] = '"etag-page2-v2"';

        const page1ChunksBefore = await getChunksForUrlQdrant(qdrantDb, `${baseUrl}/page1`);
        const page3ChunksBefore = await getChunksForUrlQdrant(qdrantDb, `${baseUrl}/page3`);

        headRequestCount = 0;
        const run3 = await runWebsiteSyncQdrant(processor, config, qdrantDb, tracker);

        expect(run3.processedUrls.has(`${baseUrl}/page2`)).toBe(true);
        expect(run3.processedUrls.has(`${baseUrl}/page1`)).toBe(false);
        expect(run3.processedUrls.has(`${baseUrl}/page3`)).toBe(false);
        expect(run3.embeddedUrls.has(`${baseUrl}/page2`)).toBe(true);
        expect(await getChunksForUrlQdrant(qdrantDb, `${baseUrl}/page1`)).toEqual(page1ChunksBefore);
        expect(await getChunksForUrlQdrant(qdrantDb, `${baseUrl}/page3`)).toEqual(page3ChunksBefore);
        expect(headRequestCount).toBe(0);

        // ═══════════════════════════════════════════════════════════════
        // RUN 4: page4 via links (ETag fallback), page1 lastmod changed
        // ═══════════════════════════════════════════════════════════════
        pageContent['/page1'] = `
            <html><head><title>Page 1</title></head><body>
            <article>
                <h1>Page One</h1>
                <p>This is the first page covering deployment strategies for production environments.</p>
                <p>It includes details about rolling updates and blue-green deployments.</p>
                <a href="/page4">See also: Advanced Deployment</a>
            </article>
            </body></html>
        `;
        pageEtags['/page1'] = '"etag-page1-v2"';

        pageContent['/page4'] = `
            <html><head><title>Page 4</title></head><body>
            <article>
                <h1>Page Four — Advanced Deployment</h1>
                <p>This page covers advanced deployment patterns including canary releases.</p>
                <p>Learn about traffic splitting and progressive delivery strategies.</p>
            </article>
            </body></html>
        `;
        pageEtags['/page4'] = '"etag-page4-v1"';

        sitemapXml = `<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>BASE_URL/</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page1</loc><lastmod>2026-02-16</lastmod></url>
                <url><loc>BASE_URL/page2</loc><lastmod>2026-02-15</lastmod></url>
                <url><loc>BASE_URL/page3</loc><lastmod>2026-01-01</lastmod></url>
            </urlset>`;

        headRequestCount = 0;
        const run4 = await runWebsiteSyncQdrant(processor, config, qdrantDb, tracker);

        expect(run4.processedUrls.has(`${baseUrl}/page1`)).toBe(true);
        expect(run4.processedUrls.has(`${baseUrl}/page4`)).toBe(true);
        expect(run4.embeddedUrls.has(`${baseUrl}/page4`)).toBe(true);
        expect(run4.processedUrls.has(`${baseUrl}/page2`)).toBe(false);
        expect(run4.processedUrls.has(`${baseUrl}/page3`)).toBe(false);
        expect(headRequestCount).toBeGreaterThan(0);
        const page4Chunks = await getChunksForUrlQdrant(qdrantDb, `${baseUrl}/page4`);
        expect(page4Chunks.length).toBeGreaterThan(0);

        // ═══════════════════════════════════════════════════════════════
        // RUN 5: Processing failure → lastmod NOT stored
        // ═══════════════════════════════════════════════════════════════
        sitemapXml = `<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url><loc>BASE_URL/</loc><lastmod>2026-01-01</lastmod></url>
                <url><loc>BASE_URL/page1</loc><lastmod>2026-02-16</lastmod></url>
                <url><loc>BASE_URL/page2</loc><lastmod>2026-02-15</lastmod></url>
                <url><loc>BASE_URL/page3</loc><lastmod>2026-03-01</lastmod></url>
            </urlset>`;

        pageContent['/page3'] = `
            <html><head><title>Page 3</title></head><body>
            <article>
                <h1>Page Three — Updated</h1>
                <p>Updated security hardening with zero-trust architecture patterns.</p>
                <p>New section on supply chain security and SBOM requirements.</p>
            </article>
            </body></html>
        `;
        pageEtags['/page3'] = '"etag-page3-v2"';

        const run5 = await runWebsiteSyncQdrant(processor, config, qdrantDb, tracker, {
            failUrls: new Set([`${baseUrl}/page3`]),
        });

        expect(run5.processedUrls.has(`${baseUrl}/page3`)).toBe(true);
        expect(run5.embeddedUrls.has(`${baseUrl}/page3`)).toBe(false);
        // page3 should still have old chunks
        expect(await getChunksForUrlQdrant(qdrantDb, `${baseUrl}/page3`)).toEqual(page3ChunksBefore);

        // ═══════════════════════════════════════════════════════════════
        // RUN 6: Retry — page3 reprocessed (lastmod wasn't stored)
        // ═══════════════════════════════════════════════════════════════
        const run6 = await runWebsiteSyncQdrant(processor, config, qdrantDb, tracker);

        expect(run6.processedUrls.has(`${baseUrl}/page3`)).toBe(true);
        expect(run6.embeddedUrls.has(`${baseUrl}/page3`)).toBe(true);
        const page3ChunksAfterRetry = await getChunksForUrlQdrant(qdrantDb, `${baseUrl}/page3`);
        expect(page3ChunksAfterRetry.length).toBeGreaterThan(0);
        expect(page3ChunksAfterRetry).not.toEqual(page3ChunksBefore);

        // Final verification
        const finalTotal = await countAllChunksQdrant(qdrantDb);
        expect(finalTotal).toBeGreaterThan(0);
        const allUrls = ['/', '/page1', '/page2', '/page3', '/page4'];
        let expectedTotal = 0;
        for (const p of allUrls) {
            expectedTotal += (await getChunksForUrlQdrant(qdrantDb, `${baseUrl}${p}`)).length;
        }
        expect(finalTotal).toBe(expectedTotal);
    }, 120000);
});
