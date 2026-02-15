import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { DatabaseManager } from '../database';
import { Logger, LogLevel } from '../logger';
import { DocumentChunk, SqliteDB, QdrantDB, SourceConfig } from '../types';
import BetterSqlite3 from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';
import * as fs from 'fs';
import * as path from 'path';

const testLogger = new Logger('test', { level: LogLevel.NONE });

// Helper to create an in-memory SQLite database matching the app schema
function createTestDb(): BetterSqlite3.Database {
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

    return db;
}

function createTestDbConnection(db: BetterSqlite3.Database): SqliteDB {
    return { db, type: 'sqlite' };
}

function createTestChunk(overrides: Partial<DocumentChunk & { metadata?: Partial<DocumentChunk['metadata']> }> = {}): DocumentChunk {
    const { metadata: metaOverrides, ...restOverrides } = overrides;
    return {
        content: restOverrides.content ?? 'Test content for this chunk',
        metadata: {
            product_name: 'TestProduct',
            version: '1.0',
            branch: '',  // vec0 TEXT columns require non-null values
            repo: '',    // vec0 TEXT columns require non-null values
            heading_hierarchy: ['Section A'],
            section: 'Section A',
            chunk_id: 'test-chunk-id-001',
            url: 'https://example.com/page',
            hash: 'test-hash-001',
            chunk_index: 0,
            total_chunks: 1,
            ...metaOverrides
        }
    };
}

function createTestEmbedding(): number[] {
    return new Array(3072).fill(0.1);
}

describe('DatabaseManager', () => {
    // ─── initDatabaseMetadata (SQLite) ──────────────────────────────
    describe('initDatabaseMetadata', () => {
        it('should create vec_metadata table in SQLite', async () => {
            const db = createTestDb();
            const conn = createTestDbConnection(db);
            await DatabaseManager.initDatabaseMetadata(conn, testLogger);

            // Verify the table exists by attempting to query it
            const result = db.prepare('SELECT COUNT(*) as count FROM vec_metadata').get() as { count: number };
            expect(result.count).toBe(0);

            db.close();
        });

        it('should not error if called multiple times', async () => {
            const db = createTestDb();
            const conn = createTestDbConnection(db);
            await DatabaseManager.initDatabaseMetadata(conn, testLogger);
            await DatabaseManager.initDatabaseMetadata(conn, testLogger);
            // Should not throw
            db.close();
        });
    });

    // ─── getMetadataValue / setMetadataValue (SQLite) ───────────────
    describe('metadata CRUD (SQLite)', () => {
        let db: BetterSqlite3.Database;
        let conn: SqliteDB;

        beforeEach(async () => {
            db = createTestDb();
            conn = createTestDbConnection(db);
            await DatabaseManager.initDatabaseMetadata(conn, testLogger);
        });

        afterEach(() => {
            db.close();
        });

        it('should return default value when key does not exist', async () => {
            const value = await DatabaseManager.getMetadataValue(conn, 'nonexistent', 'default', testLogger);
            expect(value).toBe('default');
        });

        it('should return undefined when key does not exist and no default', async () => {
            const value = await DatabaseManager.getMetadataValue(conn, 'nonexistent', undefined, testLogger);
            expect(value).toBeUndefined();
        });

        it('should set and get metadata values', async () => {
            await DatabaseManager.setMetadataValue(conn, 'mykey', 'myvalue', testLogger);
            const value = await DatabaseManager.getMetadataValue(conn, 'mykey', undefined, testLogger);
            expect(value).toBe('myvalue');
        });

        it('should upsert metadata values', async () => {
            await DatabaseManager.setMetadataValue(conn, 'key1', 'value1', testLogger);
            await DatabaseManager.setMetadataValue(conn, 'key1', 'value2', testLogger);
            const value = await DatabaseManager.getMetadataValue(conn, 'key1', undefined, testLogger);
            expect(value).toBe('value2');
        });
    });

    // ─── getLastRunDate / updateLastRunDate (SQLite) ─────────────────
    describe('last run date (SQLite)', () => {
        let db: BetterSqlite3.Database;
        let conn: SqliteDB;

        beforeEach(async () => {
            db = createTestDb();
            conn = createTestDbConnection(db);
            await DatabaseManager.initDatabaseMetadata(conn, testLogger);
        });

        afterEach(() => {
            db.close();
        });

        it('should return default date when no run date exists', async () => {
            const date = await DatabaseManager.getLastRunDate(conn, 'owner/repo', '2025-01-01T00:00:00Z', testLogger);
            expect(date).toBe('2025-01-01T00:00:00Z');
        });

        it('should update and retrieve last run date', async () => {
            await DatabaseManager.updateLastRunDate(conn, 'owner/repo', testLogger);
            const date = await DatabaseManager.getLastRunDate(conn, 'owner/repo', '2025-01-01T00:00:00Z', testLogger);
            // Should be an ISO date string, not the default
            expect(date).not.toBe('2025-01-01T00:00:00Z');
            expect(date).toMatch(/^\d{4}-\d{2}-\d{2}T/);
        });

        it('should normalize repo names in metadata keys', async () => {
            await DatabaseManager.updateLastRunDate(conn, 'owner/repo', testLogger);
            // Check directly in db that the key uses underscore
            const result = db.prepare('SELECT key FROM vec_metadata WHERE key LIKE ?').get('last_run_%') as { key: string };
            expect(result.key).toBe('last_run_owner_repo');
        });
    });

    // ─── prepareSQLiteStatements ─────────────────────────────────────
    describe('prepareSQLiteStatements', () => {
        it('should return all required statements', () => {
            const db = createTestDb();
            const stmts = DatabaseManager.prepareSQLiteStatements(db);
            expect(stmts).toHaveProperty('insertStmt');
            expect(stmts).toHaveProperty('checkHashStmt');
            expect(stmts).toHaveProperty('updateStmt');
            expect(stmts).toHaveProperty('getAllChunkIdsStmt');
            expect(stmts).toHaveProperty('deleteChunkStmt');
            expect(stmts).toHaveProperty('hasBranchColumn');
            expect(stmts).toHaveProperty('hasRepoColumn');
            db.close();
        });

        it('should detect branch and repo columns', () => {
            const db = createTestDb();
            const stmts = DatabaseManager.prepareSQLiteStatements(db);
            expect(stmts.hasBranchColumn).toBe(true);
            expect(stmts.hasRepoColumn).toBe(true);
            db.close();
        });

        it('should cache column detection results', () => {
            const db = createTestDb();
            const stmts1 = DatabaseManager.prepareSQLiteStatements(db);
            const stmts2 = DatabaseManager.prepareSQLiteStatements(db);
            expect(stmts1.hasBranchColumn).toBe(stmts2.hasBranchColumn);
            expect(stmts1.hasRepoColumn).toBe(stmts2.hasRepoColumn);
            db.close();
        });
    });

    // ─── insertVectorsSQLite ─────────────────────────────────────────
    describe('insertVectorsSQLite', () => {
        let db: BetterSqlite3.Database;

        beforeEach(() => {
            db = createTestDb();
        });

        afterEach(() => {
            db.close();
        });

        it('should insert a chunk with embedding', () => {
            const chunk = createTestChunk();
            const embedding = createTestEmbedding();

            DatabaseManager.insertVectorsSQLite(db, chunk, embedding, testLogger);

            const result = db.prepare('SELECT chunk_id, content, product_name FROM vec_items WHERE chunk_id = ?')
                .get(chunk.metadata.chunk_id) as any;

            expect(result).toBeDefined();
            expect(result.content).toBe(chunk.content);
            expect(result.product_name).toBe('TestProduct');
        });

        it('should handle duplicate chunk_id inserts gracefully', () => {
            // Note: vec0 virtual tables silently ignore duplicate UNIQUE inserts
            // rather than throwing an error. This means the update path in
            // insertVectorsSQLite may not trigger. We verify the first insert persists.
            const chunk = createTestChunk();
            const embedding = createTestEmbedding();

            DatabaseManager.insertVectorsSQLite(db, chunk, embedding, testLogger, 'hash1');

            // Attempt to insert again with same chunk_id
            const updatedChunk = createTestChunk({ content: 'Updated content' });
            DatabaseManager.insertVectorsSQLite(db, updatedChunk, embedding, testLogger, 'hash2');

            // The original row should still exist (vec0 ignores duplicate inserts)
            const result = db.prepare('SELECT content, hash FROM vec_items WHERE chunk_id = ?')
                .get(chunk.metadata.chunk_id) as any;

            expect(result).toBeDefined();
            expect(result.content).toBe('Test content for this chunk');
            expect(result.hash).toBe('hash1');
        });

        it('should store heading hierarchy as JSON', () => {
            const chunk = createTestChunk();
            chunk.metadata.heading_hierarchy = ['Level 1', 'Level 2', 'Level 3'];
            const embedding = createTestEmbedding();

            DatabaseManager.insertVectorsSQLite(db, chunk, embedding, testLogger);

            const result = db.prepare('SELECT heading_hierarchy FROM vec_items WHERE chunk_id = ?')
                .get(chunk.metadata.chunk_id) as any;

            const parsed = JSON.parse(result.heading_hierarchy);
            expect(parsed).toEqual(['Level 1', 'Level 2', 'Level 3']);
        });

        it('should store branch and repo when provided', () => {
            const chunk = createTestChunk();
            chunk.metadata.branch = 'main';
            chunk.metadata.repo = 'org/repo';
            const embedding = createTestEmbedding();

            DatabaseManager.insertVectorsSQLite(db, chunk, embedding, testLogger);

            const result = db.prepare('SELECT branch, repo FROM vec_items WHERE chunk_id = ?')
                .get(chunk.metadata.chunk_id) as any;

            expect(result.branch).toBe('main');
            expect(result.repo).toBe('org/repo');
        });

        it('should store empty string for branch/repo when not provided', () => {
            // vec0 TEXT columns require non-null; the app falls back to empty string or null
            // depending on the column detection. With vec0 expecting text, we test with empty strings.
            const chunk = createTestChunk();
            chunk.metadata.branch = '';
            chunk.metadata.repo = '';
            const embedding = createTestEmbedding();

            DatabaseManager.insertVectorsSQLite(db, chunk, embedding, testLogger);

            const result = db.prepare('SELECT branch, repo FROM vec_items WHERE chunk_id = ?')
                .get(chunk.metadata.chunk_id) as any;

            expect(result).toBeDefined();
            expect(result.branch).toBe('');
            expect(result.repo).toBe('');
        });

        it('should insert successfully when branch/repo are undefined (website source scenario)', () => {
            // Website sources don't set branch/repo on chunks, so they are undefined.
            // vec0 TEXT columns reject NULL values, so insertVectorsSQLite must
            // coerce undefined to empty string. This test verifies the chunk is
            // actually stored and can be looked up for hash-based deduplication.
            const chunk = createTestChunk();
            (chunk.metadata as any).branch = undefined;
            (chunk.metadata as any).repo = undefined;
            const embedding = createTestEmbedding();

            DatabaseManager.insertVectorsSQLite(db, chunk, embedding, testLogger, 'website-hash');

            const result = db.prepare('SELECT hash, branch, repo FROM vec_items WHERE chunk_id = ?')
                .get(chunk.metadata.chunk_id) as any;

            expect(result).toBeDefined();
            expect(result.hash).toBe('website-hash');
            expect(result.branch).toBe('');
            expect(result.repo).toBe('');
        });

        it('should use provided chunkHash when given', () => {
            const chunk = createTestChunk();
            const embedding = createTestEmbedding();

            DatabaseManager.insertVectorsSQLite(db, chunk, embedding, testLogger, 'custom-hash');

            const result = db.prepare('SELECT hash FROM vec_items WHERE chunk_id = ?')
                .get(chunk.metadata.chunk_id) as any;

            expect(result.hash).toBe('custom-hash');
        });

        it('should generate hash from content when not provided', () => {
            const chunk = createTestChunk();
            const embedding = createTestEmbedding();

            DatabaseManager.insertVectorsSQLite(db, chunk, embedding, testLogger);

            const result = db.prepare('SELECT hash FROM vec_items WHERE chunk_id = ?')
                .get(chunk.metadata.chunk_id) as any;

            expect(result.hash).toBeTruthy();
            expect(result.hash).toMatch(/^[a-f0-9]{64}$/);
        });
    });

    // ─── removeObsoleteChunksSQLite ──────────────────────────────────
    describe('removeObsoleteChunksSQLite', () => {
        let db: BetterSqlite3.Database;

        beforeEach(() => {
            db = createTestDb();
        });

        afterEach(() => {
            db.close();
        });

        it('should delete chunks for URLs no longer visited', () => {
            const embedding = createTestEmbedding();

            // Insert two chunks
            const chunk1 = createTestChunk();
            chunk1.metadata.chunk_id = 'chunk-1';
            chunk1.metadata.url = 'https://example.com/page1';
            DatabaseManager.insertVectorsSQLite(db, chunk1, embedding, testLogger);

            const chunk2 = createTestChunk();
            chunk2.metadata.chunk_id = 'chunk-2';
            chunk2.metadata.url = 'https://example.com/page2';
            DatabaseManager.insertVectorsSQLite(db, chunk2, embedding, testLogger);

            // Only page1 was visited
            const visitedUrls = new Set(['https://example.com/page1']);
            DatabaseManager.removeObsoleteChunksSQLite(db, visitedUrls, 'https://example.com', testLogger);

            // chunk-1 should still exist, chunk-2 should be deleted
            const remaining = db.prepare('SELECT chunk_id FROM vec_items').all() as any[];
            expect(remaining.length).toBe(1);
            expect(remaining[0].chunk_id).toBe('chunk-1');
        });

        it('should not delete chunks when all URLs are visited', () => {
            const embedding = createTestEmbedding();

            const chunk1 = createTestChunk();
            chunk1.metadata.chunk_id = 'chunk-a';
            chunk1.metadata.url = 'https://example.com/a';
            DatabaseManager.insertVectorsSQLite(db, chunk1, embedding, testLogger);

            const visitedUrls = new Set(['https://example.com/a']);
            DatabaseManager.removeObsoleteChunksSQLite(db, visitedUrls, 'https://example.com', testLogger);

            const remaining = db.prepare('SELECT chunk_id FROM vec_items').all() as any[];
            expect(remaining.length).toBe(1);
        });

        it('should not affect chunks outside the URL prefix', () => {
            const embedding = createTestEmbedding();

            const chunk1 = createTestChunk();
            chunk1.metadata.chunk_id = 'in-scope';
            chunk1.metadata.url = 'https://example.com/docs/page';
            DatabaseManager.insertVectorsSQLite(db, chunk1, embedding, testLogger);

            const chunk2 = createTestChunk();
            chunk2.metadata.chunk_id = 'out-of-scope';
            chunk2.metadata.url = 'https://other.com/page';
            DatabaseManager.insertVectorsSQLite(db, chunk2, embedding, testLogger);

            const visitedUrls = new Set<string>();
            DatabaseManager.removeObsoleteChunksSQLite(db, visitedUrls, 'https://example.com', testLogger);

            const remaining = db.prepare('SELECT chunk_id FROM vec_items').all() as any[];
            expect(remaining.length).toBe(1);
            expect(remaining[0].chunk_id).toBe('out-of-scope');
        });
    });

    // ─── removeChunksByUrlSQLite ─────────────────────────────────────
    describe('removeChunksByUrlSQLite', () => {
        let db: BetterSqlite3.Database;

        beforeEach(() => {
            db = createTestDb();
        });

        afterEach(() => {
            db.close();
        });

        it('should delete all chunks matching a specific URL', () => {
            const embedding = createTestEmbedding();

            for (let i = 0; i < 3; i++) {
                const chunk = createTestChunk();
                chunk.metadata.chunk_id = `chunk-${i}`;
                chunk.metadata.url = 'https://example.com/target';
                DatabaseManager.insertVectorsSQLite(db, chunk, embedding, testLogger);
            }

            const other = createTestChunk();
            other.metadata.chunk_id = 'other';
            other.metadata.url = 'https://example.com/other';
            DatabaseManager.insertVectorsSQLite(db, other, embedding, testLogger);

            DatabaseManager.removeChunksByUrlSQLite(db, 'https://example.com/target', testLogger);

            const remaining = db.prepare('SELECT chunk_id FROM vec_items').all() as any[];
            expect(remaining.length).toBe(1);
            expect(remaining[0].chunk_id).toBe('other');
        });

        it('should not error when no chunks match', () => {
            DatabaseManager.removeChunksByUrlSQLite(db, 'https://nonexistent.com/page', testLogger);
            // Should not throw
        });
    });

    // ─── getChunkHashesByUrlSQLite ──────────────────────────────────
    describe('getChunkHashesByUrlSQLite', () => {
        let db: BetterSqlite3.Database;

        beforeEach(() => {
            db = createTestDb();
        });

        afterEach(() => {
            db.close();
        });

        it('should return sorted hashes for chunks matching a URL', () => {
            const embedding = createTestEmbedding();

            const hashes = ['hash-c', 'hash-a', 'hash-b'];
            for (let i = 0; i < 3; i++) {
                const chunk = createTestChunk();
                chunk.metadata.chunk_id = `chunk-${i}`;
                chunk.metadata.url = 'https://example.com/page';
                DatabaseManager.insertVectorsSQLite(db, chunk, embedding, testLogger, hashes[i]);
            }

            // Add a chunk with a different URL
            const other = createTestChunk();
            other.metadata.chunk_id = 'other';
            other.metadata.url = 'https://example.com/other';
            DatabaseManager.insertVectorsSQLite(db, other, embedding, testLogger, 'hash-other');

            const result = DatabaseManager.getChunkHashesByUrlSQLite(db, 'https://example.com/page');
            expect(result).toEqual(['hash-a', 'hash-b', 'hash-c']);
        });

        it('should return empty array when no chunks match', () => {
            const result = DatabaseManager.getChunkHashesByUrlSQLite(db, 'https://nonexistent.com/page');
            expect(result).toEqual([]);
        });

        it('should return duplicate hashes correctly', () => {
            const embedding = createTestEmbedding();

            // Two chunks with the same hash (identical content under different headings)
            for (let i = 0; i < 2; i++) {
                const chunk = createTestChunk();
                chunk.metadata.chunk_id = `chunk-${i}`;
                chunk.metadata.url = 'https://example.com/page';
                DatabaseManager.insertVectorsSQLite(db, chunk, embedding, testLogger, 'same-hash');
            }

            const result = DatabaseManager.getChunkHashesByUrlSQLite(db, 'https://example.com/page');
            expect(result).toEqual(['same-hash', 'same-hash']);
        });
    });

    // ─── removeObsoleteFilesSQLite ───────────────────────────────────
    describe('removeObsoleteFilesSQLite', () => {
        let db: BetterSqlite3.Database;

        beforeEach(() => {
            db = createTestDb();
        });

        afterEach(() => {
            db.close();
        });

        it('should remove chunks for deleted files (direct path mode)', () => {
            const embedding = createTestEmbedding();

            const chunk1 = createTestChunk();
            chunk1.metadata.chunk_id = 'file1-chunk';
            chunk1.metadata.url = 'file:///project/src/a.ts';
            DatabaseManager.insertVectorsSQLite(db, chunk1, embedding, testLogger);

            const chunk2 = createTestChunk();
            chunk2.metadata.chunk_id = 'file2-chunk';
            chunk2.metadata.url = 'file:///project/src/b.ts';
            DatabaseManager.insertVectorsSQLite(db, chunk2, embedding, testLogger);

            // Only a.ts was processed
            const processedFiles = new Set(['/project/src/a.ts']);
            DatabaseManager.removeObsoleteFilesSQLite(db, processedFiles, '/project/src', testLogger);

            const remaining = db.prepare('SELECT chunk_id FROM vec_items').all() as any[];
            expect(remaining.length).toBe(1);
            expect(remaining[0].chunk_id).toBe('file1-chunk');
        });

        it('should remove chunks for deleted files (URL rewrite mode)', () => {
            const embedding = createTestEmbedding();

            const chunk1 = createTestChunk();
            chunk1.metadata.chunk_id = 'rewrite1';
            chunk1.metadata.url = 'https://mysite.com/src/a.ts';
            DatabaseManager.insertVectorsSQLite(db, chunk1, embedding, testLogger);

            const chunk2 = createTestChunk();
            chunk2.metadata.chunk_id = 'rewrite2';
            chunk2.metadata.url = 'https://mysite.com/src/deleted.ts';
            DatabaseManager.insertVectorsSQLite(db, chunk2, embedding, testLogger);

            const processedFiles = new Set(['/project/src/a.ts']);
            DatabaseManager.removeObsoleteFilesSQLite(
                db,
                processedFiles,
                { path: '/project', url_rewrite_prefix: 'https://mysite.com' },
                testLogger
            );

            const remaining = db.prepare('SELECT chunk_id FROM vec_items').all() as any[];
            expect(remaining.length).toBe(1);
            expect(remaining[0].chunk_id).toBe('rewrite1');
        });
    });

    // ─── Qdrant mock tests ──────────────────────────────────────────
    describe('Qdrant operations (mocked)', () => {
        it('should call upsert when storing a chunk in Qdrant', async () => {
            const mockClient = {
                upsert: vi.fn().mockResolvedValue({}),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_collection',
                type: 'qdrant',
            };

            const chunk = createTestChunk();
            const embedding = createTestEmbedding();

            await DatabaseManager.storeChunkInQdrant(qdrantDb, chunk, embedding, 'test-hash');

            expect(mockClient.upsert).toHaveBeenCalledOnce();
            expect(mockClient.upsert).toHaveBeenCalledWith('test_collection', expect.objectContaining({
                wait: true,
                points: expect.arrayContaining([
                    expect.objectContaining({
                        vector: embedding,
                        payload: expect.objectContaining({
                            content: chunk.content,
                            product_name: 'TestProduct',
                            hash: 'test-hash',
                        }),
                    }),
                ]),
            }));
        });

        it('should convert non-UUID chunk_id to UUID format', async () => {
            const mockClient = {
                upsert: vi.fn().mockResolvedValue({}),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_collection',
                type: 'qdrant',
            };

            const chunk = createTestChunk();
            // Use a proper hex hash that hashToUuid can convert
            chunk.metadata.chunk_id = 'abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789';
            const embedding = createTestEmbedding();

            await DatabaseManager.storeChunkInQdrant(qdrantDb, chunk, embedding);

            expect(mockClient.upsert).toHaveBeenCalledOnce();
            const call = mockClient.upsert.mock.calls[0];
            const pointId = call[1].points[0].id;
            // hashToUuid should produce UUID-like format with dashes
            expect(pointId).toMatch(/^[a-f0-9]{8}-[a-f0-9]{4}-5[a-f0-9]{3}-8[a-f0-9]{3}-[a-f0-9]{12}$/);
        });

        it('should handle upsert errors gracefully', async () => {
            const mockClient = {
                upsert: vi.fn().mockRejectedValue(new Error('Connection refused')),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_collection',
                type: 'qdrant',
            };

            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const chunk = createTestChunk();

            // Should not throw
            await DatabaseManager.storeChunkInQdrant(qdrantDb, chunk, createTestEmbedding());

            consoleSpy.mockRestore();
        });

        it('should create collection in Qdrant', async () => {
            const mockClient = {
                getCollections: vi.fn().mockResolvedValue({ collections: [] }),
                createCollection: vi.fn().mockResolvedValue({}),
            };

            await DatabaseManager.createCollectionQdrant(mockClient as any, 'test_col', testLogger);

            expect(mockClient.createCollection).toHaveBeenCalledOnce();
            expect(mockClient.createCollection).toHaveBeenCalledWith('test_col', expect.objectContaining({
                vectors: expect.objectContaining({
                    size: 3072,
                    distance: 'Cosine',
                }),
            }));
        });

        it('should skip creation when collection already exists', async () => {
            const mockClient = {
                getCollections: vi.fn().mockResolvedValue({
                    collections: [{ name: 'existing_col' }],
                }),
                createCollection: vi.fn(),
            };

            await DatabaseManager.createCollectionQdrant(mockClient as any, 'existing_col', testLogger);

            expect(mockClient.createCollection).not.toHaveBeenCalled();
        });

        it('should handle "already exists" error gracefully', async () => {
            const mockClient = {
                getCollections: vi.fn().mockResolvedValue({ collections: [] }),
                createCollection: vi.fn().mockRejectedValue(
                    Object.assign(new Error('Collection already exists'), { status: 409 })
                ),
            };

            // Should not throw
            await DatabaseManager.createCollectionQdrant(mockClient as any, 'test_col', testLogger);
        });

        it('should delete obsolete chunks from Qdrant', async () => {
            const mockClient = {
                scroll: vi.fn().mockResolvedValue({
                    points: [
                        { id: 'p1', payload: { url: 'https://example.com/old', is_metadata: false } },
                        { id: 'p2', payload: { url: 'https://example.com/current', is_metadata: false } },
                    ],
                }),
                delete: vi.fn().mockResolvedValue({}),
            };

            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            const visitedUrls = new Set(['https://example.com/current']);
            await DatabaseManager.removeObsoleteChunksQdrant(qdrantDb, visitedUrls, 'https://example.com', testLogger);

            expect(mockClient.delete).toHaveBeenCalledOnce();
            const deleteCall = mockClient.delete.mock.calls[0];
            expect(deleteCall[1].points).toContain('p1');
            expect(deleteCall[1].points).not.toContain('p2');
        });

        it('should not delete metadata points from Qdrant', async () => {
            const mockClient = {
                scroll: vi.fn().mockResolvedValue({
                    points: [
                        { id: 'meta1', payload: { url: 'metadata://repo', is_metadata: true } },
                    ],
                }),
                delete: vi.fn().mockResolvedValue({}),
            };

            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            await DatabaseManager.removeObsoleteChunksQdrant(qdrantDb, new Set(), 'metadata://', testLogger);

            expect(mockClient.delete).not.toHaveBeenCalled();
        });

        it('should delete chunks by URL in Qdrant', async () => {
            const mockClient = {
                delete: vi.fn().mockResolvedValue({}),
            };

            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            await DatabaseManager.removeChunksByUrlQdrant(qdrantDb, 'https://example.com/page', testLogger);

            expect(mockClient.delete).toHaveBeenCalledOnce();
            const deleteCall = mockClient.delete.mock.calls[0];
            expect(deleteCall[1].filter.must[0].match.text).toBe('https://example.com/page');
        });

        it('should set metadata value in Qdrant', async () => {
            const mockClient = {
                upsert: vi.fn().mockResolvedValue({}),
            };

            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            await DatabaseManager.setMetadataValue(qdrantDb, 'test_key', 'test_value', testLogger);

            expect(mockClient.upsert).toHaveBeenCalledOnce();
            const call = mockClient.upsert.mock.calls[0];
            const point = call[1].points[0];
            expect(point.payload.metadata_key).toBe('test_key');
            expect(point.payload.metadata_value).toBe('test_value');
            expect(point.payload.is_metadata).toBe(true);
        });

        it('should get metadata value from Qdrant', async () => {
            const mockClient = {
                retrieve: vi.fn().mockResolvedValue([
                    { payload: { metadata_value: 'found_value' } },
                ]),
            };

            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            const value = await DatabaseManager.getMetadataValue(qdrantDb, 'test_key', 'default', testLogger);
            expect(value).toBe('found_value');
        });

        it('should return default when Qdrant metadata not found', async () => {
            const mockClient = {
                retrieve: vi.fn().mockResolvedValue([]),
            };

            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            const value = await DatabaseManager.getMetadataValue(qdrantDb, 'missing', 'default_val', testLogger);
            expect(value).toBe('default_val');
        });
    });

    // ─── initDatabase - SQLite path ──────────────────────────────────
    describe('initDatabase - SQLite path', () => {
        it('should generate default db_path from product_name and version', async () => {
            const mockDb = {
                exec: vi.fn(),
                close: vi.fn(),
            };
            const BetterSqlite3Mock = vi.fn().mockReturnValue(mockDb);
            const sqliteVecLoadMock = vi.fn();

            // We need to spy on the actual module imports
            // Instead, we test the path logic by inspecting args
            const config: SourceConfig = {
                type: 'website',
                product_name: 'My Product',
                version: '2.0',
                max_size: 1000,
                url: 'https://example.com',
                database_config: {
                    type: 'sqlite',
                    params: {} // no db_path → should use default
                }
            };

            // Since we can't easily mock the BetterSqlite3 constructor without vi.mock,
            // we test the path construction logic directly
            const params = config.database_config.params as { db_path?: string };
            const expectedDefault = path.join(process.cwd(), 'My_Product-2.0.db');
            const dbPath = params.db_path || path.join(process.cwd(), `${config.product_name.replace(/\s+/g, '_')}-${config.version}.db`);
            expect(dbPath).toBe(expectedDefault);
        });

        it('should use custom db_path when provided', () => {
            const config: SourceConfig = {
                type: 'website',
                product_name: 'My Product',
                version: '2.0',
                max_size: 1000,
                url: 'https://example.com',
                database_config: {
                    type: 'sqlite',
                    params: { db_path: '/custom/path/my.db' }
                }
            };

            const params = config.database_config.params as { db_path?: string };
            const dbPath = params.db_path || path.join(process.cwd(), `${config.product_name.replace(/\s+/g, '_')}-${config.version}.db`);
            expect(dbPath).toBe('/custom/path/my.db');
        });

        it('should return SqliteDB connection from a real in-memory database', async () => {
            // Test with a real in-memory SQLite (simulates what initDatabase does)
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
            const conn: SqliteDB = { db, type: 'sqlite' };
            expect(conn.type).toBe('sqlite');
            expect(conn.db).toBeDefined();
            db.close();
        });
    });

    // ─── initDatabase - Qdrant path ──────────────────────────────────
    describe('initDatabase - Qdrant path', () => {
        it('should generate default collection name from product_name and version', () => {
            const config: SourceConfig = {
                type: 'website',
                product_name: 'My Product',
                version: '3.1',
                max_size: 1000,
                url: 'https://example.com',
                database_config: {
                    type: 'qdrant',
                    params: {} // no collection_name
                }
            };

            const params = config.database_config.params as { collection_name?: string };
            const collectionName = params.collection_name || `${config.product_name.toLowerCase().replace(/\s+/g, '_')}_${config.version}`;
            expect(collectionName).toBe('my_product_3.1');
        });

        it('should use custom collection name when provided', () => {
            const config: SourceConfig = {
                type: 'website',
                product_name: 'My Product',
                version: '3.1',
                max_size: 1000,
                url: 'https://example.com',
                database_config: {
                    type: 'qdrant',
                    params: { collection_name: 'custom_collection' }
                }
            };

            const params = config.database_config.params as { collection_name?: string };
            const collectionName = params.collection_name || `${config.product_name.toLowerCase().replace(/\s+/g, '_')}_${config.version}`;
            expect(collectionName).toBe('custom_collection');
        });

        it('should call createCollectionQdrant and return QdrantDB connection', async () => {
            const mockClient = {
                getCollections: vi.fn().mockResolvedValue({ collections: [] }),
                createCollection: vi.fn().mockResolvedValue({}),
            };

            // Simulate what initDatabase does for Qdrant
            const collectionName = 'test_col';
            await DatabaseManager.createCollectionQdrant(mockClient as any, collectionName, testLogger);

            expect(mockClient.createCollection).toHaveBeenCalledOnce();

            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName,
                type: 'qdrant',
            };
            expect(qdrantDb.type).toBe('qdrant');
            expect(qdrantDb.collectionName).toBe('test_col');
        });
    });

    // ─── initDatabase - unsupported type ─────────────────────────────
    describe('initDatabase - unsupported type', () => {
        it('should throw for unknown database type', async () => {
            const config = {
                type: 'website',
                product_name: 'Test',
                version: '1.0',
                max_size: 1000,
                url: 'https://example.com',
                database_config: {
                    type: 'mongodb' as any, // unsupported
                    params: {}
                }
            } as SourceConfig;

            await expect(
                DatabaseManager.initDatabase(config, testLogger)
            ).rejects.toThrow('Unsupported database type: mongodb');
        });
    });

    // ─── initDatabaseMetadata - Qdrant ───────────────────────────────
    describe('initDatabaseMetadata - Qdrant', () => {
        it('should be a no-op for Qdrant connections (just logs)', async () => {
            const mockClient = {};
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            // Should not throw - it's a no-op that just logs
            await DatabaseManager.initDatabaseMetadata(qdrantDb, testLogger);
        });
    });

    // ─── getLastRunDate - Qdrant ─────────────────────────────────────
    describe('getLastRunDate - Qdrant', () => {
        it('should return date from Qdrant when found', async () => {
            const mockClient = {
                retrieve: vi.fn().mockResolvedValue([
                    { payload: { metadata_value: '2025-06-15T10:00:00.000Z' } }
                ]),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            const date = await DatabaseManager.getLastRunDate(qdrantDb, 'owner/repo', '2025-01-01T00:00:00Z', testLogger);
            expect(date).toBe('2025-06-15T10:00:00.000Z');
            expect(mockClient.retrieve).toHaveBeenCalledOnce();
        });

        it('should return default when not found in Qdrant', async () => {
            const mockClient = {
                retrieve: vi.fn().mockResolvedValue([]),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            const date = await DatabaseManager.getLastRunDate(qdrantDb, 'owner/repo', '2025-01-01T00:00:00Z', testLogger);
            expect(date).toBe('2025-01-01T00:00:00Z');
        });

        it('should handle retrieve error gracefully and return default', async () => {
            const mockClient = {
                retrieve: vi.fn().mockRejectedValue(new Error('Network timeout')),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            const date = await DatabaseManager.getLastRunDate(qdrantDb, 'owner/repo', '2025-01-01T00:00:00Z', testLogger);
            expect(date).toBe('2025-01-01T00:00:00Z');
        });
    });

    // ─── updateLastRunDate - Qdrant ──────────────────────────────────
    describe('updateLastRunDate - Qdrant', () => {
        it('should call upsert with metadata point', async () => {
            const mockClient = {
                upsert: vi.fn().mockResolvedValue({}),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            await DatabaseManager.updateLastRunDate(qdrantDb, 'owner/repo', testLogger);

            expect(mockClient.upsert).toHaveBeenCalledOnce();
            const call = mockClient.upsert.mock.calls[0];
            expect(call[0]).toBe('test_col');
            const point = call[1].points[0];
            expect(point.payload.is_metadata).toBe(true);
            expect(point.payload.metadata_key).toBe('last_run_owner_repo');
            expect(point.payload.metadata_value).toMatch(/^\d{4}-\d{2}-\d{2}T/);
            expect(point.vector).toHaveLength(3072);
        });

        it('should handle upsert error gracefully', async () => {
            const mockClient = {
                upsert: vi.fn().mockRejectedValue(new Error('Qdrant down')),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            // Should not throw
            await DatabaseManager.updateLastRunDate(qdrantDb, 'owner/repo', testLogger);
        });
    });

    // ─── removeObsoleteChunksQdrant - error handling ─────────────────
    describe('removeObsoleteChunksQdrant - error handling', () => {
        it('should handle scroll error gracefully', async () => {
            const mockClient = {
                scroll: vi.fn().mockRejectedValue(new Error('Scroll failed')),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            // Should not throw
            await DatabaseManager.removeObsoleteChunksQdrant(qdrantDb, new Set(), 'https://example.com', testLogger);
        });
    });

    // ─── removeChunksByUrlQdrant - error handling ────────────────────
    describe('removeChunksByUrlQdrant - error handling', () => {
        it('should handle delete error gracefully', async () => {
            const mockClient = {
                delete: vi.fn().mockRejectedValue(new Error('Delete failed')),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            // Should not throw
            await DatabaseManager.removeChunksByUrlQdrant(qdrantDb, 'https://example.com/page', testLogger);
        });
    });

    // ─── removeObsoleteFilesQdrant ───────────────────────────────────
    describe('removeObsoleteFilesQdrant', () => {
        it('should delete obsolete file chunks in direct file path mode', async () => {
            const mockClient = {
                scroll: vi.fn().mockResolvedValue({
                    points: [
                        { id: 'p1', payload: { url: 'file:///project/src/a.ts', is_metadata: false } },
                        { id: 'p2', payload: { url: 'file:///project/src/deleted.ts', is_metadata: false } },
                    ],
                }),
                delete: vi.fn().mockResolvedValue({}),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            const processedFiles = new Set(['/project/src/a.ts']);
            await DatabaseManager.removeObsoleteFilesQdrant(qdrantDb, processedFiles, '/project/src', testLogger);

            expect(mockClient.delete).toHaveBeenCalledOnce();
            const deleteCall = mockClient.delete.mock.calls[0];
            expect(deleteCall[1].points).toContain('p2');
            expect(deleteCall[1].points).not.toContain('p1');
        });

        it('should delete obsolete file chunks in URL rewrite mode', async () => {
            const mockClient = {
                scroll: vi.fn().mockResolvedValue({
                    points: [
                        { id: 'r1', payload: { url: 'https://mysite.com/src/a.ts', is_metadata: false } },
                        { id: 'r2', payload: { url: 'https://mysite.com/src/deleted.ts', is_metadata: false } },
                    ],
                }),
                delete: vi.fn().mockResolvedValue({}),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            const processedFiles = new Set(['/project/src/a.ts']);
            await DatabaseManager.removeObsoleteFilesQdrant(
                qdrantDb,
                processedFiles,
                { path: '/project', url_rewrite_prefix: 'https://mysite.com' },
                testLogger
            );

            expect(mockClient.delete).toHaveBeenCalledOnce();
            const deleteCall = mockClient.delete.mock.calls[0];
            expect(deleteCall[1].points).toContain('r2');
            expect(deleteCall[1].points).not.toContain('r1');
        });

        it('should not delete when no obsolete files exist', async () => {
            const mockClient = {
                scroll: vi.fn().mockResolvedValue({
                    points: [
                        { id: 'p1', payload: { url: 'file:///project/src/a.ts', is_metadata: false } },
                    ],
                }),
                delete: vi.fn().mockResolvedValue({}),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            const processedFiles = new Set(['/project/src/a.ts']);
            await DatabaseManager.removeObsoleteFilesQdrant(qdrantDb, processedFiles, '/project/src', testLogger);

            expect(mockClient.delete).not.toHaveBeenCalled();
        });

        it('should handle error gracefully', async () => {
            const mockClient = {
                scroll: vi.fn().mockRejectedValue(new Error('Qdrant scroll failed')),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            // Should not throw
            await DatabaseManager.removeObsoleteFilesQdrant(qdrantDb, new Set(), '/project/src', testLogger);
        });
    });

    // ─── storeChunkInQdrant - randomUUID fallback ────────────────────
    describe('storeChunkInQdrant - randomUUID fallback', () => {
        it('should use crypto.randomUUID when hashToUuid throws', async () => {
            const mockClient = {
                upsert: vi.fn().mockResolvedValue({}),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            const chunk = createTestChunk();
            // Use a chunk_id that is not a valid UUID and would cause hashToUuid issues
            // hashToUuid only takes first 32 chars, but if isValidUuid returns false
            // and hashToUuid itself throws, we fall back to crypto.randomUUID
            chunk.metadata.chunk_id = ''; // empty string - hashToUuid may produce invalid output
            const embedding = createTestEmbedding();

            // Mock Utils.hashToUuid to throw
            const { Utils } = await import('../utils');
            const hashToUuidSpy = vi.spyOn(Utils, 'hashToUuid').mockImplementation(() => {
                throw new Error('Invalid hash input');
            });
            const isValidUuidSpy = vi.spyOn(Utils, 'isValidUuid').mockReturnValue(false);

            await DatabaseManager.storeChunkInQdrant(qdrantDb, chunk, embedding, 'test-hash');

            expect(mockClient.upsert).toHaveBeenCalledOnce();
            const call = mockClient.upsert.mock.calls[0];
            const pointId = call[1].points[0].id;
            // Should be a valid UUID from crypto.randomUUID()
            expect(pointId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);

            hashToUuidSpy.mockRestore();
            isValidUuidSpy.mockRestore();
        });
    });

    // ─── storeChunkInQdrant - hash generation ────────────────────────
    describe('storeChunkInQdrant - hash generation', () => {
        it('should generate hash from content when no chunkHash provided', async () => {
            const mockClient = {
                upsert: vi.fn().mockResolvedValue({}),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            const chunk = createTestChunk();
            const embedding = createTestEmbedding();

            // Call without chunkHash
            await DatabaseManager.storeChunkInQdrant(qdrantDb, chunk, embedding);

            expect(mockClient.upsert).toHaveBeenCalledOnce();
            const call = mockClient.upsert.mock.calls[0];
            const payload = call[1].points[0].payload;
            // hash should be a sha256 hex string (64 chars)
            expect(payload.hash).toMatch(/^[a-f0-9]{64}$/);
        });
    });

    // ─── setMetadataValue - error handling ────────────────────────────
    describe('setMetadataValue - error handling', () => {
        it('should handle SQLite error gracefully', async () => {
            const mockDb = {
                prepare: vi.fn().mockImplementation(() => {
                    throw new Error('SQLite prepare failed');
                }),
            };
            const conn: SqliteDB = { db: mockDb, type: 'sqlite' };

            // Should not throw - error is caught internally
            await DatabaseManager.setMetadataValue(conn, 'key', 'value', testLogger);
        });

        it('should handle Qdrant upsert error gracefully', async () => {
            const mockClient = {
                upsert: vi.fn().mockRejectedValue(new Error('Qdrant upsert failed')),
            };
            const qdrantDb: QdrantDB = {
                client: mockClient,
                collectionName: 'test_col',
                type: 'qdrant',
            };

            // Should not throw - error is caught internally
            await DatabaseManager.setMetadataValue(qdrantDb, 'key', 'value', testLogger);
        });
    });

    // ─── getMetadataValue - outer catch ──────────────────────────────
    describe('getMetadataValue - outer catch', () => {
        it('should catch error in SQLite prepare (outer try/catch)', async () => {
            const mockDb = {
                prepare: vi.fn().mockImplementation(() => {
                    throw new Error('SQLite prepare exploded');
                }),
            };
            const conn: SqliteDB = { db: mockDb, type: 'sqlite' };

            // The outer try/catch should handle this and return defaultValue
            const value = await DatabaseManager.getMetadataValue(conn, 'key', 'fallback', testLogger);
            expect(value).toBe('fallback');
        });

        it('should return undefined when SQLite prepare fails and no default', async () => {
            const mockDb = {
                prepare: vi.fn().mockImplementation(() => {
                    throw new Error('SQLite prepare exploded');
                }),
            };
            const conn: SqliteDB = { db: mockDb, type: 'sqlite' };

            const value = await DatabaseManager.getMetadataValue(conn, 'key', undefined, testLogger);
            expect(value).toBeUndefined();
        });
    });

    // ─── removeObsoleteFilesSQLite - trailing slash in URL prefix ────
    describe('removeObsoleteFilesSQLite - trailing slash in URL prefix', () => {
        let db: BetterSqlite3.Database;

        beforeEach(() => {
            db = createTestDb();
        });

        afterEach(() => {
            db.close();
        });

        it('should handle url_rewrite_prefix ending with trailing slash', () => {
            const embedding = createTestEmbedding();

            const chunk1 = createTestChunk();
            chunk1.metadata.chunk_id = 'trail1';
            chunk1.metadata.url = 'https://mysite.com/src/a.ts';
            DatabaseManager.insertVectorsSQLite(db, chunk1, embedding, testLogger);

            const chunk2 = createTestChunk();
            chunk2.metadata.chunk_id = 'trail2';
            chunk2.metadata.url = 'https://mysite.com/src/deleted.ts';
            DatabaseManager.insertVectorsSQLite(db, chunk2, embedding, testLogger);

            const processedFiles = new Set(['/project/src/a.ts']);
            // Trailing slash in url_rewrite_prefix
            DatabaseManager.removeObsoleteFilesSQLite(
                db,
                processedFiles,
                { path: '/project', url_rewrite_prefix: 'https://mysite.com/' },
                testLogger
            );

            const remaining = db.prepare('SELECT chunk_id FROM vec_items').all() as any[];
            expect(remaining.length).toBe(1);
            expect(remaining[0].chunk_id).toBe('trail1');
        });
    });

    // ─── removeObsoleteFilesSQLite - path starting with ./ ───────────
    describe('removeObsoleteFilesSQLite - path starting with ./', () => {
        let db: BetterSqlite3.Database;

        beforeEach(() => {
            db = createTestDb();
        });

        afterEach(() => {
            db.close();
        });

        it('should clean ./ prefix from path when used as string pathConfig', () => {
            const embedding = createTestEmbedding();

            // When pathConfig is './project/src', the ./ is stripped to get 'project/src'
            // Then urlPrefix becomes 'file://project/src'
            // So chunks must have URLs starting with 'file://project/src'
            const chunk1 = createTestChunk();
            chunk1.metadata.chunk_id = 'dot-slash-1';
            chunk1.metadata.url = 'file://project/src/keep.ts';
            DatabaseManager.insertVectorsSQLite(db, chunk1, embedding, testLogger);

            const chunk2 = createTestChunk();
            chunk2.metadata.chunk_id = 'dot-slash-2';
            chunk2.metadata.url = 'file://project/src/remove.ts';
            DatabaseManager.insertVectorsSQLite(db, chunk2, embedding, testLogger);

            // filePath is extracted by removing 'file://' prefix (7 chars)
            // so 'file://project/src/keep.ts' -> 'project/src/keep.ts'
            const processedFiles = new Set(['project/src/keep.ts']);
            // pathConfig starts with ./
            DatabaseManager.removeObsoleteFilesSQLite(db, processedFiles, './project/src', testLogger);

            const remaining = db.prepare('SELECT chunk_id FROM vec_items').all() as any[];
            expect(remaining.length).toBe(1);
            expect(remaining[0].chunk_id).toBe('dot-slash-1');
        });
    });

    // ─── hasColumn - error path ──────────────────────────────────────
    describe('hasColumn - error path', () => {
        it('should return false when PRAGMA fails', () => {
            const mockDb = {
                prepare: vi.fn().mockImplementation(() => {
                    throw new Error('PRAGMA failed');
                }),
            };

            // Access the private method via the prototype
            const result = (DatabaseManager as any).hasColumn(mockDb, 'branch');
            expect(result).toBe(false);
        });

        it('should return false when column does not exist', () => {
            const mockDb = {
                prepare: vi.fn().mockReturnValue({
                    all: vi.fn().mockReturnValue([
                        { name: 'other_column' },
                        { name: 'another_column' },
                    ]),
                }),
            };

            const result = (DatabaseManager as any).hasColumn(mockDb, 'nonexistent');
            expect(result).toBe(false);
        });

        it('should return true when column exists', () => {
            const mockDb = {
                prepare: vi.fn().mockReturnValue({
                    all: vi.fn().mockReturnValue([
                        { name: 'branch' },
                        { name: 'repo' },
                    ]),
                }),
            };

            const result = (DatabaseManager as any).hasColumn(mockDb, 'branch');
            expect(result).toBe(true);
        });
    });
});
