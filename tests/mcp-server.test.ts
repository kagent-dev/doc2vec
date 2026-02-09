import http from 'http';
import os from 'os';
import path from 'path';
import fs from 'fs';
import BetterSqlite3 from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';
import { describe, expect, it, vi } from 'vitest';
import {
    createQueryHandlers,
    createQdrantProvider,
    createSqliteDbProvider,
    filterResultsByUrl,
    filterResultsWithContent,
    normalizeExtensions,
} from '../mcp/src/server';
import { ContentProcessor } from '../content-processor';
import { DatabaseManager } from '../database';
import { Logger, LogLevel } from '../logger';
import type { WebsiteSourceConfig } from '../types';

describe('MCP server helpers', () => {
    it('normalizes extensions to lowercase and dot-prefixed', () => {
        expect(normalizeExtensions(['ts', '.JS', 'Md'])).toEqual(['.ts', '.js', '.md']);
        expect(normalizeExtensions()).toEqual([]);
    });

    it('filters results by url prefix and extensions', () => {
        const results = [
            { chunk_id: '1', distance: 0.1, content: 'a', url: 'https://docs.example.com/guide/a.ts' },
            { chunk_id: '2', distance: 0.2, content: 'b', url: 'https://docs.example.com/guide/b.md' },
            { chunk_id: '3', distance: 0.3, content: 'c', url: 'https://docs.example.com/other/c.ts' },
        ];

        const filtered = filterResultsByUrl(results, 'https://docs.example.com/guide/', ['TS']);
        expect(filtered.map((row) => row.chunk_id)).toEqual(['1']);
    });

    it('filters results with empty or non-string content', () => {
        const results = [
            { chunk_id: '1', distance: 0.1, content: 'ok' },
            { chunk_id: '2', distance: 0.2, content: '' },
            { chunk_id: '3', distance: 0.3, content: '   ' },
            { chunk_id: '4', distance: 0.4, content: 12 as unknown as string },
        ];

        const filtered = filterResultsWithContent(results);
        expect(filtered.map((row) => row.chunk_id)).toEqual(['1']);
    });
});

describe('MCP query handlers', () => {
    const createEmbeddings = vi.fn(async () => [0.1, 0.2]);
    const resolveDbPath = vi.fn(() => ({ dbPath: '/tmp/db.db', dbLabel: 'db.db' }));
    const queryCollection = vi.fn(async () => []);
    const getChunksForDocument = vi.fn(async () => []);

    it('returns validation message when query_documentation params are missing', async () => {
        const { queryDocumentationToolHandler } = createQueryHandlers({
            createEmbeddings,
            resolveDbPath,
            queryCollection,
            getChunksForDocument,
        });

        const response = await queryDocumentationToolHandler({
            queryText: 'test',
            limit: 2,
        });

        expect(response.content[0].text).toContain('Provide either productName or dbName');
    });

    it('filters empty content and url prefix in queryDocumentation', async () => {
        const collectionResults = [
            { chunk_id: '1', distance: 0.1, content: 'ok', url: 'https://docs.example.com/a' },
            { chunk_id: '2', distance: 0.2, content: '', url: 'https://docs.example.com/a' },
            { chunk_id: '3', distance: 0.3, content: 'ok', url: 'https://other.example.com/b' },
        ];

        const { queryDocumentation } = createQueryHandlers({
            createEmbeddings,
            resolveDbPath,
            queryCollection: vi.fn(async () => collectionResults),
            getChunksForDocument,
        });

        const results = await queryDocumentation('test', 'product', undefined, undefined, 'https://docs.example.com/', 4);
        expect(results).toHaveLength(1);
        expect(results[0].content).toBe('ok');
    });

    it('returns empty-content warning for query_code when all matches are empty', async () => {
        const { queryCodeToolHandler } = createQueryHandlers({
            createEmbeddings,
            resolveDbPath,
            queryCollection: vi.fn(async () => [
                { chunk_id: '1', distance: 0.1, content: '' },
                { chunk_id: '2', distance: 0.2, content: '  ' },
            ]),
            getChunksForDocument,
        });

        const response = await queryCodeToolHandler({
            queryText: 'test',
            dbName: 'repo',
            limit: 3,
        });

        expect(response.content[0].text).toContain('all matching chunks have empty content');
    });

    it('formats get_chunks results with chunk index', async () => {
        const { getChunksToolHandler } = createQueryHandlers({
            createEmbeddings,
            resolveDbPath,
            queryCollection,
            getChunksForDocument: vi.fn(async () => [
                {
                    chunk_id: '1',
                    distance: 0,
                    content: 'chunk content',
                    url: 'file://doc',
                    chunk_index: 0,
                    total_chunks: 2,
                },
            ]),
        });

        const response = await getChunksToolHandler({
            productName: 'product',
            filePath: 'file://doc',
        });

        expect(response.content[0].text).toContain('Chunk 1 of 2');
    });
});

describe('SQLite provider compatibility', () => {
    it('falls back when chunk_index column is missing', async () => {
        const sqliteVec = { load: vi.fn() };
        const fs = { existsSync: vi.fn(() => true) };

        class FakeDb {
            prepare(query: string) {
                if (query.includes('chunk_index')) {
                    return {
                        all: () => {
                            throw new Error('no such column: chunk_index');
                        },
                    };
                }
                return {
                    all: () => [
                        {
                            chunk_id: '1',
                            content: 'ok',
                            url: 'file://doc',
                            section: 's',
                            heading_hierarchy: 'h',
                        },
                    ],
                };
            }

            close() {
                return undefined;
            }
        }

        const { getChunksForDocument } = createSqliteDbProvider({
            dbDir: '/data',
            sqliteVec,
            Database: FakeDb,
            fs,
            path,
        });

        const results = await getChunksForDocument('product', undefined, 'file://doc', 0, 1, '1.0');
        expect(results).toHaveLength(1);
        expect(results[0].content).toBe('ok');
    });

    it('resolves db paths with normalized extension', () => {
        const sqliteVec = { load: vi.fn() };
        const fs = { existsSync: vi.fn(() => true) };
        class FakeDb {
            prepare() {
                return { all: () => [] };
            }
            close() {
                return undefined;
            }
        }

        const { resolveDbPath } = createSqliteDbProvider({
            dbDir: '/data',
            sqliteVec,
            Database: FakeDb,
            fs,
            path,
        });

        const resolved = resolveDbPath('my-db');
        expect(resolved.dbPath).toBe(path.join('/data', 'my-db.db'));
        expect(resolved.dbLabel).toBe('my-db.db');
    });
});

describe('Qdrant provider', () => {
    it('maps dbName to collection and returns search results', async () => {
        const client = {
            search: vi.fn(async () => ({
                result: [
                    {
                        id: 'point-1',
                        score: 0.42,
                        payload: {
                            chunk_id: 'chunk-1',
                            content: 'Hello Qdrant',
                            url: 'https://example.com/doc',
                            section: 'Intro',
                            chunk_index: 0,
                            total_chunks: 1,
                        },
                    },
                ],
            })),
            scroll: vi.fn(async () => ({ points: [], next_page_offset: null })),
        };

        const { queryCollection, resolveDbPath } = createQdrantProvider({ client });
        const { dbPath } = resolveDbPath('my-collection');

        const results = await queryCollection([0.1, 0.2], dbPath, { product_name: 'TestProduct' }, 5);
        expect(results).toHaveLength(1);
        expect(results[0].content).toBe('Hello Qdrant');
        expect(results[0].distance).toBe(0.42);
        expect(client.search).toHaveBeenCalledWith('my-collection', expect.objectContaining({ limit: 5 }));
    });

    it('scrolls chunks and sorts by chunk_index', async () => {
        const client = {
            search: vi.fn(async () => ({ result: [] })),
            scroll: vi.fn(async () => ({
                points: [
                    {
                        id: 'point-2',
                        payload: {
                            chunk_id: 'chunk-2',
                            content: 'Second',
                            url: 'file://doc',
                            chunk_index: 1,
                            total_chunks: 2,
                        },
                    },
                    {
                        id: 'point-1',
                        payload: {
                            chunk_id: 'chunk-1',
                            content: 'First',
                            url: 'file://doc',
                            chunk_index: 0,
                            total_chunks: 2,
                        },
                    },
                ],
                next_page_offset: null,
            })),
        };

        const { getChunksForDocument } = createQdrantProvider({ client });
        const results = await getChunksForDocument('Product', 'collection', 'file://doc', 0, 1, '1.0');

        expect(results.map((r) => r.content)).toEqual(['First', 'Second']);
        expect(client.scroll).toHaveBeenCalledWith('collection', expect.objectContaining({ limit: 1000 }));
    });
});

describe('MCP server end-to-end', () => {
    it('parses, stores, and retrieves via MCP handlers', async () => {
        const logger = new Logger('test', { level: LogLevel.NONE });
        const processor = new ContentProcessor(logger);
        const uniquePhrase = 'E2E Unique Phrase 123';

        const server = http.createServer((_req, res) => {
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(`
                <html>
                  <body>
                    <article>
                      <h1>Sample Doc</h1>
                      <p>${uniquePhrase}</p>
                    </article>
                  </body>
                </html>
            `);
        });

        await new Promise<void>((resolve) => server.listen(0, resolve));
        const address = server.address();
        if (!address || typeof address === 'string') {
            server.close();
            throw new Error('Failed to start test server');
        }
        const baseUrl = `http://127.0.0.1:${address.port}`;

        const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-e2e-'));
        const dbPath = path.join(tempDir, 'e2e.db');
        const db = new BetterSqlite3(dbPath, { allowExtension: true } as any);
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

        try {
            const htmlResponse = await fetch(baseUrl);
            const html = await htmlResponse.text();
            const markdown = processor.convertHtmlToMarkdown(html);

            const sourceConfig: WebsiteSourceConfig = {
                type: 'website',
                url: baseUrl,
                product_name: 'TestProduct',
                version: '1.0',
                max_size: 100_000,
                database_config: { type: 'sqlite', params: { db_path: dbPath } },
            };

            const chunks = await processor.chunkMarkdown(markdown, sourceConfig, baseUrl);
            const embedding = new Array(3072).fill(0.1);
            for (const chunk of chunks) {
                chunk.metadata.branch = '';
                chunk.metadata.repo = '';
                DatabaseManager.insertVectorsSQLite(db as any, chunk, embedding, logger);
            }

            const { resolveDbPath, queryCollection, getChunksForDocument } = createSqliteDbProvider({
                dbDir: tempDir,
                sqliteVec,
                Database: BetterSqlite3 as any,
                fs,
                path,
            });

            const { queryDocumentationToolHandler } = createQueryHandlers({
                createEmbeddings: async () => embedding,
                resolveDbPath,
                queryCollection,
                getChunksForDocument,
            });

            const response = await queryDocumentationToolHandler({
                queryText: uniquePhrase,
                productName: 'TestProduct',
                dbName: 'e2e.db',
                limit: 2,
            });

            const responseText = response.content[0].text;
            expect(responseText).toContain(uniquePhrase);
            expect(responseText).toContain(baseUrl);
        } finally {
            db.close();
            server.close();
            fs.rmSync(tempDir, { recursive: true, force: true });
        }
    });
});
