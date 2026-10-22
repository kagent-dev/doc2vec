import http from 'http';
import os from 'os';
import path from 'path';
import fs from 'fs';
import BetterSqlite3 from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';
import { describe, expect, it, vi } from 'vitest';
import {
    createDeepInfraReranker,
    createQueryHandlers,
    createQdrantProvider,
    createSqliteDbProvider,
    filterResultsByUrl,
    filterResultsWithContent,
    fuseRrf,
    normalizeExtensions,
    onePerPage,
    toFtsQuery,
    type QueryResult,
} from '../mcp/src/server';
import { ContentProcessor } from '../content-processor';
import { DatabaseManager } from '../database';
import { Logger, LogLevel } from '../logger';
import type { WebsiteSourceConfig } from '../types';

const TEST_EMBEDDING_DIMENSION = 3072;

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
            query: vi.fn(async () => ({
                points: [
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
        // The Query API, as @qdrant/js-client-rest 1.19 no longer has search
        expect(client.query).toHaveBeenCalledWith('my-collection', expect.objectContaining({ query: [0.1, 0.2], limit: 5 }));
    });

    it('scrolls chunks and sorts by chunk_index', async () => {
        const client = {
            query: vi.fn(async () => ({ points: [] })),
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
                embedding FLOAT[${TEST_EMBEDDING_DIMENSION}],
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
            const embedding = new Array(TEST_EMBEDDING_DIMENSION).fill(0.1);
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

describe('Retrieval helpers', () => {
    it('onePerPage keeps the first chunk of each page, ignoring fragments and trailing slashes', () => {
        const rows = [{ url: 'https://d/a#x' }, { url: 'https://d/b' }, { url: 'https://d/a/' }, { url: 'https://d/c' }];
        expect(onePerPage(rows).map(r => r.url)).toEqual(['https://d/a#x', 'https://d/b', 'https://d/c']);
    });

    it('fuseRrf ranks chunks found by both lists first and keeps the first list row', () => {
        const dense: QueryResult[] = [
            { chunk_id: 'a', content: 'a', distance: 0.1 },
            { chunk_id: 'b', content: 'b', distance: 0.2 },
            { chunk_id: 'c', content: 'c', distance: 0.3 },
        ];
        const keyword: QueryResult[] = [
            { chunk_id: 'b', content: 'b' },
            { chunk_id: 'd', content: 'd' },
        ];
        const fused = fuseRrf([dense, keyword]);
        expect(fused.map(r => r.chunk_id)).toEqual(['b', 'a', 'd', 'c']);
        expect(fused[0].distance).toBe(0.2);
    });

    it('fuseRrf keeps the same text on two pages as two chunks', () => {
        // chunk_id is a hash of the text, so a snippet repeated on two pages shares it
        const dense: QueryResult[] = [{ chunk_id: 'same', content: 'x', url: 'https://d/a' }];
        const keyword: QueryResult[] = [{ chunk_id: 'same', content: 'x', url: 'https://d/b' }];
        expect(fuseRrf([dense, keyword]).map(r => r.url)).toEqual(['https://d/a', 'https://d/b']);
    });

    it('toFtsQuery quotes terms so punctuation cannot break MATCH', () => {
        expect(toFtsQuery('Set "maxRequestSize" (v2.3)?')).toBe('"set" OR "maxrequestsize" OR "v2" OR "3"');
        expect(toFtsQuery('?!')).toBe('');
    });
});

describe('MCP retrieval pipeline', () => {
    const createEmbeddings = vi.fn(async () => [0.1, 0.2]);
    const resolveDbPath = vi.fn(() => ({ dbPath: '/tmp/db.db', dbLabel: 'db.db' }));
    const getChunksForDocument = vi.fn(async () => []);
    const dense: QueryResult[] = [
        { chunk_id: '1', distance: 0.1, content: 'changelog part 1', url: 'https://d/changelog/' },
        { chunk_id: '2', distance: 0.2, content: 'changelog part 2', url: 'https://d/changelog/' },
        { chunk_id: '3', distance: 0.3, content: 'changelog part 3', url: 'https://d/changelog/' },
        { chunk_id: '4', distance: 0.4, content: 'helm install', url: 'https://d/install/helm/' },
        { chunk_id: '5', distance: 0.5, content: 'mtls', url: 'https://d/mtls/' },
    ];

    it('keeps the default behaviour when no option is set', async () => {
        const queryCollection = vi.fn(async () => dense);
        const keywordSearch = vi.fn(async () => []);
        const { queryDocumentation } = createQueryHandlers({ createEmbeddings, resolveDbPath, queryCollection, getChunksForDocument, keywordSearch });
        const results = await queryDocumentation('helm chart', 'product', undefined, undefined, undefined, 2);
        expect(results.map(r => r.url)).toEqual(['https://d/changelog/', 'https://d/changelog/']);
        expect(queryCollection).toHaveBeenCalledWith([0.1, 0.2], '/tmp/db.db', expect.any(Object), 2);
        expect(keywordSearch).not.toHaveBeenCalled();
    });

    it('returns one chunk per page and fetches deeper to fill the limit', async () => {
        const queryCollection = vi.fn(async () => dense);
        const { queryDocumentation } = createQueryHandlers({
            createEmbeddings, resolveDbPath, queryCollection, getChunksForDocument, options: { onePerPage: true },
        });
        const results = await queryDocumentation('helm chart', 'product', undefined, undefined, undefined, 3);
        expect(results.map(r => r.url)).toEqual(['https://d/changelog/', 'https://d/install/helm/', 'https://d/mtls/']);
        expect(queryCollection).toHaveBeenCalledWith(expect.any(Array), '/tmp/db.db', expect.any(Object), 50);
    });

    it('fuses keyword results in hybrid mode and survives a keyword failure', async () => {
        const queryCollection = vi.fn(async () => dense.slice(0, 3));
        const keyword: QueryResult[] = [{ chunk_id: '4', content: 'helm install', url: 'https://d/install/helm/' }];
        const { queryDocumentation } = createQueryHandlers({
            createEmbeddings, resolveDbPath, queryCollection, getChunksForDocument,
            keywordSearch: vi.fn(async () => keyword), options: { hybrid: true },
        });
        const results = await queryDocumentation('helm chart', 'product', undefined, undefined, undefined, 4);
        expect(results.map(r => r.url)).toContain('https://d/install/helm/');
        // A keyword-only hit has no vector distance to report
        expect(results.find(r => r.url === 'https://d/install/helm/')?.distance).toBeUndefined();

        const failing = createQueryHandlers({
            createEmbeddings, resolveDbPath, queryCollection, getChunksForDocument,
            keywordSearch: vi.fn(async () => { throw new Error('index broken'); }), options: { hybrid: true },
        });
        const fallback = await failing.queryDocumentation('helm chart', 'product', undefined, undefined, undefined, 2);
        expect(fallback.map(r => r.content)).toEqual(['changelog part 1', 'changelog part 2']);
    });

    it('reranks the candidates, then keeps one chunk per page', async () => {
        const queryCollection = vi.fn(async () => dense);
        const rerank = vi.fn(async (_q: string, docs: string[]) => docs.map(d => (d.includes('helm') ? 5 : d.includes('mtls') ? 3 : 1)));
        const { queryDocumentation, queryDocumentationToolHandler } = createQueryHandlers({
            createEmbeddings, resolveDbPath, queryCollection, getChunksForDocument, rerank,
            options: { onePerPage: true, rerankCandidates: 4 },
        });
        const results = await queryDocumentation('helm chart', 'product', undefined, undefined, undefined, 3);
        expect(results.map(r => r.url)).toEqual(['https://d/install/helm/', 'https://d/changelog/', 'https://d/mtls/']);
        expect(results[0].relevance).toBe(5);
        // Only the top rerankCandidates go to the reranker; the rest keep their order after them
        expect(rerank.mock.calls[0][1]).toHaveLength(4);
        expect(results[2].relevance).toBeUndefined();

        const response = await queryDocumentationToolHandler({ queryText: 'helm chart', productName: 'product', limit: 1 });
        expect(response.content[0].text).toContain('Relevance: 5.0000');
        expect(response.content[0].text).toContain('Distance: 0.4000');
    });

    it('keeps retrieval order when the reranker fails', async () => {
        const { queryDocumentation } = createQueryHandlers({
            createEmbeddings, resolveDbPath, queryCollection: vi.fn(async () => dense), getChunksForDocument,
            rerank: vi.fn(async () => { throw new Error('timeout'); }), options: { onePerPage: true },
        });
        const results = await queryDocumentation('helm chart', 'product', undefined, undefined, undefined, 2);
        expect(results.map(r => r.url)).toEqual(['https://d/changelog/', 'https://d/install/helm/']);
    });

    it('applies the same pipeline to query_code, after its path filters', async () => {
        const code: QueryResult[] = [
            { chunk_id: '1', distance: 0.1, content: 'func A', url: 'https://g/repo/a.go' },
            { chunk_id: '2', distance: 0.2, content: 'func B', url: 'https://g/repo/a.go' },
            { chunk_id: '3', distance: 0.3, content: 'doc', url: 'https://g/repo/README.md' },
            { chunk_id: '4', distance: 0.4, content: 'func C', url: 'https://g/repo/b.go' },
        ];
        const { queryCode } = createQueryHandlers({
            createEmbeddings, resolveDbPath, queryCollection: vi.fn(async () => code), getChunksForDocument,
            options: { onePerPage: true },
        });
        const { results } = await queryCode('func', undefined, undefined, 'repo', undefined, undefined, ['.go'], 3);
        expect(results.map(r => r.url)).toEqual(['https://g/repo/a.go', 'https://g/repo/b.go']);
    });
});

describe('DeepInfra reranker', () => {
    it('sends the query and documents and returns one score each', async () => {
        const fetchMock = vi.fn(async () => new Response(JSON.stringify({ scores: [0.2, 1.5] }), { status: 200 }));
        const rerank = createDeepInfraReranker({ apiKey: 'test-key', fetch: fetchMock as unknown as typeof fetch });
        await expect(rerank('install helm', ['a', 'b'])).resolves.toEqual([0.2, 1.5]);
        const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
        expect(url).toBe('https://api.deepinfra.com/v1/inference/nvidia/Nemotron-3.5-Rerank-8B-BF16');
        expect(JSON.parse(String(init.body))).toEqual({ queries: ['install helm'], documents: ['a', 'b'] });
        expect((init.headers as Record<string, string>).Authorization).toBe('Bearer test-key');
    });

    it('rejects an HTTP error or a short score list', async () => {
        const failing = createDeepInfraReranker({
            apiKey: 'k', model: 'Qwen/Qwen3-Reranker-4B',
            fetch: (async () => new Response('busy', { status: 503 })) as unknown as typeof fetch,
        });
        await expect(failing('q', ['a'])).rejects.toThrow(/HTTP 503/);
        const short = createDeepInfraReranker({
            apiKey: 'k', fetch: (async () => new Response(JSON.stringify({ scores: [1] }), { status: 200 })) as unknown as typeof fetch,
        });
        await expect(short('q', ['a', 'b'])).rejects.toThrow(/one score per document/);
    });
});

describe('SQLite keyword search', () => {
    function buildDb(dir: string): string {
        const dbPath = path.join(dir, 'kw.db');
        const db = new BetterSqlite3(dbPath);
        sqliteVec.load(db);
        db.exec(`
            CREATE VIRTUAL TABLE vec_items USING vec0(
                embedding FLOAT[2], product_name TEXT, version TEXT, branch TEXT, repo TEXT,
                heading_hierarchy TEXT, section TEXT, chunk_id TEXT UNIQUE, content TEXT, url TEXT,
                hash TEXT, chunk_index INTEGER, total_chunks INTEGER
            );
        `);
        const insert = db.prepare(`INSERT INTO vec_items (embedding, product_name, version, branch, repo, heading_hierarchy, section, chunk_id, content, url, hash, chunk_index, total_chunks)
            VALUES (?, ?, 'latest', '', '', '[]', 's', ?, ?, ?, 'h', 0, 1)`);
        insert.run(new Float32Array([1, 0]), 'istio', 'c1', 'Enable STRICT PeerAuthentication for mutual TLS', 'https://d/mtls');
        insert.run(new Float32Array([0, 1]), 'istio', 'c2', 'Install the mesh with istioctl', 'https://d/install');
        insert.run(new Float32Array([0, 1]), 'cilium', 'c3', 'PeerAuthentication is not a Cilium resource', 'https://d/cilium');
        db.close();
        return dbPath;
    }

    it('ranks stored chunks by keywords, honours filters and rebuilds when the file changes', async () => {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-kw-'));
        try {
            const dbPath = buildDb(dir);
            const provider = createSqliteDbProvider({ dbDir: dir, sqliteVec, Database: BetterSqlite3 as any, fs, path });
            const hits = await provider.keywordSearch('PeerAuthentication: STRICT?', dbPath, { product_name: 'istio' }, 5);
            expect(hits.map(h => h.chunk_id)).toEqual(['c1']);
            expect(hits[0]).toMatchObject({ url: 'https://d/mtls', chunk_index: 0, total_chunks: 1 });

            // A new chunk only shows up once the index notices the file changed
            const db = new BetterSqlite3(dbPath);
            sqliteVec.load(db);
            db.prepare(`INSERT INTO vec_items (embedding, product_name, version, branch, repo, heading_hierarchy, section, chunk_id, content, url, hash, chunk_index, total_chunks)
                VALUES (?, 'istio', 'latest', '', '', '[]', 's', 'c4', 'Waypoint proxies for ambient', 'https://d/waypoint', 'h', 0, 1)`)
                .run(new Float32Array([1, 1]));
            db.close();
            const later = new Date(Date.now() + 5000);
            fs.utimesSync(dbPath, later, later);
            const again = await provider.keywordSearch('waypoint', dbPath, {}, 5);
            expect(again.map(h => h.chunk_id)).toEqual(['c4']);
        } finally {
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });

    it('feeds hybrid retrieval end to end with the real vector query', async () => {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-kw-'));
        try {
            const dbPath = buildDb(dir);
            const provider = createSqliteDbProvider({ dbDir: dir, sqliteVec, Database: BetterSqlite3 as any, fs, path });
            const { queryDocumentation } = createQueryHandlers({
                // The vector points at install; the keywords point at mtls
                createEmbeddings: async () => [0, 1],
                resolveDbPath: provider.resolveDbPath,
                queryCollection: provider.queryCollection,
                getChunksForDocument: provider.getChunksForDocument,
                keywordSearch: provider.keywordSearch,
                options: { hybrid: true, onePerPage: true },
            });
            const results = await queryDocumentation('PeerAuthentication STRICT', 'istio', 'kw', undefined, undefined, 2);
            expect(results.map(r => r.url).sort()).toEqual(['https://d/install', 'https://d/mtls']);
        } finally {
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });
});

describe('Qdrant keyword search', () => {
    const point = { id: 'p1', score: 7.3, payload: { chunk_id: 'c1', content: 'Enable STRICT mTLS', url: 'https://d/mtls', chunk_index: 0, total_chunks: 1 } };

    it('queries the BM25 vector with the same model doc2vec writes and reports no distance', async () => {
        const client = { query: vi.fn(async () => ({ points: [point] })), scroll: vi.fn() };
        const { keywordSearch } = createQdrantProvider({ client });
        const results = await keywordSearch('strict mtls', 'docs', { product_name: 'istio', version: 'latest' }, 50);
        expect(client.query).toHaveBeenCalledWith('docs', {
            query: { text: 'strict mtls', model: 'qdrant/bm25' },
            using: 'bm25',
            limit: 50,
            filter: { must: [{ key: 'product_name', match: { value: 'istio' } }, { key: 'version', match: { value: 'latest' } }] },
            with_payload: true,
            with_vector: false,
        });
        expect(results).toHaveLength(1);
        expect(results[0]).toMatchObject({ chunk_id: 'c1', url: 'https://d/mtls' });
        expect(results[0].distance).toBeUndefined();
    });

    it('stops asking a collection without the keyword vector for a while', async () => {
        const client = { query: vi.fn(async () => { throw new Error('Not existing vector name: bm25'); }), scroll: vi.fn() };
        const { keywordSearch } = createQdrantProvider({ client });
        await expect(keywordSearch('q', 'old_col', {}, 10)).rejects.toThrow(/bm25/);
        await expect(keywordSearch('q', 'old_col', {}, 10)).resolves.toEqual([]);
        expect(client.query).toHaveBeenCalledTimes(1);
    });

    it('falls back to vector results end to end when the collection has no keyword vector', async () => {
        const client = {
            // Vector queries answer; keyword queries on the missing vector fail
            query: vi.fn(async (_name: string, params: any) => {
                if (params.using === 'bm25') throw new Error('Not existing vector name: bm25');
                return { points: [point] };
            }),
            scroll: vi.fn(),
        };
        const provider = createQdrantProvider({ client });
        const { queryDocumentation } = createQueryHandlers({
            createEmbeddings: async () => [0.1, 0.2],
            resolveDbPath: provider.resolveDbPath,
            queryCollection: provider.queryCollection,
            getChunksForDocument: provider.getChunksForDocument,
            keywordSearch: provider.keywordSearch,
            options: { hybrid: true },
        });
        const results = await queryDocumentation('strict mtls', undefined, 'old_col', undefined, undefined, 4);
        expect(results.map(r => r.url)).toEqual(['https://d/mtls']);
    });
});
