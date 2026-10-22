export interface QueryResult {
    chunk_id: string;
    distance?: number;
    content: string;
    url?: string;
    section?: string;
    heading_hierarchy?: string;
    chunk_index?: number;
    total_chunks?: number;
    embedding?: Float32Array | number[];
    relevance?: number;
    [key: string]: unknown;
}

export type QueryFilter = {
    product_name?: string;
    version?: string;
    branch?: string;
    repo?: string;
    urlPrefix?: string;
    extensions?: string[];
};

export type ResolveDbPath = (dbName?: string, productName?: string, version?: string, repo?: string) => { dbPath: string; dbLabel: string };

export type QueryCollection = (
    queryEmbedding: number[],
    dbPath: string,
    filter: QueryFilter,
    topK?: number
) => Promise<QueryResult[]>;

// Keyword (BM25) search over the same chunks, for hybrid retrieval
export type KeywordSearch = (
    queryText: string,
    dbPath: string,
    filter: QueryFilter,
    topK: number
) => Promise<QueryResult[]>;

// Scores each document against the query; higher is more relevant
export type Rerank = (query: string, documents: string[]) => Promise<number[]>;

export interface RetrievalOptions {
    onePerPage?: boolean;       // keep only the best chunk of each page
    hybrid?: boolean;           // fuse vector and keyword results (needs keywordSearch)
    rerankCandidates?: number;  // how many fused candidates the reranker sees
}

// Must match what doc2vec writes (DatabaseManager.KEYWORD_VECTOR / KEYWORD_MODEL)
export const QDRANT_KEYWORD_VECTOR = 'bm25';
export const QDRANT_KEYWORD_MODEL = 'qdrant/bm25';

export const DEFAULT_RERANK_MODEL = 'nvidia/Nemotron-3.5-Rerank-8B-BF16';
export const DEFAULT_RERANK_CANDIDATES = 40;
// List depth fetched before fusion or per-page filtering, so the top results
// still fill up after repeats of a page are dropped
const CANDIDATE_DEPTH = 50;
const RRF_K = 60;

export type GetChunksForDocument = (
    productName: string | undefined,
    dbName: string | undefined,
    filePath: string,
    startIndex?: number,
    endIndex?: number,
    version?: string
) => Promise<QueryResult[]>;

type SqliteVecModule = {
    load: (db: any) => void;
};

type SqliteDatabaseStatement = {
    all: (...params: any[]) => QueryResult[];
};

type SqliteDatabase = {
    prepare: (query: string) => SqliteDatabaseStatement;
    close: () => void;
    exec?: (sql: string) => void;
    transaction?: (fn: () => void) => () => void;
};

type SqliteDatabaseCtor = new (path: string) => SqliteDatabase;

type FsModule = {
    existsSync: (path: string) => boolean;
    statSync?: (path: string) => { mtimeMs: number };
};

type PathModule = {
    isAbsolute: (path: string) => boolean;
    join: (...parts: string[]) => string;
};

type QdrantClientLike = {
    query: (collectionName: string, params: any) => Promise<any>;
    scroll: (collectionName: string, params: any) => Promise<any>;
};

export function normalizeExtensions(extensions?: string[]): string[] {
    if (!extensions || extensions.length === 0) {
        return [];
    }

    return extensions.map((ext) => (ext.startsWith('.') ? ext.toLowerCase() : `.${ext.toLowerCase()}`));
}

export function filterResultsByUrl(
    results: QueryResult[],
    urlPrefix?: string,
    extensions?: string[]
): QueryResult[] {
    const normalizedExtensions = normalizeExtensions(extensions);
    return results.filter((row) => {
        const url = typeof row.url === 'string' ? row.url : '';
        if (urlPrefix && !url.startsWith(urlPrefix)) {
            return false;
        }
        if (normalizedExtensions.length > 0) {
            const lowerUrl = url.toLowerCase();
            const matches = normalizedExtensions.some((ext) => lowerUrl.endsWith(ext));
            if (!matches) {
                return false;
            }
        }
        return true;
    });
}

export function filterResultsWithContent(results: QueryResult[]): QueryResult[] {
    return results.filter((row) => {
        if (typeof row.content !== 'string') {
            return false;
        }
        return row.content.trim().length > 0;
    });
}

// Keeps the first (best) chunk of each page, in order. Pages are compared
// without #fragment and trailing slash, as one page's chunks share a URL.
export function onePerPage<T extends { url?: string }>(results: T[]): T[] {
    const seen = new Set<string>();
    return results.filter((row) => {
        const key = (row.url ?? '').split('#')[0].replace(/\/+$/, '').toLowerCase();
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });
}

// Reciprocal Rank Fusion: each list adds 1 / (RRF_K + rank) to a chunk's
// score, so vector distances and BM25 scores need no calibration. A chunk
// found by both keeps the row of the first list it appears in. Chunks are
// matched on id and URL: the id is a hash of the text, so the same text on
// two pages shares an id and must not merge into one page's row.
export function fuseRrf(lists: QueryResult[][]): QueryResult[] {
    const scores = new Map<string, { row: QueryResult; score: number }>();
    for (const list of lists) {
        list.forEach((row, index) => {
            const key = `${row.chunk_id}\u0000${row.url ?? ''}`;
            const entry = scores.get(key) ?? { row, score: 0 };
            entry.score += 1 / (RRF_K + index + 1);
            scores.set(key, entry);
        });
    }
    return [...scores.values()].sort((a, b) => b.score - a.score).map((e) => e.row);
}

// Free text as an FTS5 OR-query of quoted terms, so punctuation in a
// question (colons, quotes, parentheses) cannot break the MATCH syntax
export function toFtsQuery(text: string): string {
    const terms = text.toLowerCase().match(/[\p{L}\p{N}_]+/gu) ?? [];
    return [...new Set(terms)].map((t) => `"${t}"`).join(' OR ');
}

// DeepInfra's inference API: one query, its candidate documents, one score each
export function createDeepInfraReranker(deps: {
    apiKey: string;
    model?: string;
    baseUrl?: string;
    timeoutMs?: number;
    fetch?: typeof fetch;
}): Rerank {
    const model = deps.model ?? DEFAULT_RERANK_MODEL;
    const baseUrl = (deps.baseUrl ?? 'https://api.deepinfra.com/v1/inference').replace(/\/+$/, '');
    const doFetch = deps.fetch ?? fetch;
    return async (query, documents) => {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), deps.timeoutMs ?? 10000);
        try {
            const response = await doFetch(`${baseUrl}/${model}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${deps.apiKey}` },
                body: JSON.stringify({ queries: [query], documents }),
                signal: controller.signal,
            });
            if (!response.ok) {
                throw new Error(`reranker returned HTTP ${response.status}: ${(await response.text()).slice(0, 200)}`);
            }
            const body = await response.json() as { scores?: unknown };
            if (!Array.isArray(body.scores) || body.scores.length !== documents.length) {
                throw new Error('reranker did not return one score per document');
            }
            return body.scores.map(Number);
        } finally {
            clearTimeout(timer);
        }
    };
}

type QueryResultOut = {
    distance?: number;
    relevance?: number;
    content: string;
    url?: string;
    section?: string;
    chunk_index?: number;
    total_chunks?: number;
};

const toOutput = (qr: QueryResult): QueryResultOut => ({
    ...(typeof qr.distance === 'number' && { distance: qr.distance }),
    ...(typeof qr.relevance === 'number' && { relevance: qr.relevance }),
    content: qr.content,
    ...(qr.url && { url: qr.url }),
    ...(qr.section && { section: qr.section }),
    ...(typeof qr.chunk_index === 'number' && { chunk_index: qr.chunk_index }),
    ...(typeof qr.total_chunks === 'number' && { total_chunks: qr.total_chunks }),
});

const formatResult = (r: QueryResultOut, index: number): string =>
    [
        `Result ${index + 1}:`,
        `  Content: ${r.content}`,
        typeof r.distance === 'number' ? `  Distance: ${r.distance.toFixed(4)}` : null,
        typeof r.relevance === 'number' ? `  Relevance: ${r.relevance.toFixed(4)}` : null,
        r.url ? `  URL: ${r.url}` : null,
        typeof r.chunk_index === 'number' && typeof r.total_chunks === 'number' && r.total_chunks > 0
            ? `  Chunk: ${r.chunk_index + 1} of ${r.total_chunks}`
            : null,
        '---',
    ].filter((line) => line !== null).join('\n');

export function createQueryHandlers(deps: {
    createEmbeddings: (text: string) => Promise<number[]>;
    resolveDbPath: ResolveDbPath;
    queryCollection: QueryCollection;
    getChunksForDocument: GetChunksForDocument;
    keywordSearch?: KeywordSearch;
    rerank?: Rerank;
    options?: RetrievalOptions;
}) {
    const { createEmbeddings, resolveDbPath, queryCollection, getChunksForDocument, keywordSearch, rerank } = deps;
    const options = deps.options ?? {};
    const rerankCandidates = options.rerankCandidates ?? DEFAULT_RERANK_CANDIDATES;

    // Vector search, optionally fused with keyword search, then post-filters,
    // reranking and one chunk per page. Keyword search and reranking are
    // best-effort: if either fails the query falls back to what it has.
    async function retrieve(
        queryText: string,
        dbPath: string,
        filter: QueryFilter,
        limit: number,
        postFilter: (rows: QueryResult[]) => QueryResult[],
        hasPostFilters: boolean
    ): Promise<{ results: QueryResult[]; candidates: QueryResult[] }> {
        const queryEmbedding = await createEmbeddings(queryText);
        const useKeyword = !!(options.hybrid && keywordSearch);
        const depth = Math.max(
            hasPostFilters ? limit * 3 : limit,
            options.onePerPage || useKeyword ? CANDIDATE_DEPTH : 0,
            rerank ? rerankCandidates : 0
        );
        let candidates = await queryCollection(queryEmbedding, dbPath, filter, depth);
        if (useKeyword) {
            try {
                const keyword = await keywordSearch!(queryText, dbPath, filter, depth);
                candidates = fuseRrf([candidates, keyword]);
            } catch (error) {
                console.error(`Keyword search failed in ${dbPath}, using vector results only:`, error);
            }
        }
        let ranked = filterResultsWithContent(postFilter(candidates));
        if (rerank && ranked.length > 1) {
            const pool = ranked.slice(0, rerankCandidates);
            try {
                const scores = await rerank(queryText, pool.map((row) => row.content));
                const reranked = pool
                    .map((row, i) => ({ ...row, relevance: scores[i] }))
                    .sort((a, b) => b.relevance - a.relevance);
                ranked = [...reranked, ...ranked.slice(rerankCandidates)];
            } catch (error) {
                console.error('Reranking failed, keeping retrieval order:', error);
            }
        }
        if (options.onePerPage) ranked = onePerPage(ranked);
        return { results: ranked.slice(0, limit), candidates };
    }

    async function queryDocumentation(
        queryText: string,
        productName: string | undefined,
        dbName: string | undefined,
        version: string | undefined,
        urlPathPrefix: string | undefined,
        limit: number = 4
    ): Promise<QueryResultOut[]> {
        const { dbPath } = resolveDbPath(dbName, productName, version);
        const { results } = await retrieve(
            queryText,
            dbPath,
            { product_name: productName, version: version, urlPrefix: urlPathPrefix },
            limit,
            (rows) => filterResultsByUrl(rows, urlPathPrefix),
            !!urlPathPrefix
        );
        return results.map(toOutput);
    }

    async function queryCode(
        queryText: string,
        productName: string | undefined,
        repo: string | undefined,
        dbName: string | undefined,
        branch: string | undefined,
        filePathPrefix: string | undefined,
        extensions: string[] | undefined,
        limit: number = 4
    ): Promise<{
        results: QueryResultOut[];
        rawCount: number;
        emptyContentCount: number;
    }> {
        const { dbPath } = resolveDbPath(dbName, productName, undefined, repo);
        const { results, candidates } = await retrieve(
            queryText,
            dbPath,
            { product_name: productName, repo, branch, urlPrefix: filePathPrefix, extensions },
            limit,
            (rows) => filterResultsByUrl(rows, filePathPrefix, extensions),
            !!filePathPrefix || !!(extensions && extensions.length > 0)
        );
        const emptyContentCount = candidates.filter((row) => typeof row.content !== 'string' || row.content.trim().length === 0).length;
        return { results: results.map(toOutput), rawCount: candidates.length, emptyContentCount };
    }

    const queryDocumentationToolHandler = async ({
        queryText,
        productName,
        dbName,
        version,
        urlPathPrefix,
        limit,
    }: {
        queryText: string;
        productName?: string;
        dbName?: string;
        version?: string;
        urlPathPrefix?: string;
        limit: number;
    }) => {
        if (!productName && !dbName) {
            return {
                content: [{ type: 'text' as const, text: 'Provide either productName or dbName for query_documentation.' }],
            };
        }

        console.error(`Received query: text="${queryText}", product="${productName || 'n/a'}", dbName="${dbName || 'n/a'}", version="${version || 'any'}", limit=${limit}`);

        try {
            const results = await queryDocumentation(queryText, productName, dbName, version, urlPathPrefix, limit);

            if (results.length === 0) {
                return {
                    content: [{
                        type: 'text' as const,
                        text: `No relevant documentation found for "${queryText}" in ${productName ? `product "${productName}"` : `db "${dbName}"`} ${version ? `(version ${version})` : ''}.`,
                    }],
                };
            }

            const formattedResults = results.map(formatResult).join('\n');

            const responseText = `Found ${results.length} relevant documentation snippets for "${queryText}" in ${productName ? `product "${productName}"` : `db "${dbName}"`} ${version ? `(version ${version})` : ''}:\n\n${formattedResults}`;
            console.error(`Handler finished processing. Payload size (approx): ${responseText.length} chars. Returning response object...`);

            return {
                content: [{ type: 'text' as const, text: responseText }],
            };
        } catch (error: any) {
            console.error("Error processing 'query_documentation' tool:", error);
            return {
                content: [{ type: 'text' as const, text: `Error querying documentation: ${error.message}` }],
            };
        }
    };

    const queryCodeToolHandler = async ({
        queryText,
        productName,
        repo,
        dbName,
        branch,
        filePathPrefix,
        extensions,
        limit,
    }: {
        queryText: string;
        productName?: string;
        repo?: string;
        dbName?: string;
        branch?: string;
        filePathPrefix?: string;
        extensions?: string[];
        limit: number;
    }) => {
        if (!dbName) {
            return {
                content: [{ type: 'text' as const, text: 'Provide dbName for query_code.' }],
            };
        }

        console.error(`Received code query: text="${queryText}", product="${productName || 'n/a'}", repo="${repo || 'n/a'}", dbName="${dbName}", branch="${branch || 'any'}", limit=${limit}`);

        try {
            const { results, rawCount, emptyContentCount } = await queryCode(
                queryText,
                productName,
                repo,
                dbName,
                branch,
                filePathPrefix,
                extensions,
                limit
            );

            const target = repo
                ? `repo "${repo}"`
                : productName
                    ? `product "${productName}"`
                    : `db "${dbName}"`;

            if (results.length === 0) {
                if (rawCount > 0 && emptyContentCount === rawCount) {
                    return {
                        content: [{ type: 'text' as const, text: `Found ${rawCount} vector matches in ${target}, but all matching chunks have empty content. Re-ingest this database to populate content fields.` }],
                    };
                }

                return {
                    content: [{ type: 'text' as const, text: `No relevant code found for "${queryText}" in ${target} ${branch ? `(branch ${branch})` : ''}.` }],
                };
            }

            const formattedResults = results.map(formatResult).join('\n');

            const responseText = `Found ${results.length} relevant code snippets for "${queryText}" in ${target} ${branch ? `(branch ${branch})` : ''}:\n\n${formattedResults}`;
            console.error(`Handler finished processing. Payload size (approx): ${responseText.length} chars. Returning response object...`);

            return {
                content: [{ type: 'text' as const, text: responseText }],
            };
        } catch (error: any) {
            console.error("Error processing 'query_code' tool:", error);
            return {
                content: [{ type: 'text' as const, text: `Error querying code: ${error.message}` }],
            };
        }
    };

    const getChunksToolHandler = async ({
        productName,
        dbName,
        filePath,
        startIndex,
        endIndex,
        version,
    }: {
        productName?: string;
        dbName?: string;
        filePath: string;
        startIndex?: number;
        endIndex?: number;
        version?: string;
    }) => {
        if (!productName && !dbName) {
            return {
                content: [{ type: 'text' as const, text: 'Provide either productName or dbName for get_chunks.' }],
            };
        }

        console.error(`Received get_chunks: filePath="${filePath}", product="${productName || 'n/a'}", dbName="${dbName || 'n/a'}", version="${version || 'any'}", startIndex=${startIndex}, endIndex=${endIndex}`);

        try {
            const results = await getChunksForDocument(productName, dbName, filePath, startIndex, endIndex, version);

            if (results.length === 0) {
                return {
                    content: [{
                        type: 'text' as const,
                        text: `No chunks found for "${filePath}" in ${productName ? `product "${productName}"` : `db "${dbName}"`} ${version ? `(version ${version})` : ''}.`,
                    }],
                };
            }

            const formattedResults = results.map((r) =>
                [
                    `Chunk ${typeof r.chunk_index === 'number' && typeof r.total_chunks === 'number' ? `${r.chunk_index + 1} of ${r.total_chunks}` : ''}`.trim(),
                    `  Content: ${r.content}`,
                    r.section ? `  Section: ${r.section}` : null,
                    r.url ? `  URL: ${r.url}` : null,
                    '---',
                ].filter((line) => line !== null).join('\n')
            ).join('\n');

            return {
                content: [{ type: 'text' as const, text: `Retrieved ${results.length} chunk(s) for "${filePath}":\n\n${formattedResults}` }],
            };
        } catch (error: any) {
            console.error("Error processing 'get_chunks' tool:", error);
            return {
                content: [{ type: 'text' as const, text: `Error retrieving chunks: ${error.message}` }],
            };
        }
    };

    return {
        queryDocumentation,
        queryCode,
        queryDocumentationToolHandler,
        queryCodeToolHandler,
        getChunksToolHandler,
    };
}

export function createSqliteDbProvider(deps: {
    dbDir: string;
    sqliteVec: SqliteVecModule;
    Database: SqliteDatabaseCtor;
    fs: FsModule;
    path: PathModule;
    keywordIndexCacheSize?: number;
}) {
    const { dbDir, sqliteVec, Database, fs, path } = deps;
    const keywordIndexCacheSize = deps.keywordIndexCacheSize ?? 8;

    const resolveDbPath: ResolveDbPath = (dbName?: string, productName?: string) => {
        if (dbName) {
            const normalizedName = dbName.endsWith('.db') ? dbName : `${dbName}.db`;
            const dbPath = path.isAbsolute(normalizedName) ? normalizedName : path.join(dbDir, normalizedName);
            return { dbPath, dbLabel: normalizedName };
        }

        if (!productName) {
            throw new Error('Either productName/repo or dbName must be provided.');
        }

        const dbPath = path.join(dbDir, `${productName}.db`);
        return { dbPath, dbLabel: `${productName}.db` };
    };

    const queryCollection: QueryCollection = async (
        queryEmbedding: number[],
        dbPath: string,
        filter: QueryFilter,
        topK: number = 10
    ): Promise<QueryResult[]> => {
        if (!fs.existsSync(dbPath)) {
            throw new Error(`Database file not found at ${dbPath}`);
        }

        let db: SqliteDatabase | null = null;
        try {
            db = new Database(dbPath);
            console.error(`[DB ${dbPath}] Opened connection.`);
            sqliteVec.load(db);
            console.error(`[DB ${dbPath}] sqliteVec loaded.`);
            let query = `
              SELECT
                  *,
                  distance
              FROM vec_items
              WHERE embedding MATCH @query_embedding`;

            if (filter.product_name) query += ` AND product_name = @product_name`;
            if (filter.version) query += ` AND version = @version`;
            if (filter.branch) query += ` AND branch = @branch`;
            if (filter.repo) query += ` AND repo = @repo`;

            query += `
              ORDER BY distance
              LIMIT @top_k;`;

            const stmt = db.prepare(query);
            console.error(`[DB ${dbPath}] Query prepared. Executing...`);
            const startTime = Date.now();
            const rows = stmt.all({
                query_embedding: new Float32Array(queryEmbedding),
                product_name: filter.product_name,
                version: filter.version,
                branch: filter.branch,
                repo: filter.repo,
                top_k: topK,
            });
            const duration = Date.now() - startTime;
            console.error(`[DB ${dbPath}] Query executed in ${duration}ms. Found ${rows.length} rows.`);

            rows.forEach((row: any) => {
                delete row.embedding;
            });

            return rows as QueryResult[];
        } catch (error) {
            console.error(`Error querying collection in ${dbPath}:`, error);
            throw new Error(`Database query failed: ${error instanceof Error ? error.message : String(error)}`);
        } finally {
            if (db) {
                db.close();
            }
        }
    };

    // The keyword index is built in memory from the stored chunk text the first
    // time a database is searched, so existing databases need no re-ingestion.
    // It is rebuilt when the file changes; the least recently used index is
    // dropped past keywordIndexCacheSize databases.
    type KeywordIndex = { mtimeMs: number; db: SqliteDatabase; hasBranch: boolean; hasRepo: boolean };
    const keywordIndexes = new Map<string, KeywordIndex>();
    const FULL_COLUMNS = ['chunk_id', 'content', 'url', 'section', 'heading_hierarchy', 'chunk_index', 'total_chunks', 'product_name', 'version', 'branch', 'repo'];
    const BASIC_COLUMNS = ['chunk_id', 'content', 'url', 'section', 'heading_hierarchy', 'product_name', 'version'];

    const keywordIndexFor = (dbPath: string): KeywordIndex => {
        const mtimeMs = fs.statSync ? fs.statSync(dbPath).mtimeMs : 0;
        const cached = keywordIndexes.get(dbPath);
        if (cached && cached.mtimeMs === mtimeMs) {
            keywordIndexes.delete(dbPath);
            keywordIndexes.set(dbPath, cached);
            return cached;
        }
        cached?.db.close();
        keywordIndexes.delete(dbPath);

        const source = new Database(dbPath);
        let rows: any[];
        let columns = FULL_COLUMNS;
        try {
            sqliteVec.load(source);
            try {
                rows = source.prepare(`SELECT ${FULL_COLUMNS.join(', ')} FROM vec_items`).all();
            } catch {
                // Databases from before code sources have no branch, repo or chunk_index
                columns = BASIC_COLUMNS;
                rows = source.prepare(`SELECT ${BASIC_COLUMNS.join(', ')} FROM vec_items`).all();
            }
        } finally {
            source.close();
        }

        const index = new Database(':memory:');
        index.exec!(`CREATE VIRTUAL TABLE chunks USING fts5(content, ${FULL_COLUMNS.filter((c) => c !== 'content').map((c) => `${c} UNINDEXED`).join(', ')})`);
        const insert = index.prepare(`INSERT INTO chunks (${FULL_COLUMNS.join(', ')}) VALUES (${FULL_COLUMNS.map(() => '?').join(', ')})`) as any;
        index.transaction!(() => {
            for (const row of rows) insert.run(...FULL_COLUMNS.map((c) => row[c] ?? null));
        })();

        const entry = { mtimeMs, db: index, hasBranch: columns === FULL_COLUMNS, hasRepo: columns === FULL_COLUMNS };
        keywordIndexes.set(dbPath, entry);
        while (keywordIndexes.size > keywordIndexCacheSize) {
            const [oldest, old] = keywordIndexes.entries().next().value as [string, KeywordIndex];
            old.db.close();
            keywordIndexes.delete(oldest);
        }
        console.error(`[DB ${dbPath}] Keyword index built over ${rows.length} chunks.`);
        return entry;
    };

    const keywordSearch: KeywordSearch = async (queryText, dbPath, filter, topK) => {
        if (!fs.existsSync(dbPath)) {
            throw new Error(`Database file not found at ${dbPath}`);
        }
        const match = toFtsQuery(queryText);
        if (!match) return [];
        const index = keywordIndexFor(dbPath);
        // A filter on a column this database lacks would match nothing; leave
        // the query to vector search alone rather than return wrong rows
        if ((filter.branch && !index.hasBranch) || (filter.repo && !index.hasRepo)) return [];
        let query = `
            SELECT chunk_id, content, url, section, heading_hierarchy, chunk_index, total_chunks
            FROM chunks
            WHERE chunks MATCH ?`;
        const params: Array<string | number> = [match];
        if (filter.product_name) { query += ` AND product_name = ?`; params.push(filter.product_name); }
        if (filter.version) { query += ` AND version = ?`; params.push(filter.version); }
        if (filter.branch) { query += ` AND branch = ?`; params.push(filter.branch); }
        if (filter.repo) { query += ` AND repo = ?`; params.push(filter.repo); }
        query += ` ORDER BY bm25(chunks) LIMIT ?`;
        params.push(topK);
        return index.db.prepare(query).all(...params) as QueryResult[];
    };

    const getChunksForDocument: GetChunksForDocument = async (
        productName: string | undefined,
        dbName: string | undefined,
        filePath: string,
        startIndex?: number,
        endIndex?: number,
        version?: string
    ): Promise<QueryResult[]> => {
        const { dbPath } = resolveDbPath(dbName, productName);

        if (!fs.existsSync(dbPath)) {
            throw new Error(`Database file not found at ${dbPath}`);
        }

        let db: SqliteDatabase | null = null;
        try {
            db = new Database(dbPath);
            sqliteVec.load(db);

            const hasRange = typeof startIndex === 'number' && typeof endIndex === 'number';

            let selectColumns = [
                'chunk_id',
                'content',
                'url',
                'section',
                'heading_hierarchy',
                'chunk_index',
                'total_chunks',
            ];

            let query = `
              SELECT
                  ${selectColumns.join(', ')}
              FROM vec_items
              WHERE url = ?`;

            if (productName) query += ` AND product_name = ?`;
            if (version) query += ` AND version = ?`;
            if (hasRange) {
                query += ` AND chunk_index >= ? AND chunk_index <= ?`;
            }

            query += `
              ORDER BY chunk_index;`;

            let stmt;
            let params: Array<string | number> = [filePath];
            if (productName) params.push(productName);
            if (version) params.push(version);
            if (hasRange) {
                params.push(startIndex as number);
                params.push(endIndex as number);
            }

            try {
                stmt = db.prepare(query);
                const rows = stmt.all(...params) as QueryResult[];
                return rows;
            } catch (error: any) {
                const errorMessage = error?.message || String(error);
                const errorStr = String(error);
                const isChunkIndexError = (errorMessage.includes('no such column') && errorMessage.includes('chunk_index')) ||
                    (errorStr.includes('no such column') && errorStr.includes('chunk_index'));

                if (isChunkIndexError) {
                    console.error('Warning: chunk_index column does not exist in database. Using backward compatible query.');

                    if (hasRange) {
                        console.error('Warning: startIndex/endIndex provided but chunk_index column does not exist. Ignoring range filter.');
                    }

                    selectColumns = [
                        'chunk_id',
                        'content',
                        'url',
                        'section',
                        'heading_hierarchy',
                    ];

                    query = `
                      SELECT
                          ${selectColumns.join(', ')}
                      FROM vec_items
                      WHERE url = ?`;

                    if (productName) query += ` AND product_name = ?`;
                    if (version) query += ` AND version = ?`;
                    query += `;`;

                    params = [filePath];
                    if (productName) params.push(productName);
                    if (version) params.push(version);

                    try {
                        stmt = db.prepare(query);
                        const rows = stmt.all(...params) as QueryResult[];
                        return rows;
                    } catch (retryError: any) {
                        throw error;
                    }
                } else {
                    throw error;
                }
            }
        } catch (error) {
            console.error(`Error retrieving chunks in ${dbPath}:`, error);
            throw new Error(`Chunk retrieval failed: ${error instanceof Error ? error.message : String(error)}`);
        } finally {
            if (db) {
                db.close();
            }
        }
    };

    return {
        resolveDbPath,
        queryCollection,
        keywordSearch,
        getChunksForDocument,
    };
}

export function createQdrantProvider(deps: { client: QdrantClientLike }) {
    const { client } = deps;

    const normalizeCollectionName = (name: string): string => {
        return name.toLowerCase().replace(/\s+/g, '_');
    };

    const resolveDbPath: ResolveDbPath = (dbName?: string, productName?: string, version?: string) => {
        if (dbName) {
            return { dbPath: dbName, dbLabel: dbName };
        }

        if (!productName) {
            throw new Error('Either productName/repo or dbName must be provided.');
        }

        if (version) {
            const normalizedProduct = normalizeCollectionName(productName);
            return { dbPath: `${normalizedProduct}_${version}`, dbLabel: `${normalizedProduct}_${version}` };
        }

        const normalizedProduct = normalizeCollectionName(productName);
        return { dbPath: normalizedProduct, dbLabel: normalizedProduct };
    };

    const buildFilterMust = (filter: QueryFilter): Array<Record<string, unknown>> => {
        const must: Array<Record<string, unknown>> = [];
        if (filter.product_name) {
            must.push({ key: 'product_name', match: { value: filter.product_name } });
        }
        if (filter.version) {
            must.push({ key: 'version', match: { value: filter.version } });
        }
        if (filter.branch) {
            must.push({ key: 'branch', match: { value: filter.branch } });
        }
        if (filter.repo) {
            must.push({ key: 'repo', match: { value: filter.repo } });
        }
        return must;
    };

    const extractPoints = (response: any): any[] => {
        if (Array.isArray(response)) {
            return response;
        }
        if (response?.result && Array.isArray(response.result)) {
            return response.result;
        }
        if (response?.points && Array.isArray(response.points)) {
            return response.points;
        }
        return [];
    };

    const mapPointToResult = (point: any): QueryResult => {
        const payload = point?.payload || {};
        const distance = typeof point?.score === 'number' ? point.score : 0;
        return {
            chunk_id: payload.chunk_id ?? String(point?.id ?? ''),
            distance,
            content: payload.content ?? '',
            url: payload.url,
            section: payload.section,
            heading_hierarchy: payload.heading_hierarchy,
            chunk_index: payload.chunk_index,
            total_chunks: payload.total_chunks,
        };
    };

    const queryCollection: QueryCollection = async (
        queryEmbedding: number[],
        dbPath: string,
        filter: QueryFilter,
        topK: number = 10
    ): Promise<QueryResult[]> => {
        const must = buildFilterMust(filter);
        // The Query API (Qdrant 1.10+); the older `search` call is gone from
        // @qdrant/js-client-rest 1.19
        const response = await client.query(dbPath, {
            query: queryEmbedding,
            limit: topK,
            filter: must.length > 0 ? { must } : undefined,
            with_payload: true,
            with_vector: false,
        });
        const points = extractPoints(response);
        return points.map(mapPointToResult);
    };

    // BM25 over the keyword vector doc2vec writes when a source sets
    // `keyword_index: true`. Qdrant tokenizes the query with the same model it
    // used for the chunks. A collection without that vector makes the query
    // fail, and the handler then keeps the vector results alone; the failure is
    // remembered for a while so such collections are not queried every time.
    const KEYWORD_RETRY_MS = 10 * 60 * 1000;
    const keywordUnavailable = new Map<string, number>();
    const keywordSearch: KeywordSearch = async (queryText, dbPath, filter, topK) => {
        const since = keywordUnavailable.get(dbPath);
        if (since !== undefined && Date.now() - since < KEYWORD_RETRY_MS) return [];
        const must = buildFilterMust(filter);
        try {
            const response = await client.query(dbPath, {
                query: { text: queryText, model: QDRANT_KEYWORD_MODEL },
                using: QDRANT_KEYWORD_VECTOR,
                limit: topK,
                filter: must.length > 0 ? { must } : undefined,
                with_payload: true,
                with_vector: false,
            });
            keywordUnavailable.delete(dbPath);
            // The score is a BM25 score, not a vector distance: report none
            return extractPoints(response).map((point) => ({ ...mapPointToResult(point), distance: undefined }));
        } catch (error) {
            keywordUnavailable.set(dbPath, Date.now());
            throw error;
        }
    };

    const getChunksForDocument: GetChunksForDocument = async (
        productName: string | undefined,
        dbName: string | undefined,
        filePath: string,
        startIndex?: number,
        endIndex?: number,
        version?: string
    ): Promise<QueryResult[]> => {
        const { dbPath } = resolveDbPath(dbName, productName, version);
        const must: Array<Record<string, unknown>> = [{ key: 'url', match: { value: filePath } }];
        if (productName) {
            must.push({ key: 'product_name', match: { value: productName } });
        }
        if (version) {
            must.push({ key: 'version', match: { value: version } });
        }
        if (typeof startIndex === 'number' || typeof endIndex === 'number') {
            const range: Record<string, number> = {};
            if (typeof startIndex === 'number') range.gte = startIndex;
            if (typeof endIndex === 'number') range.lte = endIndex;
            must.push({ key: 'chunk_index', range });
        }

        const points: any[] = [];
        let nextOffset: any = undefined;

        do {
            const response = await client.scroll(dbPath, {
                filter: { must },
                with_payload: true,
                with_vector: false,
                limit: 1000,
                offset: nextOffset,
            });
            const batch = extractPoints(response);
            points.push(...batch);
            nextOffset = response?.next_page_offset;
        } while (nextOffset !== undefined && nextOffset !== null);

        const results = points.map(mapPointToResult);
        const hasChunkIndex = results.some((row) => typeof row.chunk_index === 'number');
        if (hasChunkIndex) {
            results.sort((a, b) => (a.chunk_index ?? 0) - (b.chunk_index ?? 0));
        }
        return results;
    };

    return {
        resolveDbPath,
        queryCollection,
        keywordSearch,
        getChunksForDocument,
    };
}
