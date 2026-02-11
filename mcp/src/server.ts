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
};

type SqliteDatabaseCtor = new (path: string) => SqliteDatabase;

type FsModule = {
    existsSync: (path: string) => boolean;
};

type PathModule = {
    isAbsolute: (path: string) => boolean;
    join: (...parts: string[]) => string;
};

type QdrantClientLike = {
    search: (collectionName: string, params: any) => Promise<any>;
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

export function createQueryHandlers(deps: {
    createEmbeddings: (text: string) => Promise<number[]>;
    resolveDbPath: ResolveDbPath;
    queryCollection: QueryCollection;
    getChunksForDocument: GetChunksForDocument;
}) {
    const { createEmbeddings, resolveDbPath, queryCollection, getChunksForDocument } = deps;

    async function queryDocumentation(
        queryText: string,
        productName: string | undefined,
        dbName: string | undefined,
        version: string | undefined,
        urlPathPrefix: string | undefined,
        limit: number = 4
    ): Promise<{
        distance: number;
        content: string;
        url?: string;
        section?: string;
        chunk_index?: number;
        total_chunks?: number;
    }[]> {
        const queryEmbedding = await createEmbeddings(queryText);
        const { dbPath } = resolveDbPath(dbName, productName, version);
        const hasPostFilters = !!urlPathPrefix;
        const fetchLimit = hasPostFilters ? limit * 3 : limit;
        const results = await queryCollection(
            queryEmbedding,
            dbPath,
            { product_name: productName, version: version, urlPrefix: urlPathPrefix },
            fetchLimit
        );
        const filteredResults = filterResultsWithContent(filterResultsByUrl(results, urlPathPrefix));
        return filteredResults.slice(0, limit).map((qr: QueryResult) => ({
            distance: typeof qr.distance === 'number' ? qr.distance : 0,
            content: qr.content,
            ...(qr.url && { url: qr.url }),
            ...(qr.section && { section: qr.section }),
            ...(typeof qr.chunk_index === 'number' && { chunk_index: qr.chunk_index }),
            ...(typeof qr.total_chunks === 'number' && { total_chunks: qr.total_chunks }),
        }));
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
        results: {
            distance: number;
            content: string;
            url?: string;
            section?: string;
            chunk_index?: number;
            total_chunks?: number;
        }[];
        rawCount: number;
        emptyContentCount: number;
    }> {
        const queryEmbedding = await createEmbeddings(queryText);
        const { dbPath } = resolveDbPath(dbName, productName, undefined, repo);
        const hasPostFilters = !!filePathPrefix || (extensions && extensions.length > 0);
        const fetchLimit = hasPostFilters ? limit * 3 : limit;
        const results = await queryCollection(
            queryEmbedding,
            dbPath,
            { product_name: productName, repo, branch, urlPrefix: filePathPrefix, extensions },
            fetchLimit
        );
        const filteredResults = filterResultsWithContent(filterResultsByUrl(results, filePathPrefix, extensions));
        const mappedResults = filteredResults.slice(0, limit).map((qr: QueryResult) => ({
            distance: typeof qr.distance === 'number' ? qr.distance : 0,
            content: qr.content,
            ...(qr.url && { url: qr.url }),
            ...(qr.section && { section: qr.section }),
            ...(typeof qr.chunk_index === 'number' && { chunk_index: qr.chunk_index }),
            ...(typeof qr.total_chunks === 'number' && { total_chunks: qr.total_chunks }),
        }));
        const emptyContentCount = results.filter((row) => typeof row.content !== 'string' || row.content.trim().length === 0).length;
        return { results: mappedResults, rawCount: results.length, emptyContentCount };
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

            const formattedResults = results.map((r, index) =>
                [
                    `Result ${index + 1}:`,
                    `  Content: ${r.content}`,
                    `  Distance: ${r.distance.toFixed(4)}`,
                    r.url ? `  URL: ${r.url}` : null,
                    typeof r.chunk_index === 'number' && typeof r.total_chunks === 'number' && r.total_chunks > 0
                        ? `  Chunk: ${r.chunk_index + 1} of ${r.total_chunks}`
                        : null,
                    '---',
                ].filter((line) => line !== null).join('\n')
            ).join('\n');

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

            const formattedResults = results.map((r, index) =>
                [
                    `Result ${index + 1}:`,
                    `  Content: ${r.content}`,
                    `  Distance: ${r.distance.toFixed(4)}`,
                    r.url ? `  URL: ${r.url}` : null,
                    typeof r.chunk_index === 'number' && typeof r.total_chunks === 'number' && r.total_chunks > 0
                        ? `  Chunk: ${r.chunk_index + 1} of ${r.total_chunks}`
                        : null,
                    '---',
                ].filter((line) => line !== null).join('\n')
            ).join('\n');

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
}) {
    const { dbDir, sqliteVec, Database, fs, path } = deps;

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
        const response = await client.search(dbPath, {
            vector: queryEmbedding,
            limit: topK,
            filter: must.length > 0 ? { must } : undefined,
            with_payload: true,
            with_vector: false,
        });
        const points = extractPoints(response);
        return points.map(mapPointToResult);
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
        getChunksForDocument,
    };
}
