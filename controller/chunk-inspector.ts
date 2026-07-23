import BetterSqlite3 from 'better-sqlite3';
import * as path from 'path';
import * as sqliteVec from 'sqlite-vec';
import * as yaml from 'js-yaml';
import { QdrantClient } from '@qdrant/js-client-rest';
import { NotFoundError, ValidationError } from './types';

const MAX_CHUNKS = 500;

export interface ChunkRecord {
    chunk_id: string;
    url: string;
    product_name: string | null;
    version: string | null;
    section: string | null;
    heading_hierarchy: string[];
    chunk_index: number | null;
    total_chunks: number | null;
    hash: string | null;
    created_at: string | null;
    content: string;
}

export interface ChunkLookupResult {
    source: { product_name: string; type: string; version: string };
    database: { type: 'sqlite'; path: string } | { type: 'qdrant'; url: string; collection: string };
    chunks: ChunkRecord[];
}

export interface SourceSelector {
    product_name: string;
    type?: string;
    version?: string;
}

/**
 * Read-only lookup of the chunks stored for a URL in a source's vector store.
 * Resolves the store the same way a sync job would (same defaults as
 * DatabaseManager.initDatabase, same ${ENV_VAR} substitution as Doc2Vec's
 * config loader), but never creates tables or collections.
 */
export async function lookupChunks(configContent: string, selector: SourceSelector, url: string): Promise<ChunkLookupResult> {
    const source = findSource(configContent, selector);
    const dbConfig = source.database_config ?? {};
    const params = dbConfig.params ?? {};

    if (dbConfig.type === 'sqlite') {
        const dbPath = params.db_path
            || path.join(process.cwd(), `${String(source.product_name).replace(/\s+/g, '_')}-${source.version}.db`);
        return {
            source: { product_name: source.product_name, type: source.type, version: source.version },
            database: { type: 'sqlite', path: dbPath },
            chunks: lookupSqlite(dbPath, url),
        };
    }

    if (dbConfig.type === 'qdrant') {
        const qdrantUrl = params.qdrant_url || 'http://localhost:6333';
        const qdrantPort = params.qdrant_port || 443;
        const collectionName = params.collection_name
            || `${String(source.product_name).toLowerCase().replace(/\s+/g, '_')}_${source.version}`;
        const client = new QdrantClient({ url: qdrantUrl, apiKey: process.env.QDRANT_API_KEY, port: qdrantPort });
        return {
            source: { product_name: source.product_name, type: source.type, version: source.version },
            database: { type: 'qdrant', url: qdrantUrl, collection: collectionName },
            chunks: await lookupQdrant(client, collectionName, url),
        };
    }

    throw new ValidationError(`source has unsupported database type: ${dbConfig.type}`);
}

function findSource(configContent: string, selector: SourceSelector): any {
    // Same ${VAR} substitution the sync job applies when it loads the config
    const substituted = configContent.replace(/\$\{([^}]+)\}/g, (match, varName) =>
        process.env[varName] !== undefined ? process.env[varName]! : match
    );

    let parsed: any;
    try {
        parsed = yaml.load(substituted);
    } catch (err) {
        throw new ValidationError(`config does not parse: ${err instanceof Error ? err.message : String(err)}`);
    }

    const sources: any[] = Array.isArray(parsed?.sources) ? parsed.sources : [];
    const source = sources.find(s =>
        s?.product_name === selector.product_name
        && (selector.type === undefined || s?.type === selector.type)
        && (selector.version === undefined || String(effectiveVersion(s)) === selector.version)
    );
    if (!source) {
        throw new NotFoundError(`source '${selector.product_name}' not found in config`);
    }
    return { ...source, version: effectiveVersion(source) };
}

// Mirrors the version defaulting in Doc2Vec.loadConfig for code sources
function effectiveVersion(source: any): string {
    if (source?.version && String(source.version).trim().length > 0) return String(source.version);
    if (source?.type === 'code') {
        if (source.branch && String(source.branch).trim().length > 0) return String(source.branch);
        return 'local';
    }
    return String(source?.version ?? '');
}

function lookupSqlite(dbPath: string, url: string): ChunkRecord[] {
    let db: BetterSqlite3.Database;
    try {
        db = new BetterSqlite3(dbPath, { readonly: true, fileMustExist: true, allowExtension: true } as any);
    } catch {
        throw new NotFoundError(`SQLite database not found at ${dbPath} (has this source ever been synced?)`);
    }

    try {
        sqliteVec.load(db);
        const hasDates = db
            .prepare(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'vec_chunk_dates'`)
            .get() !== undefined;
        const rows = db.prepare(`
            SELECT v.chunk_id, v.url, v.product_name, v.version, v.section, v.heading_hierarchy,
                   v.chunk_index, v.total_chunks, v.hash, v.content
                   ${hasDates ? ', d.created_at AS created_at' : ', NULL AS created_at'}
            FROM vec_items v
            ${hasDates ? 'LEFT JOIN vec_chunk_dates d ON d.chunk_id = v.chunk_id' : ''}
            WHERE v.url = ?
            LIMIT ${MAX_CHUNKS}
        `).all(url) as any[];

        return sortChunks(rows.map(row => ({
            chunk_id: String(row.chunk_id),
            url: String(row.url),
            product_name: row.product_name ?? null,
            version: row.version ?? null,
            section: row.section ?? null,
            heading_hierarchy: parseHierarchy(row.heading_hierarchy),
            chunk_index: row.chunk_index === null || row.chunk_index === undefined ? null : Number(row.chunk_index),
            total_chunks: row.total_chunks === null || row.total_chunks === undefined ? null : Number(row.total_chunks),
            hash: row.hash ?? null,
            created_at: row.created_at ?? null,
            content: String(row.content ?? ''),
        })));
    } finally {
        db.close();
    }
}

async function lookupQdrant(client: QdrantClient, collectionName: string, url: string): Promise<ChunkRecord[]> {
    const chunks: ChunkRecord[] = [];
    let offset: any = undefined;
    do {
        const response = await client.scroll(collectionName, {
            limit: 200,
            offset,
            with_payload: true,
            with_vector: false,
            filter: {
                must: [{ key: 'url', match: { value: url } }],
                must_not: [{ key: 'is_metadata', match: { value: true } }],
            },
        });
        for (const point of response.points) {
            const p: any = point.payload ?? {};
            chunks.push({
                chunk_id: String(p.original_chunk_id ?? point.id),
                url: String(p.url ?? url),
                product_name: p.product_name ?? null,
                version: p.version ?? null,
                section: p.section ?? null,
                heading_hierarchy: parseHierarchy(p.heading_hierarchy),
                chunk_index: p.chunk_index === null || p.chunk_index === undefined ? null : Number(p.chunk_index),
                total_chunks: p.total_chunks === null || p.total_chunks === undefined ? null : Number(p.total_chunks),
                hash: p.hash ?? null,
                created_at: p.created_at ?? null,
                content: String(p.content ?? ''),
            });
            if (chunks.length >= MAX_CHUNKS) return sortChunks(chunks);
        }
        offset = response.next_page_offset;
    } while (offset !== null && offset !== undefined);

    return sortChunks(chunks);
}

function parseHierarchy(value: unknown): string[] {
    if (Array.isArray(value)) return value.map(String);
    if (typeof value === 'string' && value.trim().startsWith('[')) {
        try {
            const parsed = JSON.parse(value);
            if (Array.isArray(parsed)) return parsed.map(String);
        } catch { /* fall through */ }
    }
    return value ? [String(value)] : [];
}

// Default order: position within the page. The UI offers per-column sorting
// (including by creation date) on top of this.
function sortChunks(chunks: ChunkRecord[]): ChunkRecord[] {
    return chunks.sort((a, b) => {
        const ai = a.chunk_index ?? Number.MAX_SAFE_INTEGER;
        const bi = b.chunk_index ?? Number.MAX_SAFE_INTEGER;
        if (ai !== bi) return ai - bi;
        return a.chunk_id.localeCompare(b.chunk_id);
    });
}
