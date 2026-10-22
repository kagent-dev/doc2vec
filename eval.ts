// Retrieval evaluation: runs a set of questions with known relevant URLs
// against a sqlite-vec database and reports recall@k and MRR. It measures
// the same thing the MCP server serves (nearest chunks by distance), so a
// change to the embedding model, chunking or ranking can be judged by
// numbers instead of by eyeballing a few queries.

import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'js-yaml';
import { OpenAI, AzureOpenAI } from 'openai';
import type { EmbeddingConfig } from './types';

export interface EvalCase {
    id?: string;
    query: string;
    relevant_urls: string[];       // a hit is any retrieved chunk whose URL is one of these
    db?: string;                   // sqlite path, overrides the default db for this case
    product_name?: string;         // same filters the MCP query tools take
    version?: string;
}

export interface EvalCaseResult {
    id: string;
    query: string;
    rank: number | null;           // 1-based position of the first relevant chunk, null if not in top-k
    retrieved_urls: string[];
}

export interface EvalMetrics {
    cases: number;
    recall: Record<number, number>; // fraction of cases with a relevant chunk in the top k
    mrr: number;                     // mean reciprocal rank, 0 for misses
}

export interface EvalReport extends EvalMetrics {
    model: string;
    top_k: number;
    created_at: string;
    results: EvalCaseResult[];
}

export interface RetrievedChunk {
    rowid?: number;
    url?: string;
    distance?: number;
    content?: string;
}

// dense: nearest chunks by embedding distance (what the MCP server does today)
// bm25: SQLite FTS5 keyword ranking over the stored chunk text
// hybrid: dense and bm25 lists fused with Reciprocal Rank Fusion
export type RetrievalMode = 'dense' | 'bm25' | 'hybrid';

// Scores each document against the query; higher is more relevant
export type Rerank = (query: string, documents: string[]) => Promise<number[]>;

export interface RetrieverOptions {
    mode?: RetrievalMode;
    perPage?: boolean;     // keep only the best chunk of each page
    candidates?: number;   // list depth fetched before fusion / per-page filtering
    rerank?: Rerank;       // reorders the top rerankCandidates chunks
    rerankCandidates?: number;
}

export const RRF_K = 60;
export const DEFAULT_RERANK_MODEL = 'nvidia/Nemotron-3.5-Rerank-8B-BF16';
export const DEFAULT_RERANK_CANDIDATES = 40;

// Same request the MCP server's reranker sends (mcp/src/server.ts)
export function createDeepInfraReranker(apiKey: string, model: string = DEFAULT_RERANK_MODEL): Rerank {
    return async (query, documents) => {
        const response = await fetch(`https://api.deepinfra.com/v1/inference/${model}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
            body: JSON.stringify({ queries: [query], documents }),
        });
        if (!response.ok) {
            throw new Error(`reranker returned HTTP ${response.status}: ${(await response.text()).slice(0, 200)}`);
        }
        const body = await response.json() as { scores?: unknown };
        if (!Array.isArray(body.scores) || body.scores.length !== documents.length) {
            throw new Error('reranker did not return one score per document');
        }
        return body.scores.map(Number);
    };
}

export type Embed = (text: string) => Promise<number[]>;
export type Retrieve = (queryEmbedding: number[], evalCase: EvalCase, topK: number) => Promise<RetrievedChunk[]>;

export const DEFAULT_K_VALUES = [1, 4, 10];

// Treat https://x/a/, https://x/a and https://x/a#section as the same page:
// eval sets are written by hand and the stored URLs vary on these points.
export function normalizeUrl(url: string): string {
    let normalized = url.trim();
    const hashIndex = normalized.indexOf('#');
    if (hashIndex !== -1) normalized = normalized.substring(0, hashIndex);
    while (normalized.length > 1 && normalized.endsWith('/')) normalized = normalized.slice(0, -1);
    return normalized.toLowerCase();
}

export function firstRelevantRank(retrievedUrls: (string | undefined)[], relevantUrls: string[]): number | null {
    const relevant = new Set(relevantUrls.map(normalizeUrl));
    for (let i = 0; i < retrievedUrls.length; i++) {
        const url = retrievedUrls[i];
        if (url && relevant.has(normalizeUrl(url))) return i + 1;
    }
    return null;
}

export function computeMetrics(results: EvalCaseResult[], kValues: number[] = DEFAULT_K_VALUES): EvalMetrics {
    const recall: Record<number, number> = {};
    for (const k of kValues) {
        const hits = results.filter(r => r.rank !== null && r.rank <= k).length;
        recall[k] = results.length ? hits / results.length : 0;
    }
    const reciprocalSum = results.reduce((sum, r) => sum + (r.rank ? 1 / r.rank : 0), 0);
    return { cases: results.length, recall, mrr: results.length ? reciprocalSum / results.length : 0 };
}

// Accepts .jsonl (one case per line), .json (an array) or .yaml/.yml (a list).
export function loadEvalCases(filePath: string): EvalCase[] {
    const raw = fs.readFileSync(filePath, 'utf8');
    const ext = path.extname(filePath).toLowerCase();
    let parsed: unknown;
    if (ext === '.jsonl') {
        parsed = raw.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('//')).map(l => JSON.parse(l));
    } else if (ext === '.json') {
        parsed = JSON.parse(raw);
    } else {
        parsed = yaml.load(raw);
    }
    if (!Array.isArray(parsed)) {
        throw new Error(`Eval file ${filePath} must contain a list of cases`);
    }
    return parsed.map((c: any, i: number) => {
        if (!c || typeof c.query !== 'string' || !c.query.trim()) {
            throw new Error(`Eval case #${i + 1} has no query`);
        }
        if (!Array.isArray(c.relevant_urls) || c.relevant_urls.length === 0) {
            throw new Error(`Eval case #${i + 1} ("${c.query}") has no relevant_urls`);
        }
        return { ...c, id: c.id ?? String(i + 1) } as EvalCase;
    });
}

export async function runEval(
    cases: EvalCase[],
    deps: { embed: Embed; retrieve: Retrieve },
    options: { topK: number; model: string; kValues?: number[] }
): Promise<EvalReport> {
    const results: EvalCaseResult[] = [];
    for (const evalCase of cases) {
        const embedding = await deps.embed(evalCase.query);
        const chunks = await deps.retrieve(embedding, evalCase, options.topK);
        const retrievedUrls = chunks.map(c => c.url ?? '');
        results.push({
            id: evalCase.id ?? '',
            query: evalCase.query,
            rank: firstRelevantRank(retrievedUrls, evalCase.relevant_urls),
            retrieved_urls: retrievedUrls,
        });
    }
    const kValues = (options.kValues ?? DEFAULT_K_VALUES).filter(k => k <= options.topK);
    return {
        model: options.model,
        top_k: options.topK,
        created_at: new Date().toISOString(),
        ...computeMetrics(results, kValues),
        results,
    };
}

// Keeps the first (best) chunk of each page, in order.
export function onePerPage(chunks: RetrievedChunk[]): RetrievedChunk[] {
    const seen = new Set<string>();
    return chunks.filter(chunk => {
        const key = normalizeUrl(chunk.url ?? '');
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });
}

// Reciprocal Rank Fusion: each list contributes 1 / (RRF_K + rank) per chunk.
// Rank-based, so dense distances and bm25 scores need no calibration.
export function fuseRrf(lists: RetrievedChunk[][]): RetrievedChunk[] {
    const scores = new Map<number, { chunk: RetrievedChunk; score: number }>();
    for (const list of lists) {
        list.forEach((chunk, index) => {
            if (chunk.rowid === undefined) return;
            const entry = scores.get(chunk.rowid) ?? { chunk, score: 0 };
            entry.score += 1 / (RRF_K + index + 1);
            scores.set(chunk.rowid, entry);
        });
    }
    return [...scores.values()].sort((a, b) => b.score - a.score).map(e => e.chunk);
}

// Turns free text into an FTS5 OR-query of quoted terms, so punctuation in a
// question (colons, quotes, parentheses) cannot break the MATCH syntax.
export function toFtsQuery(text: string): string {
    const terms = text.toLowerCase().match(/[\p{L}\p{N}_]+/gu) ?? [];
    return [...new Set(terms)].map(t => `"${t}"`).join(' OR ');
}

// Queries the database the MCP server's sqlite provider reads
// (mcp/src/server.ts); dense mode runs exactly its query, so the numbers
// reflect what an agent receives today.
export function createSqliteRetriever(defaultDbPath?: string, options: RetrieverOptions = {}): Retrieve {
    const Database = require('better-sqlite3');
    const sqliteVec = require('sqlite-vec');
    const mode = options.mode ?? 'dense';
    const connections = new Map<string, { db: any; fts?: any }>();

    const open = (dbPath: string) => {
        let conn = connections.get(dbPath);
        if (!conn) {
            if (!fs.existsSync(dbPath)) throw new Error(`Database file not found at ${dbPath}`);
            const db = new Database(dbPath, { readonly: true });
            sqliteVec.load(db);
            conn = { db };
            connections.set(dbPath, conn);
        }
        return conn;
    };

    // The keyword index is built in memory from the stored chunk text, so an
    // existing database can be evaluated without re-ingesting it.
    const ftsFor = (conn: { db: any; fts?: any }) => {
        if (!conn.fts) {
            const fts = new Database(':memory:');
            fts.exec(`CREATE VIRTUAL TABLE chunks USING fts5(content, url UNINDEXED, product_name UNINDEXED, version UNINDEXED)`);
            const insert = fts.prepare('INSERT INTO chunks (rowid, content, url, product_name, version) VALUES (?, ?, ?, ?, ?)');
            const rows = conn.db.prepare('SELECT rowid, content, url, product_name, version FROM vec_items').all();
            fts.transaction(() => {
                for (const r of rows) insert.run(BigInt(r.rowid), r.content ?? '', r.url, r.product_name, r.version);
            })();
            conn.fts = fts;
        }
        return conn.fts;
    };

    const dense = (conn: { db: any }, queryEmbedding: number[], evalCase: EvalCase, limit: number): RetrievedChunk[] => {
        let query = `
            SELECT rowid, url, distance, content
            FROM vec_items
            WHERE embedding MATCH @query_embedding`;
        if (evalCase.product_name) query += ` AND product_name = @product_name`;
        if (evalCase.version) query += ` AND version = @version`;
        query += `
            ORDER BY distance
            LIMIT @top_k;`;
        return conn.db.prepare(query).all({
            query_embedding: new Float32Array(queryEmbedding),
            product_name: evalCase.product_name,
            version: evalCase.version,
            top_k: limit,
        });
    };

    const bm25 = (conn: { db: any; fts?: any }, evalCase: EvalCase, limit: number): RetrievedChunk[] => {
        const match = toFtsQuery(evalCase.query);
        if (!match) return [];
        let query = `SELECT rowid, url, content FROM chunks WHERE chunks MATCH @match`;
        if (evalCase.product_name) query += ` AND product_name = @product_name`;
        if (evalCase.version) query += ` AND version = @version`;
        query += ` ORDER BY bm25(chunks) LIMIT @limit`;
        return ftsFor(conn).prepare(query).all({
            match,
            product_name: evalCase.product_name,
            version: evalCase.version,
            limit,
        });
    };

    return async (queryEmbedding, evalCase, topK) => {
        const dbPath = evalCase.db ?? defaultDbPath;
        if (!dbPath) throw new Error(`No database for eval case ${evalCase.id}: pass --db or set "db" on the case`);
        const conn = open(dbPath);
        const rerankCandidates = options.rerankCandidates ?? DEFAULT_RERANK_CANDIDATES;
        const depth = Math.max(
            topK,
            options.candidates ?? (mode === 'dense' && !options.perPage ? topK : 50),
            options.rerank ? rerankCandidates : 0
        );
        let chunks: RetrievedChunk[];
        if (mode === 'bm25') {
            chunks = bm25(conn, evalCase, depth);
        } else if (mode === 'hybrid') {
            chunks = fuseRrf([dense(conn, queryEmbedding, evalCase, depth), bm25(conn, evalCase, depth)]);
        } else {
            chunks = dense(conn, queryEmbedding, evalCase, depth);
        }
        if (options.rerank && chunks.length > 1) {
            const pool = chunks.slice(0, rerankCandidates);
            const scores = await options.rerank(evalCase.query, pool.map(c => c.content ?? ''));
            const reranked = pool.map((c, i) => ({ c, s: scores[i] })).sort((a, b) => b.s - a.s).map(x => x.c);
            chunks = [...reranked, ...chunks.slice(rerankCandidates)];
        }
        if (options.perPage) chunks = onePerPage(chunks);
        return chunks.slice(0, topK);
    };
}

// Mirrors the provider selection in Doc2Vec's constructor and the MCP server's
// env vars, so the query is embedded with the model the database was built with.
export function createEmbedder(embeddingConfig?: EmbeddingConfig): { embed: Embed; model: string } {
    const provider = embeddingConfig?.provider || process.env.EMBEDDING_PROVIDER || 'openai';
    let client: OpenAI;
    let model: string;
    if (provider === 'azure') {
        const apiKey = embeddingConfig?.azure?.api_key || process.env.AZURE_OPENAI_KEY;
        const endpoint = embeddingConfig?.azure?.endpoint || process.env.AZURE_OPENAI_ENDPOINT;
        model = embeddingConfig?.azure?.deployment_name || process.env.AZURE_OPENAI_DEPLOYMENT_NAME || 'text-embedding-3-large';
        if (!apiKey || !endpoint) throw new Error('Azure OpenAI requires api_key and endpoint to be configured');
        client = new AzureOpenAI({
            apiKey,
            endpoint,
            deployment: model,
            apiVersion: embeddingConfig?.azure?.api_version || process.env.AZURE_OPENAI_API_VERSION || '2024-10-21',
        });
    } else {
        const apiKey = embeddingConfig?.openai?.api_key || process.env.OPENAI_API_KEY;
        const baseURL = embeddingConfig?.openai?.base_url || process.env.OPENAI_BASE_URL;
        model = embeddingConfig?.openai?.model || process.env.OPENAI_MODEL || 'text-embedding-3-large';
        if (!apiKey) throw new Error('OpenAI requires api_key to be configured');
        client = new OpenAI({ apiKey, ...(baseURL && { baseURL }) });
    }
    const embed: Embed = async (text) => {
        const response = await client.embeddings.create({ model, input: text });
        return response.data[0].embedding;
    };
    return { embed, model };
}

export function formatReport(report: EvalReport): string {
    const lines: string[] = [];
    lines.push(`Model: ${report.model}   Cases: ${report.cases}   top_k: ${report.top_k}`);
    const recallParts = Object.entries(report.recall).map(([k, v]) => `recall@${k} ${(v * 100).toFixed(1)}%`);
    lines.push(`${recallParts.join('   ')}   MRR ${report.mrr.toFixed(3)}`);
    const misses = report.results.filter(r => r.rank === null);
    if (misses.length) {
        lines.push('');
        lines.push(`Misses (${misses.length}):`);
        for (const m of misses) lines.push(`  [${m.id}] ${m.query}`);
    }
    return lines.join('\n');
}

export async function runEvalCommand(options: {
    casesPath: string;
    dbPath?: string;
    configPath?: string;
    topK: number;
    outPath?: string;
    mode?: RetrievalMode;
    perPage?: boolean;
    rerank?: string;
    rerankModel?: string;
    rerankCandidates?: number;
}): Promise<EvalReport> {
    const mode = options.mode ?? 'dense';
    if (!['dense', 'bm25', 'hybrid'].includes(mode)) {
        throw new Error(`Unknown mode '${mode}': use dense, bm25 or hybrid`);
    }
    let rerank: Rerank | undefined;
    if (options.rerank) {
        if (options.rerank !== 'deepinfra') throw new Error(`Unknown reranker '${options.rerank}': use deepinfra`);
        const apiKey = process.env.DEEPINFRA_API_KEY;
        if (!apiKey) throw new Error('--rerank deepinfra requires the DEEPINFRA_API_KEY environment variable');
        rerank = createDeepInfraReranker(apiKey, options.rerankModel);
    }
    let embeddingConfig: EmbeddingConfig | undefined;
    if (options.configPath) {
        const config = yaml.load(fs.readFileSync(options.configPath, 'utf8')) as { embedding?: EmbeddingConfig } | undefined;
        embeddingConfig = config?.embedding;
    }
    // Keyword-only retrieval never looks at the query vector, so skip the API call
    const { embed, model } = mode === 'bm25'
        ? { embed: async () => [] as number[], model: 'none (bm25)' }
        : createEmbedder(embeddingConfig);
    const cases = loadEvalCases(options.casesPath);
    const retrieve = createSqliteRetriever(options.dbPath, {
        mode, perPage: options.perPage, rerank, rerankCandidates: options.rerankCandidates,
    });
    const label = `${model}, ${mode}` +
        `${rerank ? `, reranked by ${options.rerankModel ?? DEFAULT_RERANK_MODEL} over ${options.rerankCandidates ?? DEFAULT_RERANK_CANDIDATES}` : ''}` +
        `${options.perPage ? ', one chunk per page' : ''}`;
    const report = await runEval(cases, { embed, retrieve }, { topK: options.topK, model: label });
    console.log(formatReport(report));
    if (options.outPath) {
        fs.writeFileSync(options.outPath, JSON.stringify(report, null, 2));
        console.log(`\nReport written to ${options.outPath}`);
    }
    return report;
}
