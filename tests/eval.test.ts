import os from 'os';
import path from 'path';
import fs from 'fs';
import BetterSqlite3 from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';
import { describe, expect, it } from 'vitest';
import {
    computeMetrics,
    createSqliteRetriever,
    firstRelevantRank,
    formatReport,
    fuseRrf,
    loadEvalCases,
    normalizeUrl,
    onePerPage,
    runEval,
    toFtsQuery,
    type EvalCaseResult,
} from '../eval';

function tempDir(): string {
    return fs.mkdtempSync(path.join(os.tmpdir(), 'doc2vec-eval-'));
}

describe('eval metrics', () => {
    it('normalizes fragments, trailing slashes and case', () => {
        expect(normalizeUrl(' https://Docs.example.com/a/b/#install ')).toBe('https://docs.example.com/a/b');
        expect(normalizeUrl('https://docs.example.com/a/b')).toBe('https://docs.example.com/a/b');
    });

    it('returns the 1-based rank of the first relevant url', () => {
        const retrieved = ['https://x/a', 'https://x/b/', 'https://x/c#s', 'https://x/b'];
        expect(firstRelevantRank(retrieved, ['https://x/b'])).toBe(2);
        expect(firstRelevantRank(retrieved, ['https://x/c', 'https://x/a'])).toBe(1);
        expect(firstRelevantRank(retrieved, ['https://x/z'])).toBeNull();
        expect(firstRelevantRank([undefined, 'https://x/a'], ['https://x/a'])).toBe(2);
    });

    it('computes recall@k and MRR, counting misses as zero', () => {
        const results: EvalCaseResult[] = [
            { id: '1', query: 'q1', rank: 1, retrieved_urls: [] },
            { id: '2', query: 'q2', rank: 3, retrieved_urls: [] },
            { id: '3', query: 'q3', rank: null, retrieved_urls: [] },
            { id: '4', query: 'q4', rank: 8, retrieved_urls: [] },
        ];
        const metrics = computeMetrics(results, [1, 4, 10]);
        expect(metrics.cases).toBe(4);
        expect(metrics.recall).toEqual({ 1: 0.25, 4: 0.5, 10: 0.75 });
        expect(metrics.mrr).toBeCloseTo((1 + 1 / 3 + 0 + 1 / 8) / 4);
    });

    it('handles an empty result set', () => {
        expect(computeMetrics([], [4])).toEqual({ cases: 0, recall: { 4: 0 }, mrr: 0 });
    });
});

describe('retrieval helpers', () => {
    it('onePerPage keeps the first chunk of each page in order', () => {
        const chunks = [{ url: 'https://x/a#1' }, { url: 'https://x/b' }, { url: 'https://x/a/' }, { url: 'https://x/c' }];
        expect(onePerPage(chunks).map(c => c.url)).toEqual(['https://x/a#1', 'https://x/b', 'https://x/c']);
    });

    it('fuseRrf rewards chunks ranked well in both lists', () => {
        const dense = [{ rowid: 1 }, { rowid: 2 }, { rowid: 3 }];
        const keyword = [{ rowid: 2 }, { rowid: 4 }, { rowid: 5 }];
        // 2 appears in both lists, which beats being first in one list only
        expect(fuseRrf([dense, keyword]).map(c => c.rowid)).toEqual([2, 1, 4, 3, 5]);
    });

    it('toFtsQuery quotes terms so punctuation cannot break MATCH', () => {
        expect(toFtsQuery('How do I set "maxRequestSize" (v2.3)? kgateway, kgateway')).toBe(
            '"how" OR "do" OR "i" OR "set" OR "maxrequestsize" OR "v2" OR "3" OR "kgateway"'
        );
        expect(toFtsQuery('?!')).toBe('');
    });
});

describe('loadEvalCases', () => {
    it('reads jsonl and assigns ids by position', () => {
        const dir = tempDir();
        const file = path.join(dir, 'cases.jsonl');
        fs.writeFileSync(file, [
            '{"query": "how to install", "relevant_urls": ["https://x/install"]}',
            '',
            '// comment lines are skipped',
            '{"id": "mtls", "query": "enable mtls", "relevant_urls": ["https://x/mtls"], "product_name": "istio"}',
        ].join('\n'));
        const cases = loadEvalCases(file);
        expect(cases.map(c => c.id)).toEqual(['1', 'mtls']);
        expect(cases[1].product_name).toBe('istio');
    });

    it('reads yaml lists', () => {
        const dir = tempDir();
        const file = path.join(dir, 'cases.yaml');
        fs.writeFileSync(file, '- query: a\n  relevant_urls: [https://x/a]\n');
        expect(loadEvalCases(file)).toEqual([{ id: '1', query: 'a', relevant_urls: ['https://x/a'] }]);
    });

    it('rejects cases without a query or relevant urls', () => {
        const dir = tempDir();
        const noUrls = path.join(dir, 'no-urls.json');
        fs.writeFileSync(noUrls, JSON.stringify([{ query: 'a', relevant_urls: [] }]));
        expect(() => loadEvalCases(noUrls)).toThrow(/no relevant_urls/);

        const noQuery = path.join(dir, 'no-query.json');
        fs.writeFileSync(noQuery, JSON.stringify([{ relevant_urls: ['https://x/a'] }]));
        expect(() => loadEvalCases(noQuery)).toThrow(/no query/);

        const notList = path.join(dir, 'not-list.json');
        fs.writeFileSync(notList, JSON.stringify({ query: 'a' }));
        expect(() => loadEvalCases(notList)).toThrow(/list of cases/);
    });
});

describe('runEval against sqlite-vec', () => {
    // Four 4-dimensional chunks on distinct axes: a query embedding picks its
    // nearest neighbours deterministically, with no embedding API involved.
    function buildDb(): string {
        const dbPath = path.join(tempDir(), 'eval.db');
        const db = new BetterSqlite3(dbPath);
        sqliteVec.load(db);
        db.exec(`
            CREATE VIRTUAL TABLE vec_items USING vec0(
                embedding FLOAT[4],
                product_name TEXT,
                version TEXT,
                url TEXT,
                content TEXT
            );
        `);
        const insert = db.prepare('INSERT INTO vec_items (embedding, product_name, version, url, content) VALUES (?, ?, ?, ?, ?)');
        insert.run(new Float32Array([1, 0, 0, 0]), 'istio', 'latest', 'https://docs/install', 'Install the mesh with istioctl');
        insert.run(new Float32Array([0, 1, 0, 0]), 'istio', 'latest', 'https://docs/mtls', 'Enable STRICT PeerAuthentication for mutual TLS');
        insert.run(new Float32Array([0, 0, 1, 0]), 'cilium', 'latest', 'https://docs/cilium', 'Cilium network policies and PeerAuthentication');
        insert.run(new Float32Array([0.9, 0.1, 0, 0]), 'istio', 'latest', 'https://docs/install#helm', 'Install with the Helm chart');
        db.close();
        return dbPath;
    }

    const vectors: Record<string, number[]> = {
        install: [1, 0, 0, 0],
        mtls: [0, 1, 0, 0],
        cilium: [0, 0, 1, 0],
    };
    const embed = async (text: string) => vectors[text];

    it('ranks the same way the MCP server query does and reports metrics', async () => {
        const dbPath = buildDb();
        const report = await runEval(
            [
                { id: 'a', query: 'install', relevant_urls: ['https://docs/install'] },
                { id: 'b', query: 'mtls', relevant_urls: ['https://docs/install'] },
                { id: 'c', query: 'mtls', relevant_urls: ['https://docs/cilium'], product_name: 'istio' },
            ],
            { embed, retrieve: createSqliteRetriever(dbPath) },
            { topK: 2, model: 'fake', kValues: [1, 4] }
        );

        expect(report.results.map(r => r.rank)).toEqual([1, 2, null]);
        // The product filter keeps the cilium chunk out of an istio query
        expect(report.results[2].retrieved_urls).not.toContain('https://docs/cilium');
        // k values above top_k are dropped rather than reported as misleading recall
        expect(report.recall).toEqual({ 1: 1 / 3 });
        expect(report.top_k).toBe(2);
        expect(formatReport(report)).toContain('Misses (1)');
    });

    it('lets a case point at its own database', async () => {
        const dbPath = buildDb();
        const report = await runEval(
            [{ query: 'cilium', relevant_urls: ['https://docs/cilium'], db: dbPath }],
            { embed, retrieve: createSqliteRetriever() },
            { topK: 1, model: 'fake' }
        );
        expect(report.results[0].rank).toBe(1);
    });

    it('keeps only the best chunk of each page with perPage', async () => {
        const dbPath = buildDb();
        const retrieve = createSqliteRetriever(dbPath, { perPage: true });
        const chunks = await retrieve(vectors.install, { query: 'install', relevant_urls: ['x'] }, 3);
        // The #helm chunk of the install page is dropped, freeing a slot for
        // the other pages (mtls and cilium tie on distance, so order is free)
        const urls = chunks.map(c => c.url);
        expect(urls[0]).toBe('https://docs/install');
        expect(urls).not.toContain('https://docs/install#helm');
        expect(new Set(urls.slice(1))).toEqual(new Set(['https://docs/mtls', 'https://docs/cilium']));
    });

    it('ranks by keywords in bm25 mode, ignoring the query vector', async () => {
        const dbPath = buildDb();
        const retrieve = createSqliteRetriever(dbPath, { mode: 'bm25' });
        const chunks = await retrieve([], { query: 'PeerAuthentication: STRICT?', relevant_urls: ['x'], product_name: 'istio' }, 5);
        expect(chunks.map(c => c.url)).toEqual(['https://docs/mtls']);
        expect(chunks[0].content).toContain('STRICT');
    });

    it('fuses dense and keyword lists in hybrid mode', async () => {
        const dbPath = buildDb();
        const retrieve = createSqliteRetriever(dbPath, { mode: 'hybrid' });
        // The vector points at install, the keywords at mtls: both must surface
        const chunks = await retrieve(vectors.install, { query: 'PeerAuthentication', relevant_urls: ['x'], product_name: 'istio' }, 2);
        expect(chunks.map(c => c.url)).toContain('https://docs/mtls');
        expect(chunks.map(c => c.url)).toContain('https://docs/install');
    });

    it('reranks the top candidates before keeping one chunk per page', async () => {
        const dbPath = buildDb();
        const seen: string[][] = [];
        // Prefers the cilium chunk, which the vector ranks last
        const rerank = async (_q: string, docs: string[]) => { seen.push(docs); return docs.map(d => (d.startsWith('Cilium') ? 10 : 0)); };
        const retrieve = createSqliteRetriever(dbPath, { rerank, rerankCandidates: 4, perPage: true });
        const chunks = await retrieve(vectors.install, { query: 'install', relevant_urls: ['x'] }, 4);
        expect(chunks[0].url).toBe('https://docs/cilium');
        expect(seen[0]).toHaveLength(4);
        expect(chunks.map(c => c.url)).not.toContain('https://docs/install#helm');
    });

    it('fails clearly when no database is given', async () => {
        await expect(runEval(
            [{ query: 'install', relevant_urls: ['https://docs/install'] }],
            { embed, retrieve: createSqliteRetriever() },
            { topK: 1, model: 'fake' }
        )).rejects.toThrow(/No database/);
    });
});
