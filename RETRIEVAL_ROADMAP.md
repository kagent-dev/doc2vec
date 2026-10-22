# Retrieval Roadmap

Ideas for improving retrieval quality, ranked by expected gain per effort, and
the baseline they are measured against. Measure every change with
`doc2vec eval` (see README, "Evaluating retrieval quality") before adopting it.

## Baseline (2026-10-21)

Eval set built from real support-assistant conversations: each case is either
the user's first question or one of the assistant's own search queries,
labelled with the docs pages the assistant cited in its answer (silver labels:
a cited page is relevant, an uncited one is not necessarily irrelevant). The
eval cases themselves are private and not part of the repository; only
aggregate scores are reported here.

Databases: `solo-enterprise-for-kgateway` 2.3.x docs (339 pages, 2,649 chunks)
and `solo-enterprise-for-agentgateway` kubernetes/latest docs (391 pages, 3,113
chunks); `text-embedding-3-large`, 3072 dims, sqlite-vec, same query as the MCP
server. Agentgateway labels cited under the old `/latest/` paths were mapped
through their 301 redirects.

| Docs | Case set | Cases | recall@1 | recall@4 | recall@10 | MRR |
|---|---|---|---|---|---|---|
| kgateway | User questions | 84 | 41.7% | 59.5% | 72.6% | 0.506 |
| kgateway | Agent search queries | 443 | 45.8% | 69.3% | 79.9% | 0.567 |
| agentgateway | User questions | 116 | 42.2% | 62.9% | 74.1% | 0.526 |
| agentgateway | Agent search queries | 563 | 42.3% | 63.6% | 74.8% | 0.526 |

Where the first relevant chunk lands (top-50 retrieval):

| Docs | Case set | rank 1 | 2–4 | 5–10 | 11–50 | >50 |
|---|---|---|---|---|---|---|
| kgateway | User questions | 42% | 18% | 13% | 17% | 11% |
| kgateway | Agent search queries | 46% | 23% | 11% | 13% | 7% |
| agentgateway | User questions | 42% | 21% | 11% | 14% | 12% |
| agentgateway | Agent search queries | 42% | 21% | 11% | 16% | 9% |

About a quarter of cases have the right page in the top 50 but not the top 4,
which is what a reranker recovers. The top 4 chunks also hold on average 1.2
chunks from a page already shown.

## Comparison matrix (2026-10-21)

Same 1,206 cases, scores averaged over the four case sets. Each embedding model
re-embedded the same stored chunks (queries and documents marked as such where
the model supports it). Hybrid = dense + SQLite FTS5 BM25 fused with RRF (k=60).

Recall@4 by embedding model and search method:

| Embedding | Vector | Vector, 1 chunk/page | Hybrid | Hybrid, 1 chunk/page |
|---|---|---|---|---|
| Nemotron-3.5-Embed-8B (DeepInfra) | 70.4 | **77.3** | 68.0 | 75.2 |
| Gemini embedding-2 | 67.0 | 74.0 | 68.8 | 75.8 |
| Qwen3-Embedding-8B (DeepInfra) | 66.5 | 70.2 | 66.9 | 74.3 |
| Gemini embedding-001 | 68.2 | 74.1 | 67.3 | 73.8 |
| Cohere embed-v4 (Bedrock) | 64.3 | 71.9 | 65.6 | 72.0 |
| OpenAI text-embedding-3-large (today) | 63.8 | 68.2 | 64.8 | 70.8 |
| OpenAI text-embedding-3-large @1024 | 64.0 | 69.5 | 64.2 | 70.4 |
| Mistral embed | 60.8 | 65.3 | 64.2 | 69.8 |
| OpenAI text-embedding-3-small | 55.8 | 63.8 | 61.9 | 67.9 |

Keyword search alone (BM25): 53.0, or 57.6 with one chunk per page.

Rerankers on the top 40 hybrid chunks, then one chunk per page:

| Reranker | On text-embedding-3-large: @1 / @4 / MRR@10 | On Nemotron-3.5-Embed-8B: @1 / @4 / MRR@10 | Latency p50 / p95 (one at a time) |
|---|---|---|---|
| No reranker (hybrid, 1 chunk/page) | 42.5 / 70.8 / 0.560 | 42.5 / 75.2 / 0.575 | — |
| **Nemotron-3.5-Rerank-8B** | **52.7 / 79.5 / 0.655** | **52.1 / 82.8 / 0.662** | 1.5 s / 2.8 s |
| Nemotron rerank VL 1B (current chatbot) | 44.5 / 74.6 / 0.588 | 43.5 / 76.6 / 0.589 | 0.86 s / 2.1 s |
| Qwen3-Reranker-4B | 44.5 / 76.0 / 0.588 | 44.0 / 76.4 / 0.593 | 2.3 s / 12.9 s |
| Qwen3-Reranker-8B | 40.1 / 72.7 / 0.557 | 38.7 / 74.2 / 0.553 | 2.1 s / 4.9 s |
| Cohere Rerank 3.5 (Bedrock) | 40.9 / 72.4 / 0.560 | 40.4 / 74.7 / 0.564 | 1.6 s / 2.2 s |
| Claude Haiku, listwise (user questions only) | — | 41.6 / 75.7 / 0.570 | 2.0 s / 2.6 s |

Today (text-embedding-3-large, vector search, no reranker): 43.0 / 63.8 / 0.532.

On today's vector-only candidates (top 40 of text-embedding-3-large vector search), recall@1 / @4 / MRR@10:

| Reranker | Chunks as reranked | Then 1 chunk/page |
|---|---|---|
| No reranker | 43.0 / 63.8 / 0.532 | 43.0 / 68.2 / 0.556 |
| Nemotron rerank VL 1B (current chatbot) | 43.4 / 65.9 / 0.551 | 43.4 / 73.5 / 0.578 |
| Nemotron-3.5-Rerank-8B | 52.5 / 73.8 / 0.624 | 52.5 / 78.5 / 0.647 |

The VL 1B reranker on its own adds about +0.4 recall@1 and +2 recall@4; most of
its apparent gain comes from one chunk per page.

### MCP server implementation (measured)

`mcp/src/server.ts` now runs vector search → keyword search fused by RRF
(SQLite, `HYBRID_SEARCH`) → URL filters → reranking (`RERANKER=deepinfra`) →
one chunk per page (`ONE_CHUNK_PER_PAGE`). Its built code, called through
`queryDocumentation` with `limit: 4` on the same cases, scored:

| Configuration | recall@1 | recall@4 |
|---|---|---|
| Vector only (previous behaviour) | 43.0 | 63.8 |
| One chunk per page | 43.0 | 68.2 |
| Hybrid + one chunk per page | 42.5 | 70.8 |
| Hybrid + Nemotron-3.5-Rerank-8B + one chunk per page | 52.7 | 79.5 |

These match `doc2vec eval --mode hybrid --per-page --rerank deepinfra`
(identical on three sets, one case apart on the fourth, within reranker
run-to-run variation).

### Cost: text-embedding-3-large vs Nemotron-3.5-Embed-8B

| | text-embedding-3-large (OpenAI) | Nemotron-3.5-Embed-8B (DeepInfra) |
|---|---|---|
| Price per 1M tokens | $0.13 ($0.065 via Batch API) | $0.035 |
| Re-embed the two eval sites (5,762 chunks, ~2.4M tokens) | ~$0.31 | ~$0.08 |
| Per 1M chunks (~415 tokens each) | ~$54 | ~$15 |
| Per 1M queries (~20 tokens each) | ~$2.60 | ~$0.70 |
| Vector size (float32) | 3072 dims, 12 KB per chunk | 4096 dims, 16 KB per chunk (+33%) |
| SQLite file, kgateway / agentgateway | 46 MB / 60 MB | 58 MB / 76 MB |

Nemotron supports Matryoshka truncation (1024, 768, 512 dims, re-normalized),
which would make vectors smaller than today's; quality at lower dims is not
yet measured. Token counts are estimated at 4 characters per token. Reranking
40 chunks is about 17k tokens per query: ~$0.85 per 1,000 queries with
Nemotron-3.5-Rerank-8B ($0.05/M), ~$0.17 with VL 1B ($0.01/M). Using Nemotron
embeddings in production also needs `input_type` (`query` / `passage`) sent by
ingestion and by the MCP server; the measured scores used it, and doc2vec does
not send it today.

Findings:
- One chunk per page is the largest free gain: +4 to +8 points of recall@4 for every model.
- Nemotron-3.5-Embed-8B and Gemini embedding-2 beat text-embedding-3-large by 5–9 points with the same search method; text-embedding-3-small and Mistral embed are the weakest.
- Cutting text-embedding-3-large to 1024 dims costs nothing measurable (3x smaller vectors).
- Hybrid helps weaker embeddings most; with the strongest one, vector + one chunk per page is as good or better.
- Only Nemotron-3.5-Rerank-8B clearly beats the best first stage without a reranker. Most rerankers lower recall@1 below today's.
- Swapping the chatbot's reranker for Nemotron-3.5-Rerank-8B gains about +5 points of recall@4 and +8 of recall@1 on today's embeddings, for about 0.6 s more median latency.
- The Qwen3 retrieval instruction changes nothing on DeepInfra (it likely applies a default).
- Latency under 6 parallel requests reached 5–7 s median for the 8B models on DeepInfra: plan capacity or a dedicated deployment.

## Ideas

1. ~~One chunk per page in the MCP query tools.~~ Done (`ONE_CHUNK_PER_PAGE=true`, opt-in).
2. ~~Nemotron-3.5-Rerank-8B in the MCP server.~~ Done (`RERANKER=deepinfra`, opt-in).
   Still to do: the same reranker in the support chatbot.
3. ~~Hybrid search for Qdrant.~~ Done: sources with `keyword_index: true`
   store a `bm25` sparse vector computed by Qdrant (`qdrant/bm25`), existing
   collections are extended and backfilled on the next sync (Qdrant 1.18+),
   and the MCP server queries it with `HYBRID_SEARCH=true`. SQLite builds its
   keyword index in memory at query time. Qdrant's BM25 stems English and drops
   stopwords where SQLite FTS5 does neither, so their scores can differ.
4. **Record the embedding model and dimension in each database** and check it
   at query time; prerequisite for switching models safely.
5. **Switch embeddings to Nemotron-3.5-Embed-8B or Gemini embedding-2**,
   with query/document input types. Needs (4), provider support in ingestion
   and the MCP server, and a full re-embed.
6. **Lower dimensions** if staying on text-embedding-3-large: 1024 dims
   scored the same as 3072.
7. **Contextual chunk enrichment** (LLM-written context per chunk, or a
   context-aware embedding model such as voyage-context-3).
8. **Batch embedding requests** during ingestion (currently one chunk per
   call) — speed, not quality, but it makes a re-embed for (5) cheaper.
9. **Multi-vector / late interaction (ColBERT, ColPali)** — last; storage cost
   is high and sqlite-vec has no MaxSim. ColPali only if PDFs with diagrams
   matter.
10. **Not tested (no keys):** Voyage embeddings and rerankers, Zerank-2, Cohere
    Rerank 4 Pro, Jina.

## Found while building the eval

- **Fixed.** Chunks with identical text on different pages collapsed into one Qdrant
  point: the point id was derived from `chunk_id`, a hash of the chunk text
  (breadcrumb included), so the last page written won. Loading the eval sites
  into Qdrant kept 2,303 of 2,649 kgateway chunks and 2,949 of 3,113
  agentgateway chunks; SQLite keeps them all. Both pages then looked changed
  on every sync: with the old ids, 108 of 339 kgateway pages were re-embedded
  on every pass. Point ids now come from the page URL and chunk position; a
  one-time pass on website and S3 sources restores the lost chunks (measured
  on a local Qdrant: the first sync rewrote 55 pages and restored all 2,649
  chunks, later syncs rewrote nothing).
- **Fixed.** `@qdrant/js-client-rest` 1.19 removed `search`, and the MCP
  server's `^1.16.2` range allowed 1.19 on a fresh install. Its vector search
  now uses the Query API and the client is on 1.19 (1.16 also fails on
  Node 26).
