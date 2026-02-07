#!/usr/bin/env node
// src/index.ts
import 'dotenv/config'; // Load .env file
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ListToolsRequestSchema, CallToolRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { AzureOpenAI } from "openai";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import express, { Request, Response } from "express";
import { z } from "zod";
import { randomUUID } from 'crypto';

import * as sqliteVec from "sqlite-vec";
import Database, { Database as DatabaseType } from "better-sqlite3";
import { OpenAI } from 'openai';
import { GoogleGenerativeAI } from '@google/generative-ai';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs'; // Import fs for checking file existence

// --- Configuration & Environment Check ---

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Provider configuration
// Note: Anthropic does not provide an embeddings API, only text generation
// Supported providers: 'openai', 'azure', 'gemini'
const embeddingProvider = process.env.EMBEDDING_PROVIDER || 'openai';

// OpenAI configuration
const openAIApiKey = process.env.OPENAI_API_KEY;
const openAIModel = process.env.OPENAI_MODEL || 'text-embedding-3-large';

// Azure OpenAI configuration
const azureApiKey = process.env.AZURE_OPENAI_KEY;
const azureEndpoint = process.env.AZURE_OPENAI_ENDPOINT;
const azureApiVersion = process.env.AZURE_OPENAI_API_VERSION || '2024-10-21';
const azureDeploymentName = process.env.AZURE_OPENAI_DEPLOYMENT_NAME || 'text-embedding-3-large';

// Google Gemini configuration
const geminiApiKey = process.env.GEMINI_API_KEY;
const geminiModel = process.env.GEMINI_MODEL || 'gemini-embedding-001';

const dbDir = process.env.SQLITE_DB_DIR || __dirname; // Default to current dir if not set

if (!fs.existsSync(dbDir)) {
    console.warn(`Warning: SQLITE_DB_DIR (${dbDir}) does not exist. Databases may not be found.`);
    process.exit(1);
}

const strictMode = process.env.STRICT_MODE === 'true';
if (strictMode) {
    switch (embeddingProvider) {
        case 'openai':
            if (!openAIApiKey) {
                console.error("Error: OPENAI_API_KEY environment variable is not set.");
                process.exit(1);
            }
            break;
        case 'azure':
            if (!azureApiKey || !azureEndpoint) {
                console.error("Error: AZURE_OPENAI_KEY and AZURE_OPENAI_ENDPOINT environment variables are required for Azure provider.");
                process.exit(1);
            }
            break;
        case 'gemini':
            if (!geminiApiKey) {
                console.error("Error: GEMINI_API_KEY environment variable is not set.");
                process.exit(1);
            }
            break;
        default:
            console.error(`Error: Unknown embedding provider '${embeddingProvider}'. Supported providers: openai, azure, gemini`);
            console.error("Note: Anthropic does not provide an embeddings API, only text generation models.");
            process.exit(1);
    }
}

export interface QueryResult {
    chunk_id: string;
    distance: number;
    content: string;
    url?: string;
    section?: string;
    heading_hierarchy?: string;
    chunk_index?: number;
    total_chunks?: number;
    embedding?: Float32Array | number[];
    [key: string]: unknown;
}

async function createEmbeddings(text: string): Promise<number[]> {
    try {

        switch (embeddingProvider) {
            case 'openai': {
                const openai = new OpenAI({
                    apiKey: openAIApiKey,
                });
                const response = await openai.embeddings.create({
                    model: openAIModel,
                    input: text,
                });
                if (!response.data?.[0]?.embedding) {
                    throw new Error("Failed to get embedding from OpenAI response.");
                }
                return response.data[0].embedding;
            }
            
            case 'azure': {
              const azure = new AzureOpenAI({
                apiKey: azureApiKey,
                endpoint: azureEndpoint,
                deployment: azureDeploymentName,
                apiVersion: azureApiVersion,
              });

                const response = await azure.embeddings.create({
                    model: azureDeploymentName, // Use deployment name for Azure
                    input: text,
                });
                if (!response.data?.[0]?.embedding) {
                    throw new Error("Failed to get embedding from Azure OpenAI response.");
                }
                return response.data[0].embedding;
            }
            
            case 'gemini': {
                const genAI = new GoogleGenerativeAI(geminiApiKey!);
                const model = genAI.getGenerativeModel({ model: geminiModel });
                const result = await model.embedContent(text);
                if (!result.embedding?.values) {
                    throw new Error("Failed to get embedding from Gemini response.");
                }
                return result.embedding.values;
            }
            default:
                throw new Error(`Unsupported embedding provider: ${embeddingProvider}. Supported providers: openai, azure, gemini`);
        }

    } catch (error) {
        console.error(`Error creating ${embeddingProvider} embeddings:`, error);
        throw new Error(`Failed to create embeddings with ${embeddingProvider}: ${error instanceof Error ? error.message : String(error)}`);
    }
}

type QueryFilter = {
    product_name?: string;
    version?: string;
    branch?: string;
    repo?: string;
    urlPrefix?: string;
    extensions?: string[];
};

function resolveDbPath(dbName?: string, productName?: string): { dbPath: string; dbLabel: string } {
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
}

function normalizeExtensions(extensions?: string[]): string[] {
    if (!extensions || extensions.length === 0) {
        return [];
    }

    return extensions.map((ext) => (ext.startsWith('.') ? ext.toLowerCase() : `.${ext.toLowerCase()}`));
}

function filterResultsByUrl(
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

function filterResultsWithContent(results: QueryResult[]): QueryResult[] {
    return results.filter((row) => {
        if (typeof row.content !== 'string') {
            return false;
        }
        return row.content.trim().length > 0;
    });
}

function queryCollection(
    queryEmbedding: number[],
    dbPath: string,
    filter: QueryFilter,
    topK: number = 10
): QueryResult[] {

    if (!fs.existsSync(dbPath)) {
        throw new Error(`Database file not found at ${dbPath}`);
    }

    let db: DatabaseType | null = null;
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
        })
      
        return rows as QueryResult[];
    } catch (error) {
        console.error(`Error querying collection in ${dbPath}:`, error);
        throw new Error(`Database query failed: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
        if (db) {
            db.close();
        }
    }
}

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
    const { dbPath } = resolveDbPath(dbName, productName);
    const hasPostFilters = !!urlPathPrefix;
    const fetchLimit = hasPostFilters ? limit * 3 : limit;
    const results = queryCollection(
        queryEmbedding,
        dbPath,
        { product_name: productName, version: version, urlPrefix: urlPathPrefix },
        fetchLimit
    );
    const filteredResults = filterResultsWithContent(filterResultsByUrl(results, urlPathPrefix));
    return filteredResults.slice(0, limit).map((qr: QueryResult) => ({
        distance: qr.distance,
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
    const { dbPath } = resolveDbPath(dbName, undefined);
    const hasPostFilters = !!filePathPrefix || (extensions && extensions.length > 0);
    const fetchLimit = hasPostFilters ? limit * 3 : limit;
    const results = queryCollection(
        queryEmbedding,
        dbPath,
        { product_name: productName, repo, branch, urlPrefix: filePathPrefix, extensions },
        fetchLimit
    );
    const filteredResults = filterResultsWithContent(filterResultsByUrl(results, filePathPrefix, extensions));
    const mappedResults = filteredResults.slice(0, limit).map((qr: QueryResult) => ({
        distance: qr.distance,
        content: qr.content,
        ...(qr.url && { url: qr.url }),
        ...(qr.section && { section: qr.section }),
        ...(typeof qr.chunk_index === 'number' && { chunk_index: qr.chunk_index }),
        ...(typeof qr.total_chunks === 'number' && { total_chunks: qr.total_chunks }),
    }));
    const emptyContentCount = results.filter((row) => typeof row.content !== 'string' || row.content.trim().length === 0).length;
    return { results: mappedResults, rawCount: results.length, emptyContentCount };
}

function getChunksForDocument(
    productName: string | undefined,
    dbName: string | undefined,
    filePath: string,
    startIndex?: number,
    endIndex?: number,
    version?: string
): QueryResult[] {
    const { dbPath } = resolveDbPath(dbName, productName);

    if (!fs.existsSync(dbPath)) {
        throw new Error(`Database file not found at ${dbPath}`);
    }

    let db: DatabaseType | null = null;
    try {
        db = new Database(dbPath);
        sqliteVec.load(db);

        const hasRange = typeof startIndex === 'number' && typeof endIndex === 'number';
        
        // Try to build and execute query with chunk_index/total_chunks first
        // If it fails, retry without those columns (backward compatibility)
        let selectColumns = [
            'chunk_id',
            'content',
            'url',
            'section',
            'heading_hierarchy',
            'chunk_index',
            'total_chunks'
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
        let params: (string | number)[] = [filePath];
        if (productName) params.push(productName);
        if (version) params.push(version);
        if (hasRange) {
            params.push(startIndex);
            params.push(endIndex);
        }

        try {
            stmt = db.prepare(query);
            const rows = stmt.all(...params) as QueryResult[];
            return rows;
        } catch (error: any) {
            // If query fails due to missing chunk_index column, retry without it
            const errorMessage = error?.message || String(error);
            const errorStr = String(error);
            const isChunkIndexError = (errorMessage.includes('no such column') && errorMessage.includes('chunk_index')) ||
                                     (errorStr.includes('no such column') && errorStr.includes('chunk_index'));
            
            if (isChunkIndexError) {
                console.error(`Warning: chunk_index column doesn't exist in database. Using backward compatible query.`);
                
                if (hasRange) {
                    console.error(`Warning: startIndex/endIndex provided but chunk_index column doesn't exist. Ignoring range filter.`);
                }
                
                // Build query without chunk_index/total_chunks
                selectColumns = [
                    'chunk_id',
                    'content',
                    'url',
                    'section',
                    'heading_hierarchy'
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
                    // If retry also fails, throw the original error
                    throw error;
                }
            } else {
                // Re-throw if it's a different error
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
}

// --- MCP Server Setup ---
const serverName = "sqlite-vec-doc-query"; // Store name for logging
const serverVersion = "1.0.0"; // Store version for logging

const server = new McpServer({
    name: serverName,
    version: serverVersion,
    capabilities: {},
});

// --- Define the MCP Tool Logic ---
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
            content: [{ type: "text" as const, text: "Provide either productName or dbName for query_documentation." }],
        };
    }

    console.error(`Received query: text="${queryText}", product="${productName || 'n/a'}", dbName="${dbName || 'n/a'}", version="${version || 'any'}", limit=${limit}`);

    try {
        const results = await queryDocumentation(queryText, productName, dbName, version, urlPathPrefix, limit);

        if (results.length === 0) {
        return {
            content: [{ type: "text" as const, text: `No relevant documentation found for "${queryText}" in ${productName ? `product "${productName}"` : `db "${dbName}"`} ${version ? `(version ${version})` : ''}.` }],
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
                "---"
            ].filter(line => line !== null).join("\n")
        ).join("\n");

        const responseText = `Found ${results.length} relevant documentation snippets for "${queryText}" in ${productName ? `product "${productName}"` : `db "${dbName}"`} ${version ? `(version ${version})` : ''}:\n\n${formattedResults}`;
        console.error(`Handler finished processing. Payload size (approx): ${responseText.length} chars. Returning response object...`);

        return {
            content: [{ type: "text" as const, text: responseText }],
        };
    } catch (error: any) {
        console.error("Error processing 'query_documentation' tool:", error);
        return {
            content: [{ type: "text" as const, text: `Error querying documentation: ${error.message}` }],
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
            content: [{ type: "text" as const, text: "Provide dbName for query_code." }],
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
                    content: [{ type: "text" as const, text: `Found ${rawCount} vector matches in ${target}, but all matching chunks have empty content. Re-ingest this database to populate content fields.` }],
                };
            }

            return {
                content: [{ type: "text" as const, text: `No relevant code found for "${queryText}" in ${target} ${branch ? `(branch ${branch})` : ''}.` }],
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
                "---"
            ].filter(line => line !== null).join("\n")
        ).join("\n");

        const responseText = `Found ${results.length} relevant code snippets for "${queryText}" in ${target} ${branch ? `(branch ${branch})` : ''}:\n\n${formattedResults}`;
        console.error(`Handler finished processing. Payload size (approx): ${responseText.length} chars. Returning response object...`);

        return {
            content: [{ type: "text" as const, text: responseText }],
        };
    } catch (error: any) {
        console.error("Error processing 'query_code' tool:", error);
        return {
            content: [{ type: "text" as const, text: `Error querying code: ${error.message}` }],
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
            content: [{ type: "text" as const, text: "Provide either productName or dbName for get_chunks." }],
        };
    }

    console.error(`Received get_chunks: filePath="${filePath}", product="${productName || 'n/a'}", dbName="${dbName || 'n/a'}", version="${version || 'any'}", startIndex=${startIndex}, endIndex=${endIndex}`);

    try {
        const results = getChunksForDocument(productName, dbName, filePath, startIndex, endIndex, version);

        if (results.length === 0) {
            return {
                content: [{ type: "text" as const, text: `No chunks found for "${filePath}" in ${productName ? `product "${productName}"` : `db "${dbName}"`} ${version ? `(version ${version})` : ''}.` }],
            };
        }

        const formattedResults = results.map((r) =>
            [
                `Chunk ${typeof r.chunk_index === 'number' && typeof r.total_chunks === 'number' ? `${r.chunk_index + 1} of ${r.total_chunks}` : ''}`.trim(),
                `  Content: ${r.content}`,
                r.section ? `  Section: ${r.section}` : null,
                r.url ? `  URL: ${r.url}` : null,
                "---"
            ].filter(line => line !== null).join("\n")
        ).join("\n");

        return {
            content: [{ type: "text" as const, text: `Retrieved ${results.length} chunk(s) for "${filePath}":\n\n${formattedResults}` }],
        };
    } catch (error: any) {
        console.error("Error processing 'get_chunks' tool:", error);
        return {
            content: [{ type: "text" as const, text: `Error retrieving chunks: ${error.message}` }],
        };
    }
};

// --- Define the MCP Tool ---
server.tool(
    "query_documentation",
    "Query documentation stored in a sqlite-vec database using vector search.",
    {
        queryText: z.string().min(1).describe("The natural language query to search for."),
        productName: z.string().min(1).optional().describe("The name of the product documentation database to search within (e.g., 'my-product'). Corresponds to the DB filename without .db."),
        dbName: z.string().min(1).optional().describe("The database filename to query directly (e.g., 'my-product.db' or 'my-product')."),
        version: z.string().optional().describe("The specific version of the product documentation (e.g., '1.2.0'). Optional."),
        urlPathPrefix: z.string().min(1).optional().describe("Full URL prefix to filter documentation results (e.g., 'https://docs.example.com/guide/')."),
        limit: z.number().int().positive().optional().default(4).describe("Maximum number of results to return. Defaults to 4."),
    },
    queryDocumentationToolHandler
);

server.tool(
    "query_code",
    "Query code stored in a sqlite-vec database using vector search.",
    {
        queryText: z.string().min(1).describe("The natural language query to search for."),
        productName: z.string().min(1).optional().describe("Filter results by product name stored in the DB (e.g., 'istio')."),
        repo: z.string().min(1).optional().describe("Filter results by repo name stored in the DB (e.g., 'owner/repo')."),
        dbName: z.string().min(1).describe("The database filename to query directly (e.g., 'repo.db' or 'repo')."),
        branch: z.string().min(1).optional().describe("Branch name to filter code results (e.g., 'main')."),
        filePathPrefix: z.string().min(1).optional().describe("Full file path prefix to filter code results (e.g., 'https://github.com/org/repo/blob/main/src/')."),
        extensions: z.array(z.string().min(1)).optional().describe("File extensions to include (e.g., ['.go', '.rs'])."),
        limit: z.number().int().positive().optional().default(4).describe("Maximum number of results to return. Defaults to 4."),
    },
    queryCodeToolHandler
);

server.tool(
    "get_chunks",
    "Retrieve specific chunks from a document by file path.",
    {
        productName: z.string().min(1).optional().describe("The name of the product documentation database to search within (e.g., 'my-product'). Corresponds to the DB filename without .db."),
        dbName: z.string().min(1).optional().describe("The database filename to query directly (e.g., 'my-product.db' or 'my-product')."),
        filePath: z.string().min(1).describe("The file path (url) of the document to retrieve chunks from."),
        startIndex: z.number().int().nonnegative().optional().describe("Start index of the chunk range to retrieve (0-based). If not provided, returns all chunks from the beginning."),
        endIndex: z.number().int().nonnegative().optional().describe("End index of the chunk range to retrieve (0-based, inclusive). If not provided, returns all chunks to the end."),
        version: z.string().optional().describe("The specific version of the product documentation (e.g., '1.2.0'). Optional."),
    },
    getChunksToolHandler
);

// --- Transport Setup ---
async function main() {
    const transport_type = process.env.TRANSPORT_TYPE || 'http';
    let webserver: any = null; // Store server reference for proper shutdown
    
    // Common graceful shutdown handler
    const createGracefulShutdownHandler = (transportCleanup: () => Promise<void>) => {
        return async (signal: string) => {
            console.error(`Received ${signal}, initiating graceful shutdown...`);
            
            const shutdownTimeout = parseInt(process.env.SHUTDOWN_TIMEOUT || '5000', 10);
            const forceExitTimeout = setTimeout(() => {
                console.error(`Shutdown timeout (${shutdownTimeout}ms) exceeded, force exiting...`);
                process.exit(1);
            }, shutdownTimeout);

            try {
                // Close HTTP server first to stop accepting new connections
                if (webserver) {
                    await new Promise<void>((resolve, reject) => {
                        webserver.close((err: any) => {
                            if (err) {
                                console.error('Error closing HTTP server:', err);
                                reject(err);
                            } else {
                                console.error('HTTP server closed');
                                resolve();
                            }
                        });
                    });
                }

                // Clean up transports
                await transportCleanup();

                clearTimeout(forceExitTimeout);
                console.error('Graceful shutdown complete');
                process.exit(0);
            } catch (error) {
                console.error('Error during graceful shutdown:', error);
                clearTimeout(forceExitTimeout);
                process.exit(1);
            }
        };
    };
    
    if (transport_type === 'stdio') {
        // Stdio transport for direct communication
        console.error("Starting MCP server with stdio transport...");
        const transport = new StdioServerTransport();
        await server.connect(transport);
        console.error("MCP server connected via stdio.");
        
        // Add shutdown handler for stdio transport
        const shutdownHandler = createGracefulShutdownHandler(async () => {
            console.error('Closing stdio transport...');
            // StdioServerTransport doesn't have a close method, but we can clean up the connection
            // The transport will be cleaned up when the process exits
        });
        
        process.on('SIGTERM', () => shutdownHandler('SIGTERM'));
        process.on('SIGINT', () => shutdownHandler('SIGINT'));
        
    } else if (transport_type === 'sse') {
        // SSE transport for backward compatibility
        console.error("Starting MCP server with SSE transport...");
        
        const app = express();
        
        // Storage for SSE transports by session ID
        const sseTransports: {[sessionId: string]: SSEServerTransport} = {};

        app.get("/sse", async (_: Request, res: Response) => {
            console.error('Received SSE connection request');
            const transport = new SSEServerTransport('/messages', res);
            sseTransports[transport.sessionId] = transport;
            res.on("close", () => {
                console.error(`SSE connection closed for session ${transport.sessionId}`);
                delete sseTransports[transport.sessionId];
            });
            await server.connect(transport);
        });

        app.post("/messages", async (req: Request, res: Response) => {
            console.error('Received SSE message POST request');
            const sessionId = req.query.sessionId as string;
            const transport = sseTransports[sessionId];
            if (transport) {
                await transport.handlePostMessage(req, res);
            } else {
                console.error(`No SSE transport found for sessionId: ${sessionId}`);
                res.status(400).send('No transport found for sessionId');
            }
        });

        app.get("/health", (_: Request, res: Response) => {
            res.status(200).send("OK");
        });

        const PORT = process.env.PORT || 3001;
        webserver = app.listen(PORT, () => {
            console.error(`MCP server is running on port ${PORT} with SSE transport`);
            console.error(`Connect to: http://localhost:${PORT}/sse`);
        });
        
        webserver.keepAliveTimeout = 3000;
        
        // Keep the process alive
        webserver.on('error', (error: any) => {
            console.error('HTTP server error:', error);
        });
        
        // Handle server shutdown with proper SIGTERM/SIGINT support
        const shutdownHandler = createGracefulShutdownHandler(async () => {
            console.error('Closing SSE transports...');
            
            // Close all active SSE transports
            for (const [sessionId, transport] of Object.entries(sseTransports)) {
                try {
                    console.error(`Closing SSE transport for session ${sessionId}`);
                    // SSE transports typically don't have a close method, cleanup happens via res.on("close")
                    delete sseTransports[sessionId];
                } catch (error) {
                    console.error(`Error cleaning up SSE transport for session ${sessionId}:`, error);
                }
            }
        });
        
        process.on('SIGTERM', () => shutdownHandler('SIGTERM'));
        process.on('SIGINT', () => shutdownHandler('SIGINT'));
        
    } else if (transport_type === 'http') {
        // Streamable HTTP transport for web-based communication
        console.error("Starting MCP server with HTTP transport...");
        
        const app = express();
        
        const transports: Map<string, StreamableHTTPServerTransport> = new Map<string, StreamableHTTPServerTransport>();
        const servers: Map<string, McpServer> = new Map<string, McpServer>();
        
        // Handle POST requests for MCP initialization and method calls
        app.post('/mcp', async (req: Request, res: Response) => {
            console.error('Received MCP POST request');
            try {
                // Check for existing session ID
                const sessionId = req.headers['mcp-session-id'] as string | undefined;
                let transport: StreamableHTTPServerTransport;

                if (sessionId && transports.has(sessionId)) {
                    // Reuse existing transport
                    transport = transports.get(sessionId)!;
                } else if (!sessionId) {
                    // New initialization request - create a new server instance for this session
                    const sessionServer = new McpServer({
                        name: serverName,
                        version: serverVersion,
                    }, {
                        capabilities: {
                            tools: {},
                        },
                    });

                    // Add tools to this server instance using shared handlers
                    sessionServer.tool(
                        "query_documentation",
                        "Query documentation stored in a sqlite-vec database using vector search.",
                        {
                            queryText: z.string().min(1).describe("The natural language query to search for."),
                            productName: z.string().min(1).optional().describe("The name of the product documentation database to search within (e.g., 'my-product'). Corresponds to the DB filename without .db."),
                            dbName: z.string().min(1).optional().describe("The database filename to query directly (e.g., 'my-product.db' or 'my-product')."),
                            version: z.string().optional().describe("The specific version of the product documentation (e.g., '1.2.0'). Optional."),
                            urlPathPrefix: z.string().min(1).optional().describe("Full URL prefix to filter documentation results (e.g., 'https://docs.example.com/guide/')."),
                            limit: z.number().int().positive().optional().default(4).describe("Maximum number of results to return. Defaults to 4."),
                        },
                        queryDocumentationToolHandler
                    );

                    sessionServer.tool(
                        "query_code",
                        "Query code stored in a sqlite-vec database using vector search.",
                        {
                            queryText: z.string().min(1).describe("The natural language query to search for."),
                            productName: z.string().min(1).optional().describe("Filter results by product name stored in the DB (e.g., 'istio')."),
                            repo: z.string().min(1).optional().describe("Filter results by repo name stored in the DB (e.g., 'owner/repo')."),
                            dbName: z.string().min(1).describe("The database filename to query directly (e.g., 'repo.db' or 'repo')."),
                            branch: z.string().min(1).optional().describe("Branch name to filter code results (e.g., 'main')."),
                            filePathPrefix: z.string().min(1).optional().describe("Full file path prefix to filter code results (e.g., 'https://github.com/org/repo/blob/main/src/')."),
                            extensions: z.array(z.string().min(1)).optional().describe("File extensions to include (e.g., ['.go', '.rs'])."),
                            limit: z.number().int().positive().optional().default(4).describe("Maximum number of results to return. Defaults to 4."),
                        },
                        queryCodeToolHandler
                    );
                    
                    sessionServer.tool(
                        "get_chunks",
                        "Retrieve specific chunks from a document by file path.",
                        {
                            productName: z.string().min(1).optional().describe("The name of the product documentation database to search within (e.g., 'my-product'). Corresponds to the DB filename without .db."),
                            dbName: z.string().min(1).optional().describe("The database filename to query directly (e.g., 'my-product.db' or 'my-product')."),
                            filePath: z.string().min(1).describe("The file path (url) of the document to retrieve chunks from."),
                            startIndex: z.number().int().nonnegative().optional().describe("Start index of the chunk range to retrieve (0-based). If not provided, returns all chunks from the beginning."),
                            endIndex: z.number().int().nonnegative().optional().describe("End index of the chunk range to retrieve (0-based, inclusive). If not provided, returns all chunks to the end."),
                            version: z.string().optional().describe("The specific version of the product documentation (e.g., '1.2.0'). Optional."),
                        },
                        getChunksToolHandler
                    );

                    transport = new StreamableHTTPServerTransport({
                        sessionIdGenerator: () => randomUUID(),
                        onsessioninitialized: (sessionId: string) => {
                            // Store the transport and server by session ID when session is initialized
                            console.error(`Session initialized with ID: ${sessionId}`);
                            transports.set(sessionId, transport);
                            servers.set(sessionId, sessionServer);
                        }
                    });

                    // Set up onclose handler to clean up transport and server when closed
                    transport.onclose = async () => {
                        const sid = transport.sessionId;
                        if (sid && transports.has(sid)) {
                            console.error(`Transport closed for session ${sid}, removing from transports and servers map`);
                            transports.delete(sid);
                            servers.delete(sid);
                        }
                    };

                    // Connect the transport to the session-specific MCP s
                    // erver BEFORE handling the request
                    await sessionServer.connect(transport);

                    await transport.handleRequest(req, res);
                    return; // Already handled
                } else {
                    // Invalid request - no session ID or not initialization request
                    res.status(400).json({
                        jsonrpc: '2.0',
                        error: {
                            code: -32000,
                            message: 'Bad Request: No valid session ID provided',
                        },
                        id: req?.body?.id,
                    });
                    return;
                }

                // Handle the request with existing transport
                await transport.handleRequest(req, res);
            } catch (error) {
                console.error('Error handling MCP request:', error);
                if (!res.headersSent) {
                    res.status(500).json({
                        jsonrpc: '2.0',
                        error: {
                            code: -32603,
                            message: 'Internal server error',
                        },
                        id: req?.body?.id,
                    });
                }
            }
        });

        // Handle GET requests for SSE streams
        app.get('/mcp', async (req: Request, res: Response) => {
            console.error('Received MCP GET request');
            const sessionId = req.headers['mcp-session-id'] as string | undefined;
            if (!sessionId || !transports.has(sessionId)) {
                res.status(400).json({
                    jsonrpc: '2.0',
                    error: {
                        code: -32000,
                        message: 'Bad Request: No valid session ID provided',
                    },
                    id: req?.body?.id,
                });
                return;
            }

            // Check for Last-Event-ID header for resumability
            const lastEventId = req.headers['last-event-id'] as string | undefined;
            if (lastEventId) {
                console.error(`Client reconnecting with Last-Event-ID: ${lastEventId}`);
            } else {
                console.error(`Establishing new SSE stream for session ${sessionId}`);
            }

            const transport = transports.get(sessionId);
            await transport!.handleRequest(req, res);
        });

        // Handle DELETE requests for session termination
        app.delete('/mcp', async (req: Request, res: Response) => {
            const sessionId = req.headers['mcp-session-id'] as string | undefined;
            if (!sessionId || !transports.has(sessionId)) {
                res.status(400).json({
                    jsonrpc: '2.0',
                    error: {
                        code: -32000,
                        message: 'Bad Request: No valid session ID provided',
                    },
                    id: req?.body?.id,
                });
                return;
            }

            console.error(`Received session termination request for session ${sessionId}`);

            try {
                const transport = transports.get(sessionId);
                await transport!.handleRequest(req, res);
            } catch (error) {
                console.error('Error handling session termination:', error);
                if (!res.headersSent) {
                    res.status(500).json({
                        jsonrpc: '2.0',
                        error: {
                            code: -32603,
                            message: 'Error handling session termination',
                        },
                        id: req?.body?.id,
                    });
                }
            }
        });

        app.get("/health", (_: Request, res: Response) => {
            res.status(200).send("OK");
        });
        
        const PORT = process.env.PORT || 3001;
        webserver = app.listen(PORT, () => {
            console.error(`MCP server is running on port ${PORT} with HTTP transport`);
            console.error(`Connect to: http://localhost:${PORT}/mcp`);
        });
        
        webserver.keepAliveTimeout = 3000;
        
        // Keep the process alive
        webserver.on('error', (error: any) => {
            console.error('HTTP server error:', error);
        });
        
        // Handle server shutdown with proper SIGTERM/SIGINT support and timeout
        const shutdownHandler = createGracefulShutdownHandler(async () => {
            console.error('Closing HTTP transports and servers...');

            // Close all active transports and servers with individual timeouts
            const transportClosePromises = Array.from(transports.entries()).map(async ([sessionId, transport]) => {
                try {
                    console.error(`Closing transport and server for session ${sessionId}`);
                    
                    // Add timeout to individual transport close operations
                    const closeTimeout = new Promise<void>((_, reject) => {
                        setTimeout(() => reject(new Error(`Transport close timeout for ${sessionId}`)), 2000);
                    });
                    
                    await Promise.race([
                        transport.close(),
                        closeTimeout
                    ]);
                    
                    transports.delete(sessionId);
                    servers.delete(sessionId);
                    console.error(`Transport and server closed for session ${sessionId}`);
                } catch (error) {
                    console.error(`Error closing transport for session ${sessionId}:`, error);
                    // Still remove from maps even if close failed
                    transports.delete(sessionId);
                    servers.delete(sessionId);
                }
            });

            // Wait for all transports to close, but with overall timeout handled by outer function
            await Promise.allSettled(transportClosePromises);
            console.error('All transports and servers cleanup completed');
        });
        
        process.on('SIGTERM', () => shutdownHandler('SIGTERM'));
        process.on('SIGINT', () => shutdownHandler('SIGINT'));
        
    } else {
        console.error(`Unknown transport type: ${transport_type}. Use 'stdio', 'sse', or 'http'.`);
        process.exit(1);
    }
}

// Run main when this module is executed directly
main().catch((error) => {
    console.error("Failed to start MCP server:", error);
    process.exit(1);
});
