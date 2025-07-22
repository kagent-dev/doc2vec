#!/usr/bin/env node
// src/index.ts
import 'dotenv/config'; // Load .env file
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
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

function queryCollection(queryEmbedding: number[], filter: { product_name: string; version?: string }, topK: number = 10): QueryResult[] {
    const dbPath = path.join(dbDir, `${filter.product_name}.db`);

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

async function queryDocumentation(queryText: string, productName: string, version?: string, limit: number = 4): Promise<{ distance: number, content: string, url?: string }[]> {
    const queryEmbedding = await createEmbeddings(queryText);
    const results = queryCollection(queryEmbedding, { product_name: productName, version: version }, limit);
    return results.map((qr: QueryResult) => ({
        distance: qr.distance,
        content: qr.content,
        ...(qr.url && { url: qr.url }),
    }));
}

// --- MCP Server Setup ---
const serverName = "sqlite-vec-doc-query"; // Store name for logging
const serverVersion = "1.0.0"; // Store version for logging

const server = new McpServer({
    name: serverName,
    version: serverVersion,
    capabilities: {},
});

// --- Define the MCP Tool ---
server.tool(
    "query_documentation",
    "Query documentation stored in a sqlite-vec database using vector search.",
    {
        queryText: z.string().min(1).describe("The natural language query to search for."),
        productName: z.string().min(1).describe("The name of the product documentation database to search within (e.g., 'my-product'). Corresponds to the DB filename without .db."),
        version: z.string().optional().describe("The specific version of the product documentation (e.g., '1.2.0'). Optional."),
        limit: z.number().int().positive().optional().default(4).describe("Maximum number of results to return. Defaults to 4."),
    },
    async ({ queryText, productName, version, limit }: { queryText: string; productName: string; version?: string; limit: number }) => {
        console.error(`Received query: text="${queryText}", product="${productName}", version="${version || 'any'}", limit=${limit}`);

        try {
            const results = await queryDocumentation(queryText, productName, version, limit);

            if (results.length === 0) {
                return {
                    content: [{ type: "text", text: `No relevant documentation found for "${queryText}" in product "${productName}" ${version ? `(version ${version})` : ''}.` }],
                };
            }

            const formattedResults = results.map((r, index) =>
                [
                    `Result ${index + 1}:`,
                    `  Content: ${r.content}`,
                    `  Distance: ${r.distance.toFixed(4)}`,
                    r.url ? `  URL: ${r.url}` : null,
                    "---"
                ].filter(line => line !== null).join("\n")
            ).join("\n");

            const responseText = `Found ${results.length} relevant documentation snippets for "${queryText}" in product "${productName}" ${version ? `(version ${version})` : ''}:\n\n${formattedResults}`;
            console.error(`Handler finished processing. Payload size (approx): ${responseText.length} chars. Returning response object...`);

            return {
                content: [{ type: "text", text: responseText }],
            };
        } catch (error: any) {
            console.error("Error processing 'query_documentation' tool:", error);
            return {
                content: [{ type: "text", text: `Error querying documentation: ${error.message}` }],
            };
        }
    }
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
                    // New initialization request
                    transport = new StreamableHTTPServerTransport({
                        sessionIdGenerator: () => randomUUID(),
                        onsessioninitialized: (sessionId: string) => {
                            // Store the transport by session ID when session is initialized
                            console.error(`Session initialized with ID: ${sessionId}`);
                            transports.set(sessionId, transport);
                        }
                    });

                    // Set up onclose handler to clean up transport when closed
                    transport.onclose = async () => {
                        const sid = transport.sessionId;
                        if (sid && transports.has(sid)) {
                            console.error(`Transport closed for session ${sid}, removing from transports map`);
                            transports.delete(sid);
                        }
                    };

                    // Connect the transport to the MCP server BEFORE handling the request
                    await server.connect(transport);

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
            console.error('Closing HTTP transports...');

            // Close all active transports with individual timeouts
            const transportClosePromises = Array.from(transports.entries()).map(async ([sessionId, transport]) => {
                try {
                    console.error(`Closing transport for session ${sessionId}`);
                    
                    // Add timeout to individual transport close operations
                    const closeTimeout = new Promise<void>((_, reject) => {
                        setTimeout(() => reject(new Error(`Transport close timeout for ${sessionId}`)), 2000);
                    });
                    
                    await Promise.race([
                        transport.close(),
                        closeTimeout
                    ]);
                    
                    transports.delete(sessionId);
                    console.error(`Transport closed for session ${sessionId}`);
                } catch (error) {
                    console.error(`Error closing transport for session ${sessionId}:`, error);
                    // Still remove from map even if close failed
                    transports.delete(sessionId);
                }
            });

            // Wait for all transports to close, but with overall timeout handled by outer function
            await Promise.allSettled(transportClosePromises);
            console.error('All transports cleanup completed');
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
