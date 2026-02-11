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
import Database from "better-sqlite3";
import { OpenAI } from 'openai';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { QdrantClient } from '@qdrant/js-client-rest';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs'; // Import fs for checking file existence
import { createQueryHandlers, createSqliteDbProvider, createQdrantProvider } from './server.js';

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
const vectorDbType = (process.env.VECTOR_DB_TYPE || 'sqlite').toLowerCase();

const qdrantUrl = process.env.QDRANT_URL || 'http://localhost:6333';
const qdrantApiKey = process.env.QDRANT_API_KEY;

const normalizeQdrantConfig = (rawUrl: string): { url: string; port?: number } => {
    try {
        const parsed = new URL(rawUrl);
        const portFromUrl = parsed.port ? Number(parsed.port) : undefined;
        const defaultPort = parsed.protocol === 'https:' ? 443 : 6333;
        return {
            url: `${parsed.protocol}//${parsed.hostname}`,
            port: portFromUrl ?? defaultPort,
        };
    } catch {
        return { url: rawUrl };
    }
};

if (vectorDbType === 'sqlite' && !fs.existsSync(dbDir)) {
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

    if (vectorDbType !== 'sqlite' && vectorDbType !== 'qdrant') {
        console.error(`Error: Unknown VECTOR_DB_TYPE '${vectorDbType}'. Supported: sqlite, qdrant`);
        process.exit(1);
    }
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

const sqliteProvider = createSqliteDbProvider({
    dbDir,
    sqliteVec,
    Database,
    fs,
    path,
});

const qdrantConfig = normalizeQdrantConfig(qdrantUrl);
const qdrantProvider = createQdrantProvider({
    client: new QdrantClient({
        url: qdrantConfig.url,
        port: qdrantConfig.port,
        apiKey: qdrantApiKey,
    }),
});

const activeProvider = vectorDbType === 'qdrant' ? qdrantProvider : sqliteProvider;

const { queryDocumentationToolHandler, queryCodeToolHandler, getChunksToolHandler } = createQueryHandlers({
    createEmbeddings,
    resolveDbPath: activeProvider.resolveDbPath,
    queryCollection: activeProvider.queryCollection,
    getChunksForDocument: activeProvider.getChunksForDocument,
});

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
