// src/index.ts
import 'dotenv/config'; // Load .env file
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import express, { Request, Response } from "express";
import { z } from "zod";

import * as sqliteVec from "sqlite-vec";
import Database, { Database as DatabaseType } from "better-sqlite3";
import { OpenAI } from 'openai';
import path from 'path';
import fs from 'fs'; // Import fs for checking file existence

// --- Configuration & Environment Check ---

const openAIApiKey = process.env.OPENAI_API_KEY;
const dbDir = process.env.SQLITE_DB_DIR || __dirname; // Default to current dir if not set

if (!openAIApiKey) {
    console.error("Error: OPENAI_API_KEY environment variable is not set.");
    process.exit(1);
}
if (!fs.existsSync(dbDir)) {
    console.warn(`Warning: SQLITE_DB_DIR (${dbDir}) does not exist. Databases may not be found.`);
    process.exit(1);
}

const openai = new OpenAI({
    apiKey: openAIApiKey,
});

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
        const response = await openai.embeddings.create({
            model: 'text-embedding-3-large', // Or your preferred model
            input: text,
        });
        if (!response.data?.[0]?.embedding) {
            throw new Error("Failed to get embedding from OpenAI response.");
        }
        return response.data[0].embedding;
    } catch (error) {
        console.error("Error creating OpenAI embeddings:", error);
        throw new Error(`Failed to create embeddings: ${error instanceof Error ? error.message : String(error)}`);
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
        sqliteVec.load(db);
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
        const rows = stmt.all({
          query_embedding: new Float32Array(queryEmbedding),
          product_name: filter.product_name,
          version: filter.version,
          top_k: topK,
        });
      
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
    "query-documentation",
    "Query documentation stored in a sqlite-vec database using vector search.",
    {
        queryText: z.string().min(1).describe("The natural language query to search for."),
        productName: z.string().min(1).describe("The name of the product documentation database to search within (e.g., 'my-product'). Corresponds to the DB filename without .db."),
        version: z.string().optional().describe("The specific version of the product documentation (e.g., '1.2.0'). Optional."),
        limit: z.number().int().positive().optional().default(4).describe("Maximum number of results to return. Defaults to 4."),
    },
    async ({ queryText, productName, version, limit }) => {
        console.log(`Received query: text="${queryText}", product="${productName}", version="${version || 'any'}", limit=${limit}`);

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

            return {
                content: [{ type: "text", text: responseText }],
            };
        } catch (error: any) {
            console.error("Error processing 'query-documentation' tool:", error);
            return {
                content: [{ type: "text", text: `Error querying documentation: ${error.message}` }],
            };
        }
    }
);

const app = express();

// to support multiple simultaneous connections we have a lookup object from
// sessionId to transport
const transports: {[sessionId: string]: SSEServerTransport} = {};

app.get("/sse", async (_: Request, res: Response) => {
  const transport = new SSEServerTransport('/messages', res);
  transports[transport.sessionId] = transport;
  res.on("close", () => {
    delete transports[transport.sessionId];
  });
  await server.connect(transport);
});

app.post("/messages", async (req: Request, res: Response) => {
  const sessionId = req.query.sessionId as string;
  const transport = transports[sessionId];
  if (transport) {
    await transport.handlePostMessage(req, res);
  } else {
    res.status(400).send('No transport found for sessionId');
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});