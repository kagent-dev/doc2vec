# SQLite Vector Documentation Query MCP Server

This is a Model Context Protocol (MCP) server that enables querying documentation stored in vector databases with embeddings. The server uses OpenAI's embedding API to convert natural language queries into vector embeddings and performs semantic search against documentation stored in SQLite (sqlite-vec) or Qdrant.

## Features

- Vector-based semantic search for documentation
- Filters by product name and version
- Uses OpenAI's embedding API for query embedding generation
- Fully compatible with the Model Context Protocol
- Support for multiple transport types: SSE (default), stdio, and streamable HTTP
- Session management for HTTP and SSE transports
- Backward compatibility with previous SSE implementations
- Supports SQLite (sqlite-vec) and Qdrant backends

## Prerequisites

- Node.js 20 or higher
- OpenAI API key
- Documentation stored in a vector database: SQLite (sqlite-vec) or Qdrant

## Backward Compatibility

The MCP server is **backward compatible** with databases created using older versions of doc2vec that don't include the `chunk_index` and `total_chunks` columns.

### Database Format Support

**New Format (with `chunk_index` and `total_chunks`):**
- Full functionality including range filtering with `startIndex` and `endIndex`
- Chunk ordering and pagination support
- Metadata available for page reconstruction

**Old Format (without `chunk_index` and `total_chunks`):**
- `query_documentation` tool works fully (uses `SELECT *` and handles optional fields)
- `get_chunks` tool works but returns all chunks for a document
- Range filtering (`startIndex`/`endIndex`) is gracefully ignored with a warning if requested
- No errors or failures - the server automatically detects missing columns and adapts

The server automatically detects the database schema and adapts its queries accordingly. No migration or database updates are required to use older databases.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OPENAI_API_KEY` | Your OpenAI API key (required) | - |
| `VECTOR_DB_TYPE` | Vector backend: `sqlite` or `qdrant` | `sqlite` |
| `SQLITE_DB_DIR` | Directory containing SQLite databases | Current directory |
| `QDRANT_URL` | Qdrant URL (when `VECTOR_DB_TYPE=qdrant`) | `http://localhost:6333` |
| `QDRANT_API_KEY` | Qdrant API key (optional) | - |
| `TRANSPORT_TYPE` | Transport type: 'sse', 'stdio', or 'http' | sse |
| `PORT` | Port to run the server on (HTTP/SSE transports only) | 3001 |

## Local Setup and Running

1. Install dependencies:
   ```bash
   npm install
   ```

2. Create a `.env` file with required environment variables:
   ```
   OPENAI_API_KEY=your_openai_api_key
   SQLITE_DB_DIR=/path/to/databases
   TRANSPORT_TYPE=sse
   ```

3. Build the TypeScript code:
   ```bash
   npm run build
   ```

4. Start the server:

   **For SSE transport (default, but deprecated):**
   ```bash
   npm start
   # or explicitly:
   TRANSPORT_TYPE=sse npm start
   ```

   **For stdio transport (for CLI/direct MCP usage):**
   ```bash
   TRANSPORT_TYPE=stdio npm start
   ```

   **For HTTP transport (recommended):**
   ```bash
   TRANSPORT_TYPE=http PORT=3001 npm start
   ```

## Transport Types

### SSE Transport (Default, but deprecated)

Server-Sent Events transport provides real-time streaming and is backward compatible with previous versions. This is the default transport type, but it's deprecated.

Usage:
- Set `TRANSPORT_TYPE=sse` or omit (default)
- Set `PORT` for the HTTP server (default: 3001)
- Connect to `GET http://localhost:3001/sse`
- Send messages to `POST http://localhost:3001/messages?sessionId=<session_id>`

**Endpoints:**
- Connection: `GET http://localhost:3001/sse`
- Messages: `POST http://localhost:3001/messages?sessionId=<session_id>`

### Stdio Transport

The stdio transport is the standard MCP transport for direct communication with MCP clients like Claude Desktop, IDEs, or other MCP-compatible applications.

Usage:
- Set `TRANSPORT_TYPE=stdio`
- The server will communicate via stdin/stdout
- No HTTP server is started

### Streamable HTTP Transport (recommended)

The streamable HTTP transport allows web-based clients to connect to the MCP server via HTTP with advanced session management and supports multiple concurrent connections.

Usage:
- Set `TRANSPORT_TYPE=http`
- Set `PORT` for the HTTP server (default: 3001)
- Connect to `http://localhost:3001/mcp`
- Sessions are managed automatically with UUID generation

**Endpoints:**
- Connection: `POST/GET/DELETE http://localhost:3001/mcp`

## Docker Setup

### Building the Docker Image

```bash
docker build -t sqlite-vec-mcp-server:latest .
```

This is going to include any `*.db` files in the `/data` directory of the image.

### Running with Docker

**For SSE transport (default):**
```bash
docker run -p 3001:3001 \
  -e OPENAI_API_KEY=your_openai_api_key \
  -e TRANSPORT_TYPE=sse \
  -e PORT=3001 \
  sqlite-vec-mcp-server:latest
```

**For stdio transport:**
```bash
docker run -i \
  -e OPENAI_API_KEY=your_openai_api_key \
  -e TRANSPORT_TYPE=stdio \
  sqlite-vec-mcp-server:latest
```

**For HTTP transport:**
```bash
docker run -p 3001:3001 \
  -e OPENAI_API_KEY=your_openai_api_key \
  -e TRANSPORT_TYPE=http \
  -e PORT=3001 \
  sqlite-vec-mcp-server:latest
```

### Kubernetes Deployment

### Create a Secret for the OpenAI API Key

```bash
kubectl create secret generic mcp-secrets \
  --from-literal=OPENAI_API_KEY=your_openai_api_key
```

### Create a ConfigMap for Database Configuration

```bash
kubectl create configmap mcp-config \
  --from-literal=SQLITE_DB_DIR=/data \
  --from-literal=TRANSPORT_TYPE=sse \
  --from-literal=PORT=3001
```

### Create a Deployment

Create a file named `deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mcp-sqlite-vec
  labels:
    app: mcp-sqlite-vec
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mcp-sqlite-vec
  template:
    metadata:
      labels:
        app: mcp-sqlite-vec
    spec:
      containers:
      - name: mcp-sqlite-vec
        image: sqlite-vec-mcp-server:latest
        imagePullPolicy: Always
        ports:
        - containerPort: 3001
        env:
        - name: OPENAI_API_KEY
          valueFrom:
            secretKeyRef:
              name: mcp-secrets
              key: OPENAI_API_KEY
        - name: SQLITE_DB_DIR
          valueFrom:
            configMapKeyRef:
              name: mcp-config
              key: SQLITE_DB_DIR
        - name: TRANSPORT_TYPE
          valueFrom:
            configMapKeyRef:
              name: mcp-config
              key: TRANSPORT_TYPE
        - name: PORT
          valueFrom:
            configMapKeyRef:
              name: mcp-config
              key: PORT
```

Apply it:
```bash
kubectl apply -f deployment.yaml
```

### Create a Service

Create a file named `service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: mcp-sqlite-vec
spec:
  selector:
    app: mcp-sqlite-vec
  ports:
  - port: 3001
    targetPort: 3001
  type: ClusterIP
```

Apply it:
```bash
kubectl apply -f service.yaml
```

## Using the MCP Server

The server implements three tools:
- `query_documentation` to search documentation
- `query_code` to search code repositories
- `get_chunks` to retrieve specific chunks by file path and chunk index

### query_documentation

**Parameters**
- `queryText` (string, required): The natural language query to search for
- `productName` (string, optional): The name of the product documentation database to search within
- `dbName` (string, optional): Database filename to query directly (e.g., `my-product.db` or `my-product`)
- `version` (string, optional): The specific version of the product documentation
- `urlPathPrefix` (string, optional): Full URL prefix to filter results (e.g., `https://docs.example.com/guide/`)
- `limit` (number, optional, default: 4): Maximum number of results to return

**Notes**
- Provide either `productName` or `dbName`.
- If `dbName` is provided, `productName` will be used to filter results within that database.
- Results include `chunk_index` and `total_chunks` when available, so clients can request neighboring chunks.
- When `VECTOR_DB_TYPE=qdrant`, `dbName` maps to the Qdrant collection name. If `dbName` is omitted, the server derives a collection name from `productName` and `version` (e.g. `my_product_1.2.0`).

### query_code

**Parameters**
- `queryText` (string, required): The natural language query to search for
- `productName` (string, optional): Product name to search within (e.g., `istio`)
- `repo` (string, optional): The repo name to search within (e.g., `owner/repo`)
- `dbName` (string, required): Database filename to query directly (e.g., `repo.db` or `repo`)
- `branch` (string, optional): Branch name to filter code results
- `filePathPrefix` (string, optional): Full file path prefix to filter results (e.g., `https://github.com/org/repo/blob/main/src/`)
- `extensions` (string[], optional): File extensions to include (e.g., `['.go', '.rs']`)
- `limit` (number, optional, default: 4): Maximum number of results to return

**Notes**
- `dbName` is required. `productName` and `repo` are optional filters within the database.
- `filePathPrefix` and `extensions` are applied as post-filters after the vector query.
- When `VECTOR_DB_TYPE=qdrant`, `dbName` maps to the Qdrant collection name.

### get_chunks

**Parameters**
- `productName` (string, optional): The name of the product documentation database to search within
- `dbName` (string, optional): Database filename to query directly (e.g., `my-product.db` or `my-product`)
- `filePath` (string, required): The document path (stored as `url` in the DB)
- `startIndex` (number, optional): Start index of the chunk range to retrieve (0-based). If not provided, returns all chunks from the beginning
- `endIndex` (number, optional): End index of the chunk range to retrieve (0-based, inclusive). If not provided, returns all chunks to the end
- `version` (string, optional): The specific version of the product documentation

**Notes**
- Provide either `productName` or `dbName`.
- If `dbName` is provided, `productName` will be used to filter results within that database.
- Range filtering (`startIndex`/`endIndex`) requires the `chunk_index` column in the database. For older databases without this column, the tool will return all chunks and log a warning if range parameters are provided.
- Results include `chunk_index` and `total_chunks` metadata when available (new format databases only).
- When `VECTOR_DB_TYPE=qdrant`, `dbName` maps to the Qdrant collection name. If `dbName` is omitted, the server derives a collection name from `productName` and `version`.

## Integration Examples

### Claude Desktop Configuration

For stdio transport, add to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "sqlite-vec-docs": {
      "command": "node",
      "args": ["/path/to/your/mcp/build/index.js"],
      "env": {
        "OPENAI_API_KEY": "your_openai_api_key",
        "SQLITE_DB_DIR": "/path/to/databases",
        "TRANSPORT_TYPE": "stdio"
      }
    }
  }
}
```

### Web Client (SSE Transport)

For SSE transport (default), clients can connect to `http://localhost:3001/sse` for the initial connection and send messages to `http://localhost:3001/messages?sessionId=<session_id>`.

### Web Client (HTTP Transport)

For HTTP transport, clients can connect to `http://localhost:3001/mcp` and send MCP protocol messages via HTTP requests.
