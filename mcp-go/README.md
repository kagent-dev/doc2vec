# MCP Documentation Query Server (Go)

A Go implementation of a Model Context Protocol (MCP) server that provides documentation querying capabilities using vector search with SQLite databases.

This is a Go port of the original TypeScript implementation, providing the same functionality with improved performance and easier deployment.

## Features

- **Vector Search**: Query documentation using semantic search with embeddings
- **Multiple Embedding Providers**: Support for OpenAI, Azure OpenAI, and Google Gemini
- **SQLite Vector Storage**: Uses SQLite databases with vector operations for efficient similarity search
- **Multiple Transports**: Support for stdio, SSE, and HTTP transports
- **Production Ready**: Built with Go for performance, reliability, and easy deployment
- **Docker Support**: Containerized deployment with health checks
- **Configurable**: Environment-based configuration with validation

## Quick Start

### Prerequisites

- Go 1.23 or later
- SQLite databases with vector embeddings (prepared separately)
- API key for your chosen embedding provider

### Installation

```bash
# Clone and build
git clone <repository>
cd mcp-go
go mod tidy
go build -o mcp-doc-query

# Or use Docker
docker build -t mcp-doc-query .
```

### Basic Usage

```bash
# Set up environment
export OPENAI_API_KEY="your-openai-api-key"
export SQLITE_DB_DIR="/path/to/your/databases"

# Start the server
./mcp-doc-query
```

The server will start with HTTP transport on port 3001 by default.

## Configuration

The server is configured entirely through environment variables:

### Provider Configuration

```bash
# Choose embedding provider (openai, azure, gemini)
EMBEDDING_PROVIDER=openai
```

### OpenAI Configuration

```bash
OPENAI_API_KEY=your-openai-api-key
OPENAI_MODEL=text-embedding-3-large  # optional, default shown
```

### Azure OpenAI Configuration

```bash
EMBEDDING_PROVIDER=azure
AZURE_OPENAI_KEY=your-azure-key
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_VERSION=2024-10-21  # optional, default shown
AZURE_OPENAI_DEPLOYMENT_NAME=text-embedding-3-large  # optional, default shown
```

### Google Gemini Configuration

```bash
EMBEDDING_PROVIDER=gemini
GEMINI_API_KEY=your-gemini-api-key
GEMINI_MODEL=gemini-embedding-001  # optional, default shown
```

### Database Configuration

```bash
SQLITE_DB_DIR=/path/to/databases  # directory containing .db files
```

### Server Configuration

```bash
TRANSPORT_TYPE=http     # http, stdio, or sse
PORT=3001              # HTTP server port (http/sse transports only)
STRICT_MODE=false      # enable strict validation
```

## Transport Types

### HTTP Transport (Default)

Best for web-based integrations and development:

```bash
TRANSPORT_TYPE=http PORT=3001 ./mcp-doc-query
```

Endpoints:
- `POST /mcp` - MCP protocol endpoint
- `GET /health` - Health check endpoint

### Stdio Transport

Best for direct integration with MCP clients:

```bash
TRANSPORT_TYPE=stdio ./mcp-doc-query
```

### SSE Transport

Server-Sent Events transport (simplified implementation):

```bash
TRANSPORT_TYPE=sse PORT=3001 ./mcp-doc-query
```

## Database Format

The server expects SQLite databases with the following schema:

```sql
CREATE TABLE vec_items (
    chunk_id TEXT,
    content TEXT,
    url TEXT,
    product_name TEXT,
    version TEXT,
    embedding BLOB,  -- serialized float64 vector
    distance REAL    -- computed during query
);
```

Database files should be named `{product_name}.db` and placed in the `SQLITE_DB_DIR` directory.

## MCP Tool: query_documentation

The server provides one MCP tool:

### Parameters

- `queryText` (required): Natural language query to search for
- `productName` (required): Name of the product database (without .db extension)
- `version` (optional): Specific version of the documentation
- `limit` (optional): Maximum number of results (default: 4)

### Example Usage

```json
{
  "method": "tools/call",
  "params": {
    "name": "query_documentation",
    "arguments": {
      "queryText": "How to configure authentication?",
      "productName": "my-product",
      "version": "1.2.0",
      "limit": 5
    }
  }
}
```

### Response Format

```
Found 2 relevant documentation snippets for "How to configure authentication?" in product "my-product" (version 1.2.0):

Result 1:
  Content: Authentication can be configured by setting the auth.enabled property...
  Distance: 0.1234
  URL: https://docs.example.com/auth
---

Result 2:
  Content: For OAuth configuration, use the oauth.clientId setting...
  Distance: 0.2456
  URL: https://docs.example.com/oauth
---
```

## Docker Deployment

### Build and Run

```bash
# Build the image
docker build -t mcp-doc-query .

# Run with environment variables
docker run -d \
  -e OPENAI_API_KEY=your-key \
  -e SQLITE_DB_DIR=/data \
  -v /path/to/your/databases:/data \
  -p 3001:3001 \
  mcp-doc-query
```

### Docker Compose

```yaml
version: '3.8'
services:
  mcp-doc-query:
    build: .
    environment:
      - OPENAI_API_KEY=your-key
      - TRANSPORT_TYPE=http
      - PORT=3001
    volumes:
      - ./databases:/data
    ports:
      - "3001:3001"
    healthcheck:
      test: ["CMD", "wget", "--spider", "http://localhost:3001/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

## Development

### Project Structure

```
mcp-go/
├── main.go           # Application entry point
├── config.go         # Configuration and environment handling
├── embeddings.go     # Embedding providers (OpenAI, Azure, Gemini)
├── database.go       # SQLite vector operations
├── server.go         # MCP server and tool implementations
├── transport.go      # Transport layer (stdio, SSE, HTTP)
├── go.mod           # Go module definition
├── Dockerfile       # Container build instructions
└── README.md        # This file
```

### Building

```bash
# Install dependencies
go mod tidy

# Build for current platform
go build -o mcp-doc-query

# Build for Linux (with CGO for sqlite3)
CGO_ENABLED=1 GOOS=linux go build -o mcp-doc-query

# Run tests (if you add them)
go test ./...
```

### Environment Setup

Create a `.env` file for development:

```bash
OPENAI_API_KEY=your-key
SQLITE_DB_DIR=./test-databases
TRANSPORT_TYPE=http
PORT=3001
STRICT_MODE=false
```

## Monitoring and Health Checks

### Health Check Endpoint

```bash
curl http://localhost:3001/health
```

Response:
```json
{
  "status": "healthy",
  "server": "sqlite-vec-doc-query",
  "version": "1.0.0"
}
```

### Configuration Check

```bash
./mcp-doc-query -config
```

This will show your current configuration and validate settings.

## Performance Considerations

- **Concurrent Queries**: The Go implementation handles concurrent embedding API calls efficiently
- **Memory Usage**: Embeddings are processed in memory; consider memory limits for large queries
- **Database Connections**: SQLite connections are opened per query and closed after use
- **Vector Operations**: Uses efficient binary serialization for embeddings

## Comparison with TypeScript Version

| Feature | TypeScript | Go |
|---------|------------|-----|
| Performance | Node.js runtime | Native compiled binary |
| Memory Usage | Higher (V8 overhead) | Lower (no runtime overhead) |
| Deployment | Requires Node.js | Single static binary |
| Container Size | ~200MB+ | ~20MB |
| Startup Time | ~1-2 seconds | ~100ms |
| Dependencies | npm packages | Compiled dependencies |

## Troubleshooting

### Common Issues

1. **Database not found**
   - Check `SQLITE_DB_DIR` path
   - Ensure database files exist with correct naming (`{product}.db`)

2. **Embedding API errors**
   - Verify API keys are correct
   - Check network connectivity
   - Ensure provider-specific configuration is complete

3. **Vector search failures**
   - Verify database schema matches expected format
   - Check that embeddings are properly serialized

### Debug Mode

Enable detailed logging:

```bash
STRICT_MODE=true ./mcp-doc-query -config
```

### Logs

The application logs important events:
- Server startup and configuration
- Database connections and queries
- Embedding API calls and responses
- Transport-specific events

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here] 