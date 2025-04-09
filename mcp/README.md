# SQLite Vector Documentation Query MCP Server

This is a Model Context Protocol (MCP) server that enables querying documentation stored in SQLite databases with vector embeddings. The server uses OpenAI's embedding API to convert natural language queries into vector embeddings and performs semantic search against documentation stored in SQLite databases.

## Features

- Vector-based semantic search for documentation
- Filters by product name and version
- Uses OpenAI's embedding API for query embedding generation
- Fully compatible with the Model Context Protocol
- Simple Express-based API with Server-Sent Events (SSE) for real-time communication

## Prerequisites

- Node.js 20 or higher
- OpenAI API key
- Documentation stored in SQLite vector databases (using `sqlite-vec`)

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OPENAI_API_KEY` | Your OpenAI API key (required) | - |
| `SQLITE_DB_DIR` | Directory containing SQLite databases | Current directory |
| `PORT` | Port to run the server on | 3001 |

## Local Setup and Running

1. Install dependencies:
   ```bash
   npm install
   ```

2. Create a `.env` file with required environment variables:
   ```
   OPENAI_API_KEY=your_openai_api_key
   SQLITE_DB_DIR=/path/to/databases
   PORT=3001
   ```

3. Build the TypeScript code:
   ```bash
   npm run build
   ```

4. Start the server:
   ```bash
   npm start
   ```

## Docker Setup

### Building the Docker Image

```bash
docker build -t sqlite-vec-mcp-server:latest .
```

This is going to include any `*.db` files in the `/data` directory of the image.

### Running with Docker

```bash
docker run -p 3001:3001 \
  -e OPENAI_API_KEY=your_openai_api_key \
  sqlite-vec-mcp-server:latest
```

### Create a Secret for the OpenAI API Key

```bash
kubectl create secret generic mcp-secrets \
  --from-literal=OPENAI_API_KEY=your_openai_api_key
```

### Create a ConfigMap for Database Configuration

```bash
kubectl create configmap mcp-config \
  --from-literal=SQLITE_DB_DIR=/data \
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

The server implements a tool called `query-documentation` that can be used to query documentation.

### Tool Parameters

- `queryText` (string, required): The natural language query to search for
- `productName` (string, required): The name of the product documentation database to search within
- `version` (string, optional): The specific version of the product documentation
- `limit` (number, optional, default: 4): Maximum number of results to return