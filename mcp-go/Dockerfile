# Build stage
FROM --platform=$BUILDPLATFORM cgr.dev/chainguard/wolfi-base:latest AS builder

# Import build arguments for cross-compilation
ARG BUILDPLATFORM
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

# Install build dependencies
RUN apk update && apk add --no-cache \
    go \
    gcc \
    musl-dev \
    sqlite-dev \
    ca-certificates \
    && update-ca-certificates

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY *.go ./

# Build the application with cross-compilation support
RUN echo "Building for platform: $TARGETPLATFORM" && \
    GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} CGO_ENABLED=1 \
    go build -o mcp-doc-query .

# Runtime stage
FROM cgr.dev/chainguard/wolfi-base:latest

# Install runtime dependencies
RUN apk update && apk add --no-cache \
    ca-certificates \
    sqlite \
    && update-ca-certificates

# Create non-root user
RUN addgroup -g 1001 -S nodejs && \
    adduser -S kagent -u 1001 -G nodejs

# Create app directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/mcp-doc-query .

# Create directory for databases
RUN mkdir -p /app/build

# Download database files
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/kubernetes.db /app/build/kubernetes.db
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/istio.db /app/build/istio.db
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/argo.db /app/build/argo.db
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/argo-rollouts.db /app/build/argo-rollouts.db
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/helm.db /app/build/helm.db
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/prometheus.db /app/build/prometheus.db
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/gateway-api.db /app/build/gateway-api.db
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/gloo-gateway.db /app/build/gloo-gateway.db
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/kgateway.db /app/build/kgateway.db
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/gloo-edge.db /app/build/gloo-edge.db
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/otel.db /app/build/otel.db
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/cilium.db /app/build/cilium.db
ADD https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/ambient.db /app/build/ambient.db

# Show checksum for every db file
RUN for file in /app/build/*.db; do echo "Checksum for $file:"; sha256sum "$file"; done

# Set default environment variables
ENV SQLITE_DB_DIR=/app/build
ENV TRANSPORT_TYPE=http
ENV PORT=3001

# Ensure the app directory is owned by the non-root user
RUN chown -R kagent:nodejs /app

# Add labels
LABEL org.opencontainers.image.source=https://github.com/kagent-dev/doc2vec
LABEL org.opencontainers.image.description="Kagent Doc2Vec MCP (Go)"
LABEL org.opencontainers.image.authors="Kagent Creators 🤖"

# Switch to non-root user
USER kagent

# Expose port
EXPOSE 3001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:${PORT}/health || exit 1

# Run the application
ENTRYPOINT ["./mcp-doc-query"] 