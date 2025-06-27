# Build stage
FROM --platform=$BUILDPLATFORM cgr.dev/chainguard/wolfi-base:latest AS builder

# Import build arguments for cross-compilation
ARG BUILDPLATFORM
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV NODE_ENV=production
ENV NODE_PATH=/app/node_modules

# Install only runtime dependencies
RUN apk update && apk add --no-cache \
    nodejs \
    ca-certificates \
    && update-ca-certificates \

WORKDIR /app

# Copy package files first for better layer caching
COPY package.json package-lock.json tsconfig.json ./
COPY src/ ./src/

# Install dependencies and build
RUN echo "Building for platform: $TARGETPLATFORM" && \
    apk update && apk add --no-cache  \
        bash                          \
        build-base                    \
        ca-certificates               \
        curl                          \
        make                          \
        node-gyp                      \
        nodejs                        \
        npm                           \
        openssl                       \
        python3                       \
        sqlite-dev                    \
        unzip                      && \
    echo "Installing dependencies including dev dependencies" && \
    npm ci --include=dev           && \
    echo "Building application"    && \
    npm run build                  && \
    echo "Cleaning up build... "   && \
    npm prune --production         && \
    apk del                           \
        bash                          \
        build-base                    \
        curl                          \
        make                          \
        node-gyp                      \
        openssl                       \
        python3                       \
        sqlite-dev                    \
        unzip                      && \
    rm -rf /root/.npm /root/.node-gyp /root/.cache /tmp/* /var/tmp/* && \
    rm -rf /app/src /app/package.json /app/package-lock.json /app/tsconfig.json && \
    echo "Build completed successfully"

# Create non-root user
RUN addgroup -g 1001 -S nodejs           && \
    adduser -S kagent -u 1001 -G nodejs  && \
    mkdir -p /app/build

WORKDIR /app

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

#show cheksum for every db file
RUN for file in /app/build/*.db; do echo "Checksum for $file:";sha256sum "$file";done

# Ensure the app directory is owned by the non-root user
RUN chown -R kagent:nodejs /app

LABEL org.opencontainers.image.source=https://github.com/kagent-dev/doc2vec
LABEL org.opencontainers.image.description="Kagent Doc2Vec MCP"
LABEL org.opencontainers.image.authors="Kagent Creators 🤖"

EXPOSE 3001

ENTRYPOINT ["node", "build/index.js"]