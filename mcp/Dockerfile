# Build stage
FROM --platform=linux/$BUILDARCH cgr.dev/chainguard/wolfi-base:latest AS builder

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN apk update && apk add --no-cache \
    bash \
    build-base \
    ca-certificates \
    curl \
    make \
    node-gyp \
    nodejs \
    npm \
    openssl \
    python3 \
    sqlite-dev \
    unzip \
    && update-ca-certificates \
    && rm -rf /var/cache/apk/*

WORKDIR /app

# Copy package files first for better layer caching
COPY package.json package-lock.json tsconfig.json ./
COPY src/ ./src/

# Install dependencies and build
RUN --mount=type=cache,target=/root/.npm \
    npm install typescript && \
    npm ci --only=production && \
    npm install --save-dev typescript && \
    npm run build

# Production stage
FROM cgr.dev/chainguard/wolfi-base:latest AS production

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV NODE_ENV=production

# Install only runtime dependencies
RUN apk update && apk add --no-cache \
    nodejs \
    ca-certificates \
    sqlite \
    && update-ca-certificates \
    && rm -rf /var/cache/apk/*

# Create non-root user
RUN addgroup -g 1001 -S nodejs && \
    adduser -S kagent -u 1001 -G nodejs

WORKDIR /app

# Copy built application and production dependencies from builder stage
COPY --from=builder --chown=kagent:nodejs /app/build ./build
COPY --from=builder --chown=kagent:nodejs /app/node_modules ./node_modules
COPY --from=builder --chown=kagent:nodejs /app/package.json ./

USER kagent

LABEL org.opencontainers.image.source=https://github.com/kagent-dev/doc2vec
LABEL org.opencontainers.image.description="Kagent Doc2Vec MCP"
LABEL org.opencontainers.image.authors="Kagent Creators 🤖"

EXPOSE 3001

ENTRYPOINT ["node", "build/index.js"]