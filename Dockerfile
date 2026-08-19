FROM node:20-slim AS base

WORKDIR /app

# NOTE: chromium from apt is kept as the shared-library provider and the
# linux/arm64 fallback, but it is no longer the browser we run on amd64 —
# Debian has shipped chromium builds that crash at startup in containers
# (150.0.7871 dies with SIGTRAP immediately). The runtime stage downloads
# Puppeteer's version-matched Chrome for Testing instead.
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    sqlite3 \
    curl \
    ca-certificates \
    chromium \
    chromium-sandbox \
    fonts-freefont-ttf \
    fonts-ipafont-gothic \
    fonts-kacst \
    fonts-liberation \
    fonts-noto-color-emoji \
    fonts-thai-tlwg \
    libx11-xcb1 \
    libxcb-dri3-0 \
    libxcomposite1 \
    libxdamage1 \
    libxi6 \
    libxrandr2 \
    libxshmfence1 \
    libxtst6 \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/chromium /usr/bin/chromium-browser

RUN groupadd --gid 14000 doc2vec \
    && useradd --uid 14000 --gid doc2vec --create-home --shell /bin/bash doc2vec \
    && chown -R doc2vec:doc2vec /app

FROM base AS builder

USER doc2vec

COPY --chown=doc2vec:doc2vec package*.json ./
RUN npm ci --ignore-scripts

COPY --chown=doc2vec:doc2vec . .

RUN npm run build && npm run build:ui

FROM base AS prod-deps

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    make \
    g++ \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

USER doc2vec

COPY --chown=doc2vec:doc2vec package*.json ./
RUN node -e "const fs = require('fs'); const pkg = require('./package.json'); delete pkg.devDependencies; if (pkg.scripts) { delete pkg.scripts.prepare; delete pkg.scripts.prepublishOnly; } fs.writeFileSync('package.json', JSON.stringify(pkg, null, 2) + '\n');" \
    && npm install --omit=dev --omit=peer --ignore-scripts \
    && npm rebuild better-sqlite3 \
    && npm cache clean --force

FROM base AS runtime

ENV NODE_ENV=production

COPY --from=prod-deps --chown=doc2vec:doc2vec /app/package*.json ./
COPY --from=prod-deps --chown=doc2vec:doc2vec /app/node_modules ./node_modules
COPY --from=builder --chown=doc2vec:doc2vec /app/dist ./dist
COPY --from=builder --chown=doc2vec:doc2vec /app/README.md /app/LICENSE /app/config.yaml ./

# Controller mode serves its API/UI here (one-shot sync runs ignore it)
EXPOSE 8080

USER doc2vec

# Version-matched Chrome for Testing (see note in the base stage). Published
# for linux/amd64 only — arm64 falls back to the apt chromium at runtime.
ENV PUPPETEER_CACHE_DIR=/home/doc2vec/.cache/puppeteer
RUN if [ "$(dpkg --print-architecture)" = "amd64" ]; then \
      npx puppeteer browsers install chrome; \
    fi
