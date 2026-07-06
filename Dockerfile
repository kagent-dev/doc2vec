FROM node:20-slim AS base

WORKDIR /app

ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

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

RUN npm run build

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

USER doc2vec
