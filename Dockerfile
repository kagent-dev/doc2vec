# Two images come out of this file:
#
#   runtime         doc2vec without a browser. Website sources need a browser
#                   reachable over CDP: set BROWSER_WS_ENDPOINT to one, e.g. a
#                   ghcr.io/browserless/chromium container. This is the image
#                   to run in a cluster — the browser (the one component that
#                   renders untrusted input) lives in its own pod and is
#                   updated on its own schedule.
#   runtime-chrome  the same plus a bundled Chrome, for running doc2vec as a
#                   single container. linux/amd64 only: Chrome for Testing is
#                   not published for arm64.
#
#   docker build --target runtime        -t doc2vec .
#   docker build --target runtime-chrome -t doc2vec:chrome .
#
# The base is Wolfi (Chainguard): a glibc distro with apk and busybox and
# nothing else. Its packages are rebuilt within a day of an upstream fix, so a
# rebuild of this image picks fixes up; `apk upgrade` runs first in every stage
# because a base TAG is a snapshot and `--pull` alone does not patch it.
#
# The bundled browser is Puppeteer's own Chrome for Testing, version-matched to
# the puppeteer in package-lock.json, not the distro's chromium package: distro
# chromium lags Chrome stable by several majors (Wolfi shipped 149 and Debian
# 151 while stable was 153, measured 2026-09-22) and Debian has shipped builds
# that crash at startup in containers. Note that a CVE scanner cannot see a
# Chrome CVE in this image — Chrome for Testing is not a package it reads — so
# judge the browser by its version, never by the finding count.

FROM cgr.dev/chainguard/wolfi-base:latest AS base

WORKDIR /app

RUN apk upgrade --no-cache && apk add --no-cache \
    nodejs-26 \
    git \
    ca-certificates

RUN addgroup -g 14000 doc2vec \
    && adduser -u 14000 -G doc2vec -h /home/doc2vec -s /bin/sh -D doc2vec \
    && chown -R doc2vec:doc2vec /app

# Everything that needs npm or a compiler lives in this stage and never ships.
FROM base AS build-tools

RUN apk add --no-cache \
    npm \
    python3 \
    build-base \
    sqlite-dev

USER doc2vec

FROM build-tools AS builder

COPY --chown=doc2vec:doc2vec package*.json ./
RUN npm ci --ignore-scripts

COPY --chown=doc2vec:doc2vec . .

RUN npm run build && npm run build:ui

FROM build-tools AS prod-deps

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

CMD ["node", "dist/doc2vec.js"]

FROM runtime AS runtime-chrome

USER root

# The shared libraries and fonts Chrome for Testing loads at runtime. There is
# no chromium package here on purpose (see the note at the top). The font set
# mirrors what the old Debian image carried; the full font-noto is 530MB.
# mesa-gbm is the only provider of libgbm.so.1 and hard-depends on
# mesa-libgallium, which drags in libLLVM (~190MB). Chrome links libgbm even
# with --disable-gpu, so this cannot be avoided.
RUN apk add --no-cache \
    alsa-lib \
    at-spi2-core \
    libatk-bridge-2.0 \
    cairo \
    cups-libs \
    dbus-libs \
    libexpat1 \
    fontconfig \
    freetype \
    glib \
    libdrm \
    libgcc \
    libstdc++ \
    libudev \
    libx11 \
    libxcb \
    libxcomposite \
    libxdamage \
    libxext \
    libxfixes \
    libxkbcommon \
    libxrandr \
    mesa-gbm \
    libnspr \
    libnss \
    pango \
    font-freefont \
    font-ipafont-gothic \
    font-liberation \
    font-noto-emoji

USER doc2vec

# Version-matched Chrome for Testing. puppeteer.executablePath() finds it here.
ENV PUPPETEER_CACHE_DIR=/home/doc2vec/.cache/puppeteer
RUN node node_modules/puppeteer/lib/puppeteer/node/cli.js browsers install chrome
