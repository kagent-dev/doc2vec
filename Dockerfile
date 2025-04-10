FROM node:20-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
  git \
  python3 \
  make \
  g++ \
  sqlite3 \
  libsqlite3-dev \
  curl \
  ca-certificates \
  chromium \
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
  && apt-get clean