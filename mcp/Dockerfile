FROM node:20-slim AS base

# Copy project files
COPY package.json /usr/src/app/
COPY package-lock.json /usr/src/app/
COPY tsconfig.json /usr/src/app/
COPY src/index.ts /usr/src/app/src/
COPY *.db /data/
WORKDIR /usr/src/app

RUN npm install
RUN npm run build

ENTRYPOINT ["node", "build/index.js"]