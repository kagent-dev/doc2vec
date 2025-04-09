FROM node:20-slim

WORKDIR /app

# Install Python and build tools
RUN apt-get update && apt-get install -y \
  git \
  python3 \
  make \
  g++ \
  && apt-get clean