# Image configuration
DOCKER_REGISTRY ?= ghcr.io
BASE_IMAGE_REGISTRY ?= cgr.dev
DOCKER_REPO ?= kagent-dev/doc2vec

BUILD_DATE := $(shell date -u '+%Y-%m-%d')
GIT_COMMIT := $(shell git rev-parse --short HEAD || echo "unknown")
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null | sed 's/-dirty//' | grep v || echo "v0.0.0-$(GIT_COMMIT)")

MCP_IMAGE_NAME ?= mcp

# Local architecture detection to build for the current platform
LOCALARCH ?= $(shell uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/')

DOCKER_BUILDER ?= docker buildx
DOCKER_BUILD_ARGS ?= --progress=plain --builder $(BUILDX_BUILDER_NAME) --pull --load --platform linux/$(LOCALARCH)

BUILDX_NO_DEFAULT_ATTESTATIONS=1
BUILDX_BUILDER_NAME=kagent-builder

# Build the MCP image
.PHONY: build-mcp
build-mcp:
	$(DOCKER_BUILDER) build $(DOCKER_BUILD_ARGS) -t $(DOCKER_REGISTRY)/$(DOCKER_REPO)/$(MCP_IMAGE_NAME):$(VERSION)  -f mcp/Dockerfile ./mcp

# Run the MCP image
.PHONY: run-mcp
run-mcp: build-mcp
	docker run --rm -t  -e OPENAI_API_KEY=$(OPENAI_API_KEY) -e TRANSPORT_TYPE=http --name mcp -p 3001:3001 $(DOCKER_REGISTRY)/$(DOCKER_REPO)/$(MCP_IMAGE_NAME):$(VERSION)
