# Doc2Vec

[![npm version](https://img.shields.io/npm/v/doc2vec.svg)](https://www.npmjs.com/package/doc2vec)

This project provides a configurable tool (`doc2vec`) to crawl specified websites (typically documentation sites), GitHub repositories, local directories, and Zendesk support systems, extract relevant content, convert it to Markdown, chunk it intelligently, generate vector embeddings using OpenAI, and store the chunks along with their embeddings in a vector database (SQLite with `sqlite-vec` or Qdrant).

The primary goal is to prepare documentation content for Retrieval-Augmented Generation (RAG) systems or semantic search applications.

Run it two ways:

*   **One-shot sync** — `doc2vec run config.yaml` processes every source in a config file and exits. Ideal for a cron job or CI step. Pass `--source <product_name>` (all entries with that name) or `--source-index <n>` (a single entry, by its 0-based position in the config) to sync only selected sources; both flags are repeatable.
*   **[Controller mode](#controller-mode)** — `doc2vec controller ./configs/` stays running: it schedules each config on its own cron expression, keeps run history and per-source statistics in Postgres, and serves a web UI with live log streaming, searchable run logs, and chunk inspection. Deploy it once and manage all your sources from the browser.

[![doc2vec controller dashboard](docs/images/controller-dashboard.png)](#controller-mode)

## Key Features

*   **Website Crawling:** Recursively crawls websites starting from a given base URL.
    * **Sitemap Support:** Extracts URLs from XML sitemaps to discover pages not linked in navigation. When sitemaps include `<lastmod>` dates, pages are skipped without any HTTP requests if the date hasn't changed since the last sync.
    * **PDF Support:** Automatically downloads and processes PDF files linked from websites.
*   **GitHub Issues Integration:** Retrieves GitHub issues and comments, processing them into searchable chunks.
*   **Zendesk Integration:** Fetches support tickets and knowledge base articles from Zendesk, converting them to searchable chunks.
    * **Support Tickets:** Processes tickets with metadata, descriptions, and comments.
    * **Knowledge Base Articles:** Converts help center articles from HTML to clean Markdown.
    * **Incremental Updates:** Only processes tickets/articles updated since the last run.
    * **Flexible Filtering:** Filter tickets by status and priority.
*   **Local Directory Processing:** Scans local directories for files, converts content to searchable chunks.
    * **PDF Support:** Automatically extracts text from PDF files and converts them to Markdown format using Mozilla's PDF.js.
    * **Word Document Support:** Processes both legacy `.doc` and modern `.docx` files, extracting text and formatting.
*   **Code Source Processing:** Ingests code from local directories or GitHub repositories using Chonkie code chunking.
    * **AST-aware Chunking:** Uses Chonkie-based code chunking with Tree-sitter to preserve code structure.
    * **Repository Support:** Clones GitHub repos for code ingestion and maps files to GitHub URLs.
*   **Content Extraction:** Uses Puppeteer for rendering JavaScript-heavy pages and `@mozilla/readability` to extract the main article content.
    *   **Smart H1 Preservation:** Automatically extracts and preserves page titles (H1 headings) that Readability might strip as "page chrome", ensuring proper heading hierarchy.
    *   **Flexible Content Selectors:** Supports multiple content container patterns (`.docs-content`, `.doc-content`, `.markdown-body`, `article`, etc.) for better compatibility with various documentation sites.
    *   **Tabbed Content Support:** Automatically detects WAI-ARIA tabs (`role="tab"` / `role="tabpanel"`) and injects tab labels into panel content so each tab's context is preserved after conversion to Markdown.
*   **HTML to Markdown:** Converts extracted HTML to clean Markdown using `turndown`, preserving code blocks and basic formatting.
    *   **Clean Heading Text:** Automatically removes anchor links (like `[](#section-id)`) from heading text for cleaner hierarchy display.
*   **Intelligent Chunking:** Splits Markdown content into manageable chunks based on headings and token limits, preserving context.
*   **Vector Embeddings:** Generates embeddings for each chunk using OpenAI or Azure OpenAI (configurable).
*   **Vector Storage:** Supports storing chunks, metadata, and embeddings in:
    *   **SQLite:** Using `better-sqlite3` and the `sqlite-vec` extension for efficient vector search.
    *   **Qdrant:** A dedicated vector database, using the `@qdrant/js-client-rest`.
*   **[Postgres Markdown Store](MARKDOWN_STORE.md):** Optionally stores the generated markdown for each crawled URL in a Postgres table. Useful for maintaining a searchable, raw-text copy of all documentation pages alongside the vector embeddings.
    *   **Automatic population:** On the first sync, all pages are force-processed (bypassing lastmod/ETag caching) to fully populate the store. Subsequent syncs only update rows when a change is detected.
    *   **404 cleanup:** Pages that return 404 are automatically removed from the store.
    *   **Shared table:** A single table (configurable name, default `markdown_pages`) is shared across all sources, with a `product_name` column to distinguish them.
*   **Multi-Layer Change Detection:** Four layers of change detection minimize unnecessary re-processing:
    1. **Sitemap `lastmod`:** When available, compares the sitemap's `<lastmod>` date against the stored value — skips without any HTTP request. Child URLs inherit `lastmod` from their most specific parent directory.
    2. **ETag via HEAD request:** For URLs without `lastmod`, sends a lightweight HEAD request and compares the ETag header against the stored value. Adaptive backoff prevents rate limiting (starts at 0ms delay, increases on 429 responses, decays on success).
    3. **Content hash comparison:** After full page load, compares chunk content hashes against stored values — skips embedding if content is unchanged.
    4. **Embedding:** Only re-embeds chunks when content has actually changed.
    
    ETag and lastmod values are only stored when chunking and embedding succeed, ensuring failed pages are retried on the next run.
    
    A `sync_complete` metadata flag tracks whether a full sync has ever completed successfully. If a sync is interrupted (process killed), the next run force-processes all pages regardless of lastmod/ETag values, ensuring no pages are permanently skipped.
*   **Incremental Updates:** For GitHub and Zendesk sources, tracks the last run date to only fetch new or updated issues/tickets.
*   **Cleanup:** Removes obsolete chunks from the database corresponding to pages or files that are no longer found during processing.
*   **Configuration:** Driven by a YAML configuration file (`config.yaml`) specifying sites, repositories, local directories, Zendesk instances, database types, metadata, and other parameters.
*   **[Controller Mode](#controller-mode):** Optionally runs as a long-lived controller that schedules sync jobs from multiple config files (cron `schedule` field), persists run history and statistics in Postgres, and serves a web UI with live log streaming, whole-run log search, and chunk inspection — read-only (configs from files/ConfigMap) or read-write (create/edit configs from the UI).
*   **Structured Logging:** Uses a custom logger (`logger.ts`) with levels, timestamps, colors, progress bars, and child loggers for clear execution monitoring.

## Chunk Metadata & Page Reconstruction

Each chunk stored in the database includes rich metadata that enables powerful retrieval and page reconstruction capabilities.

### Metadata Fields

| Field | Type | Description |
|-------|------|-------------|
| `product_name` | string | Product identifier from config |
| `version` | string | Version identifier from config |
| `heading_hierarchy` | string[] | Hierarchical breadcrumb trail (e.g., `["Installation", "Prerequisites", "Docker"]`) |
| `section` | string | Current section heading |
| `chunk_id` | string | Unique hash identifier for the chunk |
| `url` | string | Source URL/path of the original document |
| `hash` | string | Content hash for change detection |
| `chunk_index` | number | Position of this chunk within the page (0-based) |
| `total_chunks` | number | Total number of chunks for this page |

### Page Reconstruction

The `chunk_index` and `total_chunks` fields enable you to reconstruct full pages from chunks:

```typescript
// Example: Retrieve all chunks for a URL and reconstruct the page
const chunks = await db.query({
  filter: { url: "https://docs.example.com/guide" },
  sort: { chunk_index: "asc" }
});

// Check if there are more chunks after the current one
if (currentChunk.chunk_index < currentChunk.total_chunks - 1) {
  // More chunks available - fetch the next one
  const nextChunkIndex = currentChunk.chunk_index + 1;
}

// Reconstruct full page content
const fullPageContent = chunks
  .sort((a, b) => a.chunk_index - b.chunk_index)
  .map(c => c.content)
  .join("\n\n");
```

### Heading Hierarchy (Breadcrumbs)

Each chunk includes a `heading_hierarchy` array that provides context about where the content appears in the document structure. This is injected as a `[Topic: ...]` prefix in the chunk content to improve vector search relevance.

For example, a chunk under "Installation > Prerequisites > Docker" will have:
- `heading_hierarchy`: `["Installation", "Prerequisites", "Docker"]`
- Content prefix: `[Topic: Installation > Prerequisites > Docker]`

This ensures that searches for parent topics (like "Installation") will also match relevant child content.

## Prerequisites

*   **Node.js:** Version 18 or higher recommended (check `.nvmrc` if available).
*   **npm:** Node Package Manager (usually comes with Node.js).
*   **TypeScript:** As the project is written in TypeScript (`ts-node` is used for execution via `npm start`).
*   **OpenAI API Key or Azure OpenAI Credentials:** You need either an OpenAI API key or Azure OpenAI credentials to generate embeddings.
*   **GitHub Personal Access Token:** Required for accessing GitHub issues (set as `GITHUB_PERSONAL_ACCESS_TOKEN` in your environment).
*   **Zendesk API Token:** Required for accessing Zendesk tickets and articles (set as `ZENDESK_API_TOKEN` in your environment).
*   **(Optional) Qdrant Instance:** If using the `qdrant` database type, you need a running Qdrant instance accessible from where you run the script.
*   **(Optional) Build Tools:** Dependencies like `better-sqlite3` and `sqlite-vec` might require native compilation, which could necessitate build tools like `python`, `make`, and a C++ compiler (like `g++` or Clang) depending on your operating system.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/kagent-dev/doc2vec.git
    cd doc2vec
    ```

2.  **Install dependencies:**
    Using npm:
    ```bash
    npm install
    ```
    This will install all packages listed in `package.json`.

## Configuration

Configuration is managed through two files:

1.  **`.env` file:**
    Create a `.env` file in the project root to store sensitive information like API keys.

    ```dotenv
    # .env

    # Embedding Provider Configuration
    # Optional: Specify which provider to use (defaults to 'openai' if not set)
    # Can also be configured in config.yaml
    EMBEDDING_PROVIDER="azure"  # or "openai"

    # Required: Your OpenAI API Key (if using OpenAI provider)
    OPENAI_API_KEY="sk-..."
    OPENAI_MODEL="text-embedding-3-large"  # Optional, defaults to text-embedding-3-large

    # Optional: Override the OpenAI API base URL. Useful for pointing the
    # OpenAI SDK at an OpenAI-compatible endpoint such as Ollama, LM Studio,
    # llama.cpp, vLLM, or a proxy gateway. When unset, the SDK uses the
    # default OpenAI endpoint. Equivalent to the `embedding.openai.base_url`
    # field in config.yaml; the env var wins if both are set.
    # Example values:
    #   OPENAI_BASE_URL="http://localhost:11434/v1"        # Ollama
    #   OPENAI_BASE_URL="https://gateway.example.com/v1"   # proxy / gateway
    OPENAI_BASE_URL="http://localhost:11434/v1"

    # Optional: Embedding dimension size (defaults to 3072)
    EMBEDDING_DIMENSION="3072"

    # Required: Your Azure OpenAI credentials (if using Azure provider)
    AZURE_OPENAI_KEY="your-azure-key"
    AZURE_OPENAI_ENDPOINT="https://your-resource.openai.azure.com"
    AZURE_OPENAI_DEPLOYMENT_NAME="text-embedding-3-large"
    AZURE_OPENAI_API_VERSION="2024-10-21"

    # Required for GitHub sources
    GITHUB_PERSONAL_ACCESS_TOKEN="ghp_..."

    # Required for Zendesk sources
    ZENDESK_API_TOKEN="your-zendesk-api-token"

    # Optional: Required only if using Qdrant
    QDRANT_API_KEY="your-qdrant-api-key"
    ```

2.  **`config.yaml` file:**
    This file defines the sources to process and how to handle them. Create a `config.yaml` file (or use a different name and pass it as an argument).

    **Structure:**

    *   `sources`: An array of source configurations.
        *   `type`: Either `'website'`, `'github'`, `'local_directory'`, `'code'`, `'zendesk'`, or `'s3'`
        
        For websites (`type: 'website'`):
        *   `url`: The starting URL for crawling the documentation site.
        *   `sitemap_url`: (Optional) URL to the site's XML sitemap for discovering additional pages not linked in navigation.
        *   `markdown_store`: (Optional) Set to `true` to store generated markdown in the Postgres markdown store (requires top-level `markdown_store` config). Defaults to `false`.
        
        For GitHub repositories (`type: 'github'`):
        *   `repo`: Repository name in the format `'owner/repo'` (e.g., `'istio/istio'`).
        *   `start_date`: (Optional) Starting date to fetch issues from (e.g., `'2025-01-01'`).
        
        For local directories (`type: 'local_directory'`):
        *   `path`: Path to the local directory to process.
        *   `include_extensions`: (Optional) Array of file extensions to include (e.g., `['.md', '.txt', '.pdf', '.doc', '.docx']`). Defaults to `['.md', '.txt', '.html', '.htm', '.pdf']`.
        *   `exclude_extensions`: (Optional) Array of file extensions to exclude.
        *   `recursive`: (Optional) Whether to traverse subdirectories (defaults to `true`).
        *   `url_rewrite_prefix` (Optional) URL prefix to rewrite `file://` URLs (e.g., `https://mydomain.com`)
        *   `encoding`: (Optional) File encoding to use (defaults to `'utf8'`). Note: PDF files are processed as binary and this setting doesn't apply to them.

        For code sources (`type: 'code'`):
        *   `source`: Either `'local_directory'` or `'github'`.
        *   `path`: Path to the local directory (required when `source: 'local_directory'`).
        *   `repo`: Repository name in the format `'owner/repo'` (required when `source: 'github'`).
        *   `branch`: (Optional) Branch to clone for GitHub sources.
        *   `include_extensions`: (Optional) Array of file extensions to include (defaults to common code extensions).
        *   `exclude_extensions`: (Optional) Array of file extensions to exclude.
        *   `exclude_paths`: (Optional) Array of glob patterns, relative to the source root, that the scanner skips. See [Excluding paths from code sources](#excluding-paths-from-code-sources).
        *   `recursive`: (Optional) Whether to traverse subdirectories (defaults to `true`).
        *   `url_rewrite_prefix`: (Optional) URL prefix to rewrite `file://` URLs for local sources.
        *   `encoding`: (Optional) File encoding to use (defaults to `'utf8'`).
        *   `chunk_size`: (Optional) Chonkie chunk size for code files.
        *   `version` is optional for code sources; if omitted it defaults to `branch` (or `local` for local directories).
        *   `branch` is stored in the database and used by `query_code` filtering.
        
        For Zendesk (`type: 'zendesk'`):
        *   `zendesk_subdomain`: Your Zendesk subdomain (e.g., `'mycompany'` for mycompany.zendesk.com).
        *   `email`: Your Zendesk admin email address.
        *   `api_token`: Your Zendesk API token (reference environment variable as `'${ZENDESK_API_TOKEN}'`).
        *   `fetch_tickets`: (Optional) Whether to fetch support tickets (defaults to `true`).
        *   `fetch_articles`: (Optional) Whether to fetch knowledge base articles (defaults to `true`).
        *   `start_date`: (Optional) Only process tickets/articles updated since this date (e.g., `'2025-01-01'`).
        *   `ticket_status`: (Optional) Filter tickets by status (defaults to `['new', 'open', 'pending', 'hold', 'solved']`).
        *   `ticket_priority`: (Optional) Filter tickets by priority (defaults to all priorities).
        *   `excluded_organizations`: (Optional) An array of Zendesk organization names whose tickets should be skipped. The sync will abort if any name cannot be resolved.
        *   `include_internal_comments`: (Optional) Include non-public (internal/agent-only) comments in the indexed content (defaults to `false`, i.e. only public comments are indexed). Internal comments are labeled `(internal)` in the generated markdown.

        For S3 buckets (`type: 's3'`):
        *   `bucket`: The S3 bucket name.
        *   `prefix`: (Optional) Key prefix to filter objects (e.g., `'docs/'`). Only objects under this prefix will be processed.
        *   `region`: (Optional) AWS region (defaults to `AWS_DEFAULT_REGION` environment variable or `'us-east-1'`).
        *   `endpoint`: (Optional) Custom S3 endpoint for S3-compatible services (MinIO, LocalStack, etc.).
        *   `include_extensions`: (Optional) Array of file extensions to include (e.g., `['.md', '.txt', '.pdf']`). Defaults to `['.md', '.txt', '.html', '.htm', '.pdf', '.doc', '.docx']`.
        *   `exclude_extensions`: (Optional) Array of file extensions to exclude.
        *   `encoding`: (Optional) Text file encoding (defaults to `'utf8'`). Does not apply to binary files (PDF, DOC, DOCX).
        *   `url_rewrite_prefix`: (Optional) URL prefix to rewrite `s3://` URLs (e.g., `'https://docs.example.com'`).

        **S3 user metadata resolution:** The `product_name` and `version` fields support a `metadata(...)` syntax to dynamically resolve values from S3 object user metadata. For example, `product_name: 'metadata(x-amz-meta-product-name)'` will set `product_name` to the value of the `x-amz-meta-product-name` user metadata on each S3 object. If the metadata key doesn't exist on an object, an empty string is used. Literal values (without the `metadata(...)` wrapper) work as before.

        Authentication uses the AWS SDK default credential chain: environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), `~/.aws/credentials`, IAM roles, etc.

        Incremental sync tracks object `LastModified` timestamps so only new or updated objects are processed on subsequent runs. Deleted objects are automatically cleaned up.

        Common configuration for all types:
        *   `product_name`: A string identifying the product (used in metadata).
        *   `version`: A string identifying the product version (used in metadata).
        *   `max_size`: Maximum raw content size (in characters). For websites, this limits the raw HTML fetched by Puppeteer. Recommending 1MB (1048576).
        *   `database_config`: Configuration for the database.
            *   `type`: Specifies the storage backend (`'sqlite'` or `'qdrant'`).
            *   `params`: Parameters specific to the chosen database type.
                *   For `sqlite`:
                    *   `db_path`: (Optional) Path to the SQLite database file. Defaults to `./<product_name>-<version>.db`.
                *   For `qdrant`:
                    *   `qdrant_url`: (Optional) URL of your Qdrant instance. Defaults to `http://localhost:6333`.
                    *   `qdrant_port`: (Optional) Port for the Qdrant REST API. Defaults to `443` if `qdrant_url` starts with `https`, otherwise `6333`.
                    *   `collection_name`: (Optional) Name of the Qdrant collection to use. Defaults to `<product_name>_<version>` (lowercased, spaces replaced with underscores).

        Optional embedding configuration:
        *   `embedding.provider`: Provider for embeddings (`openai` or `azure`).
        *   `embedding.dimension`: Embedding vector size. Defaults to `3072` when not set.

        Optional Postgres markdown store (top-level):
        *   `markdown_store.connection_string`: (Optional) Full Postgres connection string (e.g., `'postgres://user:pass@host:5432/db'`). Takes priority over individual fields.
        *   `markdown_store.host`: (Optional) Postgres host.
        *   `markdown_store.port`: (Optional) Postgres port.
        *   `markdown_store.database`: (Optional) Postgres database name.
        *   `markdown_store.user`: (Optional) Postgres user.
        *   `markdown_store.password`: (Optional) Postgres password. Supports `${PG_PASSWORD}` env var substitution.
        *   `markdown_store.table_name`: (Optional) Table name. Defaults to `'markdown_pages'`.
        
        When configured, website sources with `markdown_store: true` will store the generated markdown for each URL in this Postgres table. On the first sync, all pages are force-processed (bypassing lastmod/ETag skip logic) to populate the table. On subsequent syncs, only pages with detected changes get their rows updated.

    **Example (`config.yaml`):**
    ```yaml
    # Optional: Configure embedding provider
    # Can also be set via EMBEDDING_PROVIDER environment variable
    # Defaults to OpenAI if not specified
    embedding:
      provider: 'openai'  # or 'azure'
      dimension: 3072  # Optional, defaults to 3072
      openai:
        api_key: '${OPENAI_API_KEY}'  # Optional, uses env var by default
        model: 'text-embedding-3-large'  # Optional, defaults to text-embedding-3-large
        # base_url: 'http://localhost:11434/v1'  # Optional, override OpenAI API base URL for Ollama / other OpenAI-compatible endpoints. Falls back to OPENAI_BASE_URL env var.
      # For Azure OpenAI, use this instead:
      # azure:
      #   api_key: '${AZURE_OPENAI_KEY}'
      #   endpoint: '${AZURE_OPENAI_ENDPOINT}'
      #   deployment_name: 'text-embedding-3-large'
      #   api_version: '2024-10-21'  # Optional

    # Optional: Store generated markdown in Postgres
    # markdown_store:
    #   connection_string: 'postgres://user:pass@host:5432/db'
    #   # OR use individual fields:
    #   # host: 'localhost'
    #   # port: 5432
    #   # database: 'doc2vec'
    #   # user: 'myuser'
    #   # password: '${PG_PASSWORD}'
    #   # table_name: 'markdown_pages'  # Optional, defaults to 'markdown_pages'

    sources:
      # Website source example (with markdown store enabled)
      - type: 'website'
        product_name: 'argo'
        version: 'stable'
        url: 'https://argo-cd.readthedocs.io/en/stable/'
        sitemap_url: 'https://argo-cd.readthedocs.io/en/stable/sitemap.xml'
        markdown_store: true  # Store generated markdown in Postgres
        max_size: 1048576
        database_config:
          type: 'sqlite'
          params:
            db_path: './vector-dbs/argo-cd.db'

      # GitHub repository source example
      - type: 'github'
        product_name: 'istio'
        version: 'latest'
        repo: 'istio/istio'
        start_date: '2025-01-01'
        max_size: 1048576
        database_config:
          type: 'sqlite'
          params:
            db_path: './istio-issues.db'
      
      # Local directory source example
      - type: 'local_directory'
        product_name: 'project-docs'
        version: 'current'
        path: './docs'
        include_extensions: ['.md', '.txt', '.pdf', '.doc', '.docx']
        recursive: true
        max_size: 10485760  # 10MB recommended for PDF/Word files
        database_config:
          type: 'sqlite'
          params:
            db_path: './project-docs.db'

      # Code source example (GitHub)
      - type: 'code'
        source: 'github'
        product_name: 'doc2vec'
        version: 'main'
        repo: 'kagent-dev/doc2vec'
        branch: 'main'
        include_extensions: ['.ts', '.tsx', '.md']
        exclude_paths:
          - 'node_modules/**'
          - 'dist/**'
          - '**/*.test.ts'
        max_size: 1048576
        chunk_size: 2048
        database_config:
          type: 'sqlite'
          params:
            db_path: './doc2vec-code.db'
      
      # Zendesk example
      - type: 'zendesk'
        product_name: 'MyCompany'
        version: 'latest'
        zendesk_subdomain: 'mycompany'
        email: 'admin@mycompany.com'
        api_token: '${ZENDESK_API_TOKEN}'
        fetch_tickets: true
        fetch_articles: true
        start_date: '2025-01-01'
        ticket_status: ['open', 'pending']
        ticket_priority: ['high']
        excluded_organizations: ['Acme Corp', 'Internal Testing']
        max_size: 1048576
        database_config:
          type: 'sqlite'
          params:
            db_path: './zendesk-kb.db'
      
      # S3 bucket source example
      - type: 's3'
        product_name: 'metadata(x-amz-meta-product-name)'
        version: 'latest'
        bucket: 'my-documentation-bucket'
        prefix: 'docs/v2/'
        region: 'us-west-2'
        include_extensions: ['.md', '.txt', '.pdf', '.html']
        url_rewrite_prefix: 'https://docs.example.com'
        max_size: 1048576
        database_config:
          type: 'sqlite'
          params:
            db_path: './s3-docs.db'

      # Qdrant example
      - type: 'website'
        product_name: 'Istio'
        version: 'latest'
        url: 'https://istio.io/latest/docs/'
        max_size: 1048576
        database_config:
          type: 'qdrant'
          params:
            qdrant_url: 'https://your-qdrant-instance.cloud'
            qdrant_port: 6333
            collection_name: 'istio_docs_latest'
      # ... more sources
    ```

### Excluding paths from code sources

Code sources (`type: 'code'`) accept `exclude_paths`. Each entry is a glob pattern that the scanner matches against the path of the file, relative to the source root. The root is the `path` directory for a local source, or the clone root for a GitHub source. Use it to keep vendored trees, generated code, and test files out of the index.

```yaml
      - type: 'code'
        source: 'github'
        product_name: 'my-product'
        repo: 'my-org/my-repo'
        branch: 'main'
        include_extensions: ['.go', '.md', '.yaml', '.sh', '.py', '.proto']
        exclude_paths:
          - 'vendor/**'                 # vendored dependencies
          - 'LICENSES/**'               # go-licenses output
          - 'pkg/client/**'             # k8s code-generator output
          - '**/*.pb.go'                # protobuf output
          - '**/zz_generated.*.go'      # controller-gen output
          - '**/*_test.go'              # tests
          - 'hack/tools/**'
        max_size: 1048576
        database_config:
          type: 'sqlite'
          params:
            db_path: './my-product-code.db'
```

Pattern rules:

*   `*` matches any characters inside one path segment. It does not cross a `/`.
*   `**` matches any characters, and it crosses `/`. `vendor/**` therefore matches every file below `vendor/`.
*   A leading `**/` also matches zero directories, so `**/*_test.go` matches `main_test.go` at the root and `pkg/a/main_test.go` below it.
*   `?` matches one character inside a segment.
*   A pattern that names a directory (`vendor` or `vendor/**`) prunes the whole subtree. The scanner does not walk it.
*   Matching is case-sensitive, and a pattern must match the whole relative path. Use `**/build/**`, not `build`, to exclude a directory at any depth.

The scanner treats an excluded file as if it does not exist. A full scan therefore removes the chunks of files you newly excluded. For GitHub sources in incremental mode, the removal happens on the next full scan.

## Usage

Run the script from the command line using the `start` script defined in `package.json`. This uses `ts-node` to execute the TypeScript code directly.

You can optionally provide the path to your configuration file as an argument after the `--`:

```bash
npm start -- [path/to/your/config.yaml]
```

*(Note the `--` required for `npm` when passing arguments to the script.)*

If no path is provided, the script defaults to looking for `config.yaml` in the current directory.

The script will then:
1.  Load the configuration.
2.  Initialize the structured logger.
3.  Iterate through each source defined in the config.
4.  Initialize the specified database connection.
5.  Process each source according to its type:
    - For websites: Crawl the site, process any sitemaps, extract content from HTML pages and download/process PDF files, convert to Markdown
    - For GitHub repos: Fetch issues and comments, convert to Markdown
    - For local directories: Scan files, process content (converting HTML and PDF files to Markdown if needed)
    - For Zendesk: Fetch tickets and articles, convert to Markdown
6.  For all sources: Chunk content, check for changes, generate embeddings (if needed), and store/update in the database.
7.  Cleanup obsolete chunks.
8.  Output detailed logs.

## Controller Mode

Besides the one-shot sync, doc2vec can run as a **long-lived controller** that schedules sync jobs, records run history and statistics in Postgres, and serves a web UI for monitoring and managing configs:

```bash
doc2vec controller --database-url postgres://user:pass@host:5432/doc2vec ./configs/
```

Open http://localhost:8080 to see the dashboard ([screenshot above](#doc2vec)): every config with its schedule, next run, and the outcome of its last run, plus a **Run now** button. The **▾** next to it opens a source picker to sync only a subset of the config's source entries — each entry is selectable individually, even when several share a product name (e.g. istio as github + code + website) — and the run history marks such partial runs with the selected sources.

Each config gets its own page with the full run history, per-source results, and the raw YAML:

![Config run history](docs/images/controller-runs.png)

…and a **Stats** tab with runs per day, success rate, and duration trend over the last 7/30/90 days:

![Config statistics](docs/images/controller-stats.png)

A run page shows what the sync did — duration, exit code, warning and error counts, per-source chunk counts and errors — above the run's logs.

![Run detail with filtered logs](docs/images/controller-run-logs.png)

### Scheduling

Add a top-level cron `schedule` (and optionally a display `name`) to a config file, and the controller runs it automatically:

```yaml
name: product-docs
schedule: "0 2 * * *"   # every day at 02:00
sources:
  - type: website
    ...
```

Configs without a `schedule` are still listed in the UI and can be triggered manually with **Run now** (optionally for a subset of sources via the **▾** picker). Each run executes `doc2vec run <config.yaml>` as a child process (with `--source-index <n>` flags when a subset was selected); overlapping runs of the same config are skipped, and `--max-parallel` (default 1) caps how many sync jobs run at once — keep it low, since website sources launch a headless Chromium.

### Logs

Every line a sync job writes is stored in Postgres and streamed to the run page as it happens (`● live`), with follow-on-scroll and a `↓ Follow` button when you scroll away.

Long runs produce a lot of output, so the browser only keeps the last 10,000 lines in memory — the header tells you when that is the case (`30,000 lines (showing last 10,000)`). Everything else stays one query away:

*   **Level chips** (`ERROR 4`, `WARN 93`, …) count the whole run, not the visible window, and clicking one filters **server-side** — so the 4 errors from the first minute of a 50-minute run are one click away. Results page in with **Load 2,000 more**.
*   **Keyword filter** matches the message or the module, case-insensitively, across the whole run.
*   **↓ download log** streams the complete log as plain text (`run-<id>.log`) for grepping locally or attaching to an issue. With a filter active it becomes **↓ download matches** and applies the same filter.

While a run is live, new lines matching the active filter keep arriving in the filtered view.

Run logs are pruned after `--log-retention-days` (default 14); run history and statistics are kept indefinitely.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--database-url <url>` | `DATABASE_URL` env | Postgres connection string (required) |
| `--port <n>` | `8080` (or `PORT`) | HTTP port for the API and UI |
| `--read-write` | off (read-only) | Allow creating/editing/deleting configs from the UI |
| `--config-dir <dir>` | — | Where UI-created configs are written (required with `--read-write`) |
| `--max-parallel <n>` | `1` | Max concurrent sync jobs |
| `--reload-interval <s>` | `30` | How often config files are re-read for changes |
| `--log-retention-days <n>` | `14` | Run logs older than this are pruned (run history is kept) |
| `--slack-webhook-url <url>` | `SLACK_WEBHOOK_URL` env | Slack incoming webhook — post a message when a run finishes |
| `--slack-notify <mode>` | `all` | `all` or `failures` (failures also covers canceled runs) |
| `--public-url <url>` | `PUBLIC_URL` env | Externally reachable base URL, used for "view run" links in notifications |

Positional arguments are config files and/or directories (directories are scanned for `*.yaml`/`*.yml`, and re-scanned on every reload).

### Slack notifications

With `--slack-webhook-url` (or `SLACK_WEBHOOK_URL`) set, the controller posts to a [Slack incoming webhook](https://api.slack.com/messaging/webhooks) whenever a run reaches a terminal state — ✅ succeeded, ❌ failed (naming the failing sources and their errors), or ⚠️ canceled. Overlap-`skipped` runs are not notified. Set `--slack-notify failures` to silence successes, and `--public-url https://doc2vec.example.com` to get clickable "view run" links.

### Read-only vs read-write

- **Read-only** (default): configs come solely from the files passed on the command line — ideal for Kubernetes, where they live in a ConfigMap. The UI can view configs, trigger runs, and browse history/stats, but not modify anything. ConfigMap updates are picked up automatically via content-hash polling.
- **Read-write** (`--read-write --config-dir ./configs`): the UI can also create, edit, and delete config YAML files. Files stay the source of truth on disk; concurrent edits are protected by a content-hash check.

In both modes the UI always shows raw YAML: `${ENV_VAR}` secret placeholders are **never** resolved outside the sync job itself.

### Kubernetes example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: doc2vec-controller
spec:
  replicas: 1        # keep a single replica: the scheduler is not distributed
  selector:
    matchLabels: { app: doc2vec-controller }
  template:
    metadata:
      labels: { app: doc2vec-controller }
    spec:
      containers:
        - name: controller
          image: ghcr.io/kagent-dev/doc2vec:latest
          command: ["node", "dist/doc2vec.js", "controller", "/etc/doc2vec"]
          ports:
            - containerPort: 8080
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef: { name: doc2vec-secrets, key: database-url }
            - name: OPENAI_API_KEY
              valueFrom:
                secretKeyRef: { name: doc2vec-secrets, key: openai-api-key }
          volumeMounts:
            - name: configs
              mountPath: /etc/doc2vec
          resources:
            requests: { memory: 1Gi }
            limits: { memory: 4Gi }   # website sources launch headless Chromium
      volumes:
        - name: configs
          configMap: { name: doc2vec-configs }
```

The controller detects ConfigMap updates without a restart. Health endpoint: `GET /api/health`.

### API

The UI is backed by a small REST API you can also use directly:

| Endpoint | Description |
|----------|-------------|
| `GET /api/configs` | Configs with schedule, next run, and last run |
| `POST /api/configs/:id/run` | Trigger a run now |
| `GET /api/configs/:id/stats?days=30` | Run counts, success rate, duration history |
| `GET /api/configs/:id/chunks?product_name=&url=` | Inspect the stored chunks for a URL |
| `GET /api/runs?configId=&status=&limit=&before=` | Run history |
| `GET /api/runs/:id/logs?afterSeq=&limit=&levels=&q=` | Log lines, filtered by level (csv) and/or keyword |
| `GET /api/runs/:id/logs/counts` | Line totals per level for the whole run |
| `GET /api/runs/:id/logs/download?levels=&q=` | Whole log (or the matching lines) as plain text |
| `POST /api/runs/:id/cancel` | Cancel a running job |
| `GET /api/health` | Status, mode, version |

Server-sent events: `GET /api/events` (run and config updates) and `GET /api/runs/:id/logs/stream?tail=10000` (replays the last `tail` stored lines, then tails live output).

## Database Options

### SQLite (`database_config.type: 'sqlite'`)
*   Uses `better-sqlite3` and `sqlite-vec`.
*   Requires `db_path`.
*   Native compilation might be needed.

### Qdrant (`database_config.type: 'qdrant'`)
*   Uses `@qdrant/js-client-rest`.
*   Requires `qdrant_url`, `qdrant_port`, `collection_name` and potentially `QDRANT_API_KEY`.

## PDF Processing

Doc2Vec includes built-in support for processing PDF files in both local directories and websites. PDF files are automatically detected by their `.pdf` extension and processed using [Mozilla's PDF.js](https://github.com/mozilla/pdf.js) library.

### Features
*   **Automatic Text Extraction:** Extracts text content from all pages in PDF documents
*   **Markdown Conversion:** Converts extracted text to clean Markdown format with proper structure
*   **Multi-page Support:** For multi-page PDFs, each page becomes a separate section with page headers
*   **Website Integration:** Automatically downloads and processes PDFs linked from websites during crawling
*   **Local File Support:** Processes PDF files found in local directories alongside other documents
*   **Size Management:** Respects configured size limits to prevent processing of extremely large documents
*   **Error Handling:** Graceful handling of corrupted or unsupported PDF files

### Configuration Tips for PDFs
*   **Larger Size Limits:** PDF files typically convert to more text than expected. Consider using larger `max_size` values (e.g., 10MB instead of 1MB) for directories containing PDFs:
    ```yaml
    max_size: 10485760  # 10MB recommended for PDF processing
    ```
*   **File Extensions:** Include `.pdf` in your `include_extensions` array:
    ```yaml
    include_extensions: ['.md', '.txt', '.pdf']
    ```
*   **Performance:** PDF processing is CPU-intensive. Large PDFs may take several seconds to process.
*   **Website Configuration:** For websites that may contain PDFs, use larger size limits:
    ```yaml
    - type: 'website'
      product_name: 'documentation'
      version: 'latest'
      url: 'https://docs.example.com/'
      max_size: 10485760  # 10MB to handle PDFs
      database_config:
        type: 'sqlite'
        params:
          db_path: './docs.db'
    ```

### Example Output
A PDF file named "user-guide.pdf" will be converted to Markdown format like:
```markdown
# user-guide

## Page 1
[Content from first page...]

## Page 2
[Content from second page...]
```

The resulting Markdown is then chunked and embedded using the same process as other text content.

## Word Document Processing

Doc2Vec supports processing Microsoft Word documents in both legacy `.doc` format and modern `.docx` format.

### Supported Formats

| Extension | Format | Library Used |
|-----------|--------|--------------|
| `.doc` | Legacy Word (97-2003) | [word-extractor](https://github.com/morungos/node-word-extractor) |
| `.docx` | Modern Word (2007+) | [mammoth](https://github.com/mwilliamson/mammoth.js) |

### Features

*   **Legacy .doc Support:** Extracts plain text from older Word documents using binary parsing
*   **Modern .docx Support:** Converts DOCX files to HTML first (preserving formatting), then to clean Markdown
*   **Formatting Preservation:** For `.docx` files, headings, lists, bold, italic, and links are preserved
*   **Automatic Title:** Uses the filename as an H1 heading for proper document structure
*   **Local File Support:** Processes Word files found in local directories alongside other documents

### Configuration

Include `.doc` and/or `.docx` in your `include_extensions` array:

```yaml
- type: 'local_directory'
  product_name: 'company-docs'
  version: 'current'
  path: './documents'
  include_extensions: ['.doc', '.docx', '.pdf', '.md']
  recursive: true
  max_size: 10485760  # 10MB recommended
  database_config:
    type: 'sqlite'
    params:
      db_path: './company-docs.db'
```

### Example Output

A Word document named "meeting-notes.docx" will be converted to Markdown like:

```markdown
# meeting-notes

## Agenda

1. Review Q4 results
2. Discuss roadmap

## Action Items

- **John:** Prepare budget report
- **Sarah:** Schedule follow-up meeting
```

### Notes

*   **`.doc` files:** Only plain text is extracted. Formatting like bold/italic is not preserved in legacy Word format.
*   **`.docx` files:** Full formatting is preserved including headings, lists, bold, italic, links, and tables.
*   **Embedded Images:** Images embedded in Word documents are not extracted (text-only).

## Now Available via npx

You can run `doc2vec` without cloning the repo or installing it globally. Just use:

```bash
npx doc2vec [path/to/your/config.yaml]
```

This will:

1. Fetch the latest version of doc2vec from npm.

2. Load and process the sources defined in your config.yaml.

3. Generate, embed, and store documentation chunks in the configured database(s).

If you don't specify a config path, it will look for config.yaml in the current working directory.

## Core Logic Flow

1.  **Load Config:** Read and parse `config.yaml`.
2.  **Initialize Logger:** Set up the structured logger.
3.  **Iterate Sources:** For each source in the config:
    1.  **Initialize Database:** Connect to SQLite or Qdrant, create necessary tables/collections.
    2.  **Process by Source Type:**
        - **For Websites:**
          *   Start at the base `url`.
          *   If `sitemap_url` is provided, fetch and parse the sitemap to extract URLs and `lastmod` dates.
          *   Pre-seed the crawl queue with known URLs from the database (ensures link discovery isn't lost when pages are skipped).
          *   For each URL, apply multi-layer change detection:
              1.  If `lastmod` is available (from sitemap), compare against stored value — skip if unchanged.
              2.  Otherwise, send a HEAD request and compare the ETag — skip if unchanged. Adaptive backoff prevents rate limiting.
              3.  If no skip, use Puppeteer (`processPage`) to fetch and render the full page.
          *   For PDF URLs, download and extract text using Mozilla's PDF.js.
          *   Use Readability to extract main content from HTML pages.
          *   Sanitize HTML and convert to Markdown using Turndown.
          *   Discover links from the rendered DOM to add to the crawl queue.
          *   Keep track of all visited URLs.
        - **For GitHub Repositories:**
          *   Fetch issues and comments using the GitHub API.
          *   Convert to formatted Markdown.
          *   Track last run date to support incremental updates.
        - **For Local Directories:**
          *   Recursively scan directories for files matching the configured extensions.
          *   Read file content, converting HTML to Markdown if needed.
          *   For PDF files, extract text using Mozilla's PDF.js and convert to Markdown format with proper page structure.
          *   For Word documents, extract text from `.doc` files or convert `.docx` files to Markdown with formatting.
          *   Process each file's content.
        - **For Zendesk:**
          *   Fetch tickets and articles using the Zendesk API.
          *   Convert tickets to formatted Markdown.
          *   Convert articles to formatted Markdown.
          *   Track last run date to support incremental updates.
    3.  **Process Content:** For each processed page, issue, or file:
        *   **Chunk:** Split Markdown into smaller `DocumentChunk` objects based on headings and size.
        *   **URL-Level Change Detection:** Compute content hashes for all new chunks and compare them against stored hashes for that URL. If all hashes match, the entire URL is skipped (no embedding or DB writes needed).
        *   **Re-process (if changed):** If any hash differs, delete all existing chunks for the URL and re-embed/insert all new chunks. This ensures consistent `chunk_index`/`total_chunks` values and eliminates orphaned chunks when content shifts (e.g., a paragraph is added in the middle).
        *   **Embed:** Call the OpenAI API (`createEmbeddings`) to get the vector embedding for each chunk.
        *   **Store:** Insert the chunk, metadata, hash, and embedding in the database (SQLite `vec_items` table or Qdrant collection).
    4.  **Cleanup:** After processing, remove any obsolete chunks from the database.
4.  **Complete:** Log completion status.

## Recent Changes

### Code Scans: Dangling Symlinks and Incomplete Walks
- **Dangling symlinks no longer abort a directory:** `statSync` follows symlinks, so a link whose target isn't in the clone (common in vendored forks, e.g. `crates/cel-fork/cel/README.md`) threw `ENOENT` and abandoned every remaining entry in that directory. Broken entries are now skipped with a warning and the walk continues
- **Incomplete scans never delete chunks:** if a directory genuinely can't be listed, the scan is reported as incomplete — obsolete-file cleanup, the tracked-file list, the last-mtime marker, and the last-synced git SHA are all skipped, so unscanned files keep their chunks and the next run rescans them
- **Incomplete scans fail the source:** a partial ingest is reported as a failed source instead of a silent success
- **Quieter crawls:** planned browser restarts (initial launch, the every-50-pages recycle) log at `info`; only unplanned restarts (disconnected or degraded browser) warn

### Searchable Controller Run Logs
- **Server-side log filtering:** Level chips and the keyword filter now query Postgres instead of the browser's in-memory buffer, so errors and warnings from the start of a long run are reachable (`GET /api/runs/:id/logs?levels=error,warn&q=...`, paged with **Load 2,000 more**)
- **Whole-run level counts:** New `GET /api/runs/:id/logs/counts` endpoint backs the level chips, which previously only counted the lines still held in the browser
- **Plain-text download:** `GET /api/runs/:id/logs/download` streams the full log (or just the matching lines) as `run-<id>.log`
- **Bounded replay:** The log stream accepts `?tail=N` so opening a 30,000-line run replays only the trailing window instead of the entire history
- **Index:** New `d2v_run_logs (run_id, level, seq)` index keeps level filtering fast on long runs

### Postgres Markdown Store
- **New feature:** Optionally store the generated markdown for each crawled website URL in a Postgres table (`url`, `product_name`, `markdown`, `updated_at`)
- **Top-level configuration:** Configure Postgres connection once at the top level via `connection_string` or individual `host`/`port`/`database`/`user`/`password` fields, with environment variable substitution support
- **Per-source opt-in:** Enable per website source with `markdown_store: true` (disabled by default)
- **First-sync force-processing:** When the markdown store is enabled, pages that aren't yet in the Postgres table bypass lastmod and ETag skip logic, ensuring all pages are stored on the first sync
- **Change-only updates:** On subsequent syncs, only pages with detected content changes (via lastmod/ETag) have their Postgres rows updated
- **404 cleanup:** Pages that return 404 during HEAD checks are automatically removed from the Postgres store

### Incomplete Sync Recovery
- **New feature:** Tracks whether a full sync has ever completed successfully for each website source via a `sync_complete:<url_prefix>` metadata key
- **Interrupted sync handling:** If a sync is killed mid-crawl (process terminated, crash, etc.), the stored ETags/lastmods from the partial run would otherwise cause remaining pages to be skipped permanently. The `sync_complete` flag prevents this — when absent, all pages are force-processed regardless of caching signals
- **Gated on clean completion:** The flag is only set when the crawl completes without network errors (DNS failures, connection refused, timeouts). If the site is unreachable, the next run will force a full sync again
- **Scoped per source:** Each website source has its own `sync_complete` key based on its URL prefix. Changing the source URL naturally triggers a new full sync

### Multi-Layer Change Detection for Websites
- **Sitemap `lastmod` support:** When a sitemap includes `<lastmod>` dates, pages are skipped entirely if the date hasn't changed — no HEAD request, no Puppeteer load, no chunking. One sitemap fetch replaces hundreds of individual HEAD requests.
- **`lastmod` inheritance:** Child URLs without their own `<lastmod>` inherit from the most specific parent directory URL in the sitemap (e.g., `/docs/2.10.x/reference/cli/` inherits from `/docs/2.10.x/`).
- **ETag-based change detection:** For URLs not in the sitemap, a HEAD request compares the ETag header against the stored value. Pages with unchanged ETags are skipped without a full page load.
- **Adaptive HEAD request backoff:** Starts with no delay between HEAD requests. On 429 (rate limit), backs off starting at 200ms and doubles up to 5s. On success, the delay halves back toward zero. Prevents burst rate limiting while maximizing throughput.
- **Processing success gating:** ETag and lastmod values are only stored in metadata when chunking and embedding succeed. If processing fails, the next run will retry the page instead of incorrectly skipping it.
- **`parseSitemap` now returns a `Map<string, string | undefined>`** (URL → lastmod) instead of `string[]`, enabling lastmod data to flow through the pipeline.
- **`processPageContent` callback returns `boolean`** (true = success, false = failure) to signal whether metadata should be persisted.

### Puppeteer Resilience Improvements
- Browser restart escalation when protocol errors persist after page recreation
- Periodic browser restart every 50 pages to prevent memory accumulation
- Added `--disable-dev-shm-usage` Chrome flag (critical in Docker where `/dev/shm` defaults to 64MB)
- Added `--disable-gpu` and `--disable-extensions` flags

### HTTP 429 Retry for Full Page Processing
- Added `Retry-After` header parsing (supports seconds and HTTP-date formats)
- Per-URL retry tracking (max 3 attempts) with configurable delay (30s default, 120s cap)
- Queue re-insertion on retry to allow other URLs to be processed while waiting

### Qdrant Filter Fix
- Fixed `removeChunksByUrlQdrant` to use `match: { value: url }` (exact match) instead of `match: { text: url }` (full-text tokenized search), which was causing cross-URL chunk deletion

### URL Processing Fix
- Fixed `shouldProcessUrl` to return `true` for paths ending with `/`, preventing version-like URLs (e.g., `/app/2.1.x/`) from being incorrectly rejected by `path.extname`

### Tabbed Content Support
- Automatically detects tabbed UI components using the WAI-ARIA tabs pattern (`role="tab"` + `role="tabpanel"`)
- Injects tab labels (e.g., "Anthropic v1/messages", "OpenAI-compatible") as bold headings into each panel's content
- Makes hidden tab panels visible so all tab content is captured, not just the selected tab
- Handles pages with multiple tab groups that share the same panel IDs
- Falls back to positional matching when `aria-controls` attributes are missing
- Works in both the Puppeteer crawl pipeline and the standalone `convertHtmlToMarkdown` method

### URL-Level Change Detection
- Replaced per-chunk hash comparison with URL-level change detection across all source types (website, local directory, code, Zendesk)
- Computes content hashes for all chunks of a URL and compares them against stored hashes in a single DB query
- Unchanged URLs are skipped entirely (no embedding API calls, no DB writes)
- Changed URLs get all old chunks deleted and fresh chunks inserted with correct `chunk_index` and `total_chunks`
- Eliminates orphaned chunks and inconsistent metadata that occurred when content shifted (e.g., a paragraph added in the middle)
- Consolidated four duplicated chunk processing loops into a single shared `processChunksForUrl` method

### Puppeteer Resilience
- Added `protocolTimeout: 60000` to browser launch to fail faster on stuck protocol calls (down from default 180s)
- Added `evaluateWithTimeout` helper that wraps `page.evaluate` calls with a 30-second timeout to prevent indefinite hangs on pages with heavy/infinite JavaScript
- Added `about:blank` navigation between pages to clear lingering JavaScript, timers, WebSocket connections, and event listeners
- Added automatic page recreation after errors: when a page times out or errors, the next URL gets a fresh tab instead of reusing the potentially corrupted page

### Word Document Support
- Added support for legacy `.doc` files using the `word-extractor` library
- Added support for modern `.docx` files using the `mammoth` library
- DOCX files preserve formatting (headings, lists, bold, italic, links)
- Both formats are converted to clean Markdown for embedding

### Page Reconstruction Support
- Added `chunk_index` field to track each chunk's position within a page (0-based)
- Added `total_chunks` field to indicate the total number of chunks per page
- Enables AI agents and applications to fetch additional context or reconstruct full pages
- Works consistently across all content types: websites, GitHub, Zendesk, and local directories

### Improved H1/Title Handling
- Smart H1 preservation ensures page titles aren't stripped by Readability
- Falls back to `article.title` when H1 extraction fails
- Proper heading hierarchy starting from H1 through the document structure

### Enhanced Content Extraction
- Added support for multiple content container selectors (`.docs-content`, `.doc-content`, `.markdown-body`, `article`)
- Cleaner heading text by removing anchor links like `[](#section-id)`
- Better handling of pages where H1 is outside the main content container

### Heading Hierarchy Improvements
- Fixed sparse array issues that caused `NULL` values in heading hierarchy
- Proper breadcrumb generation for nested sections
- Hierarchical context preserved across chunk boundaries
