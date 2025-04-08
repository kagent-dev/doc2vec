# Doc2Vec

This project provides a configurable tool (`doc2vec`) to crawl specified websites (typically documentation sites), GitHub repositories, and local directories, extract relevant content, convert it to Markdown, chunk it intelligently, generate vector embeddings using OpenAI, and store the chunks along with their embeddings in a vector database (SQLite with `sqlite-vec` or Qdrant).

The primary goal is to prepare documentation content for Retrieval-Augmented Generation (RAG) systems or semantic search applications.

## Key Features

*   **Website Crawling:** Recursively crawls websites starting from a given base URL.
*   **GitHub Issues Integration:** Retrieves GitHub issues and comments, processing them into searchable chunks.
*   **Local Directory Processing:** Scans local directories for files, converts content to searchable chunks.
*   **Content Extraction:** Uses Puppeteer for rendering JavaScript-heavy pages and `@mozilla/readability` to extract the main article content.
*   **HTML to Markdown:** Converts extracted HTML to clean Markdown using `turndown`, preserving code blocks and basic formatting.
*   **Intelligent Chunking:** Splits Markdown content into manageable chunks based on headings and token limits, preserving context.
*   **Vector Embeddings:** Generates embeddings for each chunk using OpenAI's `text-embedding-3-large` model.
*   **Vector Storage:** Supports storing chunks, metadata, and embeddings in:
    *   **SQLite:** Using `better-sqlite3` and the `sqlite-vec` extension for efficient vector search.
    *   **Qdrant:** A dedicated vector database, using the `@qdrant/js-client-rest`.
*   **Change Detection:** Uses content hashing to detect changes and only re-embeds and updates chunks that have actually been modified.
*   **Incremental Updates:** For GitHub sources, tracks the last run date to only fetch new or updated issues.
*   **Cleanup:** Removes obsolete chunks from the database corresponding to pages or files that are no longer found during processing.
*   **Configuration:** Driven by a YAML configuration file (`config.yaml`) specifying sites, repositories, local directories, database types, metadata, and other parameters.
*   **Structured Logging:** Uses a custom logger (`logger.ts`) with levels, timestamps, colors, progress bars, and child loggers for clear execution monitoring.

## Prerequisites

*   **Node.js:** Version 18 or higher recommended (check `.nvmrc` if available).
*   **npm:** Node Package Manager (usually comes with Node.js).
*   **TypeScript:** As the project is written in TypeScript (`ts-node` is used for execution via `npm start`).
*   **OpenAI API Key:** You need an API key from OpenAI to generate embeddings.
*   **GitHub Personal Access Token:** Required for accessing GitHub issues (set as `GITHUB_PERSONAL_ACCESS_TOKEN` in your environment).
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

    # Required: Your OpenAI API Key
    OPENAI_API_KEY="sk-..."

    # Required for GitHub sources
    GITHUB_PERSONAL_ACCESS_TOKEN="ghp_..."

    # Optional: Required only if using Qdrant
    QDRANT_API_KEY="your-qdrant-api-key"
    ```

2.  **`config.yaml` file:**
    This file defines the sources to process and how to handle them. Create a `config.yaml` file (or use a different name and pass it as an argument).

    **Structure:**

    *   `sources`: An array of source configurations.
        *   `type`: Either `'website'`, `'github'`, or `'local_directory'`
        
        For websites (`type: 'website'`):
        *   `url`: The starting URL for crawling the documentation site.
        
        For GitHub repositories (`type: 'github'`):
        *   `repo`: Repository name in the format `'owner/repo'` (e.g., `'istio/istio'`).
        *   `start_date`: (Optional) Starting date to fetch issues from (e.g., `'2025-01-01'`).
        
        For local directories (`type: 'local_directory'`):
        *   `path`: Path to the local directory to process.
        *   `include_extensions`: (Optional) Array of file extensions to include (e.g., `['.md', '.txt']`). Defaults to `['.md', '.txt', '.html', '.htm']`.
        *   `exclude_extensions`: (Optional) Array of file extensions to exclude.
        *   `recursive`: (Optional) Whether to traverse subdirectories (defaults to `true`).
        *   `encoding`: (Optional) File encoding to use (defaults to `'utf8'`).
        
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

    **Example (`config.yaml`):**
    ```yaml
    sources:
      # Website source example
      - type: 'website'
        product_name: 'argo'
        version: 'stable'
        url: 'https://argo-cd.readthedocs.io/en/stable/'
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
        include_extensions: ['.md', '.txt']
        recursive: true
        max_size: 1048576
        database_config:
          type: 'sqlite'
          params:
            db_path: './project-docs.db'
      
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
    - For websites: Crawl the site, extract content, convert to Markdown
    - For GitHub repos: Fetch issues and comments, convert to Markdown
    - For local directories: Scan files, process content (converting HTML to Markdown if needed)
6.  For all sources: Chunk content, check for changes, generate embeddings (if needed), and store/update in the database.
7.  Cleanup obsolete chunks.
8.  Output detailed logs.

## Database Options

### SQLite (`database_config.type: 'sqlite'`)
*   Uses `better-sqlite3` and `sqlite-vec`.
*   Requires `db_path`.
*   Native compilation might be needed.

### Qdrant (`database_config.type: 'qdrant'`)
*   Uses `@qdrant/js-client-rest`.
*   Requires `qdrant_url`, `qdrant_port`, `collection_name` and potentially `QDRANT_API_KEY`.

## Core Logic Flow

1.  **Load Config:** Read and parse `config.yaml`.
2.  **Initialize Logger:** Set up the structured logger.
3.  **Iterate Sources:** For each source in the config:
    1.  **Initialize Database:** Connect to SQLite or Qdrant, create necessary tables/collections.
    2.  **Process by Source Type:**
        - **For Websites:**
          *   Start at the base `url`.
          *   Use Puppeteer (`processPage`) to fetch and render HTML.
          *   Use Readability to extract main content.
          *   Sanitize HTML.
          *   Convert HTML to Markdown using Turndown.
          *   Use `axios`/`cheerio` on the *original* fetched page (before Puppeteer) to find new links to add to the crawl queue.
          *   Keep track of all visited URLs.
        - **For GitHub Repositories:**
          *   Fetch issues and comments using the GitHub API.
          *   Convert to formatted Markdown.
          *   Track last run date to support incremental updates.
        - **For Local Directories:**
          *   Recursively scan directories for files matching the configured extensions.
          *   Read file content, converting HTML to Markdown if needed.
          *   Process each file's content.
    3.  **Process Content:** For each processed page, issue, or file:
        *   **Chunk:** Split Markdown into smaller `DocumentChunk` objects based on headings and size.
        *   **Hash Check:** Generate a hash of the chunk content. Check if a chunk with the same ID exists in the DB and if its hash matches.
        *   **Embed (if needed):** If the chunk is new or changed, call the OpenAI API (`createEmbeddings`) to get the vector embedding.
        *   **Store:** Insert or update the chunk, metadata, hash, and embedding in the database (SQLite `vec_items` table or Qdrant collection).
    4.  **Cleanup:** After processing, remove any obsolete chunks from the database.
4.  **Complete:** Log completion status.