# Doc2Vec

This project provides a configurable tool (`doc2vec`) to crawl specified websites (typically documentation sites), extract relevant content, convert it to Markdown, chunk it intelligently, generate vector embeddings using OpenAI, and store the chunks along with their embeddings in a vector database (SQLite with `sqlite-vec` or Qdrant).

The primary goal is to prepare documentation content for Retrieval-Augmented Generation (RAG) systems or semantic search applications.

## Key Features

*   **Website Crawling:** Recursively crawls websites starting from a given base URL.
*   **Content Extraction:** Uses Puppeteer for rendering JavaScript-heavy pages and `@mozilla/readability` to extract the main article content.
*   **HTML to Markdown:** Converts extracted HTML to clean Markdown using `turndown`, preserving code blocks and basic formatting.
*   **Intelligent Chunking:** Splits Markdown content into manageable chunks based on headings and token limits, preserving context.
*   **Vector Embeddings:** Generates embeddings for each chunk using OpenAI's `text-embedding-3-large` model.
*   **Vector Storage:** Supports storing chunks, metadata, and embeddings in:
    *   **SQLite:** Using `better-sqlite3` and the `sqlite-vec` extension for efficient vector search.
    *   **Qdrant:** A dedicated vector database, using the `@qdrant/js-client-rest`.
*   **Change Detection:** Uses content hashing to detect changes and only re-embeds and updates chunks that have actually been modified.
*   **Cleanup:** Removes obsolete chunks from the database corresponding to pages that are no longer found during a crawl.
*   **Configuration:** Driven by a YAML configuration file (`config.yaml`) specifying sites, database types, metadata, and other parameters.
*   **Structured Logging:** Uses a custom logger (`logger.ts`) with levels, timestamps, colors, progress bars, and child loggers for clear execution monitoring.

## Prerequisites

*   **Node.js:** Version 18 or higher recommended (check `.nvmrc` if available).
*   **npm:** Node Package Manager (usually comes with Node.js).
*   **TypeScript:** As the project is written in TypeScript (`ts-node` is used for execution via `npm start`).
*   **OpenAI API Key:** You need an API key from OpenAI to generate embeddings.
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

    # Optional: Required only if using Qdrant
    QDRANT_API_KEY="your-qdrant-api-key"
    ```

2.  **`config.yaml` file:**
    This file defines the sites to crawl and how to process them. Create a `config.yaml` file (or use a different name and pass it as an argument).

    **Structure:**

    *   `sites`: An array of site configurations.
        *   `url`: The starting URL for crawling the documentation site.
        *   `database_type`: Specifies the storage backend (`sqlite` or `qdrant`).
        *   `product_name`: A string identifying the product (used in metadata).
        *   `version`: A string identifying the product version (used in metadata).
        *   `max_size`: Maximum **raw HTML content size** (in characters) fetched by Puppeteer. If a page's initial HTML exceeds this, it will be skipped *before* performing expensive DOM parsing, Readability, and Markdown conversion. Recommending 1MB (1048576).
        *   `database_params`: Parameters specific to the chosen `database_type`.
            *   For `sqlite`:
                *   `db_path`: (Optional) Path to the SQLite database file. Defaults to `./<product_name>-<version>.db`.
            *   For `qdrant`:
                *   `qdrant_url`: (Optional) URL of your Qdrant instance. Defaults to `http://localhost:6333`.
                *   `qdrant_port`: (Optional) Port for the Qdrant REST API. Defaults to `443` if `qdrant_url` starts with `https`, otherwise `6333`.
                *   `collection_name`: (Optional) Name of the Qdrant collection to use. Defaults to `<product_name>_<version>` (lowercased, spaces replaced with underscores).

    **Example (`config.yaml`):**
    ```yaml
    sites:
      - url: 'https://argo-cd.readthedocs.io/en/stable/'
        database_type: 'sqlite'
        product_name: 'argo'
        version: 'stable'
        max_size: 1048576
        database_params:
          db_path: './vector-dbs/argo-cd.db'

      - url: 'https://istio.io/latest/docs/'
        database_type: 'qdrant'
        product_name: 'Istio'
        version: 'latest'
        max_size: 1048576
        database_params:
          qdrant_url: 'https://your-qdrant-instance.cloud'
          qdrant_port: 6333
          collection_name: 'istio_docs_latest'
      # ... more sites
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
3.  Iterate through each site defined in the config.
4.  Initialize the specified database connection.
5.  Crawl the site.
6.  For each valid page: extract content, convert to Markdown, chunk, check for changes, generate embeddings (if needed), and store/update in the database.
7.  Cleanup obsolete chunks.
8.  Output detailed logs.

## Database Options

### SQLite (`database_type: 'sqlite'`)
*   Uses `better-sqlite3` and `sqlite-vec`.
*   Requires `db_path`.
*   Native compilation might be needed.

### Qdrant (`database_type: 'qdrant'`)
*   Uses `@qdrant/js-client-rest`.
*   Requires `qdrant_url`, `qdrant_port`, `collection_name` and potentially `QDRANT_API_KEY`.

## Core Logic Flow

1.  **Load Config:** Read and parse `config.yaml`.
2.  **Initialize Logger:** Set up the structured logger.
3.  **Iterate Sites:** For each site in the config:
    1.  **Initialize Database:** Connect to SQLite or Qdrant, create necessary tables/collections.
    2.  **Crawl:**
        *   Start at the base `url`.
        *   Use Puppeteer (`processPage`) to fetch and render HTML.
        *   Use Readability to extract main content.
        *   Sanitize HTML.
        *   Convert HTML to Markdown using Turndown.
        *   Use `axios`/`cheerio` on the *original* fetched page (before Puppeteer) to find new links to add to the crawl queue.
        *   Keep track of all visited URLs.
    3.  **Process Content:** For each crawled page's Markdown:
        *   **Chunk:** Split Markdown into smaller `DocumentChunk` objects based on headings and size.
        *   **Hash Check:** Generate a hash of the chunk content. Check if a chunk with the same ID exists in the DB and if its hash matches.
        *   **Embed (if needed):** If the chunk is new or changed, call the OpenAI API (`createEmbeddings`) to get the vector embedding.
        *   **Store:** Insert or update the chunk, metadata, hash, and embedding in the database (SQLite `vec_items` table or Qdrant collection).
    4.  **Cleanup:** After crawling the site, query the database for all chunks associated with that site's URL prefix. Delete any chunks whose URLs were *not* in the set of visited URLs for the current run.
4.  **Complete:** Log completion status.
