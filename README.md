# Doc2Vec

[![npm version](https://img.shields.io/npm/v/doc2vec.svg)](https://www.npmjs.com/package/doc2vec)

This project provides a configurable tool (`doc2vec`) to crawl specified websites (typically documentation sites), GitHub repositories, local directories, and Zendesk support systems, extract relevant content, convert it to Markdown, chunk it intelligently, generate vector embeddings using OpenAI, and store the chunks along with their embeddings in a vector database (SQLite with `sqlite-vec` or Qdrant).

The primary goal is to prepare documentation content for Retrieval-Augmented Generation (RAG) systems or semantic search applications.

## Key Features

*   **Website Crawling:** Recursively crawls websites starting from a given base URL.
    * **Sitemap Support:** Extracts URLs from XML sitemaps to discover pages not linked in navigation.
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
*   **Content Extraction:** Uses Puppeteer for rendering JavaScript-heavy pages and `@mozilla/readability` to extract the main article content.
    *   **Smart H1 Preservation:** Automatically extracts and preserves page titles (H1 headings) that Readability might strip as "page chrome", ensuring proper heading hierarchy.
    *   **Flexible Content Selectors:** Supports multiple content container patterns (`.docs-content`, `.doc-content`, `.markdown-body`, `article`, etc.) for better compatibility with various documentation sites.
*   **HTML to Markdown:** Converts extracted HTML to clean Markdown using `turndown`, preserving code blocks and basic formatting.
    *   **Clean Heading Text:** Automatically removes anchor links (like `[](#section-id)`) from heading text for cleaner hierarchy display.
*   **Intelligent Chunking:** Splits Markdown content into manageable chunks based on headings and token limits, preserving context.
*   **Vector Embeddings:** Generates embeddings for each chunk using OpenAI's `text-embedding-3-large` model.
*   **Vector Storage:** Supports storing chunks, metadata, and embeddings in:
    *   **SQLite:** Using `better-sqlite3` and the `sqlite-vec` extension for efficient vector search.
    *   **Qdrant:** A dedicated vector database, using the `@qdrant/js-client-rest`.
*   **Change Detection:** Uses content hashing to detect changes and only re-embeds and updates chunks that have actually been modified.
*   **Incremental Updates:** For GitHub and Zendesk sources, tracks the last run date to only fetch new or updated issues/tickets.
*   **Cleanup:** Removes obsolete chunks from the database corresponding to pages or files that are no longer found during processing.
*   **Configuration:** Driven by a YAML configuration file (`config.yaml`) specifying sites, repositories, local directories, Zendesk instances, database types, metadata, and other parameters.
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
*   **OpenAI API Key:** You need an API key from OpenAI to generate embeddings.
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

    # Required: Your OpenAI API Key
    OPENAI_API_KEY="sk-..."

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
        *   `type`: Either `'website'`, `'github'`, `'local_directory'`, or `'zendesk'`
        
        For websites (`type: 'website'`):
        *   `url`: The starting URL for crawling the documentation site.
        *   `sitemap_url`: (Optional) URL to the site's XML sitemap for discovering additional pages not linked in navigation.
        
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
        
        For Zendesk (`type: 'zendesk'`):
        *   `zendesk_subdomain`: Your Zendesk subdomain (e.g., `'mycompany'` for mycompany.zendesk.com).
        *   `email`: Your Zendesk admin email address.
        *   `api_token`: Your Zendesk API token (reference environment variable as `'${ZENDESK_API_TOKEN}'`).
        *   `fetch_tickets`: (Optional) Whether to fetch support tickets (defaults to `true`).
        *   `fetch_articles`: (Optional) Whether to fetch knowledge base articles (defaults to `true`).
        *   `start_date`: (Optional) Only process tickets/articles updated since this date (e.g., `'2025-01-01'`).
        *   `ticket_status`: (Optional) Filter tickets by status (defaults to `['new', 'open', 'pending', 'hold', 'solved']`).
        *   `ticket_priority`: (Optional) Filter tickets by priority (defaults to all priorities).

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
        sitemap_url: 'https://argo-cd.readthedocs.io/en/stable/sitemap.xml'
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
        max_size: 1048576
        database_config:
          type: 'sqlite'
          params:
            db_path: './zendesk-kb.db'
      
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
    - For websites: Crawl the site, process any sitemaps, extract content from HTML pages and download/process PDF files, convert to Markdown
    - For GitHub repos: Fetch issues and comments, convert to Markdown
    - For local directories: Scan files, process content (converting HTML and PDF files to Markdown if needed)
    - For Zendesk: Fetch tickets and articles, convert to Markdown
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
          *   If `sitemap_url` is provided, fetch and parse the sitemap to extract additional URLs.
          *   Use Puppeteer (`processPage`) to fetch and render HTML for web pages.
          *   For PDF URLs, download and extract text using Mozilla's PDF.js.
          *   Use Readability to extract main content from HTML pages.
          *   Sanitize HTML and convert to Markdown using Turndown.
          *   Use `axios`/`cheerio` on HTML pages to find new links to add to the crawl queue.
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
        *   **Hash Check:** Generate a hash of the chunk content. Check if a chunk with the same ID exists in the DB and if its hash matches.
        *   **Embed (if needed):** If the chunk is new or changed, call the OpenAI API (`createEmbeddings`) to get the vector embedding.
        *   **Store:** Insert or update the chunk, metadata, hash, and embedding in the database (SQLite `vec_items` table or Qdrant collection).
    4.  **Cleanup:** After processing, remove any obsolete chunks from the database.
4.  **Complete:** Log completion status.

## Recent Changes

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