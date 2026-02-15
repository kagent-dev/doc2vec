# Test Suite Documentation

## Overview

| Metric | Value |
|--------|-------|
| **Framework** | [Vitest](https://vitest.dev/) v4 |
| **Total tests** | 470 |
| **Test files** | 8 |
| **Total lines** | ~8,000 |
| **Execution time** | ~8s |

## Running Tests

```bash
# Run all tests
npm test

# Run in watch mode
npm run test:watch

# Run with coverage report
npm run test:coverage
```

## Test Files

| File | Tests | Source | Description |
|------|-------|--------|-------------|
| `tests/utils.test.ts` | 64 | `utils.ts` | Hashing, UUID generation, URL utilities, tokenization |
| `tests/logger.test.ts` | 47 | `logger.ts` | Log levels, formatting, colors, child loggers, progress bars |
| `tests/content-processor.test.ts` | 153 | `content-processor.ts` | HTML conversion, chunking, crawling, PDF/DOC processing, tab preprocessing |
| `tests/database.test.ts` | 72 | `database.ts` | SQLite and Qdrant operations, metadata, cleanup, URL-level hash queries |
| `tests/code-chunker.test.ts` | 62 | `code-chunker.ts` | AST-based code chunking, language support, merge behavior, function boundary integrity |
| `tests/doc2vec.test.ts` | 58 | `doc2vec.ts` | Orchestrator class, config loading, source routing, embeddings |
| `tests/mcp-server.test.ts` | 18 | `mcp/src/server.ts` | MCP server helpers, query handlers, SQLite/Qdrant providers, end-to-end |
| `tests/e2e.test.ts` | 3 | `doc2vec.ts`, `content-processor.ts`, `database.ts` | End-to-end integration tests for local directory, code source, and website source |

---

## Test Details

### `tests/utils.test.ts` (64 tests)

#### `generateHash`
- Returns a valid SHA-256 hex string
- Produces deterministic output
- Produces different hashes for different inputs
- Handles empty string, unicode content, very long strings

#### `generateMetadataUUID`
- Returns a valid UUID-format string
- Produces deterministic UUIDs for the same repo
- Produces different UUIDs for different repos
- Sets version nibble to 4

#### `getUrlPrefix`
- Returns origin + pathname
- Handles URL without query/hash, root URL, URLs with port
- Returns original string for invalid URLs

#### `normalizeUrl`
- Strips hash fragments and query parameters
- Leaves clean URLs unchanged
- Handles trailing slashes and invalid URLs

#### `buildUrl`
- Resolves relative URLs (`/path`, `./path`, `sibling`)
- Handles absolute URLs
- Returns empty string for invalid URLs

#### `shouldProcessUrl`
- Returns true for extensionless URLs, `.html`, `.htm`, `.pdf`
- Returns false for `.jpg`, `.css`, `.js`, `.png`
- Case-insensitive extension matching
- Throws on invalid URL (no try/catch in source)

#### `isPdfUrl`
- Detects `.pdf` and `.PDF` URLs
- Returns false for non-PDF, extensionless, and invalid URLs
- Handles URLs with query parameters

#### `isValidUuid`
- Validates v4 and v5 UUIDs
- Rejects invalid format, empty string, wrong version/variant nibbles
- Case insensitive

#### `hashToUuid`
- Converts hex hash to UUID format with version 5 and variant 8
- Deterministic output
- Handles short hashes (< 32 chars) and exact 32-char hashes

#### `tokenize`
- Splits by whitespace keeping separators
- Handles multiple spaces, tabs, newlines
- Handles empty and whitespace-only strings

---

### `tests/logger.test.ts` (47 tests)

#### `constructor`
- Creates logger with default config
- Accepts partial config overrides
- Respects `useTimestamp=false` and `useColor=false`

#### `log level filtering`
- Each level (DEBUG, INFO, WARN, ERROR, NONE) correctly filters messages
- NONE suppresses all output

#### `message formatting`
- Includes module name, log level, and timestamp in output
- Pads level names to 5 characters
- Formats Error objects with stack trace
- Pretty-prints objects when `prettyPrint=true`
- Handles unserializable (circular) objects
- Formats args inline when `prettyPrint=false`
- Formats objects as `[object Object]` when `prettyPrint=false`

#### `colorization`
- Applies correct ANSI codes: gray (DEBUG), blue (INFO), yellow (WARN), red (ERROR)
- Skips color when `useColor=false`

#### `child`
- Creates child logger with prefixed name (e.g., `parent:child`)
- Inherits config from parent
- Supports nested children (e.g., `a:b:c`)

#### `section`
- Outputs visual section separator with `=` characters
- Returns logger for chaining (including at WARN level)
- Does not output when level is above INFO

#### `progress`
- Creates progress tracker with `update` and `complete` methods
- Logs progress percentage, progress bar, ETA, and custom messages
- Caps percentage at 100
- Uses green ANSI color in progress bar when `useColor=true`
- Uses default "Completed" message when `complete()` called without arguments
- Does not log when level is above INFO

#### `LogLevel`
- Correct numeric values (DEBUG=0, INFO=1, WARN=2, ERROR=3, NONE=100)
- Maintains ordering

#### Edge cases
- Empty `moduleName` omits module prefix
- `defaultLogger` export has expected methods

---

### `tests/content-processor.test.ts` (153 tests)

#### `convertHtmlToMarkdown`
- Converts headings (H1-H6), bold, italic, links, lists, code blocks, inline code, blockquotes
- Handles tables with pipe escaping and empty cells
- Strips disallowed tags (img, script)
- Cleans up code block indentation
- Returns empty string for empty input

#### `chunkMarkdown`
- Creates chunks with breadcrumb context prefix (`[Topic: ...]`)
- Assigns correct metadata (product_name, version, url, chunk_index, total_chunks)
- Generates deterministic chunk IDs
- Tracks heading hierarchy
- Merges small sections (semantic merging)
- Splits large sections with 10% overlap (verified via content comparison)
- Cleans anchor links from headings in hierarchy
- Assigns sequential chunk_index values
- Uses "Introduction" as default section when no heading
- Safety valve: flushes content without headings when exceeding MAX_TOKENS
- Uses parent H2 hierarchy when merging multiple sibling H3 sections

#### `chunkCode`
- Creates chunks with `[File: ...]` context prefix
- Assigns correct metadata, normalizes backslashes in file paths
- Includes branch/repo when provided, omits when not
- Falls back to TokenChunker when CodeChunker fails

#### `parseSitemap`
- Parses standard sitemaps with `<url><loc>` entries
- Handles nested sitemaps (`<sitemap><loc>` entries with recursive processing)
- Returns empty array on axios error or empty sitemap

#### `crawlWebsite`
- Skips already-visited URLs (deduplication)
- Skips URLs with unsupported extensions
- Sets `hasNetworkErrors=true` when a network error occurs
- Integrates sitemap URLs when `sitemap_url` is configured
- Discovers links from crawled pages and adds them to the queue

#### `processPage`
- Routes PDF URLs to `downloadAndConvertPdfFromUrl`
- Returns null when PDF or HTML content exceeds `max_size`
- Returns null when PDF processing throws an error
- Returns null when Readability fails to parse
- Returns null when Puppeteer throws an error

#### `detectCodeLanguage`
- Maps all 34 extensions to correct languages:
  `.ts`/`.tsx` (TypeScript), `.js`/`.jsx`/`.mjs`/`.cjs` (JavaScript),
  `.py` (Python), `.go` (Go), `.rs` (Rust), `.java` (Java),
  `.kt`/`.kts` (Kotlin), `.swift` (Swift), `.c` (C),
  `.cc`/`.cpp`/`.h`/`.hpp` (C++), `.cs` (C#), `.rb` (Ruby),
  `.php` (PHP), `.scala` (Scala), `.sql` (SQL),
  `.sh`/`.bash`/`.zsh` (Bash), `.html` (HTML),
  `.css`/`.less` (CSS), `.scss`/`.sass` (SCSS),
  `.json` (JSON), `.yaml`/`.yml` (YAML), `.md` (Markdown)
- Returns undefined for unknown and missing extensions

#### `processDirectory` error handling
- Does not throw when directory does not exist
- Handles individual file read errors gracefully

#### `processCodeDirectory` error handling
- Does not throw when directory does not exist
- Handles individual file read errors gracefully

#### `convertPdfToMarkdown`
- Throws on non-existent file
- Throws on invalid PDF data

#### `downloadAndConvertPdfFromUrl`
- Downloads PDF and converts to markdown (mocked axios + pdfjs-dist)
- Throws on HTTP error status
- Throws on network error

#### `convertDocToMarkdown`
- Converts DOC file to markdown (mocked word-extractor)
- Throws on error

#### `convertDocxToMarkdown`
- Converts DOCX file to markdown with mammoth warnings (mocked mammoth)
- Throws on error

#### `markCodeParents`
- No-op for null node
- Marks nodes containing `<pre>` elements with article-content class
- Recursively marks parent elements
- Does not mark nodes without `<pre>` or `<code>` elements

#### `network error detection`
- Detects ENOTFOUND, ECONNREFUSED, ETIMEDOUT, ECONNRESET, EHOSTUNREACH, ENETUNREACH
- Detects axios errors without response
- Detects network-related error messages (getaddrinfo, timeout, connection, dns)
- Does not detect axios errors with response or non-network errors
- Handles null/undefined error

#### `preprocessTabs`
- Injects tab labels into panels matched by `aria-controls`
- Uses positional fallback when `aria-controls` is missing
- Removes tab buttons after processing
- Makes hidden panels visible by removing `data-state`
- Removes common hiding CSS classes from panels (`hx-hidden`, `d-none`)
- Handles inline `display:none` style
- Does nothing when no `role="tab"` elements exist
- Skips tabs with empty labels
- Works with Hextra-style tab HTML structure
- Marks panels with `article-content` class for Readability
- Does NOT duplicate labels when multiple tab groups share the same panel IDs

#### `convertHtmlToMarkdown with tabs`
- Converts tabbed HTML to markdown with tab labels
- Does not affect HTML without tabs

#### `tabs through full processPage pipeline`
- Does NOT duplicate tab labels through full `processPage` pipeline (Puppeteer + JSDOM + Readability)

---

### `tests/database.test.ts` (72 tests)

#### `initDatabase`
- **SQLite**: Generates default `db_path` from product_name and version; uses custom `db_path`; returns `SqliteDB` connection
- **Qdrant**: Generates default collection name; uses custom collection name; calls `createCollectionQdrant`; returns `QdrantDB` connection
- **Unsupported type**: Throws for unknown database type

#### `initDatabaseMetadata`
- SQLite: Creates `vec_metadata` table; safe to call multiple times
- Qdrant: No-op (just logs)

#### `metadata CRUD (SQLite)`
- Returns default value when key does not exist
- Returns undefined when no default provided
- Sets and gets metadata values
- Upserts (overwrites) metadata values

#### `last run date`
- **SQLite**: Returns default when no date exists; updates and retrieves date; normalizes repo names in keys
- **Qdrant**: Returns date when found; returns default when not found; handles retrieve error gracefully

#### `updateLastRunDate`
- **Qdrant**: Calls upsert with correct metadata point structure; handles upsert error gracefully

#### `prepareSQLiteStatements`
- Returns all required statements (insert, checkHash, update, getAllChunkIds, deleteChunk)
- Detects branch and repo columns
- Caches column detection results

#### `insertVectorsSQLite`
- Inserts chunk with embedding
- Handles duplicate `chunk_id` inserts gracefully (vec0 behavior)
- Stores heading hierarchy as JSON
- Stores branch/repo when provided; stores empty string when not
- Uses provided `chunkHash` or generates from content

#### `removeObsoleteChunksSQLite`
- Deletes chunks for URLs no longer visited
- Preserves chunks when all URLs visited
- Does not affect chunks outside the URL prefix

#### `removeChunksByUrlSQLite`
- Deletes all chunks matching a specific URL
- Does not error when no chunks match

#### `getChunkHashesByUrlSQLite`
- Returns sorted hashes for chunks matching a URL
- Returns empty array when no chunks match
- Returns duplicate hashes correctly (preserves all entries)

#### `removeObsoleteFilesSQLite`
- Direct path mode: removes chunks for deleted files
- URL rewrite mode: removes chunks for deleted files
- Handles trailing slash in `url_rewrite_prefix`
- Cleans `./` prefix from path config

#### Qdrant operations (mocked)
- `storeChunkInQdrant`: Calls upsert with correct payload; converts non-UUID chunk_id; uses `crypto.randomUUID()` fallback; generates hash when not provided; handles upsert errors
- `createCollectionQdrant`: Creates collection; skips when exists; handles "already exists" error
- `removeObsoleteChunksQdrant`: Deletes obsolete chunks; skips metadata points; handles scroll/delete errors
- `removeChunksByUrlQdrant`: Deletes by URL filter; handles delete errors
- `removeObsoleteFilesQdrant`: Direct file path mode; URL rewrite mode; no-op when no obsolete files; error handling
- Metadata: Sets and gets values in Qdrant; returns default when not found

#### `hasColumn` (private method)
- Returns false when PRAGMA fails
- Returns false when column does not exist
- Returns true when column exists

#### Error handling
- `setMetadataValue`: Handles SQLite and Qdrant errors gracefully
- `getMetadataValue`: Outer catch returns default value

---

### `tests/code-chunker.test.ts` (62 tests)

#### `create`
- Creates a CodeChunker instance
- Uses default chunkSize of 512
- Throws when chunkSize is 0 or negative
- Accepts a custom tokenCounter
- Warns if `Parser.init` throws and still proceeds

#### `chunk`
- Returns empty array for empty/whitespace-only input
- Chunks TypeScript, JavaScript, and Python code
- Includes tokenCount for each chunk
- Respects chunkSize limit (approximately)
- Preserves code content (no data loss)
- Handles mismatched language (TypeScript code with Python parser)

#### `merge behavior`
- Merges small adjacent chunks
- Does not merge chunks exceeding chunkSize
- Skips empty/whitespace chunks during merge
- Splits many small functions into multiple non-empty chunks across chunkSize boundary

#### `language support`
- Handles Go, Rust, JSON, CSS code
- Throws for unsupported languages
- Throws for missing WASM file (`nonexistent_lang_xyz`)
- Normalizes hyphenated language names (`c-sharp` -> `c_sharp`)

#### `function boundary integrity` (33 tests)
Verifies that complete function/class/method definitions remain intact within a single chunk and are never split mid-body. Uses a helper `assertBoundaryIntegrity` that:
1. **Enforces chunking actually occurred** (`chunks.length > 1`) — prevents false positives where all code fits in one chunk
2. **Checks every chunk containing a construct's opening also contains its closing** — proves the boundary was respected

Every test is designed so that total code exceeds `chunkSize` (forcing the chunker to split), while each individual construct fits within `chunkSize` (so the chunker *can* keep it intact).

**TypeScript** (6 tests):
- Standalone functions (`function add`, `function multiply`, `function greet`)
- Arrow functions (`const double`, `const triple`, `const quadruple`)
- Interfaces (`interface User`, `interface Product`, `interface Order`)
- Class methods when class is split (`add`, `subtract`, `getHistory`)
- Async functions (`async function fetchUser`, `async function fetchProducts`)
- Enum declarations (`enum Direction`, `enum Color`, `enum Status`)

**JavaScript** (2 tests):
- Standalone functions (`fibonacci`, `factorial`)
- Class definitions with methods (`EventEmitter.constructor`, `on`, `emit`)

**Python** (3 tests):
- Function definitions (`binary_search`, `merge_sort`)
- Class methods when class is split (`Stack.__init__`, `push`, `pop`, `is_empty`)
- Decorated functions (`@decorator` + `say_hello`, `say_goodbye`)

**Go** (2 tests):
- Function definitions (`fibonacci`, `isPrime`, `main`)
- Struct methods (`Point.Distance`, `Point.Translate`, `NewPoint`)

**Rust** (2 tests):
- Function definitions (`gcd`, `lcm`, `is_palindrome`)
- Impl block methods when impl is split (`Rectangle.new`, `area`, `perimeter`)

**Java** (1 test):
- Method definitions (`factorial`, `isPrime`, `fibonacci`)

**Kotlin** (1 test):
- Function definitions (`fibonacci`, `isPalindrome`, `gcd`)

**Ruby** (2 tests):
- Method definitions (`binary_search`, `quick_sort`)
- Class methods (`LinkedList.initialize`, `push`, `pop`)

**C** (1 test):
- Function definitions (`factorial`, `swap`, `main`)

**C++** (1 test):
- Standalone function definitions (`factorial`, `isPrime`, `average`)

**Swift** (1 test):
- Function definitions (`fibonacci`, `isPrime`)

**PHP** (1 test):
- Function definitions (`fibonacci`, `isPalindrome`, `arraySum`)

**Scala** (1 test):
- Function definitions (`factorial`, `fibonacci`, `gcd`)

**C#** (1 test):
- Method bodies (`Reverse`, `IsPalindrome`, `CountWords`)

**Dart** (1 test):
- Function bodies (`fibonacci`, `isPrime`, `range`)

**Zig** (1 test):
- Function definitions (`fibonacci`, `factorial`)

**Lua** (1 test):
- Function bodies (`fibonacci`, `factorial`)

**Elixir** (1 test):
- Multi-line function definitions (`factorial`, `fibonacci`)

**Large function exceeding chunkSize** (2 tests):
- TypeScript: large function splits at statement level, each `const` declaration stays intact
- Python: large function splits at statement level, each assignment stays intact

**Construct isolation** (2 tests):
- Two functions that individually fit but can't merge together stay in separate chunks (TypeScript)
- Two tiny functions that fit together are correctly merged into one chunk (JavaScript)

#### Edge cases
- Parser cache reuse (two chunkers with same language)
- Indivisible large leaf node (single node exceeding chunkSize still returned)
- No empty chunks from whitespace-heavy code

---

### `tests/doc2vec.test.ts` (58 tests)

#### `constructor`
- Creates Logger, loads config, initializes OpenAI client, initializes ContentProcessor

#### `loadConfig`
- Reads and parses YAML config file
- Substitutes environment variables (found, missing, multiple)
- Parses multiple sources
- Calls `process.exit(1)` when config file does not exist
- Calls `process.exit(1)` when config file has invalid YAML

#### `config validation: version handling`
- Uses branch as version for code source when version is empty
- Uses "local" as version when both version and branch are missing
- Keeps existing version when already set
- Calls `process.exit(1)` for non-code source (website, github) with missing version
- Handles empty branch string (falls back to "local")

#### `buildCodeShaMetadataKey` (actual class method)
- Normalizes repo and branch names (replaces non-alphanumeric with `_`)
- Handles special characters, dots, underscores, consecutive special characters
- Produces deterministic keys

#### `buildCodeFileUrl` (actual class method)
- Uses `repoUrlPrefix` when available (takes priority over `url_rewrite_prefix`)
- Uses `url_rewrite_prefix` when no `repoUrlPrefix`
- Handles trailing slash in `url_rewrite_prefix`
- Falls back to `file://` when file is outside base path or no prefix configured
- Handles nested file paths

#### `createEmbeddings` (actual class method)
- Returns embeddings on success (single and multiple texts)
- Returns empty array on API error or network timeout

#### `run()`
- Routes each source type to its correct processor (website, github, local_directory, code, zendesk)
- Logs error for unknown source type
- Processes multiple sources in order

#### `getGitChangedFiles` (actual class method)
- Returns result with mode, changedFiles (Set), and deletedPaths (array)

#### `git diff parsing logic`
- Parses added, modified, deleted, and renamed files
- Handles mixed diff output
- Skips empty lines

#### `fetchWithRetry logic`
- Implements exponential backoff (delay doubles each attempt)

#### Edge cases
- Handles empty sources array
- Config with only code sources
- Creates embeddings with correct model name (`text-embedding-3-large`)

#### Mocks specific to `doc2vec.test.ts`
- `DatabaseManager` is fully mocked with: `initDatabase`, `initDatabaseMetadata`, `prepareSQLiteStatements`, `insertVectorsSQLite`, `removeObsoleteChunksSQLite`, `removeObsoleteFilesSQLite`, `removeChunksByUrlSQLite`, `removeChunksByUrlQdrant`, `getChunkHashesByUrlSQLite`, `getChunkHashesByUrlQdrant`, `getMetadataValue`, `setMetadataValue`, `getLastRunDate`, `updateLastRunDate`, `storeChunkInQdrant`, `createCollectionQdrant`, `removeObsoleteChunksQdrant`, `removeObsoleteFilesQdrant`

---

### `tests/mcp-server.test.ts` (18 tests)

#### `MCP server helpers`
- `normalizeExtensions` normalizes extensions to lowercase and dot-prefixed
- `filterResultsByUrl` filters results by URL prefix and extensions
- `filterResultsWithContent` filters results with empty or non-string content

#### `MCP query handlers`
- Returns validation message when `query_documentation` params are missing
- Filters empty content and URL prefix in `queryDocumentation`
- Returns empty-content warning for `query_code` when all matches are empty
- Formats `get_chunks` results with chunk index

#### `SQLite provider compatibility`
- Falls back when `chunk_index` column is missing (old database schema)
- Resolves DB paths with normalized extension

#### `Qdrant provider`
- Maps `dbName` to collection and returns search results
- Scrolls chunks and sorts by `chunk_index`

#### `MCP server end-to-end`
- Full pipeline: starts local HTTP server, fetches HTML, converts to Markdown, chunks, stores in SQLite, queries via MCP handler, and verifies the unique phrase is returned in results

---

### `tests/e2e.test.ts` (3 tests)

End-to-end integration tests that validate the full processing pipeline — from source content through chunking, hashing, embedding, and storage — with URL-level change detection. Each test creates 3 documents, processes them, modifies only the 2nd, re-processes all 3, and verifies that only the modified document is re-embedded while the others are skipped entirely.

**Components used (real, not mocked):**
- SQLite with `better-sqlite3` + `sqlite-vec` (in-memory databases)
- `ContentProcessor` (real chunking, HTML conversion, Puppeteer rendering)
- `DatabaseManager` (real `insertVectorsSQLite`, `getChunkHashesByUrlSQLite`, `removeChunksByUrlSQLite`)
- Chonkie + Tree-sitter (real AST-based code chunking for code source test)
- Puppeteer (real headless browser for website source test)
- Local HTTP server (for website source test)

**Only embeddings are stubbed:** deterministic `Array(3072).fill(0.1)` with call tracking via `createEmbeddingTracker`.

#### `E2E: Local Directory (Markdown)`
- Creates 3 Markdown files in a temp directory
- First run: all 3 files are chunked, embedded, and stored
- Modifies only `doc2.md` (appends a paragraph)
- Second run: only `doc2.md` is re-embedded; `doc1.md` and `doc3.md` are skipped
- Verifies `chunk_index`/`total_chunks` consistency for all files
- Verifies `doc1` and `doc3` chunks are byte-identical across runs
- Verifies no orphaned chunks (total DB count equals sum of per-URL counts)

#### `E2E: Code Source (Local Directory)`
- Creates 3 TypeScript files in a temp directory (`greet`/`farewell`, `Calculator` class, `fetchData` async function)
- Uses real Chonkie + Tree-sitter for AST-aware code chunking
- First run: all 3 files are chunked, embedded, and stored
- Modifies only `file2.ts` (adds `subtract` and `divide` methods to Calculator)
- Second run: only `file2.ts` is re-embedded; `file1.ts` and `file3.ts` are skipped
- Verifies `chunk_index`/`total_chunks` consistency for all files
- Verifies `file1` and `file3` chunks are byte-identical across runs
- Verifies no orphaned chunks

#### `E2E: Website Source` (60s timeout)
- Starts a local HTTP server serving 3 HTML pages
- Uses real Puppeteer to render each page via `processPage`
- First run: all 3 pages are processed, chunked, embedded, and stored
- Modifies only page 2 content (adds a paragraph via mutable `pageContent` map)
- Second run: only page 2 is re-embedded; pages 1 and 3 are skipped
- Verifies `chunk_index`/`total_chunks` consistency for all pages
- Verifies page 1 and page 3 chunks are byte-identical across runs
- Verifies no orphaned chunks

---

## Mocking Strategy

### External dependencies mocked via `vi.mock()`:
- `openai` (OpenAI client)
- `puppeteer` (browser automation)
- `axios` (HTTP requests)
- `child_process` (git commands)

### Inline mock objects:
- Qdrant client (`retrieve`, `upsert`, `scroll`, `delete`, `getCollections`, `createCollection`)
- PDF.js (`getDocument`, `getPage`, `getTextContent`)
- Word extractors (`word-extractor`, `mammoth`)

### Real instances used:
- SQLite with `better-sqlite3` + `sqlite-vec` (in-memory databases)
- `ContentProcessor` with suppressed logger (`LogLevel.NONE`)
- `TurndownService` (HTML-to-Markdown conversion)
- `web-tree-sitter` (AST parsing for code chunking)
- Puppeteer (E2E website test uses real headless browser)
- Chonkie + Tree-sitter (E2E code test uses real AST-based chunking)
- Local HTTP server (E2E website test serves pages via `http.createServer`)
- Filesystem operations (temp directories created/cleaned per test)

### Private method access:
- `(instance as any).privateMethod()` pattern used for `buildCodeFileUrl`, `buildCodeShaMetadataKey`, `createEmbeddings`, `detectCodeLanguage`, `markCodeParents`, `processPage`, `convertPdfToMarkdown`, `downloadAndConvertPdfFromUrl`, `convertDocToMarkdown`, `convertDocxToMarkdown`, `hasColumn`, `isNetworkError`
