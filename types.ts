// Base configuration that applies to all source types
export interface BaseSourceConfig {
    type: 'website' | 'github' | 'local_directory' | 'zendesk' | 'code' | 's3';
    product_name: string;
    version: string;
    max_size: number;
    database_config: DatabaseConfig;
}

// Configuration specific to local directory sources
export interface LocalDirectorySourceConfig extends BaseSourceConfig {
    type: 'local_directory';
    path: string;                  // Path to the local directory
    include_extensions?: string[]; // File extensions to include (e.g., ['.md', '.txt', '.pdf'])
    exclude_extensions?: string[]; // File extensions to exclude
    recursive?: boolean;           // Whether to traverse subdirectories
    encoding?: BufferEncoding;     // File encoding (default: 'utf8')
    url_rewrite_prefix?: string;   // Optional URL prefix to rewrite file:// URLs (e.g., 'https://mydomain.com')
}

// Configuration specific to website sources
export interface WebsiteSourceConfig extends BaseSourceConfig {
    type: 'website';
    url: string;
    sitemap_url?: string; // Optional sitemap URL to extract additional URLs to crawl
    markdown_store?: boolean; // Enable storing generated markdown in Postgres (default: false)
}

// Configuration specific to GitHub repo sources
export interface GithubSourceConfig extends BaseSourceConfig {
    type: 'github';
    repo: string;
    start_date?: string;
}

// Configuration specific to Zendesk sources
export interface ZendeskSourceConfig extends BaseSourceConfig {
    type: 'zendesk';
    zendesk_subdomain: string;     // e.g., 'mycompany' for mycompany.zendesk.com
    email: string;                 // Zendesk user email for authentication
    api_token: string;             // Zendesk API token
    fetch_tickets?: boolean;       // Whether to fetch tickets (default: true)
    fetch_articles?: boolean;      // Whether to fetch help center articles (default: true)
    start_date?: string;           // For incremental updates (default: start of current year)
    ticket_status?: string[];      // Filter tickets by status (default: ['new', 'open', 'pending', 'hold', 'solved'])
    ticket_priority?: string[];    // Filter tickets by priority (default: all)
    excluded_organizations?: string[]; // Organization names whose tickets should be skipped
    include_internal_comments?: boolean; // Include non-public (internal/agent-only) comments (default: false)
}

// Configuration specific to code sources (local directory or GitHub repo)
export interface CodeSourceConfig extends BaseSourceConfig {
    type: 'code';
    source: 'local_directory' | 'github';
    path?: string;                 // Path to the local directory (when source=local_directory)
    repo?: string;                 // Repo in 'owner/repo' format (when source=github)
    branch?: string;               // Optional branch to clone (github only)
    include_extensions?: string[]; // File extensions to include (e.g., ['.ts', '.py'])
    exclude_extensions?: string[]; // File extensions to exclude
    recursive?: boolean;           // Whether to traverse subdirectories
    encoding?: BufferEncoding;     // File encoding (default: 'utf8')
    url_rewrite_prefix?: string;   // Optional URL prefix to rewrite file:// URLs
    chunk_size?: number;           // Optional chunk size for Chonkie
}

// Configuration specific to S3 sources
export interface S3SourceConfig extends BaseSourceConfig {
    type: 's3';
    bucket: string;                // S3 bucket name
    prefix?: string;               // Optional key prefix to filter objects (e.g., 'docs/')
    region?: string;               // AWS region (default: AWS_DEFAULT_REGION env var or 'us-east-1')
    endpoint?: string;             // Custom S3 endpoint for S3-compatible services (MinIO, LocalStack)
    include_extensions?: string[]; // File extensions to include (e.g., ['.md', '.txt', '.pdf'])
    exclude_extensions?: string[]; // File extensions to exclude
    encoding?: BufferEncoding;     // Text file encoding (default: 'utf8')
    url_rewrite_prefix?: string;   // Optional URL prefix to rewrite s3:// URLs
}

// Union type for all possible source configurations
export type SourceConfig = WebsiteSourceConfig | GithubSourceConfig | LocalDirectorySourceConfig | ZendeskSourceConfig | CodeSourceConfig | S3SourceConfig;

// Database configuration
export interface DatabaseConfig {
    type: 'sqlite' | 'qdrant';
    params: SqliteDatabaseParams | QdrantDatabaseParams;
}

export interface SqliteDatabaseParams {
    db_path?: string;  // Optional, will use default if not provided
}

export interface QdrantDatabaseParams {
    qdrant_url?: string;
    qdrant_port?: number;
    collection_name?: string;
}

export interface EmbeddingConfig {
    provider: 'openai' | 'azure';
    dimension?: number;
    openai?: {
        api_key?: string;   // Can also use OPENAI_API_KEY env var
        model?: string;     // Default: text-embedding-3-large
        base_url?: string;  // Override OpenAI API base URL — useful for Ollama or other OpenAI-compatible endpoints. Can also use OPENAI_BASE_URL env var.
    };
    azure?: {
        api_key?: string;        // Can also use AZURE_OPENAI_KEY env var
        endpoint?: string;       // Can also use AZURE_OPENAI_ENDPOINT env var
        deployment_name?: string; // Can also use AZURE_OPENAI_DEPLOYMENT_NAME env var
        api_version?: string;    // Default: 2024-10-21
    };
}

// Postgres markdown store configuration (top-level)
export interface MarkdownStoreConfig {
    connection_string?: string;   // e.g., 'postgres://user:pass@host:5432/db'
    host?: string;
    port?: number;
    database?: string;
    user?: string;
    password?: string;            // Can use ${PG_PASSWORD} env var substitution
    table_name?: string;          // Defaults to 'markdown_pages'
}

export interface Config {
    name?: string;                // Optional display name (used by controller mode; defaults to file basename)
    schedule?: string;            // Optional cron expression — controller mode runs this config on the schedule; ignored in one-shot mode
    sources: SourceConfig[];
    embedding?: EmbeddingConfig;  // Optional, defaults to OpenAI
    markdown_store?: MarkdownStoreConfig;  // Optional Postgres markdown store
}

export interface DocumentChunk {
    content: string;
    metadata: {
        product_name: string;
        version: string;
        branch?: string;
        repo?: string;
        heading_hierarchy: string[];
        section: string;
        chunk_id: string;
        url: string;
        hash?: string;
        chunk_index: number;   // Position of this chunk within the page (0-based)
        total_chunks: number;  // Total number of chunks for this page, allows knowing if more chunks exist
    };
}

export interface BrokenLink {
    source: string;
    target: string;
}

// Change counters accumulated while syncing a single source. "Items" are the
// source's natural unit (pages for websites, files for directories/code,
// issues for GitHub, objects for S3, tickets/articles for Zendesk).
export interface SourceRunCounters {
    items_kind: string;
    items_new: number;       // items that had no chunks stored before this run
    items_updated: number;   // items whose content changed and were re-embedded
    items_unchanged: number; // items skipped because stored chunks were identical
    items_deleted: number;   // items purged from the store (404s, obsolete files, deleted tickets…)
    chunks_added: number;
    chunks_deleted: number;
}

export function newSourceRunCounters(itemsKind: string): SourceRunCounters {
    return {
        items_kind: itemsKind,
        items_new: 0,
        items_updated: 0,
        items_unchanged: 0,
        items_deleted: 0,
        chunks_added: 0,
        chunks_deleted: 0,
    };
}

// Per-source outcome of a sync run, emitted as the `run-summary` structured event
// and consumed by the controller to build run statistics
export interface SourceRunStats {
    product_name: string;
    type: string;
    version: string;
    duration_ms: number;
    ok: boolean;
    error?: string;
    counters?: SourceRunCounters;
}

export interface SqliteDB {
    db: any; // Database from better-sqlite3
    type: 'sqlite';
}

export interface QdrantDB {
    client: any; // QdrantClient
    collectionName: string;
    type: 'qdrant';
}

export type DatabaseConnection = SqliteDB | QdrantDB; 
