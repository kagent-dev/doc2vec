// Base configuration that applies to all source types
export interface BaseSourceConfig {
    type: 'website' | 'github' | 'local_directory' | 'zendesk' | 'code';
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

// Union type for all possible source configurations
export type SourceConfig = WebsiteSourceConfig | GithubSourceConfig | LocalDirectorySourceConfig | ZendeskSourceConfig | CodeSourceConfig;

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
        api_key?: string;  // Can also use OPENAI_API_KEY env var
        model?: string;    // Default: text-embedding-3-large
    };
    azure?: {
        api_key?: string;        // Can also use AZURE_OPENAI_KEY env var
        endpoint?: string;       // Can also use AZURE_OPENAI_ENDPOINT env var
        deployment_name?: string; // Can also use AZURE_OPENAI_DEPLOYMENT_NAME env var
        api_version?: string;    // Default: 2024-10-21
    };
}

export interface Config {
    sources: SourceConfig[];
    embedding?: EmbeddingConfig;  // Optional, defaults to OpenAI
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
