// Base configuration that applies to all source types
export interface BaseSourceConfig {
    type: 'website' | 'github' | 'local_directory';
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

// Union type for all possible source configurations
export type SourceConfig = WebsiteSourceConfig | GithubSourceConfig | LocalDirectorySourceConfig;

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

export interface Config {
    sources: SourceConfig[];
}

export interface DocumentChunk {
    content: string;
    metadata: {
        product_name: string;
        version: string;
        heading_hierarchy: string[];
        section: string;
        chunk_id: string;
        url: string;
        hash?: string;
    };
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