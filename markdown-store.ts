import { Pool, PoolConfig } from 'pg';
import { MarkdownStoreConfig } from './types';
import { Logger } from './logger';

/**
 * Stores generated markdown for website pages in a Postgres table.
 *
 * Table schema (single shared table, default name `markdown_pages`):
 *   url          TEXT PRIMARY KEY
 *   product_name TEXT NOT NULL
 *   markdown     TEXT NOT NULL
 *   updated_at   TIMESTAMPTZ DEFAULT NOW()
 *
 * When a website source has `markdown_store: true`, the crawl loop uses this
 * store to decide whether to force-process a page that would otherwise be
 * skipped by lastmod / ETag caching.  If the URL is not yet in Postgres the
 * page is processed regardless of cache signals, ensuring the table is fully
 * populated after the first sync.  On subsequent syncs only pages with
 * detected changes are updated.
 */
export class MarkdownStore {
    private pool: Pool;
    private tableName: string;
    private logger: Logger;

    constructor(config: MarkdownStoreConfig, logger: Logger) {
        this.logger = logger.child('markdown-store');
        this.tableName = config.table_name ?? 'markdown_pages';

        const poolConfig: PoolConfig = {};

        if (config.connection_string) {
            poolConfig.connectionString = config.connection_string;
        } else {
            if (config.host) poolConfig.host = config.host;
            if (config.port) poolConfig.port = config.port;
            if (config.database) poolConfig.database = config.database;
            if (config.user) poolConfig.user = config.user;
            if (config.password) poolConfig.password = config.password;
        }

        this.pool = new Pool(poolConfig);
    }

    /**
     * Create the markdown table if it doesn't already exist.
     */
    async init(): Promise<void> {
        const query = `
            CREATE TABLE IF NOT EXISTS ${this.escapeIdentifier(this.tableName)} (
                url          TEXT PRIMARY KEY,
                product_name TEXT NOT NULL,
                markdown     TEXT NOT NULL,
                updated_at   TIMESTAMPTZ DEFAULT NOW()
            );
        `;
        await this.pool.query(query);
        this.logger.info(`Initialized Postgres markdown store (table: ${this.tableName})`);
    }

    /**
     * Return the set of URLs that already have markdown stored for a given URL
     * prefix (e.g., "https://istio.io/latest/docs/").  This is called once
     * before the crawl starts so the crawler can decide which pages need
     * force-processing.
     */
    async getUrlsWithMarkdown(urlPrefix: string): Promise<Set<string>> {
        const result = await this.pool.query(
            `SELECT url FROM ${this.escapeIdentifier(this.tableName)} WHERE url LIKE $1`,
            [urlPrefix + '%']
        );
        return new Set(result.rows.map((row: { url: string }) => row.url));
    }

    /**
     * Insert or update the markdown for a URL.  Called after a page is
     * successfully processed (fetched + converted to markdown).
     */
    async upsertMarkdown(url: string, productName: string, markdown: string): Promise<void> {
        const query = `
            INSERT INTO ${this.escapeIdentifier(this.tableName)} (url, product_name, markdown, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (url) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                markdown     = EXCLUDED.markdown,
                updated_at   = NOW();
        `;
        await this.pool.query(query, [url, productName, markdown]);
    }

    /**
     * Remove a URL from the store (e.g., when a HEAD request returns 404).
     */
    async deleteMarkdown(url: string): Promise<void> {
        await this.pool.query(
            `DELETE FROM ${this.escapeIdentifier(this.tableName)} WHERE url = $1`,
            [url]
        );
    }

    /**
     * Close the connection pool.  Should be called once after all sources have
     * been processed.
     */
    async close(): Promise<void> {
        await this.pool.end();
        this.logger.info('Postgres markdown store connection pool closed');
    }

    /**
     * Escape a SQL identifier (table name) to prevent injection.
     * Uses double-quoting per the SQL standard.
     */
    private escapeIdentifier(identifier: string): string {
        return '"' + identifier.replace(/"/g, '""') + '"';
    }
}
