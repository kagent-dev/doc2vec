import { Readability } from '@mozilla/readability';
import axios from 'axios';
import { load } from 'cheerio';
import crypto from 'crypto';
import { JSDOM } from 'jsdom';
import puppeteer, { Browser, Page } from 'puppeteer';
import sanitizeHtml from 'sanitize-html';
import TurndownService from 'turndown';
import * as yaml from 'js-yaml';
import * as fs from 'fs';
import * as path from 'path';
import BetterSqlite3, { Database } from "better-sqlite3";
import { OpenAI } from "openai";
import * as dotenv from "dotenv";
import * as sqliteVec from "sqlite-vec";
import { QdrantClient } from '@qdrant/js-client-rest';
import { Logger, LogLevel } from './logger';

dotenv.config();

interface Config {
    sites: SiteConfig[];
}

interface SiteConfig {
    url: string;
    database_type: 'sqlite' | 'qdrant';
    product_name: string;
    version: string;
    max_size: number;
    database_params: DatabaseParams;
}

interface DatabaseParams {
    db_path?: string;
    qdrant_url?: string;
    qdrant_port?: number;
    collection_name?: string;
}

interface DocumentChunk {
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

interface SqliteDB {
    db: Database;
    type: 'sqlite';
}

interface QdrantDB {
    client: QdrantClient;
    collectionName: string;
    type: 'qdrant';
}

type DatabaseConnection = SqliteDB | QdrantDB;

class Doc2Vec {
    private config: Config;
    private openai: OpenAI;
    private turndownService: TurndownService;
    private logger: Logger;

    constructor(configPath: string) {
        this.logger = new Logger('Doc2Vec', {
            level: LogLevel.DEBUG,
            useTimestamp: true,
            useColor: true,
            prettyPrint: true
        });
        
        this.logger.info('Initializing Doc2Vec');
        this.config = this.loadConfig(configPath);
        this.openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

        this.turndownService = new TurndownService({
            codeBlockStyle: 'fenced',
            headingStyle: 'atx'
        });
        this.setupTurndownRules();
    }

    private loadConfig(configPath: string): Config {
        try {
            const logger = this.logger.child('config');
            logger.info(`Loading configuration from ${configPath}`);
            
            const configFile = fs.readFileSync(configPath, 'utf8');
            const config = yaml.load(configFile) as Config;
            
            logger.info(`Configuration loaded successfully, found ${config.sites.length} sites`);
            return config;
        } catch (error) {
            this.logger.error(`Failed to load or parse config file at ${configPath}:`, error);
            process.exit(1);
        }
    }

    private setupTurndownRules() {
        const logger = this.logger.child('markdown');
        logger.debug('Setting up Turndown rules for markdown conversion');
        
        this.turndownService.addRule('codeBlocks', {
            filter: (node: Node): boolean => node.nodeName === 'PRE',
            replacement: (content: string, node: Node): string => {
                const htmlNode = node as HTMLElement;
                const code = htmlNode.querySelector('code');

                let codeContent;
                if (code) {
                    codeContent = code.textContent || '';
                } else {
                    codeContent = htmlNode.textContent || '';
                }

                const lines = codeContent.split('\n');
                let minIndent = Infinity;
                for (const line of lines) {
                    if (line.trim() === '') continue;
                    const leadingWhitespace = line.match(/^\s*/)?.[0] || '';
                    minIndent = Math.min(minIndent, leadingWhitespace.length);
                }

                const cleanedLines = lines.map(line => {
                    return line.substring(minIndent);
                });

                let cleanContent = cleanedLines.join('\n');
                cleanContent = cleanContent.replace(/^\s+|\s+$/g, '');
                cleanContent = cleanContent.replace(/\n{2,}/g, '\n');

                return `\n\`\`\`\n${cleanContent}\n\`\`\`\n`;
            }
        });

        this.turndownService.addRule('tableCell', {
            filter: ['th', 'td'],
            replacement: (content: string, node: Node): string => {
                const htmlNode = node as HTMLElement;

                let cellContent = '';
                if (htmlNode.querySelector('p')) {
                    cellContent = Array.from(htmlNode.querySelectorAll('p'))
                        .map(p => p.textContent || '')
                        .join(' ')
                        .trim();
                } else {
                    cellContent = content.trim();
                }

                return ` ${cellContent.replace(/\|/g, '\\|')} |`;
            }
        });

        this.turndownService.addRule('tableRow', {
            filter: 'tr',
            replacement: (content: string, node: Node): string => {
                const htmlNode = node as HTMLTableRowElement;
                const cells = Array.from(htmlNode.cells);
                const isHeader = htmlNode.parentNode?.nodeName === 'THEAD';

                let output = '|' + content.trimEnd();

                if (isHeader) {
                    const separator = cells.map(() => '---').join(' | ');
                    output += '\n|' + separator + '|';
                }

                if (!isHeader || !htmlNode.nextElementSibling) {
                    output += '\n';
                }

                return output;
            }
        });

        this.turndownService.addRule('table', {
            filter: 'table',
            replacement: (content: string): string => {
                return '\n' + content.replace(/\n+/g, '\n').trim() + '\n';
            }
        });

        this.turndownService.addRule('preserveTableWhitespace', {
            filter: (node: Node): boolean => {
                return (
                    (node.nodeName === 'TD' || node.nodeName === 'TH') &&
                    (node.textContent?.trim().length === 0)
                );
            },
            replacement: (): string => {
                return ' |';
            }
        });
        
        logger.debug('Turndown rules setup complete');
    }

    public async run(): Promise<void> {
        this.logger.section('PROCESSING SITES');
        
        for (const siteConfig of this.config.sites) {
            const siteLogger = this.logger.child(`site:${siteConfig.product_name}`);
            
            siteLogger.info(`Processing site ${siteConfig.url} (${siteConfig.database_type}) for ${siteConfig.product_name}@${siteConfig.version}`);
            await this.processSite(siteConfig, siteLogger);
        }
        
        this.logger.section('PROCESSING COMPLETE');
    }
    
    private getUrlPrefix(url: string): string {
        try {
            const parsedUrl = new URL(url);
            return parsedUrl.origin + parsedUrl.pathname;
        } catch (error) {
            return url;
        }
    }

    private async processSite(siteConfig: SiteConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for ${siteConfig.url}`);
    
        const dbConnection = await this.initDatabase(siteConfig, logger);
        const validChunkIds: Set<string> = new Set();
        const visitedUrls: Set<string> = new Set();
        const urlPrefix = this.getUrlPrefix(siteConfig.url);
    
        logger.section('CRAWL AND EMBEDDING');
    
        await this.crawlWebsite(siteConfig.url, siteConfig, async (url, content) => {
            visitedUrls.add(url);
    
            logger.info(`Processing content from ${url} (${content.length} chars markdown)`);
            try {
                const chunks = await this.chunkMarkdown(content, siteConfig, url);
                logger.info(`Created ${chunks.length} chunks`);
    
                if (chunks.length > 0) {
                    const chunkProgress = logger.progress(`Embedding chunks for ${url}`, chunks.length);
    
                    for (let i = 0; i < chunks.length; i++) {
                        const chunk = chunks[i];
                        validChunkIds.add(chunk.metadata.chunk_id);
    
                        const chunkId = chunk.metadata.chunk_id.substring(0, 8) + '...';
    
                        let needsEmbedding = true;
                        const chunkHash = this.generateHash(chunk.content);
    
                        if (dbConnection.type === 'sqlite') {
                             const { checkHashStmt } = this.prepareSQLiteStatements(dbConnection.db);
                             const existing = checkHashStmt.get(chunk.metadata.chunk_id) as { hash: string } | undefined;
    
                             if (existing && existing.hash === chunkHash) {
                                 needsEmbedding = false;
                                 chunkProgress.update(1, `Skipping unchanged chunk ${chunkId}`);
                                 logger.info(`Skipping unchanged chunk: ${chunkId}`);
                             }
                        } else if (dbConnection.type === 'qdrant') {
                            try {
                                let pointId: string;
                                try {
                                    pointId = chunk.metadata.chunk_id;
                                    if (!this.isValidUuid(pointId)) {
                                        pointId = this.hashToUuid(chunk.metadata.chunk_id);
                                    }
                                } catch (e) {
                                    pointId = crypto.randomUUID();
                                }
    
                                const existingPoints = await dbConnection.client.retrieve(dbConnection.collectionName, {
                                    ids: [pointId],
                                    with_payload: true,
                                    with_vector: false,
                                });
    
                                if (existingPoints.length > 0 && existingPoints[0].payload && existingPoints[0].payload.hash === chunkHash) {
                                    needsEmbedding = false;
                                    chunkProgress.update(1, `Skipping unchanged chunk ${chunkId}`);
                                    logger.info(`Skipping unchanged chunk: ${chunkId}`);
                                }
                            } catch (error) {
                                logger.error(`Error checking existing point in Qdrant:`, error);
                            }
                        }
    
    
                        if (needsEmbedding) {
                            const embeddings = await this.createEmbeddings([chunk.content]);
                            if (embeddings.length > 0) {
                                const embedding = embeddings[0];
                                if (dbConnection.type === 'sqlite') {
                                    this.insertVectorsSQLite(dbConnection.db, chunk, embedding, logger, chunkHash);
                                    chunkProgress.update(1, `Stored chunk ${chunkId} in SQLite`);
                                } else if (dbConnection.type === 'qdrant') {
                                    await this.storeChunkInQdrant(dbConnection, chunk, embedding, chunkHash);
                                    chunkProgress.update(1, `Stored chunk ${chunkId} in Qdrant (${dbConnection.collectionName})`);
                                }
                            } else {
                                logger.error(`Embedding failed for chunk: ${chunkId}`);
                                chunkProgress.update(1, `Failed to embed chunk ${chunkId}`);
                            }
                        }
                    }
    
                    chunkProgress.complete();
                }
    
            } catch (error) {
                logger.error(`Error during chunking or embedding for ${url}:`, error);
            }
    
        }, logger, visitedUrls);
    
        logger.info(`Found ${validChunkIds.size} valid chunks across processed pages for ${siteConfig.url}`);
    
        logger.section('CLEANUP');
        if (dbConnection.type === 'sqlite') {
            logger.info(`Running SQLite cleanup for ${urlPrefix}`);
            this.removeObsoleteChunksSQLite(dbConnection.db, visitedUrls, urlPrefix, logger);
        } else if (dbConnection.type === 'qdrant') {
            logger.info(`Running Qdrant cleanup for ${urlPrefix} in collection ${dbConnection.collectionName}`);
            await this.removeObsoleteChunksQdrant(dbConnection, visitedUrls, urlPrefix, logger);
        }
    
        logger.info(`Finished processing site: ${siteConfig.url}`);
    }

    private async initDatabase(siteConfig: SiteConfig, parentLogger: Logger): Promise<DatabaseConnection> {
        const logger = parentLogger.child('database');
        
        if (siteConfig.database_type === 'sqlite') {
            const dbPath = siteConfig.database_params.db_path || path.join(process.cwd(), `${siteConfig.product_name.replace(/\s+/g, '_')}-${siteConfig.version}.db`);
            logger.info(`Opening SQLite database at ${dbPath}`);
            
            const db = new BetterSqlite3(dbPath, { allowExtension: true } as any);
            sqliteVec.load(db);
    
            logger.debug(`Creating vec_items table if it doesn't exist`);
            db.exec(`
                CREATE VIRTUAL TABLE IF NOT EXISTS vec_items USING vec0(
                    embedding FLOAT[3072],
                    product_name TEXT,
                    version TEXT,
                    heading_hierarchy TEXT,
                    section TEXT,
                    chunk_id TEXT UNIQUE,
                    content TEXT,
                    url TEXT,
                    hash TEXT
                );
            `);
            logger.info(`SQLite database initialized successfully`);
            return { db, type: 'sqlite' };
        } else if (siteConfig.database_type === 'qdrant') {
            const qdrantUrl = siteConfig.database_params.qdrant_url || 'http://localhost:6333';
            const qdrantPort = siteConfig.database_params.qdrant_port || 443;
            const collectionName = siteConfig.database_params.collection_name || `${siteConfig.product_name.toLowerCase().replace(/\s+/g, '_')}_${siteConfig.version}`;
            
            logger.info(`Connecting to Qdrant at ${qdrantUrl}:${qdrantPort}, collection: ${collectionName}`);
            const qdrantClient = new QdrantClient({ url: qdrantUrl, apiKey: process.env.QDRANT_API_KEY, port: qdrantPort });
    
            await this.createCollectionQdrant(qdrantClient, collectionName, logger);
            logger.info(`Qdrant connection established successfully`);
            return { client: qdrantClient, collectionName, type: 'qdrant' };
        } else {
            const errMsg = `Unsupported database type: ${siteConfig.database_type}`;
            logger.error(errMsg);
            throw new Error(errMsg);
        }
    }
    
    private async createCollectionQdrant(qdrantClient: QdrantClient, collectionName: string, logger: Logger) {
        try {
            logger.debug(`Checking if collection ${collectionName} exists`);
            const collections = await qdrantClient.getCollections();
            const collectionExists = collections.collections.some(
                (collection: any) => collection.name === collectionName
            );
            
            if (collectionExists) {
                logger.info(`Collection ${collectionName} already exists`);
                return;
            }
            
            logger.info(`Creating new collection ${collectionName}`);
            await qdrantClient.createCollection(collectionName, {
                vectors: {
                    size: 3072,
                    distance: "Cosine",
                },
            });
            logger.info(`Collection ${collectionName} created successfully`);
        } catch (error) {
            if (error instanceof Error) {
                const errorMsg = error.message.toLowerCase();
                const errorString = JSON.stringify(error).toLowerCase();
                
                if (
                    errorMsg.includes("already exists") || 
                    errorString.includes("already exists") ||
                    (error as any)?.status === 409 ||
                    errorString.includes("conflict")
                ) {
                    logger.info(`Collection ${collectionName} already exists (from error response)`);
                    return;
                }
            }
            
            logger.error(`Error creating Qdrant collection:`, error);
            logger.warn(`Continuing with existing collection...`);
        }
    }

    private prepareSQLiteStatements(db: Database) {
        return {
            insertStmt: db.prepare(`
            INSERT INTO vec_items (embedding, product_name, version, heading_hierarchy, section, chunk_id, content, url, hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        `),
            checkHashStmt: db.prepare(`SELECT hash FROM vec_items WHERE chunk_id = ?`),
            updateStmt: db.prepare(`
            UPDATE vec_items SET embedding = ?, product_name = ?, version = ?, heading_hierarchy = ?, section = ?, content = ?, url = ?, hash = ?
            WHERE chunk_id = ?
        `),
            getAllChunkIdsStmt: db.prepare(`SELECT chunk_id FROM vec_items`),
            deleteChunkStmt: db.prepare(`DELETE FROM vec_items WHERE chunk_id = ?`)
        };
    }

    private insertVectorsSQLite(db: Database, chunk: DocumentChunk, embedding: number[], logger: Logger, chunkHash?: string) {
        const { insertStmt, updateStmt } = this.prepareSQLiteStatements(db);
        const hash = chunkHash || this.generateHash(chunk.content);
        
        const transaction = db.transaction(() => {
            const params = [
                new Float32Array(embedding),
                chunk.metadata.product_name,
                chunk.metadata.version,
                JSON.stringify(chunk.metadata.heading_hierarchy),
                chunk.metadata.section,
                chunk.metadata.chunk_id,
                chunk.content,
                chunk.metadata.url,
                hash
            ];
    
            try {
                insertStmt.run(params);
            } catch (error) {
                updateStmt.run([...params.slice(0, 8), chunk.metadata.chunk_id]);
            }
        });
    
        transaction();
    }
    
    private async storeChunkInQdrant(db: QdrantDB, chunk: DocumentChunk, embedding: number[], chunkHash?: string) {
        const { client, collectionName } = db;
        try {
            let pointId: string;
            try {
                pointId = chunk.metadata.chunk_id;
                if (!this.isValidUuid(pointId)) {
                    pointId = this.hashToUuid(chunk.metadata.chunk_id);
                }
            } catch (e) {
                pointId = crypto.randomUUID();
            }
            
            const hash = chunkHash || this.generateHash(chunk.content);
            
            const pointItem = {
                id: pointId,
                vector: embedding,
                payload: {
                    content: chunk.content,
                    product_name: chunk.metadata.product_name,
                    version: chunk.metadata.version,
                    heading_hierarchy: chunk.metadata.heading_hierarchy,
                    section: chunk.metadata.section,
                    url: chunk.metadata.url,
                    hash: hash,
                    original_chunk_id: chunk.metadata.chunk_id,
                },
            };
    
            await client.upsert(collectionName, {
                wait: true,
                points: [pointItem],
            });
        } catch (error) {
            this.logger.error("Error storing chunk in Qdrant:", error);
        }
    }

    private removeObsoleteChunksSQLite(db: Database, visitedUrls: Set<string>, urlPrefix: string, logger: Logger) {
        const getChunksForUrlStmt = db.prepare(`
            SELECT chunk_id, url FROM vec_items
            WHERE url LIKE ? || '%'
        `);
        const deleteChunkStmt = db.prepare(`DELETE FROM vec_items WHERE chunk_id = ?`);
    
        const existingChunks = getChunksForUrlStmt.all(urlPrefix) as { chunk_id: string; url: string }[];
        let deletedCount = 0;
    
        const transaction = db.transaction(() => {
            for (const { chunk_id, url } of existingChunks) {
                if (!visitedUrls.has(url)) {
                    logger.debug(`Deleting obsolete chunk from SQLite: ${chunk_id.substring(0, 8)}... (URL not visited)`);
                    deleteChunkStmt.run(chunk_id);
                    deletedCount++;
                }
            }
        });
        transaction();
    
        logger.info(`Deleted ${deletedCount} obsolete chunks from SQLite for URL ${urlPrefix}`);
    }
    
    private isValidUuid(str: string): boolean {
        const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
        return uuidRegex.test(str);
    }
    
    private hashToUuid(hash: string): string {
        const truncatedHash = hash.substring(0, 32);
        
        return [
            truncatedHash.substring(0, 8),
            truncatedHash.substring(8, 12),
            '5' + truncatedHash.substring(13, 16),
            '8' + truncatedHash.substring(17, 20),
            truncatedHash.substring(20, 32)
        ].join('-');
    }

    private async removeObsoleteChunksQdrant(db: QdrantDB, visitedUrls: Set<string>, urlPrefix: string, logger: Logger) {
        const { client, collectionName } = db;
        try {
    
            const response = await client.scroll(collectionName, {
                limit: 10000,
                with_payload: true,
                with_vector: false,
                filter: {
                    must: [
                        {
                            key: "url",
                            match: {
                                text: urlPrefix + "*"
                            }
                        }
                    ]
                }
            });
    
            const obsoletePointIds = response.points
                .filter((point: any) => {
                    const url = point.payload?.url;
    
                    return url && !visitedUrls.has(url);
                })
                .map((point: any) => point.id);
    
            if (obsoletePointIds.length > 0) {
                await client.delete(collectionName, {
                    points: obsoletePointIds,
                });
                logger.info(`Deleted ${obsoletePointIds.length} obsolete chunks from Qdrant for URL ${urlPrefix}`);
            } else {
                logger.info(`No obsolete chunks to delete from Qdrant for URL ${urlPrefix}`);
            }
        } catch (error) {
            logger.error(`Error removing obsolete chunks from Qdrant:`, error);
        }
    }

    private async crawlWebsite(
        baseUrl: string,
        siteConfig: SiteConfig,
        processPageContent: (url: string, content: string) => Promise<void>,
        parentLogger: Logger,
        visitedUrls: Set<string>
    ): Promise<void> {
        const logger = parentLogger.child('crawler');
        const queue: string[] = [baseUrl];
    
        logger.info(`Starting crawl from ${baseUrl}`);
        let processedCount = 0;
        let skippedCount = 0;
        let skippedSizeCount = 0;
        let errorCount = 0;
    
        while (queue.length > 0) {
            const url = queue.shift();
            if (!url) continue;
    
            const normalizedUrl = this.normalizeUrl(url);
            if (visitedUrls.has(normalizedUrl)) continue;
            visitedUrls.add(normalizedUrl);
    
            if (!this.shouldProcessUrl(url)) {
                logger.debug(`Skipping URL with unsupported extension: ${url}`);
                skippedCount++;
                continue;
            }
    
            try {
                logger.info(`Crawling: ${url}`);
                const content = await this.processPage(url, siteConfig);
    
                if (content !== null) {
                    await processPageContent(url, content);
                    processedCount++;
                } else {
                    skippedSizeCount++;
                }
    
                const response = await axios.get(url);
                const $ = load(response.data);
    
                logger.debug(`Finding links on page ${url}`);
                let newLinksFound = 0;
    
                $('a[href]').each((_, element) => {
                    const href = $(element).attr('href');
                    if (!href || href.startsWith('#') || href.startsWith('mailto:')) return;
    
                    const fullUrl = this.buildUrl(href, url);
                    if (fullUrl.startsWith(siteConfig.url) && !visitedUrls.has(this.normalizeUrl(fullUrl))) {
                         if (!queue.includes(fullUrl)) {
                             queue.push(fullUrl);
                             newLinksFound++;
                         }
                    }
                });
    
                logger.debug(`Found ${newLinksFound} new links on ${url}`);
            } catch (error) {
                logger.error(`Failed during link discovery or initial fetch for ${url}:`, error);
                errorCount++;
            }
        }
    
        logger.info(`Crawl completed. Processed: ${processedCount}, Skipped (Extension): ${skippedCount}, Skipped (Size): ${skippedSizeCount}, Errors: ${errorCount}`);
    }

    private shouldProcessUrl(url: string): boolean {
        const parsedUrl = new URL(url);
        const pathname = parsedUrl.pathname;
        const ext = path.extname(pathname);

        if (!ext) return true;
        return ['.html', '.htm'].includes(ext.toLowerCase());
    }

    private normalizeUrl(url: string): string {
        try {
            const urlObj = new URL(url);
            urlObj.hash = '';
            urlObj.search = '';
            return urlObj.toString();
        } catch (error) {
            return url;
        }
    }

    private buildUrl(href: string, currentUrl: string): string {
        try {
            return new URL(href, currentUrl).toString();
        } catch (error) {
            this.logger.warn(`Invalid URL found: ${href}`);
            return '';
        }
    }

    private async processPage(url: string, siteConfig: SiteConfig): Promise<string | null> {
        const logger = this.logger.child('page-processor');
        logger.debug(`Processing page content from ${url}`);
    
        const browser: Browser = await puppeteer.launch({
            args: ['--no-sandbox', '--disable-setuid-sandbox'],
        });
        try {
            const page: Page = await browser.newPage();
            logger.debug(`Navigating to ${url}`);
            await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
    
            const htmlContent: string = await page.evaluate(() => {
                const mainContentElement = document.querySelector('div[role="main"].document') || document.querySelector('main') || document.body;
                return mainContentElement.innerHTML;
            });
    
            if (htmlContent.length > siteConfig.max_size) {
                logger.warn(`Raw HTML content (${htmlContent.length} chars) exceeds max size (${siteConfig.max_size}). Skipping detailed processing for ${url}.`);
                await browser.close();
                return null;
            }
    
            logger.debug(`Got HTML content (${htmlContent.length} chars), creating DOM`);
            const dom = new JSDOM(htmlContent);
            const document = dom.window.document;
    
            document.querySelectorAll('pre').forEach((pre: HTMLElement) => {
                pre.classList.add('article-content');
                pre.setAttribute('data-readable-content-score', '100');
                this.markCodeParents(pre.parentElement);
            });
    
            logger.debug(`Applying Readability to extract main content`);
            const reader = new Readability(document, {
                charThreshold: 20,
                classesToPreserve: ['article-content'],
            });
            const article = reader.parse();
    
            if (!article) {
                logger.warn(`Failed to parse article content with Readability for ${url}`);
                await browser.close();
                return null;
            }
    
            logger.debug(`Sanitizing HTML (${article.content.length} chars)`);
            const cleanHtml = sanitizeHtml(article.content, {
                 allowedTags: [
                    'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'a', 'ul', 'ol',
                    'li', 'b', 'i', 'strong', 'em', 'code', 'pre',
                    'div', 'span', 'table', 'thead', 'tbody', 'tr', 'th', 'td'
                ],
                allowedAttributes: {
                    'a': ['href'],
                    'pre': ['class', 'data-language'],
                    'code': ['class', 'data-language'],
                    'div': ['class'],
                    'span': ['class']
                }
            });
    
            logger.debug(`Converting HTML to Markdown`);
            const markdown = this.turndownService.turndown(cleanHtml);
            logger.debug(`Markdown conversion complete (${markdown.length} chars)`);
            return markdown;
        } catch (error) {
            logger.error(`Error processing page ${url}:`, error);
            return null;
        } finally {
            if (browser && browser.isConnected()) {
                 await browser.close();
                 logger.debug(`Browser closed for ${url}`);
            }
        }
    }

    private markCodeParents(node: Element | null) {
        if (!node) return;

        if (node.querySelector('pre, code')) {
            node.classList.add('article-content');
            node.setAttribute('data-readable-content-score', '100');
        }
        this.markCodeParents(node.parentElement);
    }

    private async chunkMarkdown(markdown: string, siteConfig: SiteConfig, url: string): Promise<DocumentChunk[]> {
        const logger = this.logger.child('chunker');
        logger.debug(`Chunking markdown from ${url} (${markdown.length} chars)`);
        
        const MAX_TOKENS = 1000;
        const chunks: DocumentChunk[] = [];
        const lines = markdown.split("\n");
        let currentChunk = "";
        let headingHierarchy: string[] = [];

        const processChunk = () => {
            if (currentChunk.trim()) {
                const tokens = this.tokenize(currentChunk);
                if (tokens.length > MAX_TOKENS) {
                    logger.debug(`Chunk exceeds max token count (${tokens.length}), splitting into smaller chunks`);
                    let subChunk = "";
                    let tokenCount = 0;
                    const overlapSize = Math.floor(MAX_TOKENS * 0.05);
                    let lastTokens: string[] = [];

                    for (const token of tokens) {
                        if (tokenCount + 1 > MAX_TOKENS) {
                            chunks.push(createDocumentChunk(subChunk, headingHierarchy));
                            subChunk = lastTokens.join("") + token;
                            tokenCount = lastTokens.length + 1;
                            lastTokens = [];
                        } else {
                            subChunk += token;
                            tokenCount++;
                            lastTokens.push(token);
                            if (lastTokens.length > overlapSize) {
                                lastTokens.shift();
                            }
                        }
                    }
                    if (subChunk) {
                        chunks.push(createDocumentChunk(subChunk, headingHierarchy));
                    }
                } else {
                    chunks.push(createDocumentChunk(currentChunk, headingHierarchy));
                }
            }
            currentChunk = "";
        };

        const createDocumentChunk = (content: string, hierarchy: string[]): DocumentChunk => {
            const chunkId = this.generateHash(content);
            logger.debug(`Created chunk ${chunkId.substring(0, 8)}... with ${content.length} chars`);
            
            return {
                content,
                metadata: {
                    product_name: siteConfig.product_name,
                    version: siteConfig.version,
                    heading_hierarchy: [...hierarchy],
                    section: hierarchy[hierarchy.length - 1] || "Introduction",
                    chunk_id: chunkId,
                    url: url,
                    hash: this.generateHash(content)
                }
            };
        };

        for (const line of lines) {
            if (line.startsWith("#")) {
                processChunk();
                const levelMatch = line.match(/^(#+)/);
                let level = levelMatch ? levelMatch[1].length : 1;
                const heading = line.replace(/^#+\s*/, "").trim();

                logger.debug(`Found heading (level ${level}): ${heading}`);
                
                while (headingHierarchy.length < level - 1) {
                    headingHierarchy.push("");
                }

                if (level <= headingHierarchy.length) {
                    headingHierarchy = headingHierarchy.slice(0, level - 1);
                }
                headingHierarchy[level - 1] = heading;
            } else {
                currentChunk += `${line}\n`;
            }
        }
        processChunk();
        
        logger.debug(`Chunking complete, created ${chunks.length} chunks`);
        return chunks;
    }

    private tokenize(text: string): string[] {
        return text.split(/(\s+)/).filter(token => token.length > 0);
    }

    private async createEmbeddings(texts: string[]): Promise<number[][]> {
        const logger = this.logger.child('embeddings');
        try {
            logger.debug(`Creating embeddings for ${texts.length} texts`);
            const response = await this.openai.embeddings.create({
                model: "text-embedding-3-large",
                input: texts,
            });
            logger.debug(`Successfully created ${response.data.length} embeddings`);
            return response.data.map(d => d.embedding);
        } catch (error) {
            logger.error('Failed to create embeddings:', error);
            return [];
        }
    }

    private generateHash(content: string): string {
        return crypto.createHash("sha256").update(content).digest("hex");
    }
}

if (require.main === module) {
    const configPath = process.argv[2] || 'config.yaml';
    if (!fs.existsSync(configPath)) {
        console.error('Please provide a valid path to a YAML config file.');
        process.exit(1);
    }
    const doc2Vec = new Doc2Vec(configPath);
    doc2Vec.run().catch(console.error);
}