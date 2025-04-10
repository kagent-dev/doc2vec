#!/usr/bin/env node

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

const GITHUB_TOKEN = process.env.GITHUB_PERSONAL_ACCESS_TOKEN;

dotenv.config();

interface Config {
    sources: SourceConfig[];
}

// Base configuration that applies to all source types
interface BaseSourceConfig {
    type: 'website' | 'github' | 'local_directory';
    product_name: string;
    version: string;
    max_size: number;
    database_config: DatabaseConfig;
}

// Configuration specific to local directory sources
interface LocalDirectorySourceConfig extends BaseSourceConfig {
    type: 'local_directory';
    path: string;                  // Path to the local directory
    include_extensions?: string[]; // File extensions to include (e.g., ['.md', '.txt'])
    exclude_extensions?: string[]; // File extensions to exclude
    recursive?: boolean;           // Whether to traverse subdirectories
    encoding?: BufferEncoding;     // File encoding (default: 'utf8')
    url_rewrite_prefix?: string;   // Optional URL prefix to rewrite file:// URLs (e.g., 'https://mydomain.com')
}

// Configuration specific to website sources
interface WebsiteSourceConfig extends BaseSourceConfig {
    type: 'website';
    url: string;
}

// Configuration specific to GitHub repo sources
interface GithubSourceConfig extends BaseSourceConfig {
    type: 'github';
    repo: string;
    start_date?: string;
}

// Union type for all possible source configurations
type SourceConfig = WebsiteSourceConfig | GithubSourceConfig | LocalDirectorySourceConfig;

// Database configuration
interface DatabaseConfig {
    type: 'sqlite' | 'qdrant';
    params: SqliteDatabaseParams | QdrantDatabaseParams;
}

interface SqliteDatabaseParams {
    db_path?: string;  // Optional, will use default if not provided
}

interface QdrantDatabaseParams {
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
            let config = yaml.load(configFile) as any;
            
            const typedConfig = config as Config;
            logger.info(`Configuration loaded successfully, found ${typedConfig.sources.length} sources`);
            return typedConfig;
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
        this.logger.section('PROCESSING SOURCES');
        
        for (const sourceConfig of this.config.sources) {
            const sourceLogger = this.logger.child(`source:${sourceConfig.product_name}`);
            
            sourceLogger.info(`Processing ${sourceConfig.type} source for ${sourceConfig.product_name}@${sourceConfig.version}`);
            
            if (sourceConfig.type === 'github') {
                await this.processGithubRepo(sourceConfig, sourceLogger);
            } else if (sourceConfig.type === 'website') {
                await this.processWebsite(sourceConfig, sourceLogger);
            } else if (sourceConfig.type === 'local_directory') {
                await this.processLocalDirectory(sourceConfig, sourceLogger);
            } else {
                sourceLogger.error(`Unknown source type: ${(sourceConfig as any).type}`);
            }
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

    private generateMetadataUUID(repo: string): string {
        // Simple deterministic approach - hash the repo name and convert to UUID format
        const hash = crypto.createHash('md5').update(`metadata_${repo}`).digest('hex');
        // Format as UUID with version bits set correctly (version 4)
        return `${hash.substr(0, 8)}-${hash.substr(8, 4)}-4${hash.substr(13, 3)}-${hash.substr(16, 4)}-${hash.substr(20, 12)}`;
    }

    private async initDatabaseMetadata(dbConnection: DatabaseConnection): Promise<void> {
        const logger = this.logger.child('metadata');
        
        if (dbConnection.type === 'sqlite') {
            const db = dbConnection.db;
            logger.debug('Creating metadata table if it doesn\'t exist');
            db.exec(`
                CREATE TABLE IF NOT EXISTS vec_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            `);
            logger.info('SQLite metadata table initialized');
        } else if (dbConnection.type === 'qdrant') {
            // For Qdrant, we'll use the same collection but verify it exists
            logger.info(`Using existing Qdrant collection for metadata: ${dbConnection.collectionName}`);
            // Nothing special to initialize as we'll use the same collection
        }
    }

    private async getLastRunDate(dbConnection: DatabaseConnection, repo: string, defaultDate: string): Promise<string> {
        const logger = this.logger.child('metadata');
        const metadataKey = `last_run_${repo.replace('/', '_')}`;
        
        try {
            if (dbConnection.type === 'sqlite') {
                const stmt = dbConnection.db.prepare('SELECT value FROM vec_metadata WHERE key = ?');
                const result = stmt.get(metadataKey) as { value: string } | undefined;
                
                if (result) {
                    logger.info(`Retrieved last run date for ${repo}: ${result.value}`);
                    return result.value;
                }
            } else if (dbConnection.type === 'qdrant') {
                // Generate a UUID for this repo's metadata
                const metadataUUID = this.generateMetadataUUID(repo);
                logger.debug(`Looking up metadata with UUID: ${metadataUUID}`);
                
                try {
                    // Try to retrieve the metadata point for this repo
                    const response = await dbConnection.client.retrieve(dbConnection.collectionName, {
                        ids: [metadataUUID],
                        with_payload: true,
                        with_vector: false
                    });
                    
                    if (response.length > 0 && response[0].payload?.metadata_value) {
                        const lastRunDate = response[0].payload.metadata_value as string;
                        logger.info(`Retrieved last run date for ${repo}: ${lastRunDate}`);
                        return lastRunDate;
                    }
                } catch (error) {
                    logger.warn(`Failed to retrieve metadata for ${repo}:`, error);
                }
            }
        } catch (error) {
            logger.warn(`Error retrieving last run date:`, error);
        }
        
        logger.info(`No saved run date found for ${repo}, using default: ${defaultDate}`);
        return defaultDate;
    }

    private async updateLastRunDate(dbConnection: DatabaseConnection, repo: string): Promise<void> {
        const logger = this.logger.child('metadata');
        const now = new Date().toISOString();
        
        try {
            if (dbConnection.type === 'sqlite') {
                const metadataKey = `last_run_${repo.replace('/', '_')}`;
                const stmt = dbConnection.db.prepare(`
                    INSERT INTO vec_metadata (key, value) VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                `);
                stmt.run(metadataKey, now);
                logger.info(`Updated last run date for ${repo} to ${now}`);
            } else if (dbConnection.type === 'qdrant') {
                // Generate UUID for this repo's metadata
                const metadataUUID = this.generateMetadataUUID(repo);
                const metadataKey = `last_run_${repo.replace('/', '_')}`;
                
                logger.debug(`Using UUID: ${metadataUUID} for metadata`);
                
                // Generate a dummy embedding (all zeros)
                const dummyEmbeddingSize = 3072; // Same size as your content embeddings
                const dummyEmbedding = new Array(dummyEmbeddingSize).fill(0);
                
                // Create a point with special metadata payload
                const metadataPoint = {
                    id: metadataUUID,
                    vector: dummyEmbedding,
                    payload: {
                        metadata_key: metadataKey,
                        metadata_value: now,
                        is_metadata: true, // Flag to identify metadata points
                        content: `Metadata: Last run date for ${repo}`,
                        product_name: 'system',
                        version: 'metadata',
                        url: 'metadata://' + repo
                    }
                };
                
                await dbConnection.client.upsert(dbConnection.collectionName, {
                    wait: true,
                    points: [metadataPoint]
                });
                
                logger.info(`Updated last run date for ${repo} to ${now}`);
            }
        } catch (error) {
            logger.error(`Failed to update last run date for ${repo}:`, error);
        }
    }

    private async fetchAndProcessGitHubIssues(repo: string, sourceConfig: GithubSourceConfig, dbConnection: DatabaseConnection, logger: Logger): Promise<void> {
        const [owner, repoName] = repo.split('/');
        const GITHUB_API_URL = `https://api.github.com/repos/${owner}/${repoName}/issues`;
        
        // Initialize metadata storage if needed
        await this.initDatabaseMetadata(dbConnection);
        
        // Get the last run date from the database
        const startDate = sourceConfig.start_date || '2025-01-01';
        const lastRunDate = await this.getLastRunDate(dbConnection, repo, `${startDate}T00:00:00Z`);
    
        const fetchWithRetry = async (url: string, params = {}, retries = 5, delay = 5000): Promise<any> => {
            for (let attempt = 0; attempt < retries; attempt++) {
                try {
                    const response = await axios.get(url, {
                        headers: {
                            Authorization: `token ${GITHUB_TOKEN}`,
                            Accept: 'application/vnd.github.v3+json',
                        },
                        params,
                    });
                    return response.data;
                } catch (error: any) {
                    if (error.response && error.response.status === 403) {
                        const resetTime = error.response.headers['x-ratelimit-reset'];
                        const currentTime = Math.floor(Date.now() / 1000);
                        const waitTime = resetTime ? (resetTime - currentTime) * 1000 : delay * 2;
                        logger.warn(`GitHub rate limit exceeded. Waiting ${waitTime / 1000}s`);
                        await new Promise(res => setTimeout(res, waitTime));
                    } else {
                        logger.error(`GitHub fetch failed: ${error.message}`);
                        throw error;
                    }
                }
            }
            throw new Error('Max retries reached');
        };
    
        const fetchAllIssues = async (sinceDate: string): Promise<any[]> => {
            let issues: any[] = [];
            let page = 1;
            const perPage = 100;
            const sinceTimestamp = new Date(sinceDate);
    
            while (true) {
                const data = await fetchWithRetry(GITHUB_API_URL, {
                    per_page: perPage,
                    page,
                    state: 'all',
                    since: sinceDate,
                });
    
                if (data.length === 0) break;
    
                const filtered = data.filter((issue: any) => new Date(issue.created_at) >= sinceTimestamp);
                issues = issues.concat(filtered);
    
                if (filtered.length < data.length) break;
                page++;
            }
            return issues;
        };
    
        const fetchIssueComments = async (issueNumber: number): Promise<any[]> => {
            const url = `${GITHUB_API_URL}/${issueNumber}/comments`;
            return await fetchWithRetry(url);
        };
    
        const generateMarkdownForIssue = async (issue: any): Promise<string> => {
            const comments = await fetchIssueComments(issue.number);
            let md = `# Issue #${issue.number}: ${issue.title}\n\n`;
            md += `- **Author:** ${issue.user.login}\n`;
            md += `- **State:** ${issue.state}\n`;
            md += `- **Created on:** ${new Date(issue.created_at).toDateString()}\n`;
            md += `- **Updated on:** ${new Date(issue.updated_at).toDateString()}\n`;
            md += `- **Labels:** ${issue.labels.map((l: any) => `\`${l.name}\``).join(', ') || 'None'}\n\n`;
            md += `## Description\n\n${issue.body || '_No description._'}\n\n## Comments\n\n`;
    
            if (comments.length === 0) {
                md += '_No comments._\n';
            } else {
                for (const c of comments) {
                    md += `### ${c.user.login} - ${new Date(c.created_at).toDateString()}\n\n${c.body}\n\n---\n\n`;
                }
            }
    
            return md;
        };
    
        // Process a single issue and store its chunks
        const processIssue = async (issue: any): Promise<void> => {
            const issueNumber = issue.number;
            const url = `https://github.com/${repo}/issues/${issueNumber}`;
            
            logger.info(`Processing issue #${issueNumber}`);
            
            // Generate markdown for the issue
            const markdown = await generateMarkdownForIssue(issue);
            
            // Chunk the markdown content
            const issueConfig = {
                ...sourceConfig,
                product_name: sourceConfig.product_name || repo,
                max_size: sourceConfig.max_size || Infinity
            };
            
            const chunks = await this.chunkMarkdown(markdown, issueConfig, url);
            logger.info(`Issue #${issueNumber}: Created ${chunks.length} chunks`);
            
            // Process and store each chunk immediately
            for (const chunk of chunks) {
                const chunkHash = this.generateHash(chunk.content);
                const chunkId = chunk.metadata.chunk_id.substring(0, 8) + '...';
                
                if (dbConnection.type === 'sqlite') {
                    const { checkHashStmt } = this.prepareSQLiteStatements(dbConnection.db);
                    const existing = checkHashStmt.get(chunk.metadata.chunk_id) as { hash: string } | undefined;
                    
                    if (existing && existing.hash === chunkHash) {
                        logger.info(`Skipping unchanged chunk: ${chunkId}`);
                        continue;
                    }
    
                    const embeddings = await this.createEmbeddings([chunk.content]);
                    if (embeddings.length) {
                        this.insertVectorsSQLite(dbConnection.db, chunk, embeddings[0], logger, chunkHash);
                        logger.debug(`Stored chunk ${chunkId} in SQLite`);
                    } else {
                        logger.error(`Embedding failed for chunk: ${chunkId}`);
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
                            logger.info(`Skipping unchanged chunk: ${chunkId}`);
                            continue;
                        }
                        
                        const embeddings = await this.createEmbeddings([chunk.content]);
                        if (embeddings.length) {
                            await this.storeChunkInQdrant(dbConnection, chunk, embeddings[0], chunkHash);
                            logger.debug(`Stored chunk ${chunkId} in Qdrant (${dbConnection.collectionName})`);
                        } else {
                            logger.error(`Embedding failed for chunk: ${chunkId}`);
                        }
                    } catch (error) {
                        logger.error(`Error processing chunk in Qdrant:`, error);
                    }
                }
            }
        };
    
        logger.info(`Fetching GitHub issues for ${repo} since ${lastRunDate}`);
        const issues = await fetchAllIssues(lastRunDate);
        logger.info(`Found ${issues.length} updated/new issues`);
    
        // Process each issue individually, one at a time
        for (let i = 0; i < issues.length; i++) {
            logger.info(`Processing issue ${i + 1}/${issues.length}`);
            await processIssue(issues[i]);
        }
    
        // Update the last run date in the database after processing all issues
        await this.updateLastRunDate(dbConnection, repo);
        
        logger.info(`Successfully processed ${issues.length} issues`);
    }

    private async processGithubRepo(config: GithubSourceConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for GitHub repo: ${config.repo}`);
        
        const dbConnection = await this.initDatabase(config, logger);
        
        // Initialize metadata storage
        await this.initDatabaseMetadata(dbConnection);
        
        logger.section('GITHUB ISSUES');
        
        // Process GitHub issues
        await this.fetchAndProcessGitHubIssues(config.repo, config, dbConnection, logger);
        
        logger.info(`Finished processing GitHub repo: ${config.repo}`);
    }

    private async processWebsite(config: WebsiteSourceConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for website: ${config.url}`);
        
        const dbConnection = await this.initDatabase(config, logger);
        const validChunkIds: Set<string> = new Set();
        const visitedUrls: Set<string> = new Set();
        const urlPrefix = this.getUrlPrefix(config.url);
        
        logger.section('CRAWL AND EMBEDDING');
    
        await this.crawlWebsite(config.url, config, async (url, content) => {
            visitedUrls.add(url);
    
            logger.info(`Processing content from ${url} (${content.length} chars markdown)`);
            try {
                const chunks = await this.chunkMarkdown(content, config, url);
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
    
        logger.info(`Found ${validChunkIds.size} valid chunks across processed pages for ${config.url}`);
    
        logger.section('CLEANUP');
        if (dbConnection.type === 'sqlite') {
            logger.info(`Running SQLite cleanup for ${urlPrefix}`);
            this.removeObsoleteChunksSQLite(dbConnection.db, visitedUrls, urlPrefix, logger);
        } else if (dbConnection.type === 'qdrant') {
            logger.info(`Running Qdrant cleanup for ${urlPrefix} in collection ${dbConnection.collectionName}`);
            await this.removeObsoleteChunksQdrant(dbConnection, visitedUrls, urlPrefix, logger);
        }
    
        logger.info(`Finished processing website: ${config.url}`);
    }

    private async processLocalDirectory(config: LocalDirectorySourceConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for local directory: ${config.path}`);
        
        const dbConnection = await this.initDatabase(config, logger);
        const validChunkIds: Set<string> = new Set();
        const processedFiles: Set<string> = new Set();
        
        logger.section('FILE SCANNING AND EMBEDDING');
        
        await this.processDirectory(
            config.path, 
            config, 
            async (filePath, content) => {
                processedFiles.add(filePath);
                
                logger.info(`Processing content from ${filePath} (${content.length} chars)`);
                try {
                    // Generate URL based on configuration
                    let fileUrl: string;
                    
                    if (config.url_rewrite_prefix) {
                        // Replace local path with URL prefix
                        const relativePath = path.relative(config.path, filePath).replace(/\\/g, '/');
                        
                        // If relativePath starts with '..', it means the file is outside the base directory
                        if (relativePath.startsWith('..')) {
                            // For files outside the configured path, use the default file:// scheme
                            fileUrl = `file://${filePath}`;
                            logger.debug(`File outside configured path, using default URL: ${fileUrl}`);
                        } else {
                            // For files inside the configured path, rewrite the URL
                            // Handle trailing slashes in the URL prefix to avoid double slashes
                            const prefix = config.url_rewrite_prefix.endsWith('/') 
                                ? config.url_rewrite_prefix.slice(0, -1) 
                                : config.url_rewrite_prefix;
                                
                            fileUrl = `${prefix}/${relativePath}`;
                            logger.debug(`URL rewritten: ${filePath} -> ${fileUrl}`);
                        }
                    } else {
                        // Use default file:// URL
                        fileUrl = `file://${filePath}`;
                    }
                    
                    const chunks = await this.chunkMarkdown(content, config, fileUrl);
                    logger.info(`Created ${chunks.length} chunks`);
                    
                    if (chunks.length > 0) {
                        const chunkProgress = logger.progress(`Embedding chunks for ${filePath}`, chunks.length);
                        
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
                    logger.error(`Error during chunking or embedding for ${filePath}:`, error);
                }
            }, 
            logger
        );
        
        logger.section('CLEANUP');
        if (dbConnection.type === 'sqlite') {
            logger.info(`Running SQLite cleanup for local directory ${config.path}`);
            this.removeObsoleteFilesSQLite(dbConnection.db, processedFiles, config, logger);
        } else if (dbConnection.type === 'qdrant') {
            logger.info(`Running Qdrant cleanup for local directory ${config.path} in collection ${dbConnection.collectionName}`);
            await this.removeObsoleteFilesQdrant(dbConnection, processedFiles, config, logger);
        }
        
        logger.info(`Finished processing local directory: ${config.path}`);
    }

    private async processDirectory(
        dirPath: string,
        config: LocalDirectorySourceConfig,
        processFileContent: (filePath: string, content: string) => Promise<void>,
        parentLogger: Logger,
        visitedPaths: Set<string> = new Set()
    ): Promise<void> {
        const logger = parentLogger.child('directory-processor');
        logger.info(`Processing directory: ${dirPath}`);
        
        const recursive = config.recursive !== undefined ? config.recursive : true;
        const includeExtensions = config.include_extensions || ['.md', '.txt', '.html', '.htm'];
        const excludeExtensions = config.exclude_extensions || [];
        const encoding = config.encoding || 'utf8' as BufferEncoding;
        
        try {
            const files = fs.readdirSync(dirPath);
            let processedFiles = 0;
            let skippedFiles = 0;
            
            for (const file of files) {
                const filePath = path.join(dirPath, file);
                const stat = fs.statSync(filePath);
                
                // Skip already visited paths
                if (visitedPaths.has(filePath)) {
                    logger.debug(`Skipping already visited path: ${filePath}`);
                    continue;
                }
                
                visitedPaths.add(filePath);
                
                if (stat.isDirectory()) {
                    if (recursive) {
                        await this.processDirectory(filePath, config, processFileContent, logger, visitedPaths);
                    } else {
                        logger.debug(`Skipping directory ${filePath} (recursive=false)`);
                    }
                } else if (stat.isFile()) {
                    const extension = path.extname(file).toLowerCase();
                    
                    // Apply extension filters
                    if (excludeExtensions.includes(extension)) {
                        logger.debug(`Skipping file with excluded extension: ${filePath}`);
                        skippedFiles++;
                        continue;
                    }
                    
                    if (includeExtensions.length > 0 && !includeExtensions.includes(extension)) {
                        logger.debug(`Skipping file with non-included extension: ${filePath}`);
                        skippedFiles++;
                        continue;
                    }
                    
                    try {
                        logger.info(`Reading file: ${filePath}`);
                        const content = fs.readFileSync(filePath, { encoding: encoding as BufferEncoding });
                        
                        if (content.length > config.max_size) {
                            logger.warn(`File content (${content.length} chars) exceeds max size (${config.max_size}). Skipping ${filePath}.`);
                            skippedFiles++;
                            continue;
                        }
                        
                        // Convert HTML to Markdown if needed
                        let processedContent: string;
                        if (extension === '.html' || extension === '.htm') {
                            logger.debug(`Converting HTML to Markdown for ${filePath}`);
                            const cleanHtml = sanitizeHtml(content, {
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
                            processedContent = this.turndownService.turndown(cleanHtml);
                        } else {
                            processedContent = content;
                        }
                        
                        await processFileContent(filePath, processedContent);
                        processedFiles++;
                    } catch (error) {
                        logger.error(`Error processing file ${filePath}:`, error);
                    }
                }
            }
            
            logger.info(`Directory processed. Processed: ${processedFiles}, Skipped: ${skippedFiles}`);
        } catch (error) {
            logger.error(`Error reading directory ${dirPath}:`, error);
        }
    }

    private removeObsoleteFilesSQLite(
        db: Database, 
        processedFiles: Set<string>, 
        pathConfig: { path: string; url_rewrite_prefix?: string } | string, 
        logger: Logger
    ) {
        const getChunksForPathStmt = db.prepare(`
            SELECT chunk_id, url FROM vec_items
            WHERE url LIKE ? || '%'
        `);
        const deleteChunkStmt = db.prepare(`DELETE FROM vec_items WHERE chunk_id = ?`);
        
        // Determine if we're using URL rewriting or direct file paths
        const isRewriteMode = typeof pathConfig === 'object' && pathConfig.url_rewrite_prefix;
        
        // Set up the URL prefix for searching
        let urlPrefix: string;
        if (isRewriteMode) {
            // Handle URL rewriting case
            urlPrefix = (pathConfig as { path: string; url_rewrite_prefix?: string }).url_rewrite_prefix || '';
            urlPrefix = urlPrefix.endsWith('/') ? urlPrefix.slice(0, -1) : urlPrefix;
        } else {
            // Handle direct file path case
            const dirPrefix = typeof pathConfig === 'string' ? pathConfig : pathConfig.path;
            const cleanedDirPrefix = dirPrefix.replace(/^\.\/+/, '');
            urlPrefix = `file://${cleanedDirPrefix}`;
        }
        
        logger.debug(`Searching for chunks with URL prefix: ${urlPrefix}`);
        const existingChunks = getChunksForPathStmt.all(urlPrefix) as { chunk_id: string; url: string }[];
        let deletedCount = 0;
        
        const transaction = db.transaction(() => {
            for (const { chunk_id, url } of existingChunks) {
                // Skip if it's not from our URL prefix (safety check)
                if (!url.startsWith(urlPrefix)) continue;
                
                let filePath: string;
                let shouldDelete = false;
                
                if (isRewriteMode) {
                    // URL rewrite mode: extract relative path and construct full file path
                    const config = pathConfig as { path: string; url_rewrite_prefix?: string };
                    const relativePath = url.substring(urlPrefix.length + 1); // +1 for the '/'
                    filePath = path.join(config.path, relativePath);
                    shouldDelete = !processedFiles.has(filePath);
                } else {
                    // Direct file path mode: remove file:// prefix to match with processedFiles
                    filePath = url.substring(7); // Remove 'file://' prefix
                    shouldDelete = !processedFiles.has(filePath);
                }
                
                if (shouldDelete) {
                    logger.debug(`Deleting obsolete chunk from SQLite: ${chunk_id.substring(0, 8)}... (File not processed: ${filePath})`);
                    deleteChunkStmt.run(chunk_id);
                    deletedCount++;
                }
            }
        });
        transaction();
        
        logger.info(`Deleted ${deletedCount} obsolete chunks from SQLite for URL prefix ${urlPrefix}`);
    }

    private async removeObsoleteFilesQdrant(
        db: QdrantDB, 
        processedFiles: Set<string>, 
        pathConfig: { path: string; url_rewrite_prefix?: string } | string, 
        logger: Logger
    ) {
        const { client, collectionName } = db;
        try {
            // Determine if we're using URL rewriting or direct file paths
            const isRewriteMode = typeof pathConfig === 'object' && pathConfig.url_rewrite_prefix;
            
            // Set up the URL prefix for searching
            let urlPrefix: string;
            if (isRewriteMode) {
                // Handle URL rewriting case
                urlPrefix = (pathConfig as { path: string; url_rewrite_prefix?: string }).url_rewrite_prefix || '';
                urlPrefix = urlPrefix.endsWith('/') ? urlPrefix.slice(0, -1) : urlPrefix;
            } else {
                // Handle direct file path case
                const dirPrefix = typeof pathConfig === 'string' ? pathConfig : pathConfig.path;
                const cleanedDirPrefix = dirPrefix.replace(/^\.\/+/, '');
                urlPrefix = `file://${cleanedDirPrefix}`;
            }
            
            logger.debug(`Checking for obsolete chunks with URL prefix: ${urlPrefix}`);
            const response = await client.scroll(collectionName, {
                limit: 10000,
                with_payload: true,
                with_vector: false,
                filter: {
                    must: [
                        {
                            key: "url",
                            match: {
                                text: urlPrefix
                            }
                        }
                    ],
                    must_not: [
                        {
                            key: "is_metadata",
                            match: {
                                value: true
                            }
                        }
                    ]
                }
            });
            
            const obsoletePointIds = response.points
                .filter((point: any) => {
                    const url = point.payload?.url;
                    // Double check it's not a metadata record
                    if (point.payload?.is_metadata === true) {
                        return false;
                    }
                    
                    if (!url || !url.startsWith(urlPrefix)) {
                        return false;
                    }
                    
                    let filePath: string;
                    
                    if (isRewriteMode) {
                        // URL rewrite mode: extract relative path and construct full file path
                        const config = pathConfig as { path: string; url_rewrite_prefix?: string };
                        const relativePath = url.substring(urlPrefix.length + 1); // +1 for the '/'
                        filePath = path.join(config.path, relativePath);
                    } else {
                        // Direct file path mode: remove file:// prefix to match with processedFiles
                        filePath = url.startsWith('file://') ? url.substring(7) : '';
                    }
                    
                    return filePath && !processedFiles.has(filePath);
                })
                .map((point: any) => point.id);
            
            if (obsoletePointIds.length > 0) {
                await client.delete(collectionName, {
                    points: obsoletePointIds,
                });
                logger.info(`Deleted ${obsoletePointIds.length} obsolete chunks from Qdrant for URL prefix ${urlPrefix}`);
            } else {
                logger.info(`No obsolete chunks to delete from Qdrant for URL prefix ${urlPrefix}`);
            }
        } catch (error) {
            logger.error(`Error removing obsolete chunks from Qdrant:`, error);
        }
    }

    private async initDatabase(config: SourceConfig, parentLogger: Logger): Promise<DatabaseConnection> {
        const logger = parentLogger.child('database');
        const dbConfig = config.database_config;
        
        if (dbConfig.type === 'sqlite') {
            const params = dbConfig.params as SqliteDatabaseParams;
            const dbPath = params.db_path || path.join(process.cwd(), `${config.product_name.replace(/\s+/g, '_')}-${config.version}.db`);
            
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
        } else if (dbConfig.type === 'qdrant') {
            const params = dbConfig.params as QdrantDatabaseParams;
            const qdrantUrl = params.qdrant_url || 'http://localhost:6333';
            const qdrantPort = params.qdrant_port || 443;
            const collectionName = params.collection_name || `${config.product_name.toLowerCase().replace(/\s+/g, '_')}_${config.version}`;
            
            logger.info(`Connecting to Qdrant at ${qdrantUrl}:${qdrantPort}, collection: ${collectionName}`);
            const qdrantClient = new QdrantClient({ url: qdrantUrl, apiKey: process.env.QDRANT_API_KEY, port: qdrantPort });
    
            await this.createCollectionQdrant(qdrantClient, collectionName, logger);
            logger.info(`Qdrant connection established successfully`);
            return { client: qdrantClient, collectionName, type: 'qdrant' };
        } else {
            const errMsg = `Unsupported database type: ${dbConfig.type}`;
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
            // Get all points that match the URL prefix but are not metadata points
            const response = await client.scroll(collectionName, {
                limit: 10000,
                with_payload: true,
                with_vector: false,
                filter: {
                    must: [
                        {
                            key: "url",
                            match: {
                                text: urlPrefix
                            }
                        }
                    ],
                    must_not: [
                        {
                            key: "is_metadata",
                            match: {
                                value: true
                            }
                        }
                    ]
                }
            });
    
            const obsoletePointIds = response.points
                .filter((point: any) => {
                    const url = point.payload?.url;
                    // Double check it's not a metadata record
                    if (point.payload?.is_metadata === true) {
                        return false;
                    }
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
        sourceConfig: WebsiteSourceConfig,
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
                const content = await this.processPage(url, sourceConfig);
    
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
                    if (fullUrl.startsWith(sourceConfig.url) && !visitedUrls.has(this.normalizeUrl(fullUrl))) {
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

    private async processPage(url: string, sourceConfig: SourceConfig): Promise<string | null> {
        const logger = this.logger.child('page-processor');
        logger.debug(`Processing page content from ${url}`);
    
        let browser: Browser | null = null;
        try {
            browser = await puppeteer.launch({
                args: ['--no-sandbox', '--disable-setuid-sandbox'],
            });
            const page: Page = await browser.newPage();
            logger.debug(`Navigating to ${url}`);
            await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
    
            const htmlContent: string = await page.evaluate(() => {
                const mainContentElement = document.querySelector('div[role="main"].document') || document.querySelector('main') || document.body;
                return mainContentElement.innerHTML;
            });
    
            if (htmlContent.length > sourceConfig.max_size) {
                logger.warn(`Raw HTML content (${htmlContent.length} chars) exceeds max size (${sourceConfig.max_size}). Skipping detailed processing for ${url}.`);
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

    private async chunkMarkdown(markdown: string, sourceConfig: SourceConfig, url: string): Promise<DocumentChunk[]> {
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
                    product_name: sourceConfig.product_name,
                    version: sourceConfig.version,
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