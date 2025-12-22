#!/usr/bin/env node

import axios from 'axios';
import crypto from 'crypto';
import * as yaml from 'js-yaml';
import * as fs from 'fs';
import * as path from 'path';
import { Buffer } from 'buffer';
import { OpenAI } from "openai";
import * as dotenv from "dotenv";
import { Logger, LogLevel } from './logger';
import { Utils } from './utils';
import { DatabaseManager } from './database';
import { ContentProcessor } from './content-processor';
import { 
    Config, 
    SourceConfig, 
    GithubSourceConfig, 
    WebsiteSourceConfig, 
    LocalDirectorySourceConfig,
    ZendeskSourceConfig,
    DatabaseConnection,
    DocumentChunk
} from './types';

const GITHUB_TOKEN = process.env.GITHUB_PERSONAL_ACCESS_TOKEN;

dotenv.config();

class Doc2Vec {
    private config: Config;
    private openai: OpenAI;
    private contentProcessor: ContentProcessor;
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
        this.contentProcessor = new ContentProcessor(this.logger);
    }

    private loadConfig(configPath: string): Config {
        try {
            const logger = this.logger.child('config');
            logger.info(`Loading configuration from ${configPath}`);
            
            let configFile = fs.readFileSync(configPath, 'utf8');
            
            // Substitute environment variables in the format ${VAR_NAME}
            configFile = configFile.replace(/\$\{([^}]+)\}/g, (match, varName) => {
                const envValue = process.env[varName];
                if (envValue === undefined) {
                    logger.warn(`Environment variable ${varName} not found, keeping placeholder ${match}`);
                    return match;
                }
                logger.debug(`Substituted ${match} with environment variable value`);
                return envValue;
            });
            
            let config = yaml.load(configFile) as any;
            
            const typedConfig = config as Config;
            logger.info(`Configuration loaded successfully, found ${typedConfig.sources.length} sources`);
            return typedConfig;
        } catch (error) {
            this.logger.error(`Failed to load or parse config file at ${configPath}:`, error);
            process.exit(1);
        }
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
            } else if (sourceConfig.type === 'zendesk') {
                await this.processZendesk(sourceConfig, sourceLogger);
            } else {
                sourceLogger.error(`Unknown source type: ${(sourceConfig as any).type}`);
            }
        }
        
        this.logger.section('PROCESSING COMPLETE');
    }

    private async fetchAndProcessGitHubIssues(repo: string, sourceConfig: GithubSourceConfig, dbConnection: DatabaseConnection, logger: Logger): Promise<void> {
        const [owner, repoName] = repo.split('/');
        const GITHUB_API_URL = `https://api.github.com/repos/${owner}/${repoName}/issues`;
        
        // Initialize metadata storage if needed
        await DatabaseManager.initDatabaseMetadata(dbConnection, logger);
        
        // Get the last run date from the database
        const startDate = sourceConfig.start_date || '2025-01-01';
        const lastRunDate = await DatabaseManager.getLastRunDate(dbConnection, repo, `${startDate}T00:00:00Z`, logger);

        const fetchWithRetry = async (url: string, params = {}, retries = 5, delay = 5000): Promise<any> => {
            const retryableCodes = ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'EPIPE', 'EHOSTUNREACH'];
            
            for (let attempt = 0; attempt < retries; attempt++) {
                try {
                    const response = await axios.get(url, {
                        headers: {
                            Authorization: `token ${GITHUB_TOKEN}`,
                            Accept: 'application/vnd.github.v3+json',
                        },
                        params,
                        timeout: 30000, // 30 second timeout
                    });
                    return response.data;
                } catch (error: any) {
                    const isLastAttempt = attempt === retries - 1;
                    const errorCode = error.code || (error.cause && error.cause.code);
                    const isRetryableNetworkError = retryableCodes.includes(errorCode);
                    const isRateLimitError = error.response && error.response.status === 403;
                    const isServerError = error.response && error.response.status >= 500;
                    
                    if (isRateLimitError) {
                        const resetTime = error.response.headers['x-ratelimit-reset'];
                        const currentTime = Math.floor(Date.now() / 1000);
                        const waitTime = resetTime ? (resetTime - currentTime) * 1000 : delay * 2;
                        logger.warn(`GitHub rate limit exceeded. Waiting ${waitTime / 1000}s (attempt ${attempt + 1}/${retries})`);
                        await new Promise(res => setTimeout(res, waitTime));
                    } else if ((isRetryableNetworkError || isServerError) && !isLastAttempt) {
                        const backoffTime = delay * Math.pow(2, attempt); // Exponential backoff
                        logger.warn(`Network error (${errorCode || error.message}). Retrying in ${backoffTime / 1000}s (attempt ${attempt + 1}/${retries})`);
                        await new Promise(res => setTimeout(res, backoffTime));
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
            
            const chunks = await this.contentProcessor.chunkMarkdown(markdown, issueConfig, url);
            logger.info(`Issue #${issueNumber}: Created ${chunks.length} chunks`);
            
            // Process and store each chunk immediately
            for (const chunk of chunks) {
                const chunkHash = Utils.generateHash(chunk.content);
                const chunkId = chunk.metadata.chunk_id.substring(0, 8) + '...';
                
                if (dbConnection.type === 'sqlite') {
                    const { checkHashStmt } = DatabaseManager.prepareSQLiteStatements(dbConnection.db);
                    const existing = checkHashStmt.get(chunk.metadata.chunk_id) as { hash: string } | undefined;
                    
                    if (existing && existing.hash === chunkHash) {
                        logger.info(`Skipping unchanged chunk: ${chunkId}`);
                        continue;
                    }

                    const embeddings = await this.createEmbeddings([chunk.content]);
                    if (embeddings.length) {
                        DatabaseManager.insertVectorsSQLite(dbConnection.db, chunk, embeddings[0], logger, chunkHash);
                        logger.debug(`Stored chunk ${chunkId} in SQLite`);
                    } else {
                        logger.error(`Embedding failed for chunk: ${chunkId}`);
                    }
                } else if (dbConnection.type === 'qdrant') {
                    try {
                        let pointId: string;
                        try {
                            pointId = chunk.metadata.chunk_id;
                            if (!Utils.isValidUuid(pointId)) {
                                pointId = Utils.hashToUuid(chunk.metadata.chunk_id);
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
                            await DatabaseManager.storeChunkInQdrant(dbConnection, chunk, embeddings[0], chunkHash);
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
        await DatabaseManager.updateLastRunDate(dbConnection, repo, logger);
        
        logger.info(`Successfully processed ${issues.length} issues`);
    }

    private async processGithubRepo(config: GithubSourceConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for GitHub repo: ${config.repo}`);
        
        const dbConnection = await DatabaseManager.initDatabase(config, logger);
        
        // Initialize metadata storage
        await DatabaseManager.initDatabaseMetadata(dbConnection, logger);
        
        logger.section('GITHUB ISSUES');
        
        // Process GitHub issues
        await this.fetchAndProcessGitHubIssues(config.repo, config, dbConnection, logger);
        
        logger.info(`Finished processing GitHub repo: ${config.repo}`);
    }

    private async processWebsite(config: WebsiteSourceConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for website: ${config.url}`);
        
        const dbConnection = await DatabaseManager.initDatabase(config, logger);
        const validChunkIds: Set<string> = new Set();
        const visitedUrls: Set<string> = new Set();
        const urlPrefix = Utils.getUrlPrefix(config.url);
        
        logger.section('CRAWL AND EMBEDDING');

        const crawlResult = await this.contentProcessor.crawlWebsite(config.url, config, async (url, content) => {
            visitedUrls.add(url);

            logger.info(`Processing content from ${url} (${content.length} chars markdown)`);
            try {
                const chunks = await this.contentProcessor.chunkMarkdown(content, config, url);
                logger.info(`Created ${chunks.length} chunks`);

                if (chunks.length > 0) {
                    const chunkProgress = logger.progress(`Embedding chunks for ${url}`, chunks.length);

                    for (let i = 0; i < chunks.length; i++) {
                        const chunk = chunks[i];
                        validChunkIds.add(chunk.metadata.chunk_id);

                        const chunkId = chunk.metadata.chunk_id.substring(0, 8) + '...';

                        let needsEmbedding = true;
                        const chunkHash = Utils.generateHash(chunk.content);

                        if (dbConnection.type === 'sqlite') {
                            const { checkHashStmt } = DatabaseManager.prepareSQLiteStatements(dbConnection.db);
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
                                    if (!Utils.isValidUuid(pointId)) {
                                        pointId = Utils.hashToUuid(chunk.metadata.chunk_id);
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
                                    DatabaseManager.insertVectorsSQLite(dbConnection.db, chunk, embedding, logger, chunkHash);
                                    chunkProgress.update(1, `Stored chunk ${chunkId} in SQLite`);
                                } else if (dbConnection.type === 'qdrant') {
                                    await DatabaseManager.storeChunkInQdrant(dbConnection, chunk, embedding, chunkHash);
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
        
        if (crawlResult.hasNetworkErrors) {
            logger.warn('Skipping cleanup due to network errors encountered during crawling. This prevents removal of valid chunks when the site is temporarily unreachable.');
        } else {
            if (dbConnection.type === 'sqlite') {
                logger.info(`Running SQLite cleanup for ${urlPrefix}`);
                DatabaseManager.removeObsoleteChunksSQLite(dbConnection.db, visitedUrls, urlPrefix, logger);
            } else if (dbConnection.type === 'qdrant') {
                logger.info(`Running Qdrant cleanup for ${urlPrefix} in collection ${dbConnection.collectionName}`);
                await DatabaseManager.removeObsoleteChunksQdrant(dbConnection, visitedUrls, urlPrefix, logger);
            }
        }

        logger.info(`Finished processing website: ${config.url}`);
    }

    private async processLocalDirectory(config: LocalDirectorySourceConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for local directory: ${config.path}`);
        
        const dbConnection = await DatabaseManager.initDatabase(config, logger);
        const validChunkIds: Set<string> = new Set();
        const processedFiles: Set<string> = new Set();
        
        logger.section('FILE SCANNING AND EMBEDDING');
        
        await this.contentProcessor.processDirectory(
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
                    
                    const chunks = await this.contentProcessor.chunkMarkdown(content, config, fileUrl);
                    logger.info(`Created ${chunks.length} chunks`);
                    
                    if (chunks.length > 0) {
                        const chunkProgress = logger.progress(`Embedding chunks for ${filePath}`, chunks.length);
                        
                        for (let i = 0; i < chunks.length; i++) {
                            const chunk = chunks[i];
                            validChunkIds.add(chunk.metadata.chunk_id);
                            
                            const chunkId = chunk.metadata.chunk_id.substring(0, 8) + '...';
                            
                            let needsEmbedding = true;
                            const chunkHash = Utils.generateHash(chunk.content);
                            
                            if (dbConnection.type === 'sqlite') {
                                const { checkHashStmt } = DatabaseManager.prepareSQLiteStatements(dbConnection.db);
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
                                        if (!Utils.isValidUuid(pointId)) {
                                            pointId = Utils.hashToUuid(chunk.metadata.chunk_id);
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
                                        DatabaseManager.insertVectorsSQLite(dbConnection.db, chunk, embedding, logger, chunkHash);
                                        chunkProgress.update(1, `Stored chunk ${chunkId} in SQLite`);
                                    } else if (dbConnection.type === 'qdrant') {
                                        await DatabaseManager.storeChunkInQdrant(dbConnection, chunk, embedding, chunkHash);
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
            DatabaseManager.removeObsoleteFilesSQLite(dbConnection.db, processedFiles, config, logger);
        } else if (dbConnection.type === 'qdrant') {
            logger.info(`Running Qdrant cleanup for local directory ${config.path} in collection ${dbConnection.collectionName}`);
            await DatabaseManager.removeObsoleteFilesQdrant(dbConnection, processedFiles, config, logger);
        }
        
        logger.info(`Finished processing local directory: ${config.path}`);
    }

    private async processZendesk(config: ZendeskSourceConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for Zendesk: ${config.zendesk_subdomain}.zendesk.com`);
        
        const dbConnection = await DatabaseManager.initDatabase(config, logger);
        
        // Initialize metadata storage
        await DatabaseManager.initDatabaseMetadata(dbConnection, logger);
        
        const fetchTickets = config.fetch_tickets !== false; // default true
        const fetchArticles = config.fetch_articles !== false; // default true
        
        if (fetchTickets) {
            logger.section('ZENDESK TICKETS');
            await this.fetchAndProcessZendeskTickets(config, dbConnection, logger);
        }
        
        if (fetchArticles) {
            logger.section('ZENDESK ARTICLES');
            await this.fetchAndProcessZendeskArticles(config, dbConnection, logger);
        }
        
        logger.info(`Finished processing Zendesk: ${config.zendesk_subdomain}.zendesk.com`);
    }

    private async fetchAndProcessZendeskTickets(config: ZendeskSourceConfig, dbConnection: DatabaseConnection, logger: Logger): Promise<void> {
        const baseUrl = `https://${config.zendesk_subdomain}.zendesk.com/api/v2`;
        const auth = Buffer.from(`${config.email}/token:${config.api_token}`).toString('base64');
        
        // Get the last run date from the database
        const startDate = config.start_date || `${new Date().getFullYear()}-01-01`;
        const lastRunDate = await DatabaseManager.getLastRunDate(dbConnection, `zendesk_tickets_${config.zendesk_subdomain}`, `${startDate}T00:00:00Z`, logger);
        
        const fetchWithRetry = async (url: string, retries = 3): Promise<any> => {
            for (let attempt = 0; attempt < retries; attempt++) {
                try {
                    const response = await axios.get(url, {
                        headers: {
                            'Authorization': `Basic ${auth}`,
                            'Content-Type': 'application/json',
                        },
                    });
                    
                    if (response.status === 429) {
                        const retryAfter = parseInt(response.headers['retry-after'] || '60');
                        logger.warn(`Rate limited, waiting ${retryAfter}s before retry`);
                        await new Promise(res => setTimeout(res, retryAfter * 1000));
                        continue;
                    }
                    
                    return response.data;
                } catch (error: any) {
                    logger.error(`Zendesk API error (attempt ${attempt + 1}):`, error.message);
                    if (attempt === retries - 1) throw error;
                    await new Promise(res => setTimeout(res, 2000 * (attempt + 1)));
                }
            }
        };

        const generateMarkdownForTicket = (ticket: any, comments: any[]): string => {
            let md = `# Ticket #${ticket.id}: ${ticket.subject}\n\n`;
            md += `- **Status:** ${ticket.status}\n`;
            md += `- **Priority:** ${ticket.priority || 'None'}\n`;
            md += `- **Type:** ${ticket.type || 'None'}\n`;
            md += `- **Requester:** ${ticket.requester_id}\n`;
            md += `- **Assignee:** ${ticket.assignee_id || 'Unassigned'}\n`;
            md += `- **Created:** ${new Date(ticket.created_at).toDateString()}\n`;
            md += `- **Updated:** ${new Date(ticket.updated_at).toDateString()}\n`;
            
            if (ticket.tags && ticket.tags.length > 0) {
                md += `- **Tags:** ${ticket.tags.map((tag: string) => `\`${tag}\``).join(', ')}\n`;
            }
            
            // Handle ticket description
            const description = ticket.description || '';
            const cleanDescription = description || '_No description._';
            md += `\n## Description\n\n${cleanDescription}\n\n`;
            
            if (comments && comments.length > 0) {
                md += `## Comments\n\n`;
                for (const comment of comments) {
                    if (comment.public) {
                        md += `### ${comment.author_id} - ${new Date(comment.created_at).toDateString()}\n\n`;
                        
                        // Handle comment body
                        const rawBody = comment.plain_body || comment.html_body || comment.body || '';
                        const commentBody = rawBody.replace(/&nbsp;/g, " ") || '_No content._';
                        
                        md += `${commentBody}\n\n---\n\n`;
                    }
                }
            } else {
                md += `## Comments\n\n_No comments._\n`;
            }

            return md;
        };

        const processTicket = async (ticket: any): Promise<void> => {
            const ticketId = ticket.id;
            const url = `https://${config.zendesk_subdomain}.zendesk.com/agent/tickets/${ticketId}`;
            
            logger.info(`Processing ticket #${ticketId}`);
            
            // Fetch ticket comments
            const commentsUrl = `${baseUrl}/tickets/${ticketId}/comments.json`;
            const commentsData = await fetchWithRetry(commentsUrl);
            const comments = commentsData?.comments || [];
            
            // Generate markdown for the ticket
            const markdown = generateMarkdownForTicket(ticket, comments);
            
            // Chunk the markdown content
            const ticketConfig = {
                ...config,
                product_name: config.product_name || `zendesk_${config.zendesk_subdomain}`,
                max_size: config.max_size || Infinity
            };
            
            const chunks = await this.contentProcessor.chunkMarkdown(markdown, ticketConfig, url);
            logger.info(`Ticket #${ticketId}: Created ${chunks.length} chunks`);
            
            // Process and store each chunk
            for (const chunk of chunks) {
                const chunkHash = Utils.generateHash(chunk.content);
                const chunkId = chunk.metadata.chunk_id.substring(0, 8) + '...';
                
                if (dbConnection.type === 'sqlite') {
                    const { checkHashStmt } = DatabaseManager.prepareSQLiteStatements(dbConnection.db);
                    const existing = checkHashStmt.get(chunk.metadata.chunk_id) as { hash: string } | undefined;
                    
                    if (existing && existing.hash === chunkHash) {
                        logger.info(`Skipping unchanged chunk: ${chunkId}`);
                        continue;
                    }

                    const embeddings = await this.createEmbeddings([chunk.content]);
                    if (embeddings.length) {
                        DatabaseManager.insertVectorsSQLite(dbConnection.db, chunk, embeddings[0], logger, chunkHash);
                        logger.debug(`Stored chunk ${chunkId} in SQLite`);
                    } else {
                        logger.error(`Embedding failed for chunk: ${chunkId}`);
                    }
                } else if (dbConnection.type === 'qdrant') {
                    try {
                        let pointId: string;
                        try {
                            pointId = chunk.metadata.chunk_id;
                            if (!Utils.isValidUuid(pointId)) {
                                pointId = Utils.hashToUuid(chunk.metadata.chunk_id);
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
                            await DatabaseManager.storeChunkInQdrant(dbConnection, chunk, embeddings[0], chunkHash);
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

        logger.info(`Fetching Zendesk tickets updated since ${lastRunDate}`);
        
        // Build query parameters
        const statusFilter = config.ticket_status || ['new', 'open', 'pending', 'hold', 'solved'];
        const query = `updated>${lastRunDate.split('T')[0]} status:${statusFilter.join(',status:')}`;
        
        let nextPage = `${baseUrl}/search.json?query=${encodeURIComponent(query)}&sort_by=updated_at&sort_order=asc`;
        let totalTickets = 0;
        
        while (nextPage) {
            const data = await fetchWithRetry(nextPage);
            const tickets = data.results || [];
            
            logger.info(`Processing batch of ${tickets.length} tickets`);
            
            for (const ticket of tickets) {
                await processTicket(ticket);
                totalTickets++;
            }
            
            nextPage = data.next_page;
            
            if (nextPage) {
                logger.debug(`Fetching next page: ${nextPage}`);
                // Rate limiting: wait between requests
                await new Promise(res => setTimeout(res, 1000));
            }
        }

        // Update the last run date in the database
        await DatabaseManager.updateLastRunDate(dbConnection, `zendesk_tickets_${config.zendesk_subdomain}`, logger);
        
        logger.info(`Successfully processed ${totalTickets} tickets`);
    }

    private async fetchAndProcessZendeskArticles(config: ZendeskSourceConfig, dbConnection: DatabaseConnection, logger: Logger): Promise<void> {
        const baseUrl = `https://${config.zendesk_subdomain}.zendesk.com/api/v2/help_center`;
        const auth = Buffer.from(`${config.email}/token:${config.api_token}`).toString('base64');
        
        // Get the start date for filtering
        const startDate = config.start_date || `${new Date().getFullYear()}-01-01`;
        const startDateObj = new Date(startDate);
        
        const fetchWithRetry = async (url: string, retries = 3): Promise<any> => {
            for (let attempt = 0; attempt < retries; attempt++) {
                try {
                    const response = await axios.get(url, {
                        headers: {
                            'Authorization': `Basic ${auth}`,
                            'Content-Type': 'application/json',
                        },
                    });
                    
                    if (response.status === 429) {
                        const retryAfter = parseInt(response.headers['retry-after'] || '60');
                        logger.warn(`Rate limited, waiting ${retryAfter}s before retry`);
                        await new Promise(res => setTimeout(res, retryAfter * 1000));
                        continue;
                    }
                    
                    return response.data;
                } catch (error: any) {
                    logger.error(`Zendesk API error (attempt ${attempt + 1}):`, error.message);
                    if (attempt === retries - 1) throw error;
                    await new Promise(res => setTimeout(res, 2000 * (attempt + 1)));
                }
            }
        };

        const generateMarkdownForArticle = (article: any): string => {
            let md = `# ${article.title}\n\n`;
            md += `- **Author:** ${article.author_id}\n`;
            md += `- **Section:** ${article.section_id}\n`;
            md += `- **Created:** ${new Date(article.created_at).toDateString()}\n`;
            md += `- **Updated:** ${new Date(article.updated_at).toDateString()}\n`;
            md += `- **Vote Sum:** ${article.vote_sum || 0}\n`;
            md += `- **Vote Count:** ${article.vote_count || 0}\n`;
            
            if (article.label_names && article.label_names.length > 0) {
                md += `- **Labels:** ${article.label_names.map((label: string) => `\`${label}\``).join(', ')}\n`;
            }
            
            // Handle article content - convert HTML to markdown
            const articleBody = article.body || '';
            let cleanContent = '_No content._';
            if (articleBody.trim()) {
                if (articleBody.includes('<')) {
                    // HTML content - use ContentProcessor to convert to markdown
                    cleanContent = this.contentProcessor.convertHtmlToMarkdown(articleBody);
                } else {
                    // Plain text content
                    cleanContent = articleBody;
                }
            }
            
            md += `\n## Content\n\n${cleanContent}\n`;

            return md;
        };

        const processArticle = async (article: any): Promise<void> => {
            const articleId = article.id;
            const url = article.html_url || `https://${config.zendesk_subdomain}.zendesk.com/hc/articles/${articleId}`;
            
            logger.info(`Processing article #${articleId}: ${article.title}`);
            
            // Generate markdown for the article
            const markdown = generateMarkdownForArticle(article);
            
            // Chunk the markdown content
            const articleConfig = {
                ...config,
                product_name: config.product_name || `zendesk_${config.zendesk_subdomain}`,
                max_size: config.max_size || Infinity
            };
            
            const chunks = await this.contentProcessor.chunkMarkdown(markdown, articleConfig, url);
            logger.info(`Article #${articleId}: Created ${chunks.length} chunks`);
            
            // Process and store each chunk (similar to ticket processing)
            for (const chunk of chunks) {
                const chunkHash = Utils.generateHash(chunk.content);
                const chunkId = chunk.metadata.chunk_id.substring(0, 8) + '...';
                
                if (dbConnection.type === 'sqlite') {
                    const { checkHashStmt } = DatabaseManager.prepareSQLiteStatements(dbConnection.db);
                    const existing = checkHashStmt.get(chunk.metadata.chunk_id) as { hash: string } | undefined;
                    
                    if (existing && existing.hash === chunkHash) {
                        logger.info(`Skipping unchanged chunk: ${chunkId}`);
                        continue;
                    }

                    const embeddings = await this.createEmbeddings([chunk.content]);
                    if (embeddings.length) {
                        DatabaseManager.insertVectorsSQLite(dbConnection.db, chunk, embeddings[0], logger, chunkHash);
                        logger.debug(`Stored chunk ${chunkId} in SQLite`);
                    } else {
                        logger.error(`Embedding failed for chunk: ${chunkId}`);
                    }
                } else if (dbConnection.type === 'qdrant') {
                    try {
                        let pointId: string;
                        try {
                            pointId = chunk.metadata.chunk_id;
                            if (!Utils.isValidUuid(pointId)) {
                                pointId = Utils.hashToUuid(chunk.metadata.chunk_id);
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
                            await DatabaseManager.storeChunkInQdrant(dbConnection, chunk, embeddings[0], chunkHash);
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

        logger.info(`Fetching Zendesk help center articles updated since ${startDate}`);
        
        let nextPage = `${baseUrl}/articles.json`;
        let totalArticles = 0;
        let processedArticles = 0;
        
        while (nextPage) {
            const data = await fetchWithRetry(nextPage);
            const articles = data.articles || [];
            
            logger.info(`Processing batch of ${articles.length} articles`);
            
            for (const article of articles) {
                totalArticles++;
                
                // Check if article was updated since the start date
                const updatedAt = new Date(article.updated_at);
                if (updatedAt >= startDateObj) {
                    await processArticle(article);
                    processedArticles++;
                } else {
                    logger.debug(`Skipping article #${article.id} (updated ${article.updated_at}, before ${startDate})`);
                }
            }
            
            nextPage = data.next_page;
            
            if (nextPage) {
                logger.debug(`Fetching next page: ${nextPage}`);
                // Rate limiting: wait between requests
                await new Promise(res => setTimeout(res, 1000));
            }
        }
        
        logger.info(`Successfully processed ${processedArticles} of ${totalArticles} articles (filtered by date >= ${startDate})`);
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