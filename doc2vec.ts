#!/usr/bin/env node

import axios from 'axios';
import crypto from 'crypto';
import * as yaml from 'js-yaml';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { exec } from 'child_process';
import { promisify } from 'util';
import { Buffer } from 'buffer';
import { OpenAI, AzureOpenAI } from "openai";
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
    CodeSourceConfig,
    ZendeskSourceConfig,
    S3SourceConfig,
    DatabaseConnection,
    DocumentChunk,
    BrokenLink,
    EmbeddingConfig,
    MarkdownStoreConfig,
    SourceRunStats,
    SourceRunCounters,
    newSourceRunCounters
} from './types';
import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { MarkdownStore } from './markdown-store';

const GITHUB_TOKEN = process.env.GITHUB_PERSONAL_ACCESS_TOKEN;
const execAsync = promisify(exec);

dotenv.config();

export class Doc2Vec {
    private config: Config;
    private openai: OpenAI | AzureOpenAI;
    private embeddingModel: string;
    private embeddingDimension: number;
    private contentProcessor: ContentProcessor;
    private logger: Logger;
    private configDir: string;
    private brokenLinksByWebsite: Record<string, BrokenLink[]> = {};
    private markdownStore: MarkdownStore | undefined;
    // Change counters for the source currently being processed by run().
    // Sources are processed sequentially, so a single slot is sufficient.
    private counters: SourceRunCounters = newSourceRunCounters('items');

    constructor(configPath: string) {
        this.logger = new Logger('Doc2Vec', {
            level: LogLevel.DEBUG,
            useTimestamp: true,
            useColor: true,
            prettyPrint: true
        });
        
        this.logger.info('Initializing Doc2Vec');
        this.config = this.loadConfig(configPath);
        this.configDir = path.dirname(path.resolve(configPath));
        
        // Initialize OpenAI or Azure OpenAI based on configuration
        // Check environment variable if not specified in config
        const embeddingProvider = this.config.embedding?.provider || (process.env.EMBEDDING_PROVIDER as 'openai' | 'azure') || 'openai';
        const embeddingConfig = this.config.embedding || { provider: embeddingProvider };
        this.embeddingDimension = this.resolveEmbeddingDimension(embeddingConfig);
        
        if (embeddingProvider === 'azure') {
            const azureApiKey = embeddingConfig.azure?.api_key || process.env.AZURE_OPENAI_KEY;
            const azureEndpoint = embeddingConfig.azure?.endpoint || process.env.AZURE_OPENAI_ENDPOINT;
            const azureDeploymentName = embeddingConfig.azure?.deployment_name || process.env.AZURE_OPENAI_DEPLOYMENT_NAME || 'text-embedding-3-large';
            const azureApiVersion = embeddingConfig.azure?.api_version || process.env.AZURE_OPENAI_API_VERSION || '2024-10-21';
            
            if (!azureApiKey || !azureEndpoint) {
                this.logger.error('Azure OpenAI requires api_key and endpoint to be configured');
                process.exit(1);
            }
            
            this.openai = new AzureOpenAI({
                apiKey: azureApiKey,
                endpoint: azureEndpoint,
                deployment: azureDeploymentName,
                apiVersion: azureApiVersion,
            });
            this.embeddingModel = azureDeploymentName;
            this.logger.info(`Using Azure OpenAI with deployment: ${azureDeploymentName} (${this.embeddingDimension} dimensions)`);
        } else {
            const openaiApiKey = embeddingConfig.openai?.api_key || process.env.OPENAI_API_KEY;
            const openaiModel = embeddingConfig.openai?.model || process.env.OPENAI_MODEL || 'text-embedding-3-large';
            const openaiBaseURL = embeddingConfig.openai?.base_url || process.env.OPENAI_BASE_URL;

            if (!openaiApiKey) {
                this.logger.error('OpenAI requires api_key to be configured');
                process.exit(1);
            }

            this.openai = new OpenAI({
                apiKey: openaiApiKey,
                ...(openaiBaseURL && { baseURL: openaiBaseURL }),
            });
            this.embeddingModel = openaiModel;
            this.logger.info(`Using OpenAI with model: ${openaiModel} (${this.embeddingDimension} dimensions)${openaiBaseURL ? ` via ${openaiBaseURL}` : ''}`);
        }
        
        this.contentProcessor = new ContentProcessor(this.logger);

        // Initialize Postgres markdown store if configured
        if (this.config.markdown_store) {
            this.markdownStore = new MarkdownStore(this.config.markdown_store, this.logger);
        }
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
            for (const source of typedConfig.sources) {
                if (source.type === 'code') {
                    if (!source.version || String(source.version).trim().length === 0) {
                        if (source.branch && String(source.branch).trim().length > 0) {
                            source.version = source.branch;
                        } else {
                            source.version = 'local';
                        }
                    }
                } else if (!source.version || String(source.version).trim().length === 0) {
                    logger.error(`Missing required version for ${source.type} source: ${source.product_name}`);
                    process.exit(1);
                }
            }
            logger.info(`Configuration loaded successfully, found ${typedConfig.sources.length} sources`);
            return typedConfig;
        } catch (error) {
            this.logger.error(`Failed to load or parse config file at ${configPath}:`, error);
            process.exit(1);
        }
    }

    private resolveEmbeddingDimension(embeddingConfig: EmbeddingConfig | undefined): number {
        const defaultDimension = 3072;
        const rawConfigValue = embeddingConfig?.dimension;
        const rawEnvValue = process.env.EMBEDDING_DIMENSION;

        const candidate = rawConfigValue ?? (rawEnvValue ? Number(rawEnvValue) : undefined);
        if (candidate === undefined) {
            return defaultDimension;
        }

        const parsedValue = typeof candidate === 'string' ? Number(candidate) : candidate;
        if (!Number.isFinite(parsedValue) || parsedValue <= 0 || !Number.isInteger(parsedValue)) {
            this.logger.warn(`Invalid embedding dimension provided (${candidate}), falling back to ${defaultDimension}`);
            return defaultDimension;
        }

        return parsedValue;
    }

    public async run(): Promise<SourceRunStats[]> {
        // Initialize Postgres markdown store table if configured
        if (this.markdownStore) {
            await this.markdownStore.init();
        }

        this.logger.section('PROCESSING SOURCES');

        const runStats: SourceRunStats[] = [];

        for (const sourceConfig of this.config.sources) {
            const sourceLogger = this.logger.child(`source:${sourceConfig.product_name}`);

            sourceLogger.info(`Processing ${sourceConfig.type} source for ${sourceConfig.product_name}@${sourceConfig.version}`);

            const startTime = Date.now();
            let ok = true;
            let errorMessage: string | undefined;

            const itemsKindByType: Record<string, string> = {
                website: 'pages',
                github: 'issues',
                local_directory: 'files',
                code: 'files',
                s3: 'objects',
                zendesk: 'items',
            };
            this.counters = newSourceRunCounters(itemsKindByType[sourceConfig.type] ?? 'items');

            try {
                if (sourceConfig.type === 'github') {
                    await this.processGithubRepo(sourceConfig, sourceLogger);
                } else if (sourceConfig.type === 'website') {
                    await this.processWebsite(sourceConfig, sourceLogger);
                } else if (sourceConfig.type === 'local_directory') {
                    await this.processLocalDirectory(sourceConfig, sourceLogger);
                } else if (sourceConfig.type === 'code') {
                    await this.processCodeSource(sourceConfig, sourceLogger);
                } else if (sourceConfig.type === 'zendesk') {
                    await this.processZendesk(sourceConfig, sourceLogger);
                } else if (sourceConfig.type === 's3') {
                    await this.processS3(sourceConfig, sourceLogger);
                } else {
                    ok = false;
                    errorMessage = `Unknown source type: ${(sourceConfig as any).type}`;
                    sourceLogger.error(errorMessage);
                }
            } catch (error) {
                ok = false;
                errorMessage = error instanceof Error ? error.message : String(error);
                sourceLogger.error(`Failed to process ${sourceConfig.type} source for ${sourceConfig.product_name}:`, error);
            }

            runStats.push({
                product_name: sourceConfig.product_name,
                type: sourceConfig.type,
                version: sourceConfig.version,
                duration_ms: Date.now() - startTime,
                ok,
                ...(errorMessage && { error: errorMessage }),
                counters: this.counters
            });
        }

        // Close the Postgres markdown store connection pool
        if (this.markdownStore) {
            await this.markdownStore.close();
        }

        this.logger.section('PROCESSING COMPLETE');
        this.logger.event('run-summary', { sources: runStats });

        return runStats;
    }

    private async fetchAndProcessGitHubIssues(repo: string, sourceConfig: GithubSourceConfig, dbConnection: DatabaseConnection, logger: Logger): Promise<void> {
        const [owner, repoName] = repo.split('/');
        const GITHUB_API_URL = `https://api.github.com/repos/${owner}/${repoName}/issues`;
        
        // Initialize metadata storage if needed
        await DatabaseManager.initDatabaseMetadata(dbConnection, logger);
        // Capture timestamp at the start so issues updated during sync are re-processed next run
        const syncStartDate = new Date().toISOString();

        // Get the last run date from the database
        const startDate = sourceConfig.start_date || '2025-01-01';
        const lastRunDate = await DatabaseManager.getLastRunDate(dbConnection, repo, `${startDate}T00:00:00Z`, logger);

        // Retryable: 5xx, 429, rate-limited 403s, and network/timeout errors.
        // Everything else is deterministic — the same request fails identically,
        // so retrying only burns wall-clock. In particular a 422 from deep
        // page-number pagination used to cost ~75s of backoff before failing.
        const isRetryableStatus = (status: number | undefined): boolean =>
            status === undefined || status >= 500 || status === 429;

        const requestWithRetry = async (url: string, params?: Record<string, any>, retries = 5, delay = 5000): Promise<any> => {
            for (let attempt = 0; attempt < retries; attempt++) {
                try {
                    // Only log on retries to reduce noise during pagination
                    if (attempt > 0) {
                        logger.debug(`GitHub API retry: ${url} (attempt ${attempt + 1}/${retries})`);
                    }
                    // Callers need the response headers (Link) for cursor
                    // pagination, so the whole response is returned.
                    const response = await axios.get(url, {
                        headers: {
                            Authorization: `token ${GITHUB_TOKEN}`,
                            Accept: 'application/vnd.github.v3+json',
                        },
                        // Subsequent pages come from a Link URL that already
                        // carries its query string — adding params would duplicate it
                        ...(params && { params }),
                        timeout: 30000, // 30 second timeout
                    });
                    return response;
                } catch (error: any) {
                    // Enhanced error logging for debugging
                    const errorDetails = {
                        code: error.code,
                        message: error.message,
                        status: error.response?.status,
                        isTimeout: error.code === 'ECONNABORTED' || error.message?.includes('timeout'),
                        isNetworkError: !error.response && error.code,
                    };
                    logger.debug(`GitHub API error details: ${JSON.stringify(errorDetails)}`);
                    
                    if (error.response && error.response.status === 403) {
                        // Check if this is actually a rate limit error
                        const rateLimitRemaining = error.response.headers['x-ratelimit-remaining'];
                        const resetTime = error.response.headers['x-ratelimit-reset'];
                        
                        if (rateLimitRemaining === '0' && resetTime) {
                            const currentTime = Math.floor(Date.now() / 1000);
                            const resetTimestamp = parseInt(resetTime, 10);
                            let waitTime = (resetTimestamp - currentTime) * 1000;
                            
                            // Ensure waitTime is at least 1 second (in case resetTime is in the past)
                            if (waitTime < 1000) {
                                waitTime = 1000;
                            }
                            
                            logger.warn(`GitHub rate limit exceeded. Waiting ${Math.ceil(waitTime / 1000)}s (attempt ${attempt + 1}/${retries})`);
                            await new Promise(res => setTimeout(res, waitTime));
                            
                            // Retry the request after waiting
                            continue;
                        } else {
                            // Other 403 errors (e.g., forbidden access)
                            logger.error(`GitHub API returned 403 (not rate limit): ${error.message}`);
                            throw error;
                        }
                    } else if (!isRetryableStatus(error.response?.status)) {
                        // Deterministic failure (422, 404, 401, ...) — surface the
                        // API's own message, which explains what to do instead.
                        const apiMessage = error.response?.data?.message;
                        logger.error(
                            `GitHub API returned ${error.response.status} (not retryable): ${apiMessage || error.message}`
                        );
                        throw error;
                    } else {
                        // For non-403 errors, wait before retrying (exponential backoff)
                        if (attempt < retries - 1) {
                            const backoffDelay = delay * Math.pow(2, attempt);
                            logger.warn(`GitHub fetch failed (attempt ${attempt + 1}/${retries}): ${error.message}. Retrying in ${backoffDelay}ms`);
                            await new Promise(res => setTimeout(res, backoffDelay));
                        } else {
                            logger.error(`GitHub fetch failed after ${retries} attempts: ${error.message} (code: ${error.code || 'unknown'})`);
                            throw error;
                        }
                    }
                }
            }
            throw new Error('Max retries reached');
        };

        /**
         * Walk every issue updated since `sinceDate` by following GitHub's
         * `Link: rel="next"` cursor URLs.
         *
         * Page-number pagination (`?page=N`) cannot be used here: GitHub caps it
         * on this endpoint and answers HTTP 422 ("Pagination with the page
         * parameter is not supported for large datasets, please use cursor based
         * pagination") once the offset grows past its limit. That limit is well
         * below the size of a full-window rebuild on a busy repo, so page-based
         * paging aborted the whole source and left the collection empty. The
         * Link URL already contains the opaque `after=` cursor and is passed
         * through verbatim.
         */
        const fetchAllIssues = async (sinceDate: string): Promise<any[]> => {
            let issues: any[] = [];
            // sort=updated&direction=asc makes the walk stable: results are
            // ordered by the same field `since` filters on, so a page boundary
            // can't shuffle items in or out mid-walk.
            let url: string | null = GITHUB_API_URL;
            let params: Record<string, any> | undefined = {
                per_page: 100,
                state: 'all',
                since: sinceDate,
                sort: 'updated',
                direction: 'asc',
            };
            let page = 0;

            while (url) {
                page++;
                // Log progress every 10 pages to reduce noise
                if (page === 1 || page % 10 === 0) {
                    logger.debug(`Fetching issues page ${page}... (${issues.length} issues so far)`);
                }

                const response = await requestWithRetry(url, params);
                const data = response.data;
                if (!Array.isArray(data)) {
                    logger.warn(`Unexpected non-array response while paginating issues for ${repo}; stopping the walk`);
                    break;
                }

                // The `since` param already scopes results to issues whose
                // updated_at is after sinceDate — i.e. anything created, closed,
                // reopened, edited, or newly commented since the last run. Do NOT
                // re-filter by created_at: an old issue that was just closed or
                // received a new comment has an old created_at and would be
                // dropped, leaving its status and comments permanently stale.
                issues = issues.concat(data);

                const nextUrl = Utils.parseNextLink(response.headers?.link ?? response.headers?.Link);
                // Guard against a server echoing the same cursor forever
                if (!nextUrl || nextUrl === url) break;
                url = nextUrl;
                params = undefined; // the cursor URL is already fully-formed
            }

            logger.info(`Fetched ${issues.length} issues/PRs for ${repo} across ${page} page(s)`);
            return issues;
        };

        const fetchIssueComments = async (issueNumber: number): Promise<any[]> => {
            // GitHub returns 30 comments per page by default and paginates the
            // rest behind Link headers; requesting the first page only silently
            // truncated long discussions, so the stored chunks were missing the
            // newest comments.
            const comments: any[] = [];
            let url: string | null = `${GITHUB_API_URL}/${issueNumber}/comments`;
            let params: Record<string, any> | undefined = { per_page: 100 };

            while (url) {
                const response = await requestWithRetry(url, params);
                if (!Array.isArray(response.data)) break;
                comments.push(...response.data);

                const nextUrl = Utils.parseNextLink(response.headers?.link ?? response.headers?.Link);
                if (!nextUrl || nextUrl === url) break;
                url = nextUrl;
                params = undefined;
            }
            return comments;
        };

        const generateMarkdownForIssue = async (issue: any): Promise<string> => {
            const comments = await fetchIssueComments(issue.number);
            // This endpoint returns both issues and pull requests; only PRs carry
            // a `pull_request` object. Labelling every item "Issue #N" made
            // retrieved PR chunks read as issues, so use the real kind.
            const itemLabel = issue.pull_request ? 'PR' : 'Issue';
            let md = `# ${itemLabel} #${issue.number}: ${issue.title}\n\n`;
            md += `- **Type:** ${issue.pull_request ? 'Pull request' : 'Issue'}\n`;
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

        // Chunks that could not be stored (e.g. a rejected Qdrant upsert).
        // Tallied across all issues and escalated once the walk completes.
        let failedChunks = 0;

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

            // Purge the issue's existing chunks before inserting the fresh set.
            // chunk_id is a content hash, so when an issue changes (closed/reopened,
            // edited, new comments) the regenerated chunks get new ids and the old
            // ones would otherwise linger — leaving stale state ("open") and missing
            // the latest comments in search results. All chunks for an issue share
            // its unique url, so delete-by-url reconciles them exactly.
            let removedChunks = 0;
            if (dbConnection.type === 'sqlite') {
                removedChunks = DatabaseManager.removeChunksByUrlSQLite(dbConnection.db, url, logger);
            } else if (dbConnection.type === 'qdrant') {
                removedChunks = await DatabaseManager.removeChunksByUrlQdrant(dbConnection, url, logger);
            }
            if (removedChunks > 0) {
                this.counters.items_updated++;
                this.counters.chunks_deleted += removedChunks;
            } else {
                this.counters.items_new++;
            }

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
                        this.counters.chunks_added++;
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
                            this.counters.chunks_added++;
                            logger.debug(`Stored chunk ${chunkId} in Qdrant (${dbConnection.collectionName})`);
                        } else {
                            logger.error(`Embedding failed for chunk: ${chunkId}`);
                        }
                    } catch (error) {
                        // Keep going so one bad chunk can't cost the whole repo,
                        // but remember it: the count is escalated below so the run
                        // fails and the last-run date is not advanced.
                        failedChunks++;
                        logger.error(`Error processing chunk ${chunkId} for ${url} in Qdrant:`, error);
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

        // A chunk that failed to store is silent data loss, so refuse to advance
        // the last-run date: the next run reprocesses this window instead of
        // treating the gap as synced. Everything that did store is kept.
        if (failedChunks > 0) {
            throw new Error(
                `${failedChunks} chunk(s) failed to store for ${repo}; not advancing the last-run date so the next run retries them`
            );
        }

        // Update the last run date in the database after processing all issues
        await DatabaseManager.updateLastRunDate(dbConnection, repo, logger, this.embeddingDimension, syncStartDate);

        logger.info(`Successfully processed ${issues.length} issues`);
    }

    private async processGithubRepo(config: GithubSourceConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for GitHub repo: ${config.repo}`);
        
        const dbConnection = await DatabaseManager.initDatabase(config, logger, this.embeddingDimension);
        
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
        
    const dbConnection = await DatabaseManager.initDatabase(config, logger, this.embeddingDimension);
    await DatabaseManager.initDatabaseMetadata(dbConnection, logger);
        const validChunkIds: Set<string> = new Set();
        const visitedUrls: Set<string> = new Set();
        const urlPrefix = Utils.getUrlPrefix(config.url);
        
        logger.section('CRAWL AND EMBEDDING');

        // Pre-load known URLs from the database so the queue includes pages from
        // previous runs. This ensures link discovery isn't lost when pages are
        // skipped via ETag matching.
        let knownUrls: Set<string> | undefined;
        if (dbConnection.type === 'sqlite') {
            const urls = DatabaseManager.getStoredUrlsByPrefixSQLite(dbConnection.db, urlPrefix);
            if (urls.length > 0) {
                knownUrls = new Set(urls);
                logger.info(`Found ${urls.length} known URLs in database for pre-seeding`);
            }
        } else if (dbConnection.type === 'qdrant') {
            const urls = await DatabaseManager.getStoredUrlsByPrefixQdrant(dbConnection, urlPrefix);
            if (urls.length > 0) {
                knownUrls = new Set(urls);
                logger.info(`Found ${urls.length} known URLs in Qdrant for pre-seeding`);
            }
        }

        // ETag store for caching page ETags across runs
        const etagStore = {
            get: async (url: string): Promise<string | undefined> => {
                return DatabaseManager.getMetadataValue(dbConnection, `etag:${url}`, undefined, logger);
            },
            set: async (url: string, etag: string): Promise<void> => {
                await DatabaseManager.setMetadataValue(dbConnection, `etag:${url}`, etag, logger, this.embeddingDimension);
            },
        };

        const lastmodStore = {
            get: async (url: string): Promise<string | undefined> => {
                return DatabaseManager.getMetadataValue(dbConnection, `lastmod:${url}`, undefined, logger);
            },
            set: async (url: string, lastmod: string): Promise<void> => {
                await DatabaseManager.setMetadataValue(dbConnection, `lastmod:${url}`, lastmod, logger, this.embeddingDimension);
            },
        };

        // If Postgres markdown store is enabled for this source, load URLs that
        // already have markdown stored.  URLs NOT in this set will bypass
        // lastmod/ETag skip logic so that the store is fully populated on the
        // first sync.
        const useMarkdownStore = config.markdown_store === true && this.markdownStore != null;
        let markdownStoreUrls: Set<string> | undefined;
        if (useMarkdownStore) {
            markdownStoreUrls = await this.markdownStore!.getUrlsWithMarkdown(urlPrefix);
            logger.info(`Markdown store: ${markdownStoreUrls.size} URLs already stored for prefix ${urlPrefix}`);
        }

        // Check whether a full sync has ever completed successfully for this
        // source.  If not, bypass all lastmod/ETag skip logic so that every
        // page is processed at least once.  This handles the case where a
        // previous sync was interrupted (killed mid-crawl) — the stored
        // ETags/lastmods from the partial run would otherwise cause the
        // remaining pages to be skipped indefinitely.
        const syncCompleteKey = `sync_complete:${urlPrefix}`;
        const syncCompleteValue = await DatabaseManager.getMetadataValue(dbConnection, syncCompleteKey, undefined, logger);
        const forceFullSync = syncCompleteValue !== 'true';
        if (forceFullSync) {
            logger.info('Full sync has not yet completed for this source — forcing processing of all pages (bypassing lastmod/ETag skip)');
        }

        const crawlResult = await this.contentProcessor.crawlWebsite(config.url, config, async (url, content) => {
            visitedUrls.add(url);

            logger.info(`Processing content from ${url} (${content.length} chars markdown)`);
            try {
                const chunks = await this.contentProcessor.chunkMarkdown(content, config, url);
                logger.info(`Created ${chunks.length} chunks`);

                for (const chunk of chunks) {
                    validChunkIds.add(chunk.metadata.chunk_id);
                }

                await this.processChunksForUrl(chunks, url, dbConnection, logger);

                // Store the generated markdown in Postgres
                if (useMarkdownStore) {
                    try {
                        await this.markdownStore!.upsertMarkdown(url, config.product_name, content);
                    } catch (pgError) {
                        logger.error(`Failed to store markdown in Postgres for ${url}:`, pgError);
                    }
                }

                return true;
            } catch (error) {
                logger.error(`Error during chunking or embedding for ${url}:`, error);
                return false;
            }

        }, logger, visitedUrls, { knownUrls, etagStore, lastmodStore, markdownStoreUrls, forceFullSync });

        this.recordBrokenLinks(config.url, crawlResult.brokenLinks);
        this.writeBrokenLinksReport();

        logger.info(`Found ${validChunkIds.size} valid chunks across processed pages for ${config.url}`);

        logger.section('CLEANUP');

        // Remove URLs that returned 404 during the crawl. A 404 is a definitive
        // "this page is gone" signal (network errors fall through to full
        // processing and never land here), so we purge these unconditionally —
        // even when hasNetworkErrors below skips the broad obsolete cleanup.
        if (crawlResult.notFoundUrls && crawlResult.notFoundUrls.size > 0) {
            logger.info(`Removing ${crawlResult.notFoundUrls.size} not-found (404) URLs`);
            for (const url of crawlResult.notFoundUrls) {
                // Postgres markdown store
                if (useMarkdownStore) {
                    try {
                        await this.markdownStore!.deleteMarkdown(url);
                    } catch (pgError) {
                        logger.error(`Failed to delete markdown from Postgres for ${url}:`, pgError);
                    }
                }
                // Vector DB chunks
                try {
                    let removedChunks = 0;
                    if (dbConnection.type === 'sqlite') {
                        removedChunks = DatabaseManager.removeChunksByUrlSQLite(dbConnection.db, url, logger);
                    } else if (dbConnection.type === 'qdrant') {
                        removedChunks = await DatabaseManager.removeChunksByUrlQdrant(dbConnection, url, logger);
                    }
                    if (removedChunks > 0) {
                        this.counters.items_deleted++;
                        this.counters.chunks_deleted += removedChunks;
                    }
                } catch (dbError) {
                    logger.error(`Failed to delete chunks for 404 URL ${url}:`, dbError);
                }
            }
        }

        if (crawlResult.hasNetworkErrors) {
            logger.warn('Skipping cleanup due to network errors encountered during crawling. This prevents removal of valid chunks when the site is temporarily unreachable.');
        } else {
            // Mark this source as having completed a full sync.  On subsequent
            // runs, lastmod/ETag skip logic will function normally.  If the
            // process is killed before reaching this point, the flag stays
            // unset and the next run will force-process all pages again.
            if (forceFullSync) {
                await DatabaseManager.setMetadataValue(dbConnection, syncCompleteKey, 'true', logger, this.embeddingDimension);
                logger.info('Full sync completed successfully — subsequent runs will use normal caching');
            }
            let removed = { items: 0, chunks: 0 };
            if (dbConnection.type === 'sqlite') {
                logger.info(`Running SQLite cleanup for ${urlPrefix}`);
                removed = DatabaseManager.removeObsoleteChunksSQLite(dbConnection.db, visitedUrls, urlPrefix, logger);
            } else if (dbConnection.type === 'qdrant') {
                logger.info(`Running Qdrant cleanup for ${urlPrefix} in collection ${dbConnection.collectionName}`);
                removed = await DatabaseManager.removeObsoleteChunksQdrant(dbConnection, visitedUrls, urlPrefix, logger);
            }
            this.counters.items_deleted += removed.items;
            this.counters.chunks_deleted += removed.chunks;
        }

        logger.info(`Finished processing website: ${config.url}`);
    }

    private recordBrokenLinks(baseUrl: string, brokenLinks: BrokenLink[]): void {
        const uniqueByKey = new Map<string, BrokenLink>();
        for (const link of brokenLinks) {
            const key = `${link.source} -> ${link.target}`;
            if (!uniqueByKey.has(key)) {
                uniqueByKey.set(key, link);
            }
        }
        const unique = Array.from(uniqueByKey.values()).sort((a, b) => {
            const sourceCompare = a.source.localeCompare(b.source);
            if (sourceCompare !== 0) return sourceCompare;
            return a.target.localeCompare(b.target);
        });
        this.brokenLinksByWebsite[baseUrl] = unique;
    }

    private writeBrokenLinksReport(): void {
        // The default (next to the config file) breaks when the config lives on
        // a read-only mount (e.g. a Kubernetes ConfigMap in controller mode) —
        // DOC2VEC_REPORT_DIR points the report somewhere writable instead.
        const reportDir = process.env.DOC2VEC_REPORT_DIR || this.configDir;
        const reportPath = path.join(reportDir, '404.yaml');
        const orderedEntries = Object.entries(this.brokenLinksByWebsite)
            .sort(([a], [b]) => a.localeCompare(b));
        const reportPayload = orderedEntries.map(([website, links]) => ({
            website,
            'broken-links': links
        }));

        try {
            fs.writeFileSync(reportPath, yaml.dump(reportPayload, { noRefs: true }), 'utf8');
            this.logger.info(`Wrote broken link report to ${reportPath}`);
        } catch (error) {
            this.logger.error(`Failed to write broken link report to ${reportPath}:`, error);
        }
    }

    private async processLocalDirectory(config: LocalDirectorySourceConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for local directory: ${config.path}`);
        
        const dbConnection = await DatabaseManager.initDatabase(config, logger, this.embeddingDimension);
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

                    for (const chunk of chunks) {
                        validChunkIds.add(chunk.metadata.chunk_id);
                    }

                    await this.processChunksForUrl(chunks, fileUrl, dbConnection, logger);
                } catch (error) {
                    logger.error(`Error during chunking or embedding for ${filePath}:`, error);
                }
            }, 
            logger
        );
        
        logger.section('CLEANUP');
        let removed = { items: 0, chunks: 0 };
        if (dbConnection.type === 'sqlite') {
            logger.info(`Running SQLite cleanup for local directory ${config.path}`);
            removed = DatabaseManager.removeObsoleteFilesSQLite(dbConnection.db, processedFiles, config, logger);
        } else if (dbConnection.type === 'qdrant') {
            logger.info(`Running Qdrant cleanup for local directory ${config.path} in collection ${dbConnection.collectionName}`);
            removed = await DatabaseManager.removeObsoleteFilesQdrant(dbConnection, processedFiles, config, logger);
        }
        this.counters.items_deleted += removed.items;
        this.counters.chunks_deleted += removed.chunks;

        logger.info(`Finished processing local directory: ${config.path}`);
    }

    private async processS3(config: S3SourceConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for S3 bucket: ${config.bucket}${config.prefix ? ` (prefix: ${config.prefix})` : ''}`);

        const dbConnection = await DatabaseManager.initDatabase(config, logger, this.embeddingDimension);
        await DatabaseManager.initDatabaseMetadata(dbConnection, logger);
        const processedFiles: Set<string> = new Set();
        // Capture timestamp at the start so objects modified during sync are re-processed next run
        const syncStartTimestamp = Date.now();

        const s3Client = new S3Client({
            region: config.region || process.env.AWS_DEFAULT_REGION || 'us-east-1',
            ...(config.endpoint ? { endpoint: config.endpoint, forcePathStyle: true } : {})
        });

        // Metadata keys for incremental sync
        const sanitizedPrefix = (config.prefix || '').replace(/[^a-zA-Z0-9]+/g, '_');
        const bucketKey = `${config.bucket}_${sanitizedPrefix}`;
        const lastSyncKey = `s3_last_sync_${bucketKey}`;
        const fileListKey = `s3_filelist_${bucketKey}`;

        const lastSyncValue = await DatabaseManager.getMetadataValue(dbConnection, lastSyncKey, '0', logger);
        const lastSyncTimestamp = parseInt(lastSyncValue || '0', 10);

        const includeExtensions = config.include_extensions || ['.md', '.txt', '.html', '.htm', '.pdf', '.doc', '.docx'];
        const excludeExtensions = config.exclude_extensions || [];
        const encoding = config.encoding || 'utf8' as BufferEncoding;

        // List all matching objects
        logger.section('S3 OBJECT LISTING');
        const allObjects: Array<{ key: string; lastModified: Date; size: number }> = [];
        let continuationToken: string | undefined;

        do {
            const response = await s3Client.send(new ListObjectsV2Command({
                Bucket: config.bucket,
                Prefix: config.prefix || undefined,
                ContinuationToken: continuationToken,
            }));

            for (const obj of response.Contents || []) {
                if (!obj.Key || obj.Key.endsWith('/')) continue; // skip folder markers

                const extension = path.extname(obj.Key).toLowerCase();

                if (excludeExtensions.includes(extension)) continue;
                if (includeExtensions.length > 0 && !includeExtensions.includes(extension)) continue;

                allObjects.push({
                    key: obj.Key,
                    lastModified: obj.LastModified!,
                    size: obj.Size || 0,
                });
            }

            continuationToken = response.NextContinuationToken;
        } while (continuationToken);

        logger.info(`Found ${allObjects.length} matching objects in S3 bucket`);

        // Process objects
        logger.section('S3 FILE PROCESSING AND EMBEDDING');
        let processedCount = 0;
        let skippedCount = 0;

        for (const obj of allObjects) {
            processedFiles.add(obj.key);

            // Incremental check: skip if object hasn't been modified since last sync
            if (lastSyncTimestamp > 0 && obj.lastModified.getTime() <= lastSyncTimestamp) {
                logger.debug(`Skipping unchanged object: ${obj.key}`);
                skippedCount++;
                continue;
            }

            // Size check
            if (obj.size > config.max_size) {
                logger.warn(`Object ${obj.key} (${obj.size} bytes) exceeds max_size (${config.max_size}). Skipping.`);
                skippedCount++;
                continue;
            }

            logger.info(`Processing S3 object: ${obj.key} (${obj.size} bytes)`);

            try {
                const getResponse = await s3Client.send(new GetObjectCommand({
                    Bucket: config.bucket,
                    Key: obj.key,
                }));

                const extension = path.extname(obj.key).toLowerCase();
                let content: string;

                const binaryExtensions = ['.pdf', '.doc', '.docx'];
                if (binaryExtensions.includes(extension)) {
                    // Binary files: write to temp file and convert
                    const bodyBytes = await getResponse.Body!.transformToByteArray();
                    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 's3-doc2vec-'));
                    const tempFilePath = path.join(tempDir, path.basename(obj.key));
                    try {
                        fs.writeFileSync(tempFilePath, Buffer.from(bodyBytes));
                        content = await this.contentProcessor.convertFileToMarkdown(tempFilePath, extension, logger);
                    } finally {
                        // Cleanup temp files
                        try { fs.unlinkSync(tempFilePath); } catch {}
                        try { fs.rmdirSync(tempDir); } catch {}
                    }
                } else if (extension === '.html' || extension === '.htm') {
                    // HTML files: write to temp, convert via contentProcessor
                    const bodyString = await getResponse.Body!.transformToString(encoding);
                    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 's3-doc2vec-'));
                    const tempFilePath = path.join(tempDir, path.basename(obj.key));
                    try {
                        fs.writeFileSync(tempFilePath, bodyString, { encoding });
                        content = await this.contentProcessor.convertFileToMarkdown(tempFilePath, extension, logger);
                    } finally {
                        try { fs.unlinkSync(tempFilePath); } catch {}
                        try { fs.rmdirSync(tempDir); } catch {}
                    }
                } else {
                    // Text files: read directly as string
                    content = await getResponse.Body!.transformToString(encoding);
                }

                if (content.length > config.max_size) {
                    logger.warn(`Processed content for ${obj.key} (${content.length} chars) exceeds max_size. Skipping.`);
                    skippedCount++;
                    continue;
                }

                // Generate URL
                let fileUrl: string;
                if (config.url_rewrite_prefix) {
                    const prefix = config.url_rewrite_prefix.endsWith('/')
                        ? config.url_rewrite_prefix.slice(0, -1)
                        : config.url_rewrite_prefix;
                    const relativePath = config.prefix
                        ? obj.key.substring(config.prefix.length).replace(/^\//, '')
                        : obj.key;
                    fileUrl = `${prefix}/${relativePath}`;
                } else {
                    fileUrl = `s3://${config.bucket}/${obj.key}`;
                }

                // Resolve metadata(...) references in product_name and version
                const s3Meta = getResponse.Metadata || {};
                const resolvedConfig = {
                    ...config,
                    product_name: this.resolveS3MetadataValue(config.product_name, s3Meta),
                    version: this.resolveS3MetadataValue(config.version, s3Meta),
                };

                const chunks = await this.contentProcessor.chunkMarkdown(content, resolvedConfig, fileUrl);
                logger.info(`Created ${chunks.length} chunks for ${obj.key}`);

                await this.processChunksForUrl(chunks, fileUrl, dbConnection, logger);
                processedCount++;
            } catch (error) {
                logger.error(`Error processing S3 object ${obj.key}:`, error);
            }
        }

        logger.info(`Processed: ${processedCount}, Skipped: ${skippedCount}`);

        // Cleanup: remove chunks for deleted objects
        logger.section('CLEANUP');
        const previousListValue = await DatabaseManager.getMetadataValue(dbConnection, fileListKey, '[]', logger);
        const previousList: string[] = previousListValue ? JSON.parse(previousListValue) : [];
        const deletedKeys = previousList.filter(key => !processedFiles.has(key));

        if (deletedKeys.length > 0) {
            logger.info(`Removing chunks for ${deletedKeys.length} deleted objects`);
            for (const deletedKey of deletedKeys) {
                let fileUrl: string;
                if (config.url_rewrite_prefix) {
                    const prefix = config.url_rewrite_prefix.endsWith('/')
                        ? config.url_rewrite_prefix.slice(0, -1)
                        : config.url_rewrite_prefix;
                    const relativePath = config.prefix
                        ? deletedKey.substring(config.prefix.length).replace(/^\//, '')
                        : deletedKey;
                    fileUrl = `${prefix}/${relativePath}`;
                } else {
                    fileUrl = `s3://${config.bucket}/${deletedKey}`;
                }

                let removedChunks = 0;
                if (dbConnection.type === 'sqlite') {
                    removedChunks = DatabaseManager.removeChunksByUrlSQLite(dbConnection.db, fileUrl, logger);
                } else if (dbConnection.type === 'qdrant') {
                    removedChunks = await DatabaseManager.removeChunksByUrlQdrant(dbConnection, fileUrl, logger);
                }
                if (removedChunks > 0) {
                    this.counters.items_deleted++;
                    this.counters.chunks_deleted += removedChunks;
                }
            }
        }

        // Persist sync state
        const currentKeys = Array.from(processedFiles);
        await DatabaseManager.setMetadataValue(dbConnection, fileListKey, JSON.stringify(currentKeys), logger, this.embeddingDimension);
        await DatabaseManager.setMetadataValue(dbConnection, lastSyncKey, `${syncStartTimestamp}`, logger, this.embeddingDimension);

        logger.info(`Finished processing S3 bucket: ${config.bucket}`);
    }

    /**
     * Resolves a config value that may use the metadata(...) syntax.
     * e.g. "metadata(x-amz-meta-product-name)" looks up "product-name" in the S3 object's user metadata.
     * Returns the original value if no metadata(...) pattern is found.
     * Returns empty string if the referenced metadata key doesn't exist on the object.
     */
    private resolveS3MetadataValue(configValue: string, s3Metadata: Record<string, string>): string {
        const match = configValue.match(/^metadata\((.+)\)$/);
        if (!match) return configValue;
        const metaKey = match[1];
        // AWS SDK returns user metadata keys without the x-amz-meta- prefix
        const lookupKey = metaKey.replace(/^x-amz-meta-/, '');
        return s3Metadata[lookupKey] ?? '';
    }

    private async processCodeSource(config: CodeSourceConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for code source (${config.source})`);

        const dbConnection = await DatabaseManager.initDatabase(config, logger, this.embeddingDimension);
        const validChunkIds: Set<string> = new Set();
        const processedFiles: Set<string> = new Set();

        let basePath: string | undefined;
        let cleanupPathConfig: { path: string; url_rewrite_prefix?: string } | string;
        let tempDir: string | null = null;
        let repoUrlPrefix: string | undefined;
        let repoBranch: string | undefined;
        let incrementalMode = false;
        let deleteUrls: string[] = [];
        let allowedFiles: Set<string> | undefined;
        let mtimeCutoff: number | undefined;
        let fileListKey: string | undefined;
        let lastMtimeKey: string | undefined;
        let trackedFiles: Set<string> | undefined;
        let maxObservedMtime = 0;
        // Set when a directory could not be listed: the files we saw are only a
        // subset of what is on disk, so nothing may be treated as deleted and no
        // "last scanned" marker may advance past this run
        let scanIncomplete = false;

        if (config.source === 'local_directory') {
            if (!config.path) {
                logger.error('Code source type local_directory requires a path.');
                return;
            }
            basePath = config.path;
            cleanupPathConfig = config.url_rewrite_prefix
                ? { path: basePath, url_rewrite_prefix: config.url_rewrite_prefix }
                : basePath;

            const resolvedPath = path.resolve(basePath);
            const pathKey = resolvedPath.replace(/[^a-zA-Z0-9]+/g, '_');
            lastMtimeKey = `code_last_mtime_${pathKey}`;
            fileListKey = `code_filelist_${pathKey}`;

            await DatabaseManager.initDatabaseMetadata(dbConnection, logger);
            const lastMtimeValue = await DatabaseManager.getMetadataValue(dbConnection, lastMtimeKey, '0', logger);
            mtimeCutoff = lastMtimeValue ? parseFloat(lastMtimeValue) : 0;
            trackedFiles = new Set<string>();
            incrementalMode = true;
        } else if (config.source === 'github') {
            if (!config.repo) {
                logger.error('Code source type github requires a repo in owner/repo format.');
                return;
            }
            const cloneResult = await this.cloneGithubRepo(config, logger);
            basePath = cloneResult.path;
            tempDir = cloneResult.path;
            repoUrlPrefix = cloneResult.urlPrefix;
            repoBranch = cloneResult.branch;
            cleanupPathConfig = { path: basePath, url_rewrite_prefix: repoUrlPrefix };

            await DatabaseManager.initDatabaseMetadata(dbConnection, logger);
            const shaKey = this.buildCodeShaMetadataKey(config.repo, repoBranch);
            const lastSha = await DatabaseManager.getMetadataValue(dbConnection, shaKey, undefined, logger);
            const headSha = await this.getRepoHeadSha(basePath, logger);

            if (lastSha && headSha) {
                if (headSha === lastSha) {
                    incrementalMode = true;
                    allowedFiles = new Set();
                    deleteUrls = [];
                } else {
                    const diffResult = await this.getGitChangedFiles(basePath, lastSha, repoBranch, logger);
                    if (diffResult.mode === 'incremental') {
                        incrementalMode = true;
                        allowedFiles = diffResult.changedFiles;
                        deleteUrls = diffResult.deletedPaths
                            .map((relativePath) => this.buildCodeFileUrl(path.join(basePath as string, relativePath), basePath as string, config, repoUrlPrefix));
                    } else {
                        logger.warn('Falling back to full scan for GitHub code source.');
                    }
                }
            }
        } else {
            logger.error(`Unknown code source: ${config.source}`);
            return;
        }

        logger.section('CODE SCANNING AND EMBEDDING');

        try {
            const scanResult = await this.contentProcessor.processCodeDirectory(
                basePath,
                config,
                async (filePath, content) => {
                    processedFiles.add(filePath);

                    const relativePath = path.relative(basePath as string, filePath).replace(/\\/g, '/');
                    const fileUrl = this.buildCodeFileUrl(filePath, basePath as string, config, repoUrlPrefix);

                    logger.info(`Processing code from ${relativePath || filePath} (${content.length} chars)`);
                    try {
                        const chunks = await this.contentProcessor.chunkCode(
                            content,
                            config,
                            fileUrl,
                            relativePath || filePath,
                            repoBranch || config.branch,
                            config.repo
                        );
                        logger.info(`Created ${chunks.length} chunks`);

                        for (const chunk of chunks) {
                            validChunkIds.add(chunk.metadata.chunk_id);
                        }

                        await this.processChunksForUrl(chunks, fileUrl, dbConnection, logger);
                    } catch (error) {
                        logger.error(`Error during code chunking or embedding for ${filePath}:`, error);
                    }
                },
                logger,
                undefined,
                {
                    allowedFiles,
                    mtimeCutoff,
                    trackFiles: trackedFiles
                }
            );

            scanIncomplete = scanResult.incomplete;
            if (scanIncomplete) {
                logger.error('Directory scan was incomplete — skipping cleanup so unscanned files keep their chunks');
            }

            if (trackedFiles) {
                maxObservedMtime = scanResult.maxMtime;
            }
        } finally {
            logger.section('CLEANUP');

            if (scanIncomplete) {
                logger.warn('Skipping cleanup and last-scanned markers: the scan did not cover the whole tree');
            } else if (incrementalMode) {
                if (deleteUrls.length > 0) {
                    logger.info(`Cleaning up ${deleteUrls.length} deleted/renamed files`);
                    for (const url of deleteUrls) {
                        let removedChunks = 0;
                        if (dbConnection.type === 'sqlite') {
                            removedChunks = DatabaseManager.removeChunksByUrlSQLite(dbConnection.db, url, logger);
                        } else if (dbConnection.type === 'qdrant') {
                            removedChunks = await DatabaseManager.removeChunksByUrlQdrant(dbConnection, url, logger);
                        }
                        if (removedChunks > 0) {
                            this.counters.items_deleted++;
                            this.counters.chunks_deleted += removedChunks;
                        }
                    }
                } else {
                    logger.info('No deleted/renamed files to clean up.');
                }

                if (trackedFiles && fileListKey) {
                    const previousListValue = await DatabaseManager.getMetadataValue(dbConnection, fileListKey, '[]', logger);
                    const previousList = previousListValue ? JSON.parse(previousListValue) as string[] : [];
                    const currentList = Array.from(trackedFiles);
                    const deletedFiles = previousList.filter((filePath) => !trackedFiles?.has(filePath));

                    for (const deletedFile of deletedFiles) {
                        const url = this.buildCodeFileUrl(deletedFile, basePath as string, config, repoUrlPrefix);
                        let removedChunks = 0;
                        if (dbConnection.type === 'sqlite') {
                            removedChunks = DatabaseManager.removeChunksByUrlSQLite(dbConnection.db, url, logger);
                        } else if (dbConnection.type === 'qdrant') {
                            removedChunks = await DatabaseManager.removeChunksByUrlQdrant(dbConnection, url, logger);
                        }
                        if (removedChunks > 0) {
                            this.counters.items_deleted++;
                            this.counters.chunks_deleted += removedChunks;
                        }
                    }

                    await DatabaseManager.setMetadataValue(dbConnection, fileListKey, JSON.stringify(currentList), logger, this.embeddingDimension);
                    if (lastMtimeKey) {
                        const nextMtime = maxObservedMtime > 0 ? maxObservedMtime : Date.now();
                        await DatabaseManager.setMetadataValue(dbConnection, lastMtimeKey, `${nextMtime}`, logger, this.embeddingDimension);
                    }
                }
            } else {
                let removed = { items: 0, chunks: 0 };
                if (dbConnection.type === 'sqlite') {
                    logger.info(`Running SQLite cleanup for code source ${basePath}`);
                    removed = DatabaseManager.removeObsoleteFilesSQLite(dbConnection.db, processedFiles, cleanupPathConfig, logger);
                } else if (dbConnection.type === 'qdrant') {
                    logger.info(`Running Qdrant cleanup for code source ${basePath} in collection ${dbConnection.collectionName}`);
                    removed = await DatabaseManager.removeObsoleteFilesQdrant(dbConnection, processedFiles, cleanupPathConfig, logger);
                }
                this.counters.items_deleted += removed.items;
                this.counters.chunks_deleted += removed.chunks;
            }

            // Storing the SHA after an incomplete scan would make the next run
            // diff from it and never revisit the files this run missed
            if (config.source === 'github' && basePath && repoBranch && !scanIncomplete) {
                const headSha = await this.getRepoHeadSha(basePath, logger);
                if (headSha) {
                    const shaKey = this.buildCodeShaMetadataKey(config.repo as string, repoBranch);
                    await DatabaseManager.setMetadataValue(dbConnection, shaKey, headSha, logger, this.embeddingDimension);
                }
            }

            if (tempDir) {
                try {
                    fs.rmSync(tempDir, { recursive: true, force: true });
                    logger.debug(`Removed temporary repo at ${tempDir}`);
                } catch (error) {
                    logger.warn(`Failed to remove temporary repo at ${tempDir}:`, error);
                }
            }
        }

        // Fail the source rather than reporting a partial ingest as a success —
        // the collection is still serving whatever it had for the unscanned files
        if (scanIncomplete) {
            throw new Error(`Code scan of ${basePath} was incomplete: at least one directory could not be read`);
        }

        logger.info(`Finished processing code source (${config.source})`);
    }

    private buildCodeShaMetadataKey(repo: string, branch: string): string {
        const normalizedRepo = repo.replace(/[^a-zA-Z0-9]+/g, '_');
        const normalizedBranch = branch.replace(/[^a-zA-Z0-9]+/g, '_');
        return `code_last_sha_${normalizedRepo}_${normalizedBranch}`;
    }

    private async getRepoHeadSha(repoPath: string, logger: Logger): Promise<string | undefined> {
        try {
            const { stdout } = await execAsync(`git -C "${repoPath}" rev-parse HEAD`);
            return stdout.trim() || undefined;
        } catch (error) {
            logger.warn(`Failed to resolve HEAD sha for ${repoPath}:`, error);
            return undefined;
        }
    }

    private async getGitChangedFiles(
        repoPath: string,
        lastSha: string,
        branch: string,
        logger: Logger
    ): Promise<{ mode: 'incremental' | 'full'; changedFiles: Set<string>; deletedPaths: string[] }> {
        const diffCommand = `git -C "${repoPath}" diff --name-status ${lastSha}..HEAD`;

        const attemptDiff = async () => {
            const { stdout } = await execAsync(diffCommand);
            return stdout;
        };

        let diffOutput: string | undefined;

        try {
            diffOutput = await attemptDiff();
        } catch (error) {
            logger.warn(`Failed to diff against ${lastSha}. Fetching more history...`);
            const fetchDepths = [200, 1000, 5000];
            let fetched = false;
            for (const depth of fetchDepths) {
                try {
                    logger.info(`Fetching with --depth=${depth}...`);
                    await execAsync(`git -C "${repoPath}" fetch --depth=${depth} origin "${branch}"`);
                    diffOutput = await attemptDiff();
                    fetched = true;
                    break;
                } catch (fetchError) {
                    logger.warn(`Diff still failed at --depth=${depth}.`);
                }
            }
            if (!fetched) {
                try {
                    logger.info(`Attempting full unshallow fetch...`);
                    await execAsync(`git -C "${repoPath}" fetch --unshallow origin "${branch}"`);
                    diffOutput = await attemptDiff();
                } catch (unshallowError) {
                    logger.warn(`Failed to diff even after full unshallow. Falling back to full scan.`, unshallowError);
                    return { mode: 'full', changedFiles: new Set(), deletedPaths: [] };
                }
            }
        }

        if (!diffOutput) {
            logger.warn('No diff output available. Falling back to full scan.');
            return { mode: 'full', changedFiles: new Set(), deletedPaths: [] };
        }

        const changedFiles = new Set<string>();
        const deletedPaths: string[] = [];

        for (const line of diffOutput.split('\n')) {
            const trimmed = line.trim();
            if (!trimmed) continue;
            const parts = trimmed.split('\t');
            const status = parts[0];

            if (status.startsWith('R')) {
                const oldPath = parts[1];
                const newPath = parts[2];
                if (oldPath) deletedPaths.push(oldPath);
                if (newPath) changedFiles.add(path.join(repoPath, newPath));
            } else if (status === 'D') {
                const deletedPath = parts[1];
                if (deletedPath) deletedPaths.push(deletedPath);
            } else if (status === 'A' || status === 'M') {
                const changedPath = parts[1];
                if (changedPath) changedFiles.add(path.join(repoPath, changedPath));
            }
        }

        logger.info(`Git diff changes: ${changedFiles.size} modified/added, ${deletedPaths.length} deleted/renamed.`);
        return { mode: 'incremental', changedFiles, deletedPaths };
    }

    private buildCodeFileUrl(
        filePath: string,
        basePath: string,
        config: CodeSourceConfig,
        repoUrlPrefix?: string
    ): string {
        const relativePath = path.relative(basePath, filePath).replace(/\\/g, '/');

        if (repoUrlPrefix) {
            return `${repoUrlPrefix}/${relativePath}`;
        }

        if (config.url_rewrite_prefix) {
            if (relativePath.startsWith('..')) {
                return `file://${filePath}`;
            }

            const prefix = config.url_rewrite_prefix.endsWith('/')
                ? config.url_rewrite_prefix.slice(0, -1)
                : config.url_rewrite_prefix;

            return `${prefix}/${relativePath}`;
        }

        return `file://${filePath}`;
    }

    private async cloneGithubRepo(
        config: CodeSourceConfig,
        logger: Logger
    ): Promise<{ path: string; branch: string; urlPrefix: string }> {
        const repo = config.repo as string;
        const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'doc2vec-code-'));
        const requestedBranch = config.branch;
        const encodedToken = GITHUB_TOKEN ? encodeURIComponent(GITHUB_TOKEN) : '';
        const repoUrl = encodedToken
            ? `https://x-access-token:${encodedToken}@github.com/${repo}.git`
            : `https://github.com/${repo}.git`;

        const branchArg = requestedBranch ? `--branch "${requestedBranch}"` : '';
        logger.info(`Cloning ${repo} to ${tempDir}`);

        try {
            await execAsync(`git clone --depth 1 ${branchArg} "${repoUrl}" "${tempDir}"`);
        } catch (error) {
            try {
                fs.rmSync(tempDir, { recursive: true, force: true });
            } catch (cleanupError) {
                logger.warn(`Failed to clean up temp dir after clone failure: ${tempDir}`, cleanupError);
            }
            logger.error(`Failed to clone repo ${repo}:`, error);
            throw error;
        }

        let resolvedBranch = requestedBranch;
        if (!resolvedBranch) {
            resolvedBranch = await this.getRepoBranch(tempDir, logger);
        }

        const branch = resolvedBranch || 'main';
        const urlPrefix = `https://github.com/${repo}/blob/${branch}`;

        return { path: tempDir, branch, urlPrefix };
    }

    private async getRepoBranch(repoPath: string, logger: Logger): Promise<string | undefined> {
        try {
            const { stdout } = await execAsync(`git -C "${repoPath}" symbolic-ref --short HEAD`);
            const branch = stdout.trim();
            return branch || undefined;
        } catch (error) {
            logger.warn(`Failed to resolve repo branch for ${repoPath}:`, error);
            return undefined;
        }
    }

    private async processZendesk(config: ZendeskSourceConfig, parentLogger: Logger): Promise<void> {
        const logger = parentLogger.child('process');
        logger.info(`Starting processing for Zendesk: ${config.zendesk_subdomain}.zendesk.com`);
        
        const dbConnection = await DatabaseManager.initDatabase(config, logger, this.embeddingDimension);
        
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
        // Capture timestamp at the start so tickets updated during sync are re-processed next run
        const syncStartDate = new Date().toISOString();

        // Get the last run date from the database
        const startDate = config.start_date || `${new Date().getFullYear()}-01-01`;
        const lastRunDate = await DatabaseManager.getLastRunDate(dbConnection, `zendesk_tickets_${config.zendesk_subdomain}`, `${startDate}T00:00:00Z`, logger);

        // Status filter applied client-side (includes 'closed' by default so tickets
        // transitioning to closed get updated rather than left stale)
        const statusFilter = new Set(config.ticket_status || ['new', 'open', 'pending', 'hold', 'solved', 'closed']);

        const excludedOrgNames = new Set((config.excluded_organizations || []).map(n => n.toLowerCase()));
        const excludedOrgIds = new Set<number>();

        const fetchWithRetry = async (url: string, retries = 3): Promise<any> => {
            for (let attempt = 0; attempt < retries; attempt++) {
                try {
                    const response = await axios.get(url, {
                        headers: {
                            'Authorization': `Basic ${auth}`,
                            'Content-Type': 'application/json',
                        },
                    });
                    return response.data;
                } catch (error: any) {
                    // 403 is a permissions error — retrying won't help
                    if (error.response?.status === 403) throw error;

                    if (error.response?.status === 429) {
                        const retryAfter = parseInt(error.response.headers?.['retry-after'] || '60', 10);
                        logger.warn(`Rate limited by Zendesk, waiting ${retryAfter}s before retry`);
                        await new Promise(res => setTimeout(res, retryAfter * 1000));
                        attempt--; // Don't burn a retry on rate-limit waits
                        continue;
                    }

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
                    // Skip non-public comments unless internal comments are explicitly enabled
                    if (!comment.public && !config.include_internal_comments) {
                        continue;
                    }

                    const visibility = comment.public ? '' : ' (internal)';
                    md += `### ${comment.author_id} - ${new Date(comment.created_at).toDateString()}${visibility}\n\n`;

                    // Handle comment body
                    const rawBody = comment.plain_body || comment.html_body || comment.body || '';
                    const commentBody = rawBody.replace(/&nbsp;/g, " ") || '_No content._';

                    md += `${commentBody}\n\n---\n\n`;
                }
            } else {
                md += `## Comments\n\n_No comments._\n`;
            }

            return md;
        };

        const processTicket = async (ticket: any): Promise<void> => {
            const ticketId = ticket.id;
            const url = `https://${config.zendesk_subdomain}.zendesk.com/agent/tickets/${ticketId}`;

            // Deleted tickets — remove their chunks and stop
            if (ticket.status === 'deleted') {
                logger.info(`Ticket #${ticketId} was deleted in Zendesk — removing its chunks`);
                let removedChunks = 0;
                if (dbConnection.type === 'sqlite') {
                    removedChunks = DatabaseManager.removeChunksByUrlSQLite(dbConnection.db, url, logger);
                } else {
                    removedChunks = await DatabaseManager.removeChunksByUrlQdrant(dbConnection, url, logger);
                }
                if (removedChunks > 0) {
                    this.counters.items_deleted++;
                    this.counters.chunks_deleted += removedChunks;
                }
                return;
            }

            // Skip tickets belonging to excluded organizations
            if (ticket.organization_id && excludedOrgIds.has(ticket.organization_id)) {
                logger.debug(`Ticket #${ticketId} belongs to excluded organization ${ticket.organization_id} — skipping`);
                return;
            }

            // Skip tickets whose status is outside the configured filter
            if (!statusFilter.has(ticket.status)) {
                logger.debug(`Ticket #${ticketId} has status '${ticket.status}' outside configured filter — skipping`);
                return;
            }

            logger.info(`Processing ticket #${ticketId}`);

            // Fetch ticket comments. Zendesk returns at most 100 comments per page
            // (ordered oldest-first), so follow next_page to capture the newest
            // comments on tickets with more than 100 — otherwise they're silently dropped.
            const comments: any[] = [];
            let commentsUrl: string | null = `${baseUrl}/tickets/${ticketId}/comments.json`;
            while (commentsUrl) {
                const commentsData: any = await fetchWithRetry(commentsUrl);
                comments.push(...(commentsData?.comments || []));
                commentsUrl = commentsData?.next_page || null;
                if (commentsUrl) await new Promise(res => setTimeout(res, 1000));
            }

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

            // Use processChunksForUrl which performs a URL-level diff:
            // deletes all existing chunks for this URL before reinserting,
            // so stale chunks from previous versions are never left behind.
            await this.processChunksForUrl(chunks, url, dbConnection, logger);
        };

        if (excludedOrgNames.size > 0) {
            logger.info(`Resolving ${excludedOrgNames.size} excluded organization name(s) to IDs`);
            const resolvedNames = new Set<string>();
            let orgsUrl: string | null = `${baseUrl}/organizations.json?page[size]=100`;
            while (orgsUrl) {
                const orgsData: any = await fetchWithRetry(orgsUrl);
                for (const org of orgsData?.organizations || []) {
                    const orgName = (org.name || '').toLowerCase();
                    if (excludedOrgNames.has(orgName)) {
                        excludedOrgIds.add(org.id);
                        resolvedNames.add(orgName);
                    }
                }
                orgsUrl = orgsData?.meta?.has_more ? orgsData?.links?.next : null;
            }
            logger.info(`Excluding tickets from ${excludedOrgIds.size} organization(s): ${[...excludedOrgIds].join(', ')}`);
            const unresolved = [...excludedOrgNames].filter(name => !resolvedNames.has(name));
            if (unresolved.length > 0) {
                throw new Error(`Cannot resolve excluded organization(s): ${unresolved.join(', ')}. Aborting to avoid syncing data for them.`);
            }
        }

        logger.info(`Fetching Zendesk tickets updated since ${lastRunDate}`);

        // Build query parameters — use the status filter for the search query
        const statusList = Array.from(statusFilter);
        const statusClause = `status:${statusList.join(',status:')}`;

        // Zendesk /search.json caps results at 1000 (10 pages * 100). Bisect the date range
        // into smaller windows whenever a window would exceed that cap, so we never hit page 11.
        let totalTickets = 0;
        let failedTickets = 0;

        const processResults = async (results: any[]) => {
            for (const ticket of results) {
                try {
                    await processTicket(ticket);
                    totalTickets++;
                } catch (error: any) {
                    failedTickets++;
                    logger.error(`Failed to process ticket #${ticket.id}, will retry next run: ${error.message}`);
                }
            }
        };

        // Work queue of [start, end) date windows (ISO strings). Seed with [lastRunDate, syncStartDate).
        const windows: Array<[string, string]> = [[lastRunDate, syncStartDate]];
        const MAX_PER_WINDOW = 1000;

        while (windows.length > 0) {
            const [wStart, wEnd] = windows.shift()!;
            const q = `updated>${wStart} updated<${wEnd} ${statusClause}`;
            const firstUrl = `${baseUrl}/search.json?query=${encodeURIComponent(q)}&sort_by=updated_at&sort_order=asc`;

            logger.debug(`Searching window ${wStart} .. ${wEnd}`);
            const firstData = await fetchWithRetry(firstUrl);
            const count = firstData?.count ?? 0;

            if (count > MAX_PER_WINDOW) {
                const startMs = new Date(wStart).getTime();
                const endMs = new Date(wEnd).getTime();
                if (endMs - startMs <= 1000) {
                    logger.warn(`Window ${wStart} .. ${wEnd} has ${count} tickets but is already ≤1s wide — processing first 1000 only, some may be missed`);
                } else {
                    const midIso = new Date(startMs + Math.floor((endMs - startMs) / 2)).toISOString();
                    logger.debug(`Window has ${count} tickets (>${MAX_PER_WINDOW}) — bisecting at ${midIso}`);
                    windows.unshift([wStart, midIso], [midIso, wEnd]);
                    continue;
                }
            }

            logger.info(`Processing window ${wStart}..${wEnd}: ${count} tickets`);
            await processResults(firstData.results || []);

            let nextPage: string | null = firstData.next_page || null;
            while (nextPage) {
                logger.debug(`Fetching next page: ${nextPage}`);
                await new Promise(res => setTimeout(res, 1000));
                const data = await fetchWithRetry(nextPage);
                await processResults(data.results || []);
                nextPage = data.next_page || null;
            }
        }

        // Only advance the watermark when all tickets succeeded
        if (failedTickets === 0) {
            await DatabaseManager.updateLastRunDate(dbConnection, `zendesk_tickets_${config.zendesk_subdomain}`, logger, this.embeddingDimension, syncStartDate);
            logger.info(`Successfully processed ${totalTickets} tickets`);
        } else {
            logger.warn(
                `Run completed with ${failedTickets} ticket failure(s). ` +
                `Watermark NOT advanced — failed tickets will be retried next run. ` +
                `Successfully processed: ${totalTickets}.`
            );
        }
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

            await this.processChunksForUrl(chunks, url, dbConnection, logger);
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

    /**
     * Process chunks for a given URL with change detection.
     * 
     * Compares the new chunk hashes against existing hashes stored in the DB.
     * - If all hashes match (same content, same count): skip entirely (no embedding, no DB writes).
     * - If any hash differs: delete all existing chunks for this URL and re-embed/insert all new chunks.
     * 
     * This ensures chunk_index and total_chunks are always consistent, and no orphaned
     * chunks are left behind when content shifts (e.g., a paragraph is added in the middle).
     * 
     * @returns The number of chunks that were embedded (0 if skipped).
     */
    private async processChunksForUrl(
        chunks: DocumentChunk[],
        url: string,
        dbConnection: DatabaseConnection,
        logger: Logger
    ): Promise<number> {
        if (chunks.length === 0) return 0;

        // 1. Compute hashes for all new chunks
        const newHashes = chunks.map(c => Utils.generateHash(c.content));
        const newHashesSorted = newHashes.slice().sort();

        // 2. Fetch existing hashes for this URL from the DB
        let existingHashesSorted: string[];
        if (dbConnection.type === 'sqlite') {
            existingHashesSorted = DatabaseManager.getChunkHashesByUrlSQLite(dbConnection.db, url);
        } else {
            existingHashesSorted = await DatabaseManager.getChunkHashesByUrlQdrant(dbConnection, url);
        }

        // 3. Compare: if identical sorted arrays, content is unchanged — skip
        const unchanged = newHashesSorted.length === existingHashesSorted.length &&
            newHashesSorted.every((h, i) => h === existingHashesSorted[i]);

        if (unchanged) {
            logger.info(`Skipping unchanged URL (${chunks.length} chunks): ${url}`);
            this.counters.items_unchanged++;
            return 0;
        }

        // 4. Content changed — delete all existing chunks for this URL
        if (existingHashesSorted.length > 0) {
            logger.info(`Content changed for ${url} (${existingHashesSorted.length} old chunks → ${chunks.length} new chunks), re-embedding all`);
            if (dbConnection.type === 'sqlite') {
                DatabaseManager.removeChunksByUrlSQLite(dbConnection.db, url, logger);
            } else {
                await DatabaseManager.removeChunksByUrlQdrant(dbConnection, url, logger);
            }
            this.counters.items_updated++;
            this.counters.chunks_deleted += existingHashesSorted.length;
        } else {
            this.counters.items_new++;
        }

        // 5. Embed and insert all new chunks
        let embeddedCount = 0;
        const chunkProgress = logger.progress(`Embedding chunks for ${url}`, chunks.length);

        for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];
            const chunkHash = newHashes[i];
            const chunkId = chunk.metadata.chunk_id.substring(0, 8) + '...';

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
                embeddedCount++;
            } else {
                logger.error(`Embedding failed for chunk: ${chunkId}`);
                chunkProgress.update(1, `Failed to embed chunk ${chunkId}`);
            }
        }

        chunkProgress.complete();
        this.counters.chunks_added += embeddedCount;
        return embeddedCount;
    }

    // Embedding model token limit and character-based estimate.
    // OpenAI's text-embedding-3-large has an 8,191-token context limit.
    // Using ~4 chars per BPE token as a conservative estimate (actual average
    // is ~3.5 for English, lower for code/URLs/config paths).
    private static readonly MAX_EMBEDDING_TOKENS = 8191;
    private static readonly CHARS_PER_TOKEN = 4;
    private static readonly MAX_EMBEDDING_CHARS = Doc2Vec.MAX_EMBEDDING_TOKENS * Doc2Vec.CHARS_PER_TOKEN;

    private async createEmbeddings(texts: string[]): Promise<number[][]> {
        const logger = this.logger.child('embeddings');
        try {
            // Truncate any texts that exceed the embedding model's token limit.
            // This is a safety net for pages with dense content (e.g., large API
            // reference pages with deeply nested config paths) where the chunker's
            // whitespace-based token count drastically underestimates BPE tokens.
            const safeTexts = texts.map(text => {
                if (text.length > Doc2Vec.MAX_EMBEDDING_CHARS) {
                    const estimatedTokens = Math.ceil(text.length / Doc2Vec.CHARS_PER_TOKEN);
                    logger.warn(
                        `Truncating oversized chunk (${text.length} chars, ~${estimatedTokens} tokens) ` +
                        `to ${Doc2Vec.MAX_EMBEDDING_CHARS} chars (~${Doc2Vec.MAX_EMBEDDING_TOKENS} tokens) ` +
                        `to fit embedding model limit`
                    );
                    return text.substring(0, Doc2Vec.MAX_EMBEDDING_CHARS);
                }
                return text;
            });

            logger.debug(`Creating embeddings for ${safeTexts.length} texts`);
            const response = await this.openai.embeddings.create({
                model: this.embeddingModel,
                input: safeTexts,
            }, { timeout: 60000 });
            logger.debug(`Successfully created ${response.data.length} embeddings`);
            return response.data.map(d => d.embedding);
        } catch (error) {
            logger.error('Failed to create embeddings:', error);
            return [];
        }
    }
}

function runOneShot(configPath: string): void {
    if (!fs.existsSync(configPath)) {
        console.error('Please provide a valid path to a YAML config file.');
        process.exit(1);
    }
    const doc2Vec = new Doc2Vec(configPath);
    doc2Vec.run()
        .then((stats) => process.exit(stats.some(s => !s.ok) ? 1 : 0))
        .catch((err) => { console.error(err); process.exit(1); });
}

if (require.main === module) {
    const { Command } = require('commander') as typeof import('commander');
    const program = new Command();

    program
        .name('doc2vec')
        .description('Crawl documentation sources and store embeddings in vector databases')
        // Legacy invocation: `doc2vec [config.yaml]` runs a one-shot sync
        .argument('[config]', 'path to a YAML config file', 'config.yaml')
        .action((configPath: string) => runOneShot(configPath));

    program
        .command('run <config>')
        .description('Run a one-shot sync for a config file (what the controller spawns)')
        .action((configPath: string) => runOneShot(configPath));

    program
        .command('controller [configs...]')
        .description('Run as a long-lived controller: schedule sync jobs per config file, persist runs in Postgres, serve a web UI')
        .option('--database-url <url>', 'Postgres connection string (or DATABASE_URL env var)')
        .option('--port <port>', 'HTTP port for the API/UI (or PORT env var)', (v: string) => parseInt(v, 10))
        .option('--read-write', 'allow creating/editing configs from the UI (default: read-only)', false)
        .option('--config-dir <dir>', 'directory where UI-created configs are written (required with --read-write)')
        .option('--max-parallel <n>', 'maximum number of sync jobs running at once', (v: string) => parseInt(v, 10), 1)
        .option('--reload-interval <seconds>', 'how often to re-poll config files for changes', (v: string) => parseInt(v, 10), 30)
        .option('--log-retention-days <days>', 'delete run logs older than this many days', (v: string) => parseInt(v, 10), 14)
        .option('--slack-webhook-url <url>', 'Slack incoming webhook for run notifications (or SLACK_WEBHOOK_URL env var)')
        .option('--slack-notify <mode>', "which runs to notify about: 'all' or 'failures'", 'all')
        .option('--public-url <url>', 'externally reachable base URL, used for links in notifications (or PUBLIC_URL env var)')
        .action(async (configs: string[], options: any) => {
            // Lazy-require so the one-shot path doesn't load express/pg
            const { startController } = require('./controller') as typeof import('./controller');
            try {
                await startController({
                    configArgs: configs,
                    databaseUrl: options.databaseUrl || process.env.DATABASE_URL,
                    port: options.port || (process.env.PORT ? parseInt(process.env.PORT, 10) : 8080),
                    readWrite: options.readWrite,
                    configDir: options.configDir,
                    maxParallel: options.maxParallel,
                    reloadIntervalSec: options.reloadInterval,
                    logRetentionDays: options.logRetentionDays,
                    slackWebhookUrl: options.slackWebhookUrl || process.env.SLACK_WEBHOOK_URL,
                    slackNotify: options.slackNotify === 'failures' ? 'failures' : 'all',
                    publicUrl: options.publicUrl || process.env.PUBLIC_URL,
                });
            } catch (err) {
                console.error(err instanceof Error ? err.message : err);
                process.exit(1);
            }
        });

    program.parse();
} 
