import * as path from 'path';
import crypto from 'crypto';
import BetterSqlite3, { Database } from "better-sqlite3";
import * as sqliteVec from "sqlite-vec";
import { QdrantClient } from '@qdrant/js-client-rest';
import { Logger } from './logger';
import { Utils } from './utils';
import { 
    SourceConfig, 
    DatabaseConnection, 
    SqliteDB, 
    QdrantDB, 
    DocumentChunk,
    SqliteDatabaseParams,
    QdrantDatabaseParams,
    LocalDirectorySourceConfig
} from './types';

export class DatabaseManager {
    static async initDatabase(config: SourceConfig, parentLogger: Logger): Promise<DatabaseConnection> {
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

    static async createCollectionQdrant(qdrantClient: QdrantClient, collectionName: string, logger: Logger) {
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

    static async initDatabaseMetadata(dbConnection: DatabaseConnection, logger: Logger): Promise<void> {
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

    static async getLastRunDate(dbConnection: DatabaseConnection, repo: string, defaultDate: string, logger: Logger): Promise<string> {
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
                const metadataUUID = Utils.generateMetadataUUID(repo);
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

    static async updateLastRunDate(dbConnection: DatabaseConnection, repo: string, logger: Logger): Promise<void> {
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
                const metadataUUID = Utils.generateMetadataUUID(repo);
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

    static prepareSQLiteStatements(db: Database) {
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

    static insertVectorsSQLite(db: Database, chunk: DocumentChunk, embedding: number[], logger: Logger, chunkHash?: string) {
        const { insertStmt, updateStmt } = this.prepareSQLiteStatements(db);
        const hash = chunkHash || Utils.generateHash(chunk.content);
        
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

    static async storeChunkInQdrant(db: QdrantDB, chunk: DocumentChunk, embedding: number[], chunkHash?: string) {
        const { client, collectionName } = db;
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
            
            const hash = chunkHash || Utils.generateHash(chunk.content);
            
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
            console.error("Error storing chunk in Qdrant:", error);
        }
    }

    static removeObsoleteChunksSQLite(db: Database, visitedUrls: Set<string>, urlPrefix: string, logger: Logger) {
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

    static async removeObsoleteChunksQdrant(db: QdrantDB, visitedUrls: Set<string>, urlPrefix: string, logger: Logger) {
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

    static removeObsoleteFilesSQLite(
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

    static async removeObsoleteFilesQdrant(
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
} 