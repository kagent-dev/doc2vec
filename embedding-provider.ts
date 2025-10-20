import axios from 'axios';
import { OpenAI } from 'openai';
import { Logger } from './logger';

/**
 * Abstract interface for embedding providers
 */
export interface EmbeddingProvider {
    createEmbeddings(texts: string[]): Promise<number[][]>;
    getProviderName(): string;
}

/**
 * Configuration for embedding providers
 */
export interface EmbeddingConfig {
    provider: 'openai' | 'custom';
    endpoint?: string;  // For custom provider
    model?: string;     // Model to use
    timeout?: number;   // Timeout for custom provider
}

/**
 * OpenAI embedding provider implementation
 */
export class OpenAIEmbeddingProvider implements EmbeddingProvider {
    private openai: OpenAI;
    private logger: Logger;
    private model: string;

    constructor(logger: Logger) {
        this.logger = logger.child('openai-embeddings');

        const apiKey = process.env.OPENAI_API_KEY;
        if (!apiKey) {
            throw new Error('OpenAI API key not found in environment variable: OPENAI_API_KEY');
        }

        // Use EMBEDDING_MODEL env var or default to text-embedding-3-large
        this.model = process.env.EMBEDDING_MODEL || 'text-embedding-3-large';

        this.openai = new OpenAI({ apiKey });
        this.logger.info(`Initialized OpenAI embedding provider with model: ${this.model}`);
    }

    async createEmbeddings(texts: string[]): Promise<number[][]> {
        const maxRetries = 3;
        const baseDelay = 1000; // 1 second

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                this.logger.debug(`Creating embeddings for ${texts.length} texts (attempt ${attempt}/${maxRetries})`);

                const response = await this.openai.embeddings.create({
                    model: this.model,
                    input: texts,
                });

                this.logger.debug(`Successfully created ${response.data.length} embeddings`);
                return response.data.map(d => d.embedding);

            } catch (error: any) {
                this.logger.warn(`OpenAI embedding attempt ${attempt} failed:`, error.message);

                if (attempt === maxRetries) {
                    this.logger.error('All OpenAI embedding attempts failed');
                    throw error;
                }

                // Exponential backoff
                const delay = baseDelay * Math.pow(2, attempt - 1);
                this.logger.debug(`Retrying in ${delay}ms...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }

        return [];
    }

    getProviderName(): string {
        return 'openai';
    }
}

/**
 * Custom endpoint embedding provider implementation (OpenAI-compatible)
 */
export class CustomEmbeddingProvider implements EmbeddingProvider {
    private endpoint: string;
    private model: string;
    private apiKey?: string;
    private timeout: number;
    private logger: Logger;

    constructor(endpoint: string, logger: Logger) {
        this.logger = logger.child('custom-embeddings');

        this.endpoint = endpoint;
        this.timeout = 30000; // 30 seconds default

        // Use OPENAI_API_KEY for authentication (same as OpenAI provider)
        this.apiKey = process.env.OPENAI_API_KEY;
        if (!this.apiKey) {
            throw new Error('OpenAI API key not found in environment variable: OPENAI_API_KEY');
        }

        // Use EMBEDDING_MODEL env var or default to text-embedding-ada-002 for custom
        this.model = process.env.EMBEDDING_MODEL || 'text-embedding-3-large';

        this.logger.info(`Initialized custom embedding provider: ${this.endpoint} with model: ${this.model}`);
    }

    async createEmbeddings(texts: string[]): Promise<number[][]> {
        const maxRetries = 3;
        const baseDelay = 1000; // 1 second

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                this.logger.debug(`Creating embeddings for ${texts.length} texts (attempt ${attempt}/${maxRetries})`);

                const headers: Record<string, string> = {
                    'Content-Type': 'application/json',
                };

                if (this.apiKey) {
                    headers['Authorization'] = `Bearer ${this.apiKey}`;
                }

                const requestBody = {
                    model: this.model,
                    input: texts,
                };

                const response = await axios.post(this.endpoint, requestBody, {
                    headers,
                    timeout: this.timeout,
                });

                if (!response.data || !response.data.data) {
                    throw new Error('Invalid response format from custom embedding endpoint');
                }

                const embeddings = response.data.data.map((item: any) => {
                    if (!item.embedding || !Array.isArray(item.embedding)) {
                        throw new Error('Invalid embedding format in response');
                    }
                    return item.embedding;
                });

                this.logger.debug(`Successfully created ${embeddings.length} embeddings`);
                return embeddings;

            } catch (error: any) {
                this.logger.warn(`Custom embedding attempt ${attempt} failed:`, error.message);

                if (attempt === maxRetries) {
                    this.logger.error('All custom embedding attempts failed');
                    throw error;
                }

                // Exponential backoff
                const delay = baseDelay * Math.pow(2, attempt - 1);
                this.logger.debug(`Retrying in ${delay}ms...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }

        return [];
    }

    getProviderName(): string {
        return 'custom';
    }
}
