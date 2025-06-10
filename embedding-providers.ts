import { OpenAI } from "openai";
import { AutoTokenizer, AutoModel, env } from "@huggingface/transformers";
import { Logger } from './logger';
import { EmbeddingConfig, OpenAIEmbeddingParams, TransformersEmbeddingParams } from './types';

export interface EmbeddingProvider {
    createEmbeddings(texts: string[]): Promise<number[][]>;
    getDimensions(): number;
    getModelName(): string;
    cleanup?(): Promise<void>;
}

export class OpenAIEmbeddingProvider implements EmbeddingProvider {
    private openai: OpenAI;
    private model: string;
    private logger: Logger;

    constructor(params: OpenAIEmbeddingParams, logger: Logger) {
        this.logger = logger.child('openai-embeddings');
        this.model = params.model || 'text-embedding-3-large';
        this.openai = new OpenAI({ 
            apiKey: params.api_key || process.env.OPENAI_API_KEY 
        });
        
        this.logger.info(`OpenAI embedding provider initialized with model: ${this.model}`);
    }

    async createEmbeddings(texts: string[]): Promise<number[][]> {
        try {
            this.logger.debug(`Creating embeddings for ${texts.length} texts using OpenAI`);
            const response = await this.openai.embeddings.create({
                model: this.model,
                input: texts,
            });
            this.logger.debug(`Successfully created ${response.data.length} embeddings`);
            return response.data.map(d => d.embedding);
        } catch (error) {
            this.logger.error('Failed to create OpenAI embeddings:', error);
            return [];
        }
    }

    getDimensions(): number {
        // Return dimensions based on model
        if (this.model === 'text-embedding-3-large') return 3072;
        if (this.model === 'text-embedding-3-small') return 1536;
        if (this.model === 'text-embedding-ada-002') return 1536;
        return 1536; // Default fallback
    }

    getModelName(): string {
        return this.model;
    }
}

export class TransformersEmbeddingProvider implements EmbeddingProvider {
    private model: any;
    private tokenizer: any;
    private modelName: string;
    private dimensions: number = 384; // Default, will be updated after model loading
    private logger: Logger;
    private isInitialized: boolean = false;

    constructor(private params: TransformersEmbeddingParams, logger: Logger) {
        this.logger = logger.child('transformers-embeddings');
        this.modelName = params.model;
        
        // Configure transformers.js environment
        if (params.local_files_only) {
            env.allowLocalModels = true;
            env.allowRemoteModels = false;
        }
        
        if (params.cache_dir) {
            env.cacheDir = params.cache_dir;
        }
        
        this.logger.info(`Transformers embedding provider initialized with model: ${this.modelName}`);
    }

    private async initialize(): Promise<void> {
        if (this.isInitialized) return;
        
        try {
            this.logger.info(`Loading transformers model: ${this.modelName}`);
            
            // Load tokenizer and model
            this.tokenizer = await AutoTokenizer.from_pretrained(this.modelName);
            this.model = await AutoModel.from_pretrained(this.modelName);
            
            // Try to determine dimensions from model config
            if (this.model.config && this.model.config.hidden_size) {
                this.dimensions = this.model.config.hidden_size;
            }
            
            this.isInitialized = true;
            this.logger.info(`Transformers model loaded successfully. Dimensions: ${this.dimensions}`);
        } catch (error: any) {
            this.logger.error(`Failed to load transformers model: ${this.modelName}`, error);
            
            // Provide helpful error messages for common issues
            if (error.message?.includes('Could not locate file') && error.message?.includes('onnx')) {
                this.logger.error(`Model ${this.modelName} does not appear to have ONNX format files required by transformers.js`);
                this.logger.error('Please use a model with "Xenova/" prefix or check the model repository for ONNX files');
                this.logger.error('Popular working models: Xenova/all-MiniLM-L6-v2, Xenova/all-mpnet-base-v2, Xenova/bge-small-en-v1.5');
            }
            
            throw error;
        }
    }

    async createEmbeddings(texts: string[]): Promise<number[][]> {
        await this.initialize();
        
        try {
            this.logger.debug(`Creating embeddings for ${texts.length} texts using transformers.js`);
            
            const embeddings: number[][] = [];
            
            // Process texts in batches to avoid memory issues
            const batchSize = Math.min(8, texts.length);
            for (let i = 0; i < texts.length; i += batchSize) {
                const batch = texts.slice(i, i + batchSize);
                
                for (const text of batch) {
                    // Tokenize the text
                    const tokens = await this.tokenizer(text, {
                        truncation: true,
                        padding: true,
                        max_length: 512,
                        return_tensors: 'pt'
                    });
                    
                    // Get model output
                    const output = await this.model(tokens);
                    
                    // Mean pooling to get sentence embeddings
                    let embedding: number[];
                    if (output.last_hidden_state) {
                        // Mean pooling over sequence length
                        const tensor = output.last_hidden_state;
                        const meanPooled = tensor.mean(1); // Mean along sequence dimension
                        embedding = Array.from(meanPooled.data) as number[];
                    } else if (output.pooler_output) {
                        // Use pooler output if available
                        embedding = Array.from(output.pooler_output.data) as number[];
                    } else {
                        throw new Error('Unable to extract embeddings from model output');
                    }
                    
                    embeddings.push(embedding);
                }
                
                // Log progress for large batches
                if (texts.length > 10) {
                    this.logger.debug(`Processed ${Math.min(i + batchSize, texts.length)}/${texts.length} texts`);
                }
            }
            
            this.logger.debug(`Successfully created ${embeddings.length} embeddings`);
            return embeddings;
        } catch (error) {
            this.logger.error('Failed to create transformers embeddings:', error);
            return [];
        }
    }

    getDimensions(): number {
        return this.dimensions;
    }

    getModelName(): string {
        return this.modelName;
    }

    async cleanup(): Promise<void> {
        // Clean up model resources if needed
        this.model = null;
        this.tokenizer = null;
        this.isInitialized = false;
        this.logger.info('Transformers embedding provider cleaned up');
    }
}

export function createEmbeddingProvider(config: EmbeddingConfig, logger: Logger): EmbeddingProvider {
    if (config.provider === 'openai') {
        return new OpenAIEmbeddingProvider(config.params as OpenAIEmbeddingParams, logger);
    } else if (config.provider === 'transformers') {
        return new TransformersEmbeddingProvider(config.params as TransformersEmbeddingParams, logger);
    } else {
        throw new Error(`Unsupported embedding provider: ${config.provider}`);
    }
} 