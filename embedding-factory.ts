import { Logger } from './logger';
import {
    EmbeddingProvider,
    EmbeddingConfig,
    OpenAIEmbeddingProvider,
    CustomEmbeddingProvider
} from './embedding-provider';

/**
 * Factory class for creating embedding providers
 */
export class EmbeddingProviderFactory {
    /**
     * Creates an embedding provider based on environment variables
     * @param logger Logger instance
     * @returns Configured embedding provider
     */
    static createProvider(logger: Logger): EmbeddingProvider {
        const factoryLogger = logger.child('embedding-factory');

        // Get provider from PROVIDER environment variable, default to openai
        const provider = (process.env.PROVIDER || 'openai').toLowerCase();

        factoryLogger.info(`Creating embedding provider: ${provider}`);

        switch (provider) {
            case 'openai':
                return new OpenAIEmbeddingProvider(logger);

            case 'custom':
                const endpoint = process.env.CUSTOM_ENDPOINT;
                if (!endpoint) {
                    throw new Error('CUSTOM_ENDPOINT environment variable is required when using custom provider');
                }

                // Validate endpoint URL format
                try {
                    new URL(endpoint);
                } catch (error) {
                    throw new Error(`Invalid custom embedding endpoint URL: ${endpoint}`);
                }

                return new CustomEmbeddingProvider(endpoint, logger);

            default:
                throw new Error(`Unknown embedding provider: ${provider}. Must be 'openai' or 'custom'`);
        }
    }
}
