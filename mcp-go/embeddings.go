package main

import (
	"context"
	"fmt"
	"log"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"google.golang.org/genai"
)

// EmbeddingService handles creating embeddings using various providers
type EmbeddingService struct {
	openaiClient *openai.Client
	geminiClient *genai.Client
	config       *Config
}

// NewEmbeddingService creates a new embedding service with the given configuration
func NewEmbeddingService(config *Config) (*EmbeddingService, error) {
	service := &EmbeddingService{config: config}

	switch config.EmbeddingProvider {
	case ProviderOpenAI:
		client, err := createOpenAIClient(config)
		if err != nil {
			return nil, err
		}
		service.openaiClient = client
	case ProviderAzure:
		client, err := createAzureOpenAIClient(config)
		if err != nil {
			return nil, err
		}
		service.openaiClient = client
	case ProviderGemini:
		client, err := createGeminiClient(config)
		if err != nil {
			return nil, err
		}
		service.geminiClient = client
	default:
		return nil, fmt.Errorf("unsupported embedding provider: %s. Supported providers: openai, azure, gemini", config.EmbeddingProvider)
	}

	return service, nil
}

// createOpenAIClient creates an OpenAI client
func createOpenAIClient(config *Config) (*openai.Client, error) {
	if config.OpenAIAPIKey == "" {
		return nil, fmt.Errorf("OpenAI API key is required")
	}

	log.Printf("[EMBEDDING] Creating OpenAI client with model: %s", config.OpenAIModel)

	client := openai.NewClient(
		option.WithAPIKey(config.OpenAIAPIKey),
	)

	return &client, nil
}

// createAzureOpenAIClient creates an Azure OpenAI client
func createAzureOpenAIClient(config *Config) (*openai.Client, error) {
	if config.AzureAPIKey == "" || config.AzureEndpoint == "" {
		return nil, fmt.Errorf("Azure OpenAI API key and endpoint are required")
	}

	log.Printf("[EMBEDDING] Creating Azure OpenAI client with endpoint: %s, deployment: %s", config.AzureEndpoint, config.AzureDeployment)

	// For Azure OpenAI, construct the base URL
	baseURL := fmt.Sprintf("%s/openai/deployments/%s?api-version=%s", config.AzureEndpoint, config.AzureDeployment, config.AzureAPIVersion)

	client := openai.NewClient(
		option.WithAPIKey(config.AzureAPIKey),
		option.WithBaseURL(baseURL),
	)

	return &client, nil
}

// createGeminiClient creates a Gemini client
func createGeminiClient(config *Config) (*genai.Client, error) {
	if config.GeminiAPIKey == "" {
		return nil, fmt.Errorf("Gemini API key is required")
	}

	log.Printf("[EMBEDDING] Creating Gemini client with model: %s", config.GeminiModel)

	ctx := context.Background()
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey: config.GeminiAPIKey,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Gemini client: %w", err)
	}

	return client, nil
}

// CreateEmbeddings creates embeddings for the given text using the configured provider
func (e *EmbeddingService) CreateEmbeddings(ctx context.Context, text string) ([]float64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	switch e.config.EmbeddingProvider {
	case ProviderOpenAI, ProviderAzure:
		return e.createOpenAIEmbeddings(ctx, text)
	case ProviderGemini:
		return e.createGeminiEmbeddings(ctx, text)
	default:
		return nil, fmt.Errorf("unsupported embedding provider: %s", e.config.EmbeddingProvider)
	}
}

// createOpenAIEmbeddings creates embeddings using OpenAI or Azure OpenAI
func (e *EmbeddingService) createOpenAIEmbeddings(ctx context.Context, text string) ([]float64, error) {
	// Determine the model to use
	model := e.config.OpenAIModel
	if e.config.EmbeddingProvider == ProviderAzure {
		// For Azure, use the deployment name as the model
		model = e.config.AzureDeployment
	}

	log.Printf("[EMBEDDING] Creating embedding for text (length: %d) using model: %s", len(text), model)

	// Create embedding request
	resp, err := e.openaiClient.Embeddings.New(ctx, openai.EmbeddingNewParams{
		Input: openai.EmbeddingNewParamsInputUnion{
			OfString: openai.Opt(text),
		},
		Model: openai.EmbeddingModel(model),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create embeddings with %s: %w", e.config.EmbeddingProvider, err)
	}

	if len(resp.Data) == 0 {
		return nil, fmt.Errorf("no embeddings returned from %s", e.config.EmbeddingProvider)
	}

	// Convert []float32 to []float64
	embedding := resp.Data[0].Embedding
	result := make([]float64, len(embedding))
	for i, v := range embedding {
		result[i] = float64(v)
	}

	log.Printf("[EMBEDDING] Successfully created embedding with %d dimensions", len(result))
	return result, nil
}

// createGeminiEmbeddings creates embeddings using Google Gemini
func (e *EmbeddingService) createGeminiEmbeddings(ctx context.Context, text string) ([]float64, error) {
	log.Printf("[EMBEDDING] Creating embedding for text (length: %d) using Gemini model: %s", len(text), e.config.GeminiModel)

	// Get the embedding model
	resp, err := e.geminiClient.Models.EmbedContent(ctx, e.config.GeminiModel, []*genai.Content{
		{
			Parts: []*genai.Part{
				{
					Text: text,
				},
			},
		},
	}, &genai.EmbedContentConfig{})
	if err != nil {
		return nil, fmt.Errorf("failed to create embeddings with Gemini: %w", err)
	}

	if resp.Embeddings == nil || len(resp.Embeddings[0].Values) == 0 {
		return nil, fmt.Errorf("no embeddings returned from Gemini")
	}

	// Gemini returns []float32, convert to []float64
	result := make([]float64, len(resp.Embeddings[0].Values))
	for i, v := range resp.Embeddings[0].Values {
		result[i] = float64(v)
	}

	log.Printf("[EMBEDDING] Successfully created embedding with %d dimensions", len(result))
	return result, nil
}
