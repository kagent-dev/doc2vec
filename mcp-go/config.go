package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

// EmbeddingProvider represents the supported embedding providers
type EmbeddingProvider string

const (
	ProviderOpenAI EmbeddingProvider = "openai"
	ProviderAzure  EmbeddingProvider = "azure"
	ProviderGemini EmbeddingProvider = "gemini"
)

// Config holds all configuration for the MCP server
type Config struct {
	// Provider configuration
	EmbeddingProvider EmbeddingProvider `envconfig:"EMBEDDING_PROVIDER" default:"openai"`

	// OpenAI configuration
	OpenAIAPIKey string `envconfig:"OPENAI_API_KEY"`
	OpenAIModel  string `envconfig:"OPENAI_MODEL" default:"text-embedding-3-large"`

	// Azure OpenAI configuration
	AzureAPIKey     string `envconfig:"AZURE_OPENAI_KEY"`
	AzureEndpoint   string `envconfig:"AZURE_OPENAI_ENDPOINT"`
	AzureAPIVersion string `envconfig:"AZURE_OPENAI_API_VERSION" default:"2024-10-21"`
	AzureDeployment string `envconfig:"AZURE_OPENAI_DEPLOYMENT_NAME" default:"text-embedding-3-large"`

	// Google Gemini configuration
	GeminiAPIKey string `envconfig:"GEMINI_API_KEY"`
	GeminiModel  string `envconfig:"GEMINI_MODEL" default:"gemini-embedding-001"`

	// Database configuration
	SQLiteDBDir string `envconfig:"SQLITE_DB_DIR" default:"."`

	// Transport configuration
	TransportType string `envconfig:"TRANSPORT_TYPE" default:"http"`
	Port          int    `envconfig:"PORT" default:"3001"`

	// Server configuration
	StrictMode bool `envconfig:"STRICT_MODE" default:"false"`
}

// LoadConfig loads configuration from environment variables using envconfig
func LoadConfig() (*Config, error) {
	// Try to load .env file if it exists (ignore errors if it doesn't exist)
	_ = godotenv.Load()

	var config Config

	// Process environment variables into the config struct
	if err := envconfig.Process("", &config); err != nil {
		return nil, fmt.Errorf("failed to process environment variables: %w", err)
	}

	// Validate configuration based on strict mode and provider
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return &config, nil
}

// Validate performs configuration validation
func (c *Config) Validate() error {
	// Validate embedding provider
	switch c.EmbeddingProvider {
	case ProviderOpenAI, ProviderAzure, ProviderGemini:
		// Valid providers
	default:
		return fmt.Errorf("unsupported embedding provider '%s'. Supported providers: openai, azure, gemini", c.EmbeddingProvider)
	}

	// Validate transport type
	switch c.TransportType {
	case "stdio", "sse", "http":
		// Valid transport types
	default:
		return fmt.Errorf("unsupported transport type '%s'. Supported types: stdio, sse, http", c.TransportType)
	}

	// Validate port range
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("invalid port %d. Port must be between 1 and 65535", c.Port)
	}

	// Validate database directory
	if !filepath.IsAbs(c.SQLiteDBDir) {
		// Convert relative path to absolute
		absPath, err := filepath.Abs(c.SQLiteDBDir)
		if err != nil {
			return fmt.Errorf("failed to resolve database directory path: %w", err)
		}
		c.SQLiteDBDir = absPath
	}

	// Check if database directory exists
	if _, err := os.Stat(c.SQLiteDBDir); os.IsNotExist(err) {
		return fmt.Errorf("database directory does not exist: %s", c.SQLiteDBDir)
	}

	// Strict mode validation - check required API keys
	if c.StrictMode {
		switch c.EmbeddingProvider {
		case ProviderOpenAI:
			if c.OpenAIAPIKey == "" {
				return fmt.Errorf("OPENAI_API_KEY is required when using OpenAI provider in strict mode")
			}
		case ProviderAzure:
			if c.AzureAPIKey == "" {
				return fmt.Errorf("AZURE_OPENAI_KEY is required when using Azure provider in strict mode")
			}
			if c.AzureEndpoint == "" {
				return fmt.Errorf("AZURE_OPENAI_ENDPOINT is required when using Azure provider in strict mode")
			}
		case ProviderGemini:
			if c.GeminiAPIKey == "" {
				return fmt.Errorf("GEMINI_API_KEY is required when using Gemini provider in strict mode")
			}
		}
	}

	return nil
}

// GetDBPath returns the full path to a database file for a given product
func (c *Config) GetDBPath(productName string) string {
	return filepath.Join(c.SQLiteDBDir, productName+".db")
}

// GetListenAddress returns the address to listen on for HTTP/SSE transports
func (c *Config) GetListenAddress() string {
	return ":" + strconv.Itoa(c.Port)
}

// String returns a string representation of the configuration (without sensitive data)
func (c *Config) String() string {
	return fmt.Sprintf(`Configuration:
  Embedding Provider: %s
  Transport Type: %s
  Port: %d
  Database Directory: %s
  Strict Mode: %t
  OpenAI Model: %s
  Azure API Version: %s
  Azure Deployment: %s
  Gemini Model: %s`,
		c.EmbeddingProvider,
		c.TransportType,
		c.Port,
		c.SQLiteDBDir,
		c.StrictMode,
		c.OpenAIModel,
		c.AzureAPIVersion,
		c.AzureDeployment,
		c.GeminiModel,
	)
}
