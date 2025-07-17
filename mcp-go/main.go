package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	_ "github.com/asg017/sqlite-vec-go-bindings/ncruces"
)

func main() {
	// Parse command line flags
	var (
		configHelp = flag.Bool("help", false, "Show help information")
		showConfig = flag.Bool("config", false, "Show current configuration and exit")
	)
	flag.Parse()

	// Show help if requested
	if *configHelp {
		showHelp()
		return
	}

	// Load configuration
	config, err := LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Show configuration if requested
	if *showConfig {
		fmt.Println(config.String())
		return
	}

	// Print startup information
	log.Printf("Starting MCP server: sqlite-vec-doc-query v1.0.0")
	log.Printf("Embedding provider: %s", config.EmbeddingProvider)
	log.Printf("Transport type: %s", config.TransportType)
	log.Printf("Database directory: %s", config.SQLiteDBDir)
	log.Printf("Strict mode: %t", config.StrictMode)

	// Create MCP server
	mcpServer, err := NewMCPServer(config)
	if err != nil {
		log.Fatalf("Failed to create MCP server: %v", err)
	}

	// Create transport manager
	transportManager := NewTransportManager(mcpServer, config)

	// Start server with signal handling
	if err := transportManager.StartWithSignalHandling(); err != nil {
		log.Fatalf("Failed to start MCP server: %v", err)
	}
}

// showHelp displays help information
func showHelp() {
	log.Println("MCP Documentation Query Server")
	log.Println()
	log.Println("A Go implementation of a Model Context Protocol server that provides")
	log.Println("documentation querying capabilities using vector search with SQLite.")
	log.Println()
	log.Println("USAGE:")
	log.Println("  mcp-doc-query [flags]")
	log.Println()
	log.Println("FLAGS:")
	log.Println("  -help     Show this help information")
	log.Println("  -config   Show current configuration and exit")
	log.Println()
	log.Println("ENVIRONMENT VARIABLES:")
	log.Println()
	log.Println("Provider Configuration:")
	log.Println("  EMBEDDING_PROVIDER         Provider to use (openai, azure, gemini) [default: openai]")
	log.Println()
	log.Println("OpenAI Configuration:")
	log.Println("  OPENAI_API_KEY            OpenAI API key")
	log.Println("  OPENAI_MODEL              OpenAI model name [default: text-embedding-3-large]")
	log.Println()
	log.Println("Azure OpenAI Configuration:")
	log.Println("  AZURE_OPENAI_KEY          Azure OpenAI API key")
	log.Println("  AZURE_OPENAI_ENDPOINT     Azure OpenAI endpoint URL")
	log.Println("  AZURE_OPENAI_API_VERSION  Azure OpenAI API version [default: 2024-10-21]")
	log.Println("  AZURE_OPENAI_DEPLOYMENT_NAME  Azure deployment name [default: text-embedding-3-large]")
	log.Println()
	log.Println("Google Gemini Configuration:")
	log.Println("  GEMINI_API_KEY            Google Gemini API key")
	log.Println("  GEMINI_MODEL              Gemini model name [default: gemini-embedding-001]")
	log.Println()
	log.Println("Database Configuration:")
	log.Println("  SQLITE_DB_DIR             Directory containing SQLite databases [default: current directory]")
	log.Println()
	log.Println("Server Configuration:")
	log.Println("  STRICT_MODE               Enable strict mode validation [default: false]")
	log.Println("  TRANSPORT_TYPE            Transport type (stdio, sse, http) [default: http]")
	log.Println("  PORT                      HTTP server port [default: 3001]")
	log.Println()
	log.Println("EXAMPLES:")
	log.Println("  # Start with OpenAI provider")
	log.Println("  OPENAI_API_KEY=your-key mcp-doc-query")
	log.Println()
	log.Println("  # Start with Azure OpenAI provider")
	log.Println("  EMBEDDING_PROVIDER=azure \\")
	log.Println("  AZURE_OPENAI_KEY=your-key \\")
	log.Println("  AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/ \\")
	log.Println("  mcp-doc-query")
	log.Println()
	log.Println("  # Start with stdio transport")
	log.Println("  TRANSPORT_TYPE=stdio OPENAI_API_KEY=your-key mcp-doc-query")
	log.Println()
	log.Println("  # Show current configuration")
	log.Println("  mcp-doc-query -config")
}

// showConfiguration displays the current configuration
func showConfiguration(config *Config) {
	log.Println("Current Configuration:")
	log.Println("=====================")
	log.Printf("Embedding Provider: %s", config.EmbeddingProvider)
	log.Printf("Transport Type: %s", config.TransportType)
	log.Printf("Database Directory: %s", config.SQLiteDBDir)
	log.Printf("Strict Mode: %t", config.StrictMode)
	log.Printf("Port: %d", config.Port)
	log.Println()

	log.Println("Provider-specific Configuration:")
	switch config.EmbeddingProvider {
	case ProviderOpenAI:
		log.Printf("  OpenAI API Key: %s", maskAPIKey(config.OpenAIAPIKey))
		log.Printf("  OpenAI Model: %s", config.OpenAIModel)
	case ProviderAzure:
		log.Printf("  Azure API Key: %s", maskAPIKey(config.AzureAPIKey))
		log.Printf("  Azure Endpoint: %s", config.AzureEndpoint)
		log.Printf("  Azure API Version: %s", config.AzureAPIVersion)
		log.Printf("  Azure Deployment: %s", config.AzureDeployment)
	case ProviderGemini:
		log.Printf("  Gemini API Key: %s", maskAPIKey(config.GeminiAPIKey))
		log.Printf("  Gemini Model: %s", config.GeminiModel)
	}
	log.Println()

	// Check database directory
	if info, err := os.Stat(config.SQLiteDBDir); err != nil {
		log.Printf("Database Directory Status: ERROR - %v", err)
	} else if !info.IsDir() {
		log.Printf("Database Directory Status: ERROR - not a directory")
	} else {
		log.Printf("Database Directory Status: OK")
	}

	// Note: Provider configuration validation is now done in LoadConfig()
	log.Printf("Provider Configuration: OK (validated during config load)")
}

// maskAPIKey masks an API key for display purposes
func maskAPIKey(key string) string {
	if key == "" {
		return "<not set>"
	}
	if len(key) <= 8 {
		return "***"
	}
	return key[:4] + "..." + key[len(key)-4:]
}
