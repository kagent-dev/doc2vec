package main

import (
	"context"
	"fmt"
	"log"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// MCPServer wraps the MCP server functionality
type MCPServer struct {
	server           *server.MCPServer
	embeddingService *EmbeddingService
	databaseService  *DatabaseService
	config           *Config
}

// NewMCPServer creates a new MCP server instance
func NewMCPServer(config *Config) (*MCPServer, error) {
	// Create services
	embeddingService, err := NewEmbeddingService(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create embedding service: %w", err)
	}
	databaseService := NewDatabaseService(config)

	// Create MCP server
	mcpServer := server.NewMCPServer(
		"sqlite-vec-doc-query",
		"1.0.0",
		server.WithRecovery(),
	)

	// Create our server wrapper
	s := &MCPServer{
		server:           mcpServer,
		embeddingService: embeddingService,
		databaseService:  databaseService,
		config:           config,
	}

	// Register tools
	if err := s.registerTools(); err != nil {
		return nil, fmt.Errorf("failed to register tools: %w", err)
	}

	return s, nil
}

// registerTools registers all available tools with the MCP server
func (s *MCPServer) registerTools() error {
	// Create the query_documentation tool
	queryDocTool := mcp.NewTool(
		"query_documentation",
		mcp.WithDescription("Query documentation stored in a sqlite-vec database using vector search."),
		mcp.WithString(
			"queryText",
			mcp.Required(),
			mcp.Description("The natural language query to search for."),
			mcp.MinLength(1),
		),
		mcp.WithString(
			"productName",
			mcp.Required(),
			mcp.Description("The name of the product documentation database to search within (e.g., 'my-product'). Corresponds to the DB filename without .db."),
			mcp.MinLength(1),
		),
		mcp.WithString(
			"version",
			mcp.Description("The specific version of the product documentation (e.g., '1.2.0'). Optional."),
		),
		mcp.WithNumber(
			"limit",
			mcp.Description("Maximum number of results to return. Defaults to 4."),
			mcp.DefaultNumber(4),
			mcp.Min(1),
		),
	)

	// Register the tool with its handler
	s.server.AddTool(queryDocTool, s.handleQueryDocumentation)

	return nil
}

// handleQueryDocumentation handles the query_documentation tool call
func (s *MCPServer) handleQueryDocumentation(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Extract parameters from the request
	queryText, err := request.RequireString("queryText")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Invalid queryText parameter: %v", err)), nil
	}

	productName, err := request.RequireString("productName")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Invalid productName parameter: %v", err)), nil
	}

	// Optional parameters
	version := request.GetString("version", "")
	limit := int(request.GetFloat("limit", 4))

	// Validate limit
	if limit < 1 {
		limit = 4
	}

	log.Printf("Received query: text=\"%s\", product=\"%s\", version=\"%s\", limit=%d",
		queryText, productName, version, limit)

	// Create documentation query
	query := DocumentationQuery{
		QueryText:   queryText,
		ProductName: productName,
		Version:     version,
		Limit:       limit,
	}

	// Execute the query
	results, err := s.databaseService.QueryDocumentation(s.embeddingService, query)
	if err != nil {
		log.Printf("Error processing 'query_documentation' tool: %v", err)
		return mcp.NewToolResultError(fmt.Sprintf("Error querying documentation: %v", err)), nil
	}

	// Handle no results case
	if len(results) == 0 {
		message := fmt.Sprintf("No relevant documentation found for \"%s\" in product \"%s\"", queryText, productName)
		if version != "" {
			message += fmt.Sprintf(" (version %s)", version)
		}
		message += "."
		return mcp.NewToolResultText(message), nil
	}

	// Format results for response
	responseText := s.formatQueryResults(queryText, productName, version, results)

	log.Printf("Handler finished processing. Payload size (approx): %d chars. Returning response object...", len(responseText))

	return mcp.NewToolResultText(responseText), nil
}

// formatQueryResults formats the query results into a human-readable response
func (s *MCPServer) formatQueryResults(queryText, productName, version string, results []DocumentationResult) string {
	var response string

	// Build header
	versionInfo := ""
	if version != "" {
		versionInfo = fmt.Sprintf(" (version %s)", version)
	}

	response = fmt.Sprintf("Found %d relevant documentation snippets for \"%s\" in product \"%s\"%s:\n\n",
		len(results), queryText, productName, versionInfo)

	// Format each result
	for i, result := range results {
		response += fmt.Sprintf("Result %d:\n", i+1)
		response += fmt.Sprintf("  Content: %s\n", result.Content)
		response += fmt.Sprintf("  Distance: %.4f\n", result.Distance)

		if result.URL != "" {
			response += fmt.Sprintf("  URL: %s\n", result.URL)
		}

		response += "---\n"

		// Add separator between results except for the last one
		if i < len(results)-1 {
			response += "\n"
		}
	}

	return response
}

// GetServer returns the underlying MCP server instance
func (s *MCPServer) GetServer() *server.MCPServer {
	return s.server
}
