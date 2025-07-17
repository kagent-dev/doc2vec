package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mark3labs/mcp-go/server"
)

// TransportManager handles different transport types for the MCP server
type TransportManager struct {
	mcpServer *MCPServer
	config    *Config
}

// NewTransportManager creates a new transport manager
func NewTransportManager(mcpServer *MCPServer, config *Config) *TransportManager {
	return &TransportManager{
		mcpServer: mcpServer,
		config:    config,
	}
}

// Start starts the MCP server with the configured transport
func (tm *TransportManager) Start(ctx context.Context) error {
	switch tm.config.TransportType {
	case "stdio":
		return tm.startStdioTransport(ctx)
	case "sse":
		return tm.startSSETransport(ctx)
	case "http":
		return tm.startHTTPTransport(ctx)
	default:
		return fmt.Errorf("unknown transport type: %s. Use 'stdio', 'sse', or 'http'", tm.config.TransportType)
	}
}

// startStdioTransport starts the server with stdio transport
func (tm *TransportManager) startStdioTransport(ctx context.Context) error {
	log.Println("Starting MCP server with stdio transport...")

	// Use the mcp-go server's stdio transport
	return server.ServeStdio(tm.mcpServer.GetServer())
}

// startSSETransport starts the server with Server-Sent Events transport
func (tm *TransportManager) startSSETransport(ctx context.Context) error {
	log.Println("Starting MCP server with SSE transport...")

	// Create HTTP server for SSE
	mux := http.NewServeMux()

	// Note: The mcp-go library doesn't have built-in SSE transport like the Node.js version
	// This is a simplified implementation that would need to be expanded for full SSE support
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		log.Println("Received SSE connection request")

		// Set SSE headers
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// For a full implementation, you would need to:
		// 1. Create an SSE transport adapter for mcp-go
		// 2. Handle the MCP protocol over SSE
		// 3. Manage connection lifecycle

		fmt.Fprintf(w, "data: SSE endpoint ready\n\n")
		w.(http.Flusher).Flush()

		// Keep connection alive
		<-r.Context().Done()
	})

	httpServer := &http.Server{
		Addr:    tm.config.GetListenAddress(),
		Handler: mux,
	}

	// Start server in goroutine
	go func() {
		log.Printf("MCP server is running on port %d with SSE transport", tm.config.Port)
		log.Printf("Connect to: http://localhost:%d/sse", tm.config.Port)

		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return httpServer.Shutdown(shutdownCtx)
}

// startHTTPTransport starts the server with HTTP transport
func (tm *TransportManager) startHTTPTransport(ctx context.Context) error {
	log.Println("Starting MCP server with HTTP transport...")

	// Create HTTP server for the MCP protocol
	mux := http.NewServeMux()

	// The mcp-go library should provide HTTP transport, but if not available,
	// we need to implement a basic HTTP handler for MCP
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Received MCP %s request", r.Method)

		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, mcp-session-id")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// For a full implementation, you would need to:
		// 1. Parse the MCP request from the HTTP body
		// 2. Route it to the appropriate MCP server handler
		// 3. Return the MCP response as HTTP response
		// 4. Handle session management

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"jsonrpc": "2.0", "result": {"message": "MCP HTTP endpoint ready"}}`)
	})

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status": "healthy", "server": "sqlite-vec-doc-query", "version": "1.0.0"}`)
	})

	httpServer := &http.Server{
		Addr:    tm.config.GetListenAddress(),
		Handler: mux,
		// Configure timeouts
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Printf("MCP server is running on port %d with HTTP transport", tm.config.Port)
		log.Printf("Connect to: http://localhost:%d/mcp", tm.config.Port)
		log.Printf("Health check: http://localhost:%d/health", tm.config.Port)

		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Graceful shutdown
	log.Println("Shutting down server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return httpServer.Shutdown(shutdownCtx)
}

// StartWithSignalHandling starts the transport manager with proper signal handling
func (tm *TransportManager) StartWithSignalHandling() error {
	// Create context that cancels on interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start transport in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- tm.Start(ctx)
	}()

	// Wait for either error or interrupt signal
	select {
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("transport error: %w", err)
		}
		return nil
	case sig := <-sigChan:
		log.Printf("Received signal %v, shutting down...", sig)
		cancel()

		// Wait for graceful shutdown with timeout
		select {
		case err := <-errChan:
			if err != nil {
				log.Printf("Error during shutdown: %v", err)
			}
			log.Println("Server shutdown complete")
			return nil
		case <-time.After(35 * time.Second):
			return fmt.Errorf("shutdown timeout exceeded")
		}
	}
}
