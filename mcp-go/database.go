package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"unsafe"

	_ "github.com/asg017/sqlite-vec-go-bindings/ncruces"
	"github.com/ncruces/go-sqlite3"
)

// Embedder defines the interface for embedding services
type Embedder interface {
	CreateEmbeddings(ctx context.Context, text string) ([]float64, error)
	CreateEmbeddingsBatch(ctx context.Context, texts []string) ([][]float64, error)
}

// QueryResult represents the result of a vector search query
type QueryResult struct {
	ChunkID  string  `json:"chunk_id"`
	Distance float64 `json:"distance"`
	Content  string  `json:"content"`
	URL      string  `json:"url,omitempty"`
}

// DatabaseService handles SQLite vector operations
type DatabaseService struct {
	config *Config
}

// NewDatabaseService creates a new database service
func NewDatabaseService(config *Config) *DatabaseService {
	return &DatabaseService{config: config}
}

// QueryFilter represents the filter parameters for database queries
type QueryFilter struct {
	ProductName string
	Version     string
}

// QueryCollection performs a vector similarity search on the specified collection
func (d *DatabaseService) QueryCollection(queryEmbedding []float64, filter QueryFilter, topK int) ([]QueryResult, error) {
	dbPath := d.config.GetDBPath(filter.ProductName)

	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("database file not found at %s", dbPath)
	}

	// Open database connection
	db, err := sqlite3.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database %s: %w", dbPath, err)
	}
	defer db.Close()

	log.Printf("[DB %s] Opened connection", dbPath)

	// Convert float64 slice to byte array for vector comparison
	vectorBytes, err := float64SliceToBytes(queryEmbedding)
	if err != nil {
		return nil, fmt.Errorf("failed to convert query embedding to bytes: %w", err)
	}

	// Build the query based on filters - matching the TypeScript implementation
	query := `
		SELECT
			*,
			distance
		FROM vec_items
		WHERE embedding MATCH ?`

	args := []any{vectorBytes}

	if filter.ProductName != "" {
		query += ` AND product_name = ?`
		args = append(args, filter.ProductName)
	}

	if filter.Version != "" {
		query += ` AND version = ?`
		args = append(args, filter.Version)
	}

	query += `
		ORDER BY distance
		LIMIT ?`
	args = append(args, topK)

	// Prepare and execute the statement
	stmt, _, err := db.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare query: %w", err)
	}
	defer stmt.Close()

	log.Printf("[DB %s] Query prepared. Executing...", dbPath)

	// Bind parameters
	for i, arg := range args {
		switch v := arg.(type) {
		case []byte:
			err = stmt.BindBlob(i+1, v)
		case string:
			err = stmt.BindText(i+1, v)
		case int:
			err = stmt.BindInt64(i+1, int64(v))
		default:
			err = fmt.Errorf("unsupported parameter type at index %d", i)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to bind parameter %d: %w", i+1, err)
		}
	}

	// Execute query and collect results
	var results []QueryResult
	log.Printf("[DB %s] Executing vector search query...", dbPath)

	for {
		hasRow := stmt.Step()
		if stmt.Err() != nil {
			return nil, fmt.Errorf("error executing query: %w", stmt.Err())
		}
		if !hasRow {
			break
		}

		// Since we're selecting *, we need to find the columns by name
		// Initialize result with default values
		result := QueryResult{}

		// Try to extract data from available columns
		for i := 0; i < stmt.ColumnCount(); i++ {
			columnName := stmt.ColumnName(i)
			switch columnName {
			case "chunk_id", "id":
				if stmt.ColumnType(i) != sqlite3.NULL {
					result.ChunkID = stmt.ColumnText(i)
				}
			case "distance":
				if stmt.ColumnType(i) != sqlite3.NULL {
					result.Distance = stmt.ColumnFloat(i)
				}
			case "content", "text":
				if stmt.ColumnType(i) != sqlite3.NULL {
					result.Content = stmt.ColumnText(i)
				}
			case "url", "source", "link":
				if stmt.ColumnType(i) != sqlite3.NULL {
					result.URL = stmt.ColumnText(i)
				}
			}
		}

		// If we don't have essential fields, try to use the first few columns
		if result.ChunkID == "" && stmt.ColumnCount() > 0 {
			if stmt.ColumnType(0) != sqlite3.NULL {
				result.ChunkID = fmt.Sprintf("row_%s", stmt.ColumnText(0))
			}
		}

		// If we still don't have content, try to find any text column
		if result.Content == "" {
			for i := 0; i < stmt.ColumnCount(); i++ {
				if stmt.ColumnType(i) == sqlite3.TEXT {
					text := stmt.ColumnText(i)
					if len(text) > 10 { // Assume longer text is content
						result.Content = text
						break
					}
				}
			}
		}

		results = append(results, result)
	}

	log.Printf("[DB %s] Query completed. Found %d rows", dbPath, len(results))

	return results, nil
}

// TestConnection tests if the database connection and sqlite-vec extension work
func (d *DatabaseService) TestConnection(productName string) error {
	dbPath := d.config.GetDBPath(productName)

	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database file not found at %s", dbPath)
	}

	// Open database connection
	db, err := sqlite3.Open(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database %s: %w", dbPath, err)
	}
	defer db.Close()

	// Test basic SQLite functionality
	stmt, _, err := db.Prepare("SELECT sqlite_version()")
	if err != nil {
		return fmt.Errorf("failed to prepare SQLite version query: %w", err)
	}
	defer stmt.Close()

	hasRow := stmt.Step()
	if stmt.Err() != nil {
		return fmt.Errorf("failed to execute SQLite version query: %w", stmt.Err())
	}
	if !hasRow {
		return fmt.Errorf("no result from SQLite version query")
	}

	sqliteVersion := stmt.ColumnText(0)
	log.Printf("SQLite version: %s", sqliteVersion)

	// Test sqlite-vec extension
	stmt2, _, err := db.Prepare("SELECT vec_version()")
	if err != nil {
		return fmt.Errorf("failed to prepare vec_version query - sqlite-vec extension may not be loaded: %w", err)
	}
	defer stmt2.Close()

	hasRow = stmt2.Step()
	if stmt2.Err() != nil {
		return fmt.Errorf("failed to execute vec_version query: %w", stmt2.Err())
	}
	if !hasRow {
		return fmt.Errorf("no result from vec_version query")
	}

	vecVersion := stmt2.ColumnText(0)
	log.Printf("sqlite-vec version: %s", vecVersion)

	return nil
}

// float64SliceToBytes converts a slice of float64 to bytes for SQLite vector operations
func float64SliceToBytes(values []float64) ([]byte, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("empty embedding vector")
	}

	// Convert to float32 first as most vector databases use float32
	float32Values := make([]float32, len(values))
	for i, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("invalid float value at index %d: %f", i, v)
		}
		float32Values[i] = float32(v)
	}

	// Convert to bytes
	buf := make([]byte, len(float32Values)*4)
	for i, v := range float32Values {
		binary.LittleEndian.PutUint32(buf[i*4:(i+1)*4], *(*uint32)(unsafe.Pointer(&v)))
	}

	return buf, nil
}

// DocumentationQuery represents a high-level documentation query
type DocumentationQuery struct {
	QueryText   string
	ProductName string
	Version     string
	Limit       int
}

// DocumentationResult represents a simplified result for the MCP tool
type DocumentationResult struct {
	Distance float64 `json:"distance"`
	Content  string  `json:"content"`
	URL      string  `json:"url,omitempty"`
}

// QueryDocumentation performs a complete documentation query including embedding creation
func (d *DatabaseService) QueryDocumentation(embeddingService *EmbeddingService, query DocumentationQuery) ([]DocumentationResult, error) {
	// Create embeddings for the query text
	queryEmbedding, err := embeddingService.CreateEmbeddings(nil, query.QueryText)
	if err != nil {
		return nil, fmt.Errorf("failed to create embeddings: %w", err)
	}

	// Query the collection
	filter := QueryFilter{
		ProductName: query.ProductName,
		Version:     query.Version,
	}

	results, err := d.QueryCollection(queryEmbedding, filter, query.Limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query collection: %w", err)
	}

	// Convert to documentation results
	docResults := make([]DocumentationResult, len(results))
	for i, result := range results {
		docResults[i] = DocumentationResult{
			Distance: result.Distance,
			Content:  result.Content,
			URL:      result.URL,
		}
	}

	return docResults, nil
}
