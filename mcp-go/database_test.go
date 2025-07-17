package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/asg017/sqlite-vec-go-bindings/ncruces"
	"github.com/ncruces/go-sqlite3"
)

// setupTestEmbeddingService creates an embedding service for testing
func setupTestEmbeddingService(t *testing.T) *EmbeddingService {
	// Check if OpenAI API key is available
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		t.Skip("OPENAI_API_KEY environment variable not set, skipping test with real embeddings")
	}

	// Create test config for OpenAI
	config := &Config{
		EmbeddingProvider: ProviderOpenAI,
		OpenAIAPIKey:      apiKey,
		OpenAIModel:       "text-embedding-3-large", // Match the model used to create the database
	}

	// Create embedding service
	embeddingService, err := NewEmbeddingService(config)
	if err != nil {
		t.Fatalf("Failed to create embedding service: %v", err)
	}

	return embeddingService
}

// downloadRealDatabase downloads a real database file from the S3 URLs used in the Dockerfile
func downloadRealDatabase(t *testing.T, dbPath string) {
	// Use the kubernetes.db as it's likely to be stable and have good content
	dbURL := "https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/kubernetes.db"

	t.Logf("Downloading real database from %s to %s", dbURL, dbPath)

	resp, err := http.Get(dbURL)
	if err != nil {
		t.Fatalf("Failed to download database: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to download database: HTTP %d", resp.StatusCode)
	}

	file, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database file: %v", err)
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		t.Fatalf("Failed to write database file: %v", err)
	}

	t.Logf("Successfully downloaded database to %s", dbPath)
}

// TestDatabaseService_QueryDocumentation tests the QueryDocumentation method with real data
func TestDatabaseService_QueryDocumentation(t *testing.T) {
	// Set up embedding service (will skip if no API key)
	embeddingService := setupTestEmbeddingService(t)

	// Create temporary directory for test databases
	tempDir, err := os.MkdirTemp("", "test_databases_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Download real database
	testDBPath := filepath.Join(tempDir, "kubernetes.db")
	downloadRealDatabase(t, testDBPath)

	// Create test config
	config := &Config{
		SQLiteDBDir: tempDir,
	}

	// Create database service
	dbService := NewDatabaseService(config)

	tests := []struct {
		name          string
		query         DocumentationQuery
		expectedError bool
		errorContains string
		minResults    int // Minimum expected results (since real embeddings may vary)
		maxResults    int // Maximum expected results
	}{
		{
			name: "successful query about pods",
			query: DocumentationQuery{
				QueryText:   "kubernetes pods",
				ProductName: "kubernetes",
				Version:     "",
				Limit:       5,
			},
			expectedError: false,
			minResults:    1, // Should find pod-related content
			maxResults:    5, // Limited by query limit
		},
		{
			name: "successful query about services",
			query: DocumentationQuery{
				QueryText:   "kubernetes services networking",
				ProductName: "kubernetes",
				Version:     "",
				Limit:       3,
			},
			expectedError: false,
			minResults:    1, // Should find service-related content
			maxResults:    3, // Limited by query limit
		},
		{
			name: "query with very specific version (likely no results)",
			query: DocumentationQuery{
				QueryText:   "pods",
				ProductName: "kubernetes",
				Version:     "v999.999.999", // Non-existent version
				Limit:       5,
			},
			expectedError: false,
			minResults:    0,
			maxResults:    0, // No results expected for non-existent version
		},
		{
			name: "database not found",
			query: DocumentationQuery{
				QueryText:   "test query",
				ProductName: "non-existent-product",
				Version:     "",
				Limit:       5,
			},
			expectedError: true,
			errorContains: "database file not found",
		},
		{
			name: "query with unrelated content",
			query: DocumentationQuery{
				QueryText:   "completely unrelated topic like cooking recipes",
				ProductName: "kubernetes",
				Version:     "",
				Limit:       5,
			},
			expectedError: false,
			minResults:    0, // May find some results but should be low relevance
			maxResults:    5, // Limited by query limit
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute query
			results, err := dbService.QueryDocumentation(embeddingService, tt.query)

			// Check error expectations
			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errorContains, err)
				}
				return
			}

			// Check success case
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(results) < tt.minResults || len(results) > tt.maxResults {
				t.Errorf("Expected %d-%d results, got %d", tt.minResults, tt.maxResults, len(results))
				return
			}

			// Validate result structure
			for i, result := range results {
				if result.Content == "" {
					t.Errorf("Result %d has empty content", i)
				}
				if result.Distance < 0 {
					t.Errorf("Result %d has negative distance: %f", i, result.Distance)
				}
			}

			// For successful queries with results, log them for inspection
			if len(results) > 0 {
				t.Logf("Query '%s' returned %d results:", tt.query.QueryText, len(results))
				for i, result := range results {
					t.Logf("  Result %d: distance=%.4f, content=%.100s...", i+1, result.Distance, result.Content)
				}
			}
		})
	}
}

// TestDatabaseService_QueryCollection tests the lower-level QueryCollection method with real embeddings
func TestDatabaseService_QueryCollection(t *testing.T) {
	// Set up embedding service (will skip if no API key)
	embeddingService := setupTestEmbeddingService(t)

	// Create temporary directory for test databases
	tempDir, err := os.MkdirTemp("", "test_databases_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Download real database
	testDBPath := filepath.Join(tempDir, "kubernetes.db")
	downloadRealDatabase(t, testDBPath)

	// Create test config
	config := &Config{
		SQLiteDBDir: tempDir,
	}

	// Create database service
	dbService := NewDatabaseService(config)

	// Create a real embedding for testing
	testEmbedding, err := embeddingService.CreateEmbeddings(nil, "kubernetes pods containers")
	if err != nil {
		t.Fatalf("Failed to create test embedding: %v", err)
	}

	tests := []struct {
		name          string
		filter        QueryFilter
		topK          int
		minResults    int
		maxResults    int
		expectedError bool
	}{
		{
			name: "query without version filter",
			filter: QueryFilter{
				ProductName: "kubernetes",
				Version:     "",
			},
			topK:          5,
			minResults:    1, // Should find at least some results
			maxResults:    5, // Limited by topK
			expectedError: false,
		},
		{
			name: "query with specific version",
			filter: QueryFilter{
				ProductName: "kubernetes",
				Version:     "v1.29", // Common kubernetes version
			},
			topK:          3,
			minResults:    0, // May or may not have this specific version
			maxResults:    3, // Limited by topK
			expectedError: false,
		},
		{
			name: "query with limit 1",
			filter: QueryFilter{
				ProductName: "kubernetes",
				Version:     "",
			},
			topK:          1,
			minResults:    1,
			maxResults:    1, // Limited by topK
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := dbService.QueryCollection(testEmbedding, tt.filter, tt.topK)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(results) < tt.minResults || len(results) > tt.maxResults {
				t.Errorf("Expected %d-%d results, got %d", tt.minResults, tt.maxResults, len(results))
			}

			// Validate result structure
			for i, result := range results {
				if result.ChunkID == "" {
					t.Errorf("Result %d has empty chunk_id", i)
				}
				if result.Content == "" {
					t.Errorf("Result %d has empty content", i)
				}
			}

			// Log results for inspection
			if len(results) > 0 {
				t.Logf("QueryCollection returned %d results", len(results))
				for i, result := range results {
					t.Logf("  Result %d: chunk_id=%s, distance=%.4f, content=%.100s...",
						i+1, result.ChunkID, result.Distance, result.Content)
				}
			}
		})
	}
}

// TestDatabaseService_TestConnection tests the TestConnection method
func TestDatabaseService_TestConnection(t *testing.T) {
	// Create temporary directory for test databases
	tempDir, err := os.MkdirTemp("", "test_databases_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Download real database
	testDBPath := filepath.Join(tempDir, "kubernetes.db")
	downloadRealDatabase(t, testDBPath)

	// Create test config
	config := &Config{
		SQLiteDBDir: tempDir,
	}

	// Create database service
	dbService := NewDatabaseService(config)

	// Test connection to existing database
	err = dbService.TestConnection("kubernetes")
	if err != nil {
		t.Errorf("TestConnection failed for existing database: %v", err)
	}

	// Test connection to non-existent database
	err = dbService.TestConnection("non-existent")
	if err == nil {
		t.Errorf("TestConnection should fail for non-existent database")
	}
}

// TestDatabaseSchema inspects the actual database schema
func TestDatabaseSchema(t *testing.T) {
	// Create temporary directory for test databases
	tempDir, err := os.MkdirTemp("", "test_databases_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Download real database
	testDBPath := filepath.Join(tempDir, "kubernetes.db")
	downloadRealDatabase(t, testDBPath)

	// Open database connection
	db, err := sqlite3.Open(testDBPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Get table info
	stmt, _, err := db.Prepare("SELECT name FROM sqlite_master WHERE type='table';")
	if err != nil {
		t.Fatalf("Failed to prepare table query: %v", err)
	}
	defer stmt.Close()

	t.Log("Tables in database:")
	for {
		hasRow := stmt.Step()
		if stmt.Err() != nil {
			t.Fatalf("Error getting tables: %v", stmt.Err())
		}
		if !hasRow {
			break
		}
		tableName := stmt.ColumnText(0)
		t.Logf("  Table: %s", tableName)

		// Get column info for each table
		columnStmt, _, err := db.Prepare("PRAGMA table_info(" + tableName + ");")
		if err != nil {
			t.Logf("    Failed to get column info: %v", err)
			continue
		}

		t.Logf("    Columns for %s:", tableName)
		for {
			hasColRow := columnStmt.Step()
			if columnStmt.Err() != nil {
				t.Logf("      Error getting columns: %v", columnStmt.Err())
				break
			}
			if !hasColRow {
				break
			}
			colName := columnStmt.ColumnText(1)
			colType := columnStmt.ColumnText(2)
			t.Logf("      - %s (%s)", colName, colType)
		}
		columnStmt.Close()
	}
}

// TestVecItemsStructure queries the vec_items table to understand its structure
func TestVecItemsStructure(t *testing.T) {
	// Create temporary directory for test databases
	tempDir, err := os.MkdirTemp("", "test_databases_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Download real database
	testDBPath := filepath.Join(tempDir, "kubernetes.db")
	downloadRealDatabase(t, testDBPath)

	// Open database connection
	db, err := sqlite3.Open(testDBPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Try to select a few rows to see what columns are available
	stmt, _, err := db.Prepare("SELECT * FROM vec_items LIMIT 1;")
	if err != nil {
		t.Fatalf("Failed to prepare select query: %v", err)
	}
	defer stmt.Close()

	hasRow := stmt.Step()
	if stmt.Err() != nil {
		t.Fatalf("Error selecting from vec_items: %v", stmt.Err())
	}

	if hasRow {
		t.Logf("vec_items table has %d columns", stmt.ColumnCount())
		for i := 0; i < stmt.ColumnCount(); i++ {
			columnName := stmt.ColumnName(i)
			columnType := stmt.ColumnType(i)
			var value string
			switch columnType {
			case sqlite3.TEXT:
				value = stmt.ColumnText(i)
				if len(value) > 100 {
					value = value[:100] + "..."
				}
			case sqlite3.INTEGER:
				value = fmt.Sprintf("%d", stmt.ColumnInt64(i))
			case sqlite3.FLOAT:
				value = fmt.Sprintf("%f", stmt.ColumnFloat(i))
			case sqlite3.BLOB:
				value = "BLOB"
			case sqlite3.NULL:
				value = "NULL"
			default:
				value = "UNKNOWN"
			}
			t.Logf("  Column %d: %s (%s) = %s", i, columnName, columnType, value)
		}
	} else {
		t.Log("No rows found in vec_items table")
	}
}

// TestVecItemsInfo inspects the vec_items_info table to understand table configuration
func TestVecItemsInfo(t *testing.T) {
	// Create temporary directory for test databases
	tempDir, err := os.MkdirTemp("", "test_databases_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Download real database
	testDBPath := filepath.Join(tempDir, "kubernetes.db")
	downloadRealDatabase(t, testDBPath)

	// Open database connection directly
	db, err := sqlite3.Open(testDBPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Query the vec_items_info table to understand configuration
	stmt, _, err := db.Prepare("SELECT key, value FROM vec_items_info")
	if err != nil {
		t.Fatalf("Failed to prepare vec_items_info query: %v", err)
	}
	defer stmt.Close()

	t.Log("vec_items_info contents:")
	for {
		hasRow := stmt.Step()
		if stmt.Err() != nil {
			t.Fatalf("Error querying vec_items_info: %v", stmt.Err())
		}
		if !hasRow {
			break
		}

		key := stmt.ColumnText(0)
		value := stmt.ColumnText(1)
		t.Logf("  %s: %s", key, value)
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || (len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			indexOfSubstring(s, substr) >= 0)))
}

func indexOfSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// Benchmark for QueryDocumentation performance with real data
func BenchmarkDatabaseService_QueryDocumentation(b *testing.B) {
	// Check if OpenAI API key is available
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		b.Skip("OPENAI_API_KEY environment variable not set, skipping benchmark with real embeddings")
	}

	// Setup
	tempDir, err := os.MkdirTemp("", "bench_databases_*")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Download real database
	testDBPath := filepath.Join(tempDir, "kubernetes.db")

	// Use a simple download for benchmark (without test logging)
	resp, err := http.Get("https://doc-sqlite-db.s3.sa-east-1.amazonaws.com/kubernetes.db")
	if err != nil {
		b.Fatalf("Failed to download database: %v", err)
	}
	defer resp.Body.Close()

	file, err := os.Create(testDBPath)
	if err != nil {
		b.Fatalf("Failed to create database file: %v", err)
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		b.Fatalf("Failed to write database file: %v", err)
	}

	// Create services
	config := &Config{
		SQLiteDBDir:       tempDir,
		EmbeddingProvider: ProviderOpenAI,
		OpenAIAPIKey:      apiKey,
		OpenAIModel:       "text-embedding-3-large", // Match the database dimensions
	}

	embeddingService, err := NewEmbeddingService(config)
	if err != nil {
		b.Fatalf("Failed to create embedding service: %v", err)
	}

	dbService := NewDatabaseService(config)

	query := DocumentationQuery{
		QueryText:   "kubernetes pods",
		ProductName: "kubernetes",
		Version:     "",
		Limit:       5,
	}

	// Run benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := dbService.QueryDocumentation(embeddingService, query)
		if err != nil {
			b.Fatalf("Benchmark failed: %v", err)
		}
	}
}

// TestBasicVectorSearch tests basic vector search without any filtering
func TestBasicVectorSearch(t *testing.T) {
	// Set up embedding service (will skip if no API key)
	embeddingService := setupTestEmbeddingService(t)

	// Create temporary directory for test databases
	tempDir, err := os.MkdirTemp("", "test_databases_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Download real database
	testDBPath := filepath.Join(tempDir, "kubernetes.db")
	downloadRealDatabase(t, testDBPath)

	// Open database connection directly
	db, err := sqlite3.Open(testDBPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a real embedding for testing
	testEmbedding, err := embeddingService.CreateEmbeddings(nil, "kubernetes pods containers")
	if err != nil {
		t.Fatalf("Failed to create test embedding: %v", err)
	}

	// Convert embedding to bytes
	vectorBytes, err := float64SliceToBytes(testEmbedding)
	if err != nil {
		t.Fatalf("Failed to convert embedding to bytes: %v", err)
	}

	// Try basic vector search without any additional filtering
	basicQuery := `SELECT * FROM vec_items WHERE embedding MATCH ? LIMIT 3`

	stmt, _, err := db.Prepare(basicQuery)
	if err != nil {
		t.Fatalf("Failed to prepare basic query: %v", err)
	}
	defer stmt.Close()

	// Bind the vector
	err = stmt.BindBlob(1, vectorBytes)
	if err != nil {
		t.Fatalf("Failed to bind vector parameter: %v", err)
	}

	// Execute query and see what we get
	resultCount := 0
	for {
		hasRow := stmt.Step()
		if stmt.Err() != nil {
			t.Fatalf("Error executing basic query: %v", stmt.Err())
		}
		if !hasRow {
			break
		}

		resultCount++
		t.Logf("Result %d:", resultCount)
		for i := 0; i < stmt.ColumnCount(); i++ {
			columnName := stmt.ColumnName(i)
			columnType := stmt.ColumnType(i)
			var value string
			switch columnType {
			case sqlite3.TEXT:
				value = stmt.ColumnText(i)
				if len(value) > 100 {
					value = value[:100] + "..."
				}
			case sqlite3.INTEGER:
				value = fmt.Sprintf("%d", stmt.ColumnInt64(i))
			case sqlite3.FLOAT:
				value = fmt.Sprintf("%f", stmt.ColumnFloat(i))
			case sqlite3.BLOB:
				value = "BLOB"
			case sqlite3.NULL:
				value = "NULL"
			default:
				value = "UNKNOWN"
			}
			t.Logf("  %s: %s", columnName, value)
		}
	}

	if resultCount == 0 {
		t.Log("No results found in basic vector search")
	} else {
		t.Logf("Basic vector search found %d results", resultCount)
	}
}
