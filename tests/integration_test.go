package tests

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

const (
	testDBHost     = "localhost"
	testDBPort     = 54322
	testDBUser     = "postgres"
	testDBPassword = "postgres"
	testDBName     = "postgres"
)

func getDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		testDBUser, testDBPassword, testDBHost, testDBPort, testDBName)
}

func getTestDSN() string {
	return getDSN() + "?sslmode=disable"
}

func setupTestDB(t *testing.T) *sql.DB {
	// Wait for PostgreSQL to be ready
	maxRetries := 30
	var db *sql.DB
	var err error
	
	for i := 0; i < maxRetries; i++ {
		db, err = sql.Open("postgres", getTestDSN())
		if err == nil {
			err = db.Ping()
			if err == nil {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	// Create test tables
	_, err = db.Exec(`
		DROP SCHEMA public CASCADE;
		CREATE SCHEMA public;
		
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE posts (
			id SERIAL PRIMARY KEY,
			user_id INTEGER REFERENCES users(id),
			title TEXT NOT NULL,
			content TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		INSERT INTO users (name, email) VALUES
			('Test User 1', 'test1@example.com'),
			('Test User 2', 'test2@example.com');

		INSERT INTO posts (user_id, title, content) VALUES
			(1, 'First Post', 'Hello World'),
			(2, 'Second Post', 'Another post');
	`)
	if err != nil {
		t.Fatalf("Failed to create test tables: %v", err)
	}

	return db
}

func TestCLI(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tmpDir, err := os.MkdirTemp("", "reseeder-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Build the CLI tool
	cmd := exec.Command("go", "build", "-o", filepath.Join(tmpDir, "reseeder"))
	cmd.Dir = ".."
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to build CLI: %v", err)
	}

	cliPath := filepath.Join(tmpDir, "reseeder")

	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(cliPath, tt.args...)
			output, err := cmd.CombinedOutput()
			
			if (err != nil) != tt.wantErr {
				t.Errorf("CLI command failed: %v\nOutput: %s", err, output)
				return
			}

			// Verify database state after each operation
			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
			if err != nil {
				t.Errorf("Failed to query users count: %v", err)
			}
			if count != 2 {
				t.Errorf("Expected 2 users, got %d", count)
			}

			err = db.QueryRow("SELECT COUNT(*) FROM posts").Scan(&count)
			if err != nil {
				t.Errorf("Failed to query posts count: %v", err)
			}
			if count != 2 {
				t.Errorf("Expected 2 posts, got %d", count)
			}
		})
	}
}

func TestGenerateFakeData(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tmpDir, err := os.MkdirTemp("", "reseeder-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Build the CLI tool
	cmd := exec.Command("go", "build", "-o", filepath.Join(tmpDir, "reseeder"))
	cmd.Dir = ".."
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to build CLI: %v", err)
	}

	cliPath := filepath.Join(tmpDir, "reseeder")


	// Generate fake data
	cmd = exec.Command(cliPath, "generate-fake-data",
		"--schema", filepath.Join(tmpDir, "schema.json"),
		"--output-dir", tmpDir,
		"--rows", "10")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to generate fake data: %v\nOutput: %s", err, output)
	}

	// Verify CSV files were created
	tables := []string{"users", "posts"}
	for _, table := range tables {
		csvPath := filepath.Join(tmpDir, fmt.Sprintf("%s.csv", table))
		if _, err := os.Stat(csvPath); os.IsNotExist(err) {
			t.Errorf("Expected CSV file not found: %s", csvPath)
		}
	}
} 