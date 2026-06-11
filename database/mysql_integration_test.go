package db

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

// TestMySQLIntegration_ExportSeedRoundtrip exercises the full
// Export → Truncate → Restore cycle against a real MySQL database.
//
// Gated on SEEDMANCER_MYSQL_DATABASE_URL so default `go test ./...`
// runs don't hit the network or wipe anyone's local data.
//
// Usage:
//
//	SEEDMANCER_MYSQL_DATABASE_URL="mysql://root:root@127.0.0.1:3306/testdb" \
//	    go test -run TestMySQLIntegration ./database/...
func TestMySQLIntegration_ExportSeedRoundtrip(t *testing.T) {
	rawURL := os.Getenv("SEEDMANCER_MYSQL_DATABASE_URL")
	if rawURL == "" {
		t.Skip("SEEDMANCER_MYSQL_DATABASE_URL not set; skipping integration test")
	}

	normalizedDSN, scheme, err := normalizeDSN(rawURL)
	if err != nil {
		t.Fatalf("normalize DSN: %v", err)
	}
	if scheme != "mysql" {
		t.Fatalf("expected mysql scheme, got %q", scheme)
	}

	raw, err := sql.Open("mysql", normalizedDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if err := raw.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	dropAll := `
DROP TABLE IF EXISTS seedmancer_it_books;
DROP TABLE IF EXISTS seedmancer_it_authors;
`
	if _, err := raw.Exec(dropAll); err != nil {
		t.Fatalf("pre-clean: %v", err)
	}
	t.Cleanup(func() { _, _ = raw.Exec(dropAll) })

	ddl := `
CREATE TABLE seedmancer_it_authors (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    name       VARCHAR(200) NOT NULL,
    active     TINYINT(1) DEFAULT 1,
    created_at DATETIME DEFAULT NOW()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE seedmancer_it_books (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    author_id  INT NOT NULL,
    title      VARCHAR(300) NOT NULL,
    rating     DECIMAL(3,2),
    CONSTRAINT seedmancer_it_books_author_id_fk
        FOREIGN KEY (author_id) REFERENCES seedmancer_it_authors(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO seedmancer_it_authors (id, name, active) VALUES
    (1, 'Alice',    1),
    (2, 'Bob',      0),
    (3, 'Carol''s', 1);

INSERT INTO seedmancer_it_books (id, author_id, title, rating) VALUES
    (10, 1, 'First Book',  4.50),
    (11, 1, 'Second Book', NULL),
    (12, 2, 'Quiet Night', 3.25);
`
	if _, err := raw.Exec(ddl); err != nil {
		t.Fatalf("ddl: %v", err)
	}

	m := &MySQLManager{}
	if err := m.ConnectWithDSN(normalizedDSN); err != nil {
		t.Fatalf("connect: %v", err)
	}

	tmp := t.TempDir()
	schemaDir := filepath.Join(tmp, "schema")
	dataDir := filepath.Join(tmp, "data")
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := m.ExportSchema(schemaDir); err != nil {
		t.Fatalf("export schema: %v", err)
	}
	if _, err := os.Stat(filepath.Join(schemaDir, "schema.json")); err != nil {
		t.Fatalf("schema.json missing: %v", err)
	}

	if err := m.ExportToCSV(dataDir); err != nil {
		t.Fatalf("export csv: %v", err)
	}
	for _, tbl := range []string{"seedmancer_it_authors", "seedmancer_it_books"} {
		if _, err := os.Stat(filepath.Join(dataDir, tbl+".csv")); err != nil {
			t.Fatalf("csv for %s missing: %v", tbl, err)
		}
	}

	// Drop tables so RestoreFromCSV has to recreate + repopulate them.
	if _, err := raw.Exec(dropAll); err != nil {
		t.Fatalf("mid-clean: %v", err)
	}

	restoreDir := filepath.Join(tmp, "restore")
	if err := os.MkdirAll(restoreDir, 0755); err != nil {
		t.Fatal(err)
	}
	mustCopyDir(t, schemaDir, restoreDir)
	mustCopyDir(t, dataDir, restoreDir)

	if err := m.RestoreFromCSV(restoreDir); err != nil {
		t.Fatalf("restore: %v", err)
	}

	var authorsN, booksN int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM seedmancer_it_authors`).Scan(&authorsN); err != nil {
		t.Fatalf("count authors: %v", err)
	}
	if err := raw.QueryRow(`SELECT COUNT(*) FROM seedmancer_it_books`).Scan(&booksN); err != nil {
		t.Fatalf("count books: %v", err)
	}
	if authorsN != 3 || booksN != 3 {
		t.Fatalf("row counts: authors=%d books=%d, want 3/3", authorsN, booksN)
	}

	// Single-quote in a value must survive the CSV round-trip.
	var name string
	if err := raw.QueryRow(`SELECT name FROM seedmancer_it_authors WHERE id = 3`).Scan(&name); err != nil {
		t.Fatalf("scan name: %v", err)
	}
	if name != "Carol's" {
		t.Fatalf("name round-trip = %q, want %q", name, "Carol's")
	}

	// NULL value in a nullable column must be restored as NULL.
	var rating sql.NullFloat64
	if err := raw.QueryRow(`SELECT rating FROM seedmancer_it_books WHERE id = 11`).Scan(&rating); err != nil {
		t.Fatalf("scan rating: %v", err)
	}
	if rating.Valid {
		t.Fatalf("expected NULL rating for book id=11, got %v", rating.Float64)
	}

	// FK constraint must be intact — inserting an orphan book should fail.
	_, fkErr := raw.Exec(`INSERT INTO seedmancer_it_books (author_id, title) VALUES (9999, 'Orphan')`)
	if fkErr == nil {
		t.Fatal("expected FK violation when inserting orphan book, got nil error")
	}
}
