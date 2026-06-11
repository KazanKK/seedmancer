package db

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/lib/pq"
)

// TestPostgresIntegration_ExportSeedRoundtrip exercises the full
// Export → Truncate → Restore cycle against a real Postgres database.
//
// The PostgresManager hardcodes `nspname = 'public'` in its schema probes,
// so this test creates its tables directly in `public` and cleans up
// afterwards with DROP TABLE … CASCADE. Any other user-defined tables in
// `public` on the target database will show up in the export too — the
// intended usage is a throwaway Postgres service container.
//
// Gated on SEEDMANCER_INTEGRATION_DATABASE_URL so default `go test ./...`
// runs don't hit the network or wipe anyone's local data.
//
// Usage:
//
//	SEEDMANCER_INTEGRATION_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable" \
//	    go test -run TestPostgresIntegration ./database/...
func TestPostgresIntegration_ExportSeedRoundtrip(t *testing.T) {
	dsn := os.Getenv("SEEDMANCER_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("SEEDMANCER_INTEGRATION_DATABASE_URL not set; skipping integration test")
	}

	raw, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })

	// Tear down any leftover state from a previous failed run before we
	// start, and again when the test finishes — DROP CASCADE covers the FK.
	dropAll := `
DROP TABLE IF EXISTS public.seedmancer_it_books   CASCADE;
DROP TABLE IF EXISTS public.seedmancer_it_authors CASCADE;
`
	if _, err := raw.Exec(dropAll); err != nil {
		t.Fatalf("pre-clean: %v", err)
	}
	t.Cleanup(func() { _, _ = raw.Exec(dropAll) })

	ddl := `
CREATE TABLE public.seedmancer_it_authors (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    active      BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE public.seedmancer_it_books (
    id          INTEGER PRIMARY KEY,
    author_id   INTEGER NOT NULL REFERENCES public.seedmancer_it_authors(id),
    title       TEXT NOT NULL,
    rating      NUMERIC(3,2)
);

INSERT INTO public.seedmancer_it_authors (id, name, active) VALUES
    (1, 'Alice',    TRUE),
    (2, 'Bob',      FALSE),
    (3, 'Carol''s', TRUE);

INSERT INTO public.seedmancer_it_books (id, author_id, title, rating) VALUES
    (10, 1, 'First Book',  4.50),
    (11, 1, 'Second Book', NULL),
    (12, 2, 'Quiet Night', 3.25);
`
	if _, err := raw.Exec(ddl); err != nil {
		t.Fatalf("ddl: %v", err)
	}

	pg := &PostgresManager{}
	if err := pg.ConnectWithDSN(dsn); err != nil {
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

	if err := pg.ExportSchema(schemaDir); err != nil {
		t.Fatalf("export schema: %v", err)
	}
	if _, err := os.Stat(filepath.Join(schemaDir, "schema.json")); err != nil {
		t.Fatalf("schema.json missing: %v", err)
	}
	if err := pg.ExportToCSV(dataDir); err != nil {
		t.Fatalf("export csv: %v", err)
	}

	// Drop the tables so RestoreFromCSV has to recreate + repopulate them.
	// TRUNCATE alone wouldn't prove much because the schema would still exist.
	if _, err := raw.Exec(dropAll); err != nil {
		t.Fatalf("mid-clean: %v", err)
	}

	// Flatten schema+data into one directory as RestoreFromCSV expects.
	restoreDir := filepath.Join(tmp, "restore")
	if err := os.MkdirAll(restoreDir, 0755); err != nil {
		t.Fatal(err)
	}
	mustCopyDir(t, schemaDir, restoreDir)
	mustCopyDir(t, dataDir, restoreDir)

	if err := pg.RestoreFromCSV(restoreDir); err != nil {
		t.Fatalf("restore: %v", err)
	}

	var authorsN, booksN int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM public.seedmancer_it_authors`).Scan(&authorsN); err != nil {
		t.Fatalf("count authors: %v", err)
	}
	if err := raw.QueryRow(`SELECT COUNT(*) FROM public.seedmancer_it_books`).Scan(&booksN); err != nil {
		t.Fatalf("count books: %v", err)
	}
	if authorsN != 3 || booksN != 3 {
		t.Fatalf("row counts after restore: authors=%d books=%d, want 3/3", authorsN, booksN)
	}

	// Single-quote in a value must survive CSV round-trip (no double-escape).
	var name string
	if err := raw.QueryRow(`SELECT name FROM public.seedmancer_it_authors WHERE id = 3`).Scan(&name); err != nil {
		t.Fatalf("scan name: %v", err)
	}
	if name != "Carol's" {
		t.Fatalf("name round-trip = %q, want %q", name, "Carol's")
	}
}

func mustCopyDir(t *testing.T, src, dst string) {
	t.Helper()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("readdir %s: %v", src, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0644); err != nil {
			t.Fatalf("write %s: %v", e.Name(), err)
		}
	}
}
