package cmd

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "github.com/lib/pq"

	"github.com/KazanKK/seedmancer/internal/scenario"
)

// TestDatasetSQLPath_layout encodes the on-disk contract: dataset.sql
// is a sibling of data/, not inside it, so the CSV scanners that walk
// data/ never see it.
func TestDatasetSQLPath_layout(t *testing.T) {
	revDir := filepath.Join("scenarios", "billing", "revisions", "r001")
	got := DatasetSQLPath(revDir)
	want := filepath.Join(revDir, "dataset.sql")
	if got != want {
		t.Fatalf("DatasetSQLPath = %q, want %q", got, want)
	}
}

// TestRunGenerateLocal_rejectsMissingInherit verifies that the inherit
// argument is required up front, before the runner touches the DB. This
// is enforced at the top of RunGenerateLocal so a stray call from a
// legacy script can't accidentally seed an empty baseline.
func TestRunGenerateLocal_rejectsMissingInherit(t *testing.T) {
	_, err := RunGenerateLocal(context.Background(), GenerateLocalInput{
		SQL:      "SELECT 1;",
		Scenario: "x",
		// Inherit intentionally empty.
	})
	if err == nil {
		t.Fatal("expected error when inherit is missing")
	}
	if !strings.Contains(err.Error(), "inherit is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRunGenerateLocal_rejectsEmptySQL mirrors the inherit guard for the
// SQL body — silently accepting an empty SQL would produce a revision
// identical to the inherit base, which is never what the caller intended.
func TestRunGenerateLocal_rejectsEmptySQL(t *testing.T) {
	_, err := RunGenerateLocal(context.Background(), GenerateLocalInput{
		SQL:      "   \n\t  ",
		Scenario: "x",
		Inherit:  "baseline",
	})
	if err == nil {
		t.Fatal("expected error when SQL is empty")
	}
	if !strings.Contains(err.Error(), "sql cannot be empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRunGenerateLocal_integration drives the full seed → apply SQL →
// export → save dataset.sql pipeline against a real Postgres database.
// The flow:
//
//  1. Set up a tmp project with seedmancer.yaml pointing at the test DB.
//  2. Create the schema with starter data.
//  3. RunExport("baseline") to capture the seed-able baseline.
//  4. RunGenerateLocal(scenario="updated", inherit="baseline", sql=<DML>).
//  5. Assert: revision contains data/<tables>.csv (reflecting the SQL)
//     and dataset.sql alongside it (verbatim agent input).
//  6. Round-trip the SQL via RunGetDatasetSQL.
//
// Gated on SEEDMANCER_INTEGRATION_DATABASE_URL — default `go test ./...`
// runs skip this.
func TestRunGenerateLocal_integration(t *testing.T) {
	dsn := os.Getenv("SEEDMANCER_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("SEEDMANCER_INTEGRATION_DATABASE_URL not set; skipping integration test")
	}

	raw, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })

	// Pre/post-clean keeps reruns from tripping on leftover schema. CASCADE
	// because books FKs into authors.
	const dropAll = `
DROP TABLE IF EXISTS public.seedmancer_gen_books   CASCADE;
DROP TABLE IF EXISTS public.seedmancer_gen_authors CASCADE;
`
	if _, err := raw.Exec(dropAll); err != nil {
		t.Fatalf("pre-clean: %v", err)
	}
	t.Cleanup(func() { _, _ = raw.Exec(dropAll) })

	const ddl = `
CREATE TABLE public.seedmancer_gen_authors (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE TABLE public.seedmancer_gen_books (
    id        INTEGER PRIMARY KEY,
    author_id INTEGER NOT NULL REFERENCES public.seedmancer_gen_authors(id),
    title     TEXT NOT NULL
);
INSERT INTO public.seedmancer_gen_authors (id, name) VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO public.seedmancer_gen_books (id, author_id, title) VALUES
    (10, 1, 'First Book'),
    (11, 2, 'Second Book');
`
	if _, err := raw.Exec(ddl); err != nil {
		t.Fatalf("ddl: %v", err)
	}

	// Stage a fake project rooted at t.TempDir() so FindConfigFile finds
	// our seedmancer.yaml. The pwd chdir is reverted in t.Cleanup.
	projectRoot := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(projectRoot, "seedmancer.yaml"),
		[]byte("storage_path: .seedmancer\ndefault_env: local\nenvironments:\n  local:\n    database_url: "+dsn+"\n"),
		0644,
	); err != nil {
		t.Fatalf("writing config: %v", err)
	}
	prevWD, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prevWD) })
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	ctx := context.Background()

	// 1) Capture baseline. Two scenarios both pin to it: "baseline" is the
	//    inherit source; "updated" gets generated via the SQL flow.
	if _, err := RunExport(ctx, ExportInput{Scenario: "baseline", Env: "local"}); err != nil {
		t.Fatalf("RunExport: %v", err)
	}

	// 2) Apply DML on top of baseline. We:
	//    - delete one book
	//    - insert a third author + book
	const sqlBlock = `
DELETE FROM seedmancer_gen_books WHERE id = 11;
INSERT INTO seedmancer_gen_authors (id, name) VALUES (3, 'Carol');
INSERT INTO seedmancer_gen_books   (id, author_id, title) VALUES (12, 3, 'Third Book');
`
	out, err := RunGenerateLocal(ctx, GenerateLocalInput{
		SQL:         sqlBlock,
		Scenario:    "updated",
		Inherit:     "baseline",
		Env:         "local",
		Description: "integration test",
	})
	if err != nil {
		t.Fatalf("RunGenerateLocal: %v", err)
	}

	if out.Revision != "r001" {
		t.Fatalf("revision = %q, want r001", out.Revision)
	}
	if out.InheritedFrom != "baseline" {
		t.Fatalf("inheritedFrom = %q, want baseline", out.InheritedFrom)
	}
	if !out.GeneratorSQLStored {
		t.Fatal("expected GeneratorSQLStored=true")
	}
	if out.Env != "local" {
		t.Fatalf("env = %q, want local", out.Env)
	}

	// Sanity-check exported tables.
	wantTables := []string{"seedmancer_gen_authors", "seedmancer_gen_books"}
	sort.Strings(out.Tables)
	for _, want := range wantTables {
		found := false
		for _, got := range out.Tables {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing table %q in output (got %v)", want, out.Tables)
		}
	}

	// dataset.sql must exist at <revDir>/dataset.sql with the exact bytes
	// passed in. This is what get_dataset_sql round-trips.
	revDir := scenario.RevisionDir(projectRoot, ".seedmancer", "updated", out.Revision)
	sqlOnDisk, err := os.ReadFile(DatasetSQLPath(revDir))
	if err != nil {
		t.Fatalf("reading dataset.sql: %v", err)
	}
	if string(sqlOnDisk) != sqlBlock {
		t.Fatalf("dataset.sql contents drifted: got %q, want %q", string(sqlOnDisk), sqlBlock)
	}

	// Row counts on the exported CSVs should reflect the SQL: 3 authors, 2 books.
	if got := out.RowCounts["seedmancer_gen_authors"]; got != 3 {
		t.Errorf("authors row count = %d, want 3", got)
	}
	if got := out.RowCounts["seedmancer_gen_books"]; got != 2 {
		t.Errorf("books row count = %d, want 2", got)
	}

	// Round-trip the SQL via get_dataset_sql.
	sqlOut, err := RunGetDatasetSQL(ctx, GetDatasetSQLInput{Scenario: "updated"})
	if err != nil {
		t.Fatalf("RunGetDatasetSQL: %v", err)
	}
	if sqlOut.SQL != sqlBlock {
		t.Fatalf("get_dataset_sql drift: got %q, want %q", sqlOut.SQL, sqlBlock)
	}
	if sqlOut.Revision != out.Revision {
		t.Fatalf("get_dataset_sql revision = %q, want %q", sqlOut.Revision, out.Revision)
	}
}

// TestRunGetDatasetSQL_missing reports a clear error when a revision was
// produced by export (no dataset.sql) and the agent still asks for it.
func TestRunGetDatasetSQL_missing(t *testing.T) {
	dsn := os.Getenv("SEEDMANCER_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("SEEDMANCER_INTEGRATION_DATABASE_URL not set; skipping integration test")
	}

	raw, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })

	const dropAll = `DROP TABLE IF EXISTS public.seedmancer_nosql_t CASCADE;`
	if _, err := raw.Exec(dropAll); err != nil {
		t.Fatalf("pre-clean: %v", err)
	}
	t.Cleanup(func() { _, _ = raw.Exec(dropAll) })

	if _, err := raw.Exec(`CREATE TABLE public.seedmancer_nosql_t (id INT PRIMARY KEY); INSERT INTO public.seedmancer_nosql_t VALUES (1);`); err != nil {
		t.Fatalf("ddl: %v", err)
	}

	projectRoot := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(projectRoot, "seedmancer.yaml"),
		[]byte("storage_path: .seedmancer\ndefault_env: local\nenvironments:\n  local:\n    database_url: "+dsn+"\n"),
		0644,
	); err != nil {
		t.Fatalf("writing config: %v", err)
	}
	prevWD, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prevWD) })
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	if _, err := RunExport(context.Background(), ExportInput{Scenario: "plain", Env: "local"}); err != nil {
		t.Fatalf("RunExport: %v", err)
	}

	_, err = RunGetDatasetSQL(context.Background(), GetDatasetSQLInput{Scenario: "plain"})
	if err == nil {
		t.Fatal("expected error from get_dataset_sql on export-only revision")
	}
	if !strings.Contains(err.Error(), "dataset.sql") {
		t.Fatalf("unexpected error: %v", err)
	}
}
