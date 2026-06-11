package cmd

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestMaterializeRestoreDir verifies that materializeRestoreDir produces a
// flat directory containing schema sidecars + dataset files, exactly as
// RestoreFromCSV expects.
func TestMaterializeRestoreDir(t *testing.T) {
	root := t.TempDir()
	schemaDir := filepath.Join(root, "schema")
	datasetDir := filepath.Join(root, "dataset")

	writeFile(t, filepath.Join(schemaDir, "schema.json"), `{"tables":[{"name":"users","columns":[{"name":"id","type":"uuid"}]}]}`)
	writeFile(t, filepath.Join(schemaDir, "set_updated_at_func.sql"), "CREATE FUNCTION ...;")
	writeFile(t, filepath.Join(schemaDir, "users_users_updated_at_trigger.sql"), "-- seedmancer:trigger\n-- name: users_updated_at\n-- table_schema: public\n-- table_name: users\nCREATE TRIGGER users_updated_at ...;")
	writeFile(t, filepath.Join(schemaDir, "README.md"), "ignored")

	writeFile(t, filepath.Join(datasetDir, "users.csv"), "id\n1\n")
	writeFile(t, filepath.Join(datasetDir, "ignored.txt"), "nope")

	merged, cleanup, err := materializeRestoreDir(schemaDir, datasetDir)
	if err != nil {
		t.Fatalf("materializeRestoreDir: %v", err)
	}
	defer cleanup()

	entries, err := os.ReadDir(merged)
	if err != nil {
		t.Fatalf("reading merged dir: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	want := []string{
		"schema.json",
		"set_updated_at_func.sql",
		"users.csv",
		"users_users_updated_at_trigger.sql",
	}
	sort.Strings(want)

	if len(names) != len(want) {
		t.Fatalf("got %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("got %v, want %v", names, want)
		}
	}
}

func TestMaterializeRestoreDir_emptyDatasetErrors(t *testing.T) {
	root := t.TempDir()
	schemaDir := filepath.Join(root, "schema")
	datasetDir := filepath.Join(root, "dataset")
	writeFile(t, filepath.Join(schemaDir, "schema.json"), `{"tables":[]}`)
	if err := os.MkdirAll(datasetDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	if _, _, err := materializeRestoreDir(schemaDir, datasetDir); err == nil {
		t.Fatal("expected error for empty dataset dir")
	}
}

func TestLinkOrCopy(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src.txt")
	dst := filepath.Join(root, "dst.txt")
	writeFile(t, src, "hello")

	if err := linkOrCopy(src, dst); err != nil {
		t.Fatalf("linkOrCopy: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("reading dst: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("dst content = %q, want %q", got, "hello")
	}
}
