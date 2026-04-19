package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestOutputJSON(t *testing.T) {
	// Capture stdout since outputJSON prints directly to os.Stdout.
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	payload := listOutput{
		Local: []listEntry{{Schema: "abc", Dataset: "basic"}},
	}
	err = outputJSON(payload)

	w.Close()
	os.Stdout = orig
	if err != nil {
		t.Fatalf("outputJSON: %v", err)
	}

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)

	var decoded listOutput
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(decoded.Local) != 1 || decoded.Local[0].Schema != "abc" {
		t.Fatalf("unexpected payload: %+v", decoded)
	}
}

// TestListLocalEntries_localLayout exercises the full on-disk walk: create a
// fake `.seedmancer/schemas/<fp-short>/datasets/<name>/` tree and make sure
// the entry comes back.
func TestListLocalEntries_localLayout(t *testing.T) {
	dir := t.TempDir()
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Setenv("HOME", dir)

	writeFile(t, filepath.Join(dir, "seedmancer.yaml"), "storage_path: .seedmancer\n")
	fp := `{"tables":[{"name":"t","columns":[{"name":"id","type":"uuid"}]}]}`
	writeFile(t, filepath.Join(dir, ".seedmancer/schemas/abcd12345678/schema.json"), fp)
	writeFile(t, filepath.Join(dir, ".seedmancer/schemas/abcd12345678/datasets/basic/t.csv"), "id\n1\n")

	entries, err := listLocalEntries()
	if err != nil {
		t.Fatalf("listLocalEntries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d (%+v)", len(entries), entries)
	}
	if entries[0].Dataset != "basic" {
		t.Errorf("Dataset = %q, want %q", entries[0].Dataset, "basic")
	}
}
