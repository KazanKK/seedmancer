package cmd

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestCompressFiles(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "schema.json")
	b := filepath.Join(root, "users.csv")
	writeFile(t, a, `{"tables":[]}`)
	writeFile(t, b, "id\n1\n")

	buf, err := compressFiles([]string{a, b})
	if err != nil {
		t.Fatalf("compressFiles: %v", err)
	}
	r, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("opening zip: %v", err)
	}

	var got []string
	for _, f := range r.File {
		got = append(got, f.Name)
		// Each entry must be keyed by basename only, not the full source path.
		if filepath.Base(f.Name) != f.Name {
			t.Fatalf("zip entry %q should be flat (no subdir)", f.Name)
		}
	}
	sort.Strings(got)

	want := []string{"schema.json", "users.csv"}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("zip entries = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("zip entries = %v, want %v", got, want)
		}
	}
}

func TestCompressFiles_missingSourceErrors(t *testing.T) {
	if _, err := compressFiles([]string{"/nonexistent/file.csv"}); err == nil {
		t.Fatal("expected error on missing source file")
	}
}

func TestNewSchemaBadge(t *testing.T) {
	if got := newSchemaBadge(true); got != "  (new)" {
		t.Errorf("newSchemaBadge(true) = %q", got)
	}
	if got := newSchemaBadge(false); got != "" {
		t.Errorf("newSchemaBadge(false) = %q", got)
	}
}

// Quick integration-style check that extracting the produced zip yields the
// same files we put in, with the same content.
func TestCompressFiles_roundtripContent(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "payload.csv")
	writeFile(t, p, "id,name\n1,alice\n")

	buf, err := compressFiles([]string{p})
	if err != nil {
		t.Fatalf("compressFiles: %v", err)
	}
	r, _ := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	f, err := r.File[0].Open()
	if err != nil {
		t.Fatalf("open entry: %v", err)
	}
	defer f.Close()
	out, _ := os.ReadFile(p)
	data := make([]byte, len(out))
	n, _ := f.Read(data)
	if string(data[:n]) != string(out) {
		t.Fatalf("roundtrip mismatch: got %q want %q", string(data[:n]), string(out))
	}
}
