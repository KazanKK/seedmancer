package cmd

import (
	"archive/zip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	utils "github.com/KazanKK/seedmancer/internal/utils"
)

func strPtr(s string) *string { return &s }

func TestDisplayLabelForSchema(t *testing.T) {
	if got := displayLabelForSchema(nil); got != "(orphan)" {
		t.Errorf("got %q", got)
	}
	s := &schemaRefShort{FingerprintShort: "abcd12345678"}
	if got := displayLabelForSchema(s); got != "abcd12345678" {
		t.Errorf("got %q", got)
	}
	s.DisplayName = strPtr("main")
	if got := displayLabelForSchema(s); got != "main [abcd12345678]" {
		t.Errorf("got %q", got)
	}
}

func TestResolveFetchOutput_withFlagUsesAbsolutePath(t *testing.T) {
	dir := t.TempDir()
	out, err := resolveFetchOutput(filepath.Join(dir, "out"), datasetAPI{}, "ds1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !filepath.IsAbs(out) {
		t.Fatalf("output not absolute: %q", out)
	}
}

func TestResolveFetchOutput_noConfigNoFlagErrors(t *testing.T) {
	dir := t.TempDir()
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	// Stub HOME so no ~/.seedmancer/config.yaml lookup interferes with the test.
	t.Setenv("HOME", dir)

	_, err := resolveFetchOutput("", datasetAPI{Name: "ds1"}, "ds1")
	if err == nil {
		t.Fatal("expected error when config missing and --output not set")
	}
}

func TestResolveFetchOutput_usesConfigStoragePath(t *testing.T) {
	dir := t.TempDir()
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Setenv("HOME", dir)

	writeFile(t, filepath.Join(dir, "seedmancer.yaml"), "storage_path: .seedmancer\n")

	got, err := resolveFetchOutput("", datasetAPI{
		Name:   "ds1",
		Schema: &schemaRefShort{FingerprintShort: "abcd12345678"},
	}, "ds1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	want := utils.DatasetPath(dir, ".seedmancer", "abcd12345678", "ds1")
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFindRemoteDataset_singleMatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1.0/datasets" {
			http.NotFound(w, r)
			return
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			http.Error(w, "unauth", http.StatusUnauthorized)
			return
		}
		resp := datasetListResponse{Datasets: []datasetAPI{
			{ID: "1", Name: "basic", Schema: &schemaRefShort{ID: "s1", Fingerprint: "abc", FingerprintShort: "abc"}},
			{ID: "2", Name: "other"},
		}}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	got, err := findRemoteDataset(srv.URL, "tok", "basic", "")
	if err != nil {
		t.Fatalf("findRemoteDataset: %v", err)
	}
	if got.ID != "1" {
		t.Fatalf("got id %q, want %q", got.ID, "1")
	}
}

func TestFindRemoteDataset_ambiguousFailsWithHint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := datasetListResponse{Datasets: []datasetAPI{
			{ID: "1", Name: "basic", Schema: &schemaRefShort{FingerprintShort: "aaa"}},
			{ID: "2", Name: "basic", Schema: &schemaRefShort{FingerprintShort: "bbb"}},
		}}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	_, err := findRemoteDataset(srv.URL, "tok", "basic", "")
	if err == nil {
		t.Fatal("expected ambiguous error")
	}
	if !strings.Contains(err.Error(), "aaa") || !strings.Contains(err.Error(), "bbb") {
		t.Errorf("error should list ambiguous fingerprints; got: %v", err)
	}
}

func TestFindRemoteDataset_notFoundErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(datasetListResponse{})
	}))
	defer srv.Close()

	if _, err := findRemoteDataset(srv.URL, "tok", "missing", ""); err == nil {
		t.Fatal("expected not-found error")
	}
}

func TestFindRemoteDataset_unauthorizedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauth", http.StatusUnauthorized)
	}))
	defer srv.Close()

	_, err := findRemoteDataset(srv.URL, "bad", "ds", "")
	if err == nil || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("want unauthorized error, got: %v", err)
	}
}

func TestDownloadAndExtractZip(t *testing.T) {
	// Serve a small in-memory zip and verify extract writes the files.
	buf, err := compressTestZip(map[string]string{
		"schema.json": `{"tables":[]}`,
		"users.csv":   "id\n1\n",
	})
	if err != nil {
		t.Fatalf("build zip: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(buf)
	}))
	defer srv.Close()

	outDir := t.TempDir()
	extracted, err := downloadAndExtractZip(srv.URL, outDir)
	if err != nil {
		t.Fatalf("downloadAndExtractZip: %v", err)
	}
	sort.Strings(extracted)
	want := []string{"schema.json", "users.csv"}
	if len(extracted) != len(want) {
		t.Fatalf("extracted %v, want %v", extracted, want)
	}

	for _, n := range want {
		if _, err := os.Stat(filepath.Join(outDir, n)); err != nil {
			t.Fatalf("%s not written: %v", n, err)
		}
	}
}

// compressTestZip builds a flat zip buffer for the given name→content map.
// It mirrors the flat on-disk layout that the server produces.
func compressTestZip(files map[string]string) ([]byte, error) {
	var buf buffer
	zw := zip.NewWriter(&buf)
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			return nil, err
		}
		if _, err := io.WriteString(w, content); err != nil {
			return nil, err
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.b, nil
}

// buffer is a minimal Writer so we don't drag in bytes.Buffer just for this.
type buffer struct{ b []byte }

func (bb *buffer) Write(p []byte) (int, error) {
	bb.b = append(bb.b, p...)
	return len(p), nil
}
