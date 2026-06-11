package cmd

import (
	"archive/zip"
	"encoding/json"
	"errors"
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
	if err == nil || !errors.Is(err, utils.ErrInvalidAPIToken) {
		t.Fatalf("want ErrInvalidAPIToken, got: %v", err)
	}
}

func TestRunFetch_skipsDownloadWhenUpToDate(t *testing.T) {
	dir := t.TempDir()
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Setenv("HOME", dir)
	writeFile(t, filepath.Join(dir, "seedmancer.yaml"), "storage_path: .seedmancer\n")

	// Local scenario whose latest revision is stamped with the cloud
	// revision id + updatedAt (as a previous pull/push would leave it).
	scDir := filepath.Join(dir, ".seedmancer", "scenarios", "bench", "x")
	revDir := filepath.Join(scDir, "revisions", "r001")
	writeFile(t, filepath.Join(scDir, "manifest.json"),
		`{"scenario":"bench/x","createdAt":"2026-06-10T00:00:00Z","updatedAt":"2026-06-10T00:00:00Z","latest":"r001"}`)
	writeFile(t, filepath.Join(revDir, "manifest.json"),
		`{"scenario":"bench/x","revision":"r001","schemaFingerprint":"abc","createdAt":"2026-06-10T00:00:00Z","source":"pull","tables":["users"],"services":["postgres"],"rowCounts":{"users":1},"remoteId":"rev_1","remoteUpdatedAt":"2026-06-10T12:00:00Z"}`)
	writeFile(t, filepath.Join(revDir, "data", "users.csv"), "id\n1\n")

	downloadCalls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1.0/datasets":
			_ = json.NewEncoder(w).Encode(datasetListResponse{Datasets: []datasetAPI{{
				ID:        "rev_1",
				Name:      "bench/x",
				UpdatedAt: "2026-06-10T12:00:00Z",
				Schema:    &schemaRefShort{ID: "s1", Fingerprint: "abc", FingerprintShort: "abc"},
			}}})
		default:
			downloadCalls++
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	t.Setenv("SEEDMANCER_API_URL", srv.URL)

	out, err := RunFetch(t.Context(), FetchInput{Scenario: "bench/x", Token: "tok"})
	if err != nil {
		t.Fatalf("RunFetch: %v", err)
	}
	if !out.UpToDate {
		t.Fatalf("expected UpToDate=true, got %+v", out)
	}
	if out.Revision != "r001" {
		t.Fatalf("revision = %q, want r001", out.Revision)
	}
	if downloadCalls != 0 {
		t.Fatalf("download endpoints hit %d times; want 0", downloadCalls)
	}
}

func TestLiftSchemaSidecars_movesSchemaAndSQLOnly(t *testing.T) {
	root := t.TempDir()
	schemaDir := filepath.Join(root, "schemas", "abcd")
	datasetDir := filepath.Join(schemaDir, "datasets", "basic")

	writeFile(t, filepath.Join(datasetDir, "schema.json"), `{"tables":[]}`)
	writeFile(t, filepath.Join(datasetDir, "users_updated_at_trigger.sql"), "-- trigger")
	writeFile(t, filepath.Join(datasetDir, "do_stuff_func.sql"), "-- func")
	writeFile(t, filepath.Join(datasetDir, "users.csv"), "id\n1\n")
	writeFile(t, filepath.Join(datasetDir, "orders.json"), `[]`)

	moved, err := liftSchemaSidecars(datasetDir, schemaDir)
	if err != nil {
		t.Fatalf("liftSchemaSidecars: %v", err)
	}
	if moved != 3 {
		t.Fatalf("moved = %d, want 3", moved)
	}

	for _, n := range []string{"schema.json", "users_updated_at_trigger.sql", "do_stuff_func.sql"} {
		if _, err := os.Stat(filepath.Join(schemaDir, n)); err != nil {
			t.Errorf("expected %s in schemaDir: %v", n, err)
		}
		if _, err := os.Stat(filepath.Join(datasetDir, n)); !os.IsNotExist(err) {
			t.Errorf("%s should be gone from datasetDir (stat err=%v)", n, err)
		}
	}
	for _, n := range []string{"users.csv", "orders.json"} {
		if _, err := os.Stat(filepath.Join(datasetDir, n)); err != nil {
			t.Errorf("dataset payload %s should stay put: %v", n, err)
		}
	}
}

func TestLiftSchemaSidecars_overwritesExistingSidecar(t *testing.T) {
	root := t.TempDir()
	schemaDir := filepath.Join(root, "schemas", "abcd")
	datasetDir := filepath.Join(schemaDir, "datasets", "basic")

	writeFile(t, filepath.Join(schemaDir, "schema.json"), `{"stale":true}`)
	writeFile(t, filepath.Join(datasetDir, "schema.json"), `{"fresh":true}`)

	if _, err := liftSchemaSidecars(datasetDir, schemaDir); err != nil {
		t.Fatalf("liftSchemaSidecars: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(schemaDir, "schema.json"))
	if err != nil {
		t.Fatalf("read schema.json: %v", err)
	}
	if !strings.Contains(string(got), "fresh") {
		t.Fatalf("schema.json not overwritten: %q", got)
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
	extracted, downloaded, err := downloadAndExtractZip(srv.URL, outDir)
	if err != nil {
		t.Fatalf("downloadAndExtractZip: %v", err)
	}
	if downloaded != int64(len(buf)) {
		t.Fatalf("downloaded bytes = %d, want %d", downloaded, len(buf))
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
