package cmd

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/urfave/cli/v2"
)

func TestPushCommand_allPushesEachScenarioLatest(t *testing.T) {
	dir := t.TempDir()
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Setenv("HOME", dir)

	if err := os.WriteFile(filepath.Join(dir, "seedmancer.yaml"), []byte("storage_path: .seedmancer\n"), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	schemaShort := "deadbeefcafe"
	schemaDir := filepath.Join(dir, ".seedmancer", "schemas", schemaShort)
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		t.Fatalf("mkdir schema: %v", err)
	}
	if err := os.WriteFile(filepath.Join(schemaDir, "schema.json"), []byte("{}"), 0600); err != nil {
		t.Fatalf("write schema.json: %v", err)
	}

	fp := "deadbeefcafebabe0000"
	now := time.Now().UTC()
	for _, name := range []string{"alpha", "beta"} {
		writeScenarioForPush(t, dir, ".seedmancer", name, fp, now)
	}

	var uploadNames []string
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1.0/datasets/sync/upload-url":
			uploadNames = append(uploadNames, r.URL.Query().Get("name"))
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(uploadURLResponse{
				UploadURL: server.URL + "/blob",
				Path:      "staging/test.zip",
			})
		case r.Method == http.MethodPut && r.URL.Path == "/blob":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v1.0/datasets/sync/confirm":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(syncUploadResult{
				ID:               "ds_1",
				Name:             r.URL.Query().Get("name"),
				FingerprintShort: schemaShort,
				FileCount:        2,
			})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	t.Setenv("SEEDMANCER_API_URL", server.URL)

	app := &cli.App{
		Name:     "seedmancer",
		Writer:   io.Discard,
		ErrWriter: io.Discard,
		Commands: []*cli.Command{PushCommand()},
	}
	if err := app.Run([]string{"seedmancer", "push", "--all", "--token", "tok_test"}); err != nil {
		t.Fatalf("push --all: %v", err)
	}
	if len(uploadNames) != 2 {
		t.Fatalf("expected 2 upload-url calls, got %v", uploadNames)
	}
	if uploadNames[0] != "alpha" || uploadNames[1] != "beta" {
		t.Fatalf("unexpected push order: %v", uploadNames)
	}
}

func TestPushCommand_allRejectsExtraArg(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "seedmancer.yaml"), []byte("storage_path: .seedmancer\n"), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Setenv("HOME", dir)

	app := &cli.App{
		Name:      "seedmancer",
		Writer:    io.Discard,
		ErrWriter: io.Discard,
		Commands:  []*cli.Command{PushCommand()},
	}
	err := app.Run([]string{"seedmancer", "push", "--all", "nope", "--token", "tok_test"})
	if err == nil {
		t.Fatal("expected error when combining --all with scenario name")
	}
}

func writeScenarioForPush(t *testing.T, projectRoot, storagePath, scenarioPath, schemaFP string, now time.Time) {
	t.Helper()
	scDir := scenario.ScenarioDir(projectRoot, storagePath, scenarioPath)
	if err := os.MkdirAll(scDir, 0755); err != nil {
		t.Fatalf("mkdir scenario %s: %v", scenarioPath, err)
	}
	if err := scenario.WriteManifest(scDir, scenario.Manifest{
		Scenario:       scenarioPath,
		CreatedAt:      now,
		UpdatedAt:      now,
		LatestRevision: "r001",
	}); err != nil {
		t.Fatalf("write manifest %s: %v", scenarioPath, err)
	}
	if err := scenario.WritePointers(scDir, scenario.Pointers{Latest: "r001"}); err != nil {
		t.Fatalf("write pointers %s: %v", scenarioPath, err)
	}
	revDir := scenario.RevisionDir(projectRoot, storagePath, scenarioPath, "r001")
	dataDir := filepath.Join(revDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("mkdir data %s: %v", scenarioPath, err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "User.csv"), []byte("id\n1\n"), 0600); err != nil {
		t.Fatalf("write csv %s: %v", scenarioPath, err)
	}
	if err := scenario.WriteRevisionManifest(revDir, scenario.RevisionManifest{
		Scenario:          scenarioPath,
		Revision:          "r001",
		SchemaFingerprint: schemaFP,
		CreatedAt:         now,
		Source:            "export",
		Tables:            []string{"User"},
		Services:          []string{"postgres"},
		RowCounts:         map[string]int{"User": 1},
	}); err != nil {
		t.Fatalf("write revision manifest %s: %v", scenarioPath, err)
	}
}
