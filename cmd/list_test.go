package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KazanKK/seedmancer/internal/scenario"
)

func TestOutputJSON(t *testing.T) {
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	payload := struct {
		Scenarios []listEntry `json:"scenarios"`
	}{
		Scenarios: []listEntry{{Scenario: "billing/pro", Latest: "r001", Schema: "abcd"}},
	}
	err = outputJSON(payload)

	w.Close()
	os.Stdout = orig
	if err != nil {
		t.Fatalf("outputJSON: %v", err)
	}

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)

	var decoded struct {
		Scenarios []listEntry `json:"scenarios"`
	}
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(decoded.Scenarios) != 1 || decoded.Scenarios[0].Scenario != "billing/pro" {
		t.Fatalf("unexpected payload: %+v", decoded)
	}
}

// TestListLocalEntries_scenarioLayout exercises the scenario walk: build a
// fake .seedmancer/scenarios/<path>/ tree and ensure the entry comes back
// with the right pointers.
func TestListLocalEntries_scenarioLayout(t *testing.T) {
	dir := t.TempDir()
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Setenv("HOME", dir)

	writeFile(t, filepath.Join(dir, "seedmancer.yaml"), "storage_path: .seedmancer\n")

	scenarioPath := "billing/pro"
	scDir := scenario.ScenarioDir(dir, ".seedmancer", scenarioPath)
	if err := os.MkdirAll(scDir, 0755); err != nil {
		t.Fatalf("mkdir scenario: %v", err)
	}
	now := time.Now().UTC()
	if err := scenario.WriteManifest(scDir, scenario.Manifest{
		Scenario:  scenarioPath,
		CreatedAt: now,
		UpdatedAt: now,
		Latest:    "r001",
	}); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	revDir := scenario.RevisionDir(dir, ".seedmancer", scenarioPath, "r001")
	dataDir := filepath.Join(revDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("mkdir rev: %v", err)
	}
	if err := scenario.WriteRevisionManifest(revDir, scenario.RevisionManifest{
		Scenario:          scenarioPath,
		Revision:          "r001",
		SchemaFingerprint: "deadbeefcafebabe",
		CreatedAt:         now,
		Source:            "export",
		Tables:            []string{"User"},
		Services:          []string{"postgres"},
		RowCounts:         map[string]int{"User": 1},
	}); err != nil {
		t.Fatalf("write revision manifest: %v", err)
	}

	entries, err := listLocalEntries()
	if err != nil {
		t.Fatalf("listLocalEntries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d (%+v)", len(entries), entries)
	}
	if entries[0].Scenario != scenarioPath {
		t.Errorf("Scenario = %q, want %q", entries[0].Scenario, scenarioPath)
	}
	if entries[0].Latest != "r001" {
		t.Errorf("Latest = %q, want %q", entries[0].Latest, "r001")
	}
}
