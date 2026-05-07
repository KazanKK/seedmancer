package scenario

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Manifest is the top-level scenario metadata stored at
// <scenarioDir>/manifest.json. Pointers live in pointers.json so writes
// to either file don't have to coordinate.
type Manifest struct {
	Scenario       string    `json:"scenario"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
	LatestRevision string    `json:"latestRevision,omitempty"`
	StableRevision string    `json:"stableRevision,omitempty"`
}

// RevisionManifest is the per-revision metadata stored at
// <revisionDir>/manifest.json. Every export creates a new revision with
// its own immutable manifest.
type RevisionManifest struct {
	Scenario          string         `json:"scenario"`
	Revision          string         `json:"revision"`
	SchemaFingerprint string         `json:"schemaFingerprint"`
	CreatedAt         time.Time      `json:"createdAt"`
	Source            string         `json:"source"`
	Tables            []string       `json:"tables"`
	Services          []string       `json:"services"`
	RowCounts         map[string]int `json:"rowCounts"`
	Description       string         `json:"description,omitempty"`
}

// manifestName / revisionManifestName / pointersName are kept private so
// callers always go through the typed helpers and we can change the
// filenames in one place if needed.
const (
	manifestName         = "manifest.json"
	revisionManifestName = "manifest.json"
)

// ScenarioManifestPath returns the on-disk path of a scenario manifest.
func ScenarioManifestPath(scenarioDir string) string {
	return filepath.Join(scenarioDir, manifestName)
}

// RevisionManifestPath returns the on-disk path of a revision manifest.
func RevisionManifestPath(revisionDir string) string {
	return filepath.Join(revisionDir, revisionManifestName)
}

// ReadManifest loads a scenario manifest. Missing file returns
// (zero-value, os.IsNotExist-true error) so callers can distinguish
// "scenario doesn't exist yet" from "manifest is corrupt".
func ReadManifest(scenarioDir string) (Manifest, error) {
	data, err := os.ReadFile(ScenarioManifestPath(scenarioDir))
	if err != nil {
		return Manifest{}, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("parsing %s: %w", ScenarioManifestPath(scenarioDir), err)
	}
	return m, nil
}

// WriteManifest writes the scenario manifest atomically (tmp file + rename).
func WriteManifest(scenarioDir string, m Manifest) error {
	if err := os.MkdirAll(scenarioDir, 0755); err != nil {
		return fmt.Errorf("creating %s: %w", scenarioDir, err)
	}
	return writeJSONAtomic(ScenarioManifestPath(scenarioDir), m)
}

// ReadRevisionManifest loads a revision manifest.
func ReadRevisionManifest(revisionDir string) (RevisionManifest, error) {
	data, err := os.ReadFile(RevisionManifestPath(revisionDir))
	if err != nil {
		return RevisionManifest{}, err
	}
	var m RevisionManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return RevisionManifest{}, fmt.Errorf("parsing %s: %w", RevisionManifestPath(revisionDir), err)
	}
	return m, nil
}

// WriteRevisionManifest writes the revision manifest atomically.
func WriteRevisionManifest(revisionDir string, m RevisionManifest) error {
	if err := os.MkdirAll(revisionDir, 0755); err != nil {
		return fmt.Errorf("creating %s: %w", revisionDir, err)
	}
	return writeJSONAtomic(RevisionManifestPath(revisionDir), m)
}

// writeJSONAtomic marshals v with indented JSON and writes it via a
// temp file in the same directory plus os.Rename. A crashed write
// therefore never leaves a half-written manifest on disk.
func writeJSONAtomic(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling %s: %w", path, err)
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("writing %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("renaming %s -> %s: %w", tmp, path, err)
	}
	return nil
}
