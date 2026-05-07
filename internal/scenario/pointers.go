package scenario

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Pointers holds the named revision aliases for a scenario. `Latest`
// flips on every export; `Stable` only changes when the user explicitly
// runs `seedmancer pin`.
type Pointers struct {
	Latest string `json:"latest,omitempty"`
	Stable string `json:"stable,omitempty"`
}

const pointersName = "pointers.json"

// PointersPath returns the on-disk path for a scenario's pointers file.
func PointersPath(scenarioDir string) string {
	return filepath.Join(scenarioDir, pointersName)
}

// ReadPointers loads the pointers file. Missing file returns a
// zero-value Pointers and a nil error so callers don't have to special-
// case "scenario has no revisions yet".
func ReadPointers(scenarioDir string) (Pointers, error) {
	data, err := os.ReadFile(PointersPath(scenarioDir))
	if err != nil {
		if os.IsNotExist(err) {
			return Pointers{}, nil
		}
		return Pointers{}, err
	}
	var p Pointers
	if err := json.Unmarshal(data, &p); err != nil {
		return Pointers{}, fmt.Errorf("parsing %s: %w", PointersPath(scenarioDir), err)
	}
	return p, nil
}

// WritePointers writes the pointers file atomically.
func WritePointers(scenarioDir string, p Pointers) error {
	if err := os.MkdirAll(scenarioDir, 0755); err != nil {
		return fmt.Errorf("creating %s: %w", scenarioDir, err)
	}
	return writeJSONAtomic(PointersPath(scenarioDir), p)
}
