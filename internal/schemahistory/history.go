// Package schemahistory tracks the sequence of schema fingerprints seen in a
// project. It records when each schema was first detected, its version number,
// and a brief diff summary from the previous schema so that commands can show
// how many schema versions behind a scenario revision is.
package schemahistory

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/KazanKK/seedmancer/internal/schemadiff"
)

// SchemaDiffSummary is a compact count-based summary of structural changes
// between two schema versions. It is stored in history.json so the CLI can
// display drift severity without re-running a full diff.
type SchemaDiffSummary struct {
	AddedTables    int `json:"addedTables"`
	RemovedTables  int `json:"removedTables"`
	AddedColumns   int `json:"addedColumns"`
	RemovedColumns int `json:"removedColumns"`
	ChangedColumns int `json:"changedColumns"`
}

// String returns a compact human-readable summary such as "+1 tbl +3 cols -1 col".
// Returns "-" when there are no changes.
func (d SchemaDiffSummary) String() string {
	if d.AddedTables == 0 && d.RemovedTables == 0 &&
		d.AddedColumns == 0 && d.RemovedColumns == 0 &&
		d.ChangedColumns == 0 {
		return "-"
	}
	var parts []string
	if d.AddedTables > 0 {
		parts = append(parts, fmt.Sprintf("+%d tbl", d.AddedTables))
	}
	if d.RemovedTables > 0 {
		parts = append(parts, fmt.Sprintf("-%d tbl", d.RemovedTables))
	}
	if d.AddedColumns > 0 {
		parts = append(parts, fmt.Sprintf("+%d col", d.AddedColumns))
	}
	if d.RemovedColumns > 0 {
		parts = append(parts, fmt.Sprintf("-%d col", d.RemovedColumns))
	}
	if d.ChangedColumns > 0 {
		parts = append(parts, fmt.Sprintf("~%d col", d.ChangedColumns))
	}
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += " "
		}
		out += p
	}
	return out
}

// SchemaHistoryEntry records metadata about one schema fingerprint.
type SchemaHistoryEntry struct {
	Fingerprint         string             `json:"fingerprint"`
	Version             int                `json:"version"`
	FirstDetectedAt     time.Time          `json:"firstDetectedAt"`
	LastSeenAt          time.Time          `json:"lastSeenAt"`
	PreviousFingerprint string             `json:"previousFingerprint,omitempty"`
	DiffFromPrevious    *SchemaDiffSummary `json:"diffFromPrevious,omitempty"`
}

// SchemaHistory is the contents of history.json. Current is the full
// fingerprint of the most recently seen schema.
type SchemaHistory struct {
	Current string               `json:"current"`
	Schemas []SchemaHistoryEntry `json:"schemas"`
}

// LoadSchemaHistory reads history.json from path. If the file does not exist
// an empty history is returned without error so callers can always start fresh.
func LoadSchemaHistory(path string) (*SchemaHistory, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &SchemaHistory{}, nil
		}
		return nil, fmt.Errorf("reading schema history: %w", err)
	}
	var h SchemaHistory
	if err := json.Unmarshal(data, &h); err != nil {
		return nil, fmt.Errorf("parsing schema history: %w", err)
	}
	return &h, nil
}

// SaveSchemaHistory writes h to path atomically (write .tmp then rename).
// The parent directory is created if it does not exist.
func SaveSchemaHistory(path string, h *SchemaHistory) error {
	data, err := json.MarshalIndent(h, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling schema history: %w", err)
	}
	data = append(data, '\n')

	if err := os.MkdirAll(dirOf(path), 0755); err != nil {
		return fmt.Errorf("creating schema history dir: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("writing schema history tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("renaming schema history: %w", err)
	}
	return nil
}

// UpdateSchemaHistory records fingerprint in history.json at historyPath.
//
// schemaJSONPath is a function that returns the on-disk path of a schema.json
// given its 12-character short fingerprint — used to compute DiffFromPrevious
// when the previous schema JSON is available.
//
// If fingerprint already exists in the history, only LastSeenAt and Current
// are updated. If it is new, a new entry is appended with the next version
// number and a diff summary (if both schema JSON files can be read).
//
// The function is intentionally lenient: diff computation failures are silently
// skipped so that a missing previous schema.json does not block the update.
func UpdateSchemaHistory(
	historyPath string,
	fingerprint string,
	schemaJSONPath func(fpShort string) string,
	now time.Time,
) (*SchemaHistoryEntry, error) {
	h, err := LoadSchemaHistory(historyPath)
	if err != nil {
		return nil, err
	}

	// Check whether fingerprint already exists.
	for i := range h.Schemas {
		if h.Schemas[i].Fingerprint == fingerprint {
			h.Schemas[i].LastSeenAt = now
			h.Current = fingerprint
			if err := SaveSchemaHistory(historyPath, h); err != nil {
				return nil, err
			}
			entry := h.Schemas[i]
			return &entry, nil
		}
	}

	// New fingerprint — assign next version.
	maxVersion := 0
	for _, e := range h.Schemas {
		if e.Version > maxVersion {
			maxVersion = e.Version
		}
	}

	entry := SchemaHistoryEntry{
		Fingerprint:     fingerprint,
		Version:         maxVersion + 1,
		FirstDetectedAt: now,
		LastSeenAt:      now,
	}

	// Record previous fingerprint and compute diff if possible.
	prevFP := h.Current
	if prevFP != "" && prevFP != fingerprint {
		entry.PreviousFingerprint = prevFP
		if schemaJSONPath != nil {
			prevShort := shortFP(prevFP)
			currShort := shortFP(fingerprint)
			oldJSON, oldErr := os.ReadFile(schemaJSONPath(prevShort))
			newJSON, newErr := os.ReadFile(schemaJSONPath(currShort))
			if oldErr == nil && newErr == nil {
				if diff, err := SummarizeSchemaDiff(oldJSON, newJSON); err == nil {
					entry.DiffFromPrevious = &diff
				}
			}
		}
	}

	h.Schemas = append(h.Schemas, entry)
	h.Current = fingerprint

	if err := SaveSchemaHistory(historyPath, h); err != nil {
		return nil, err
	}
	return &entry, nil
}

// VersionsBehind returns how many schema versions fromFP is behind toFP.
// Both fingerprints must be full (not short) fingerprints present in the
// history. Returns (0, false) if either fingerprint is not found or if the
// result would be negative (fromFP is newer than toFP).
func VersionsBehind(h *SchemaHistory, fromFP, toFP string) (int, bool) {
	if fromFP == toFP {
		var found bool
		for _, e := range h.Schemas {
			if e.Fingerprint == fromFP {
				found = true
				break
			}
		}
		if !found {
			return 0, false
		}
		return 0, true
	}

	fromVersion := -1
	toVersion := -1
	for _, e := range h.Schemas {
		if e.Fingerprint == fromFP {
			fromVersion = e.Version
		}
		if e.Fingerprint == toFP {
			toVersion = e.Version
		}
	}
	if fromVersion < 0 || toVersion < 0 {
		return 0, false
	}
	diff := toVersion - fromVersion
	if diff < 0 {
		return 0, false
	}
	return diff, true
}

// SummarizeSchemaDiff runs schemadiff.Diff on the two schema JSON blobs and
// returns a count-based summary of the changes.
func SummarizeSchemaDiff(oldJSON, newJSON []byte) (SchemaDiffSummary, error) {
	changes, err := schemadiff.Diff(oldJSON, newJSON)
	if err != nil {
		return SchemaDiffSummary{}, err
	}
	var s SchemaDiffSummary
	for _, c := range changes {
		switch c.Kind {
		case schemadiff.TableAdded:
			s.AddedTables++
		case schemadiff.TableRemoved:
			s.RemovedTables++
		case schemadiff.ColumnAdded:
			s.AddedColumns++
		case schemadiff.ColumnRemoved:
			s.RemovedColumns++
		case schemadiff.ColumnChanged:
			s.ChangedColumns++
		}
	}
	return s, nil
}

// shortFP returns the first 12 characters of a fingerprint, matching
// utils.FingerprintShortLen without importing the utils package.
func shortFP(fp string) string {
	const shortLen = 12
	if len(fp) <= shortLen {
		return fp
	}
	return fp[:shortLen]
}

// dirOf returns the directory component of a file path.
func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			return path[:i]
		}
	}
	return "."
}
