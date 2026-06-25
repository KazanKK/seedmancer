// Package envmarker resolves @env:KEY markers embedded in CSV cell values
// during seeding. Markers allow a single dataset revision to be portable across
// environments (e.g. local vs staging) where certain IDs differ.
//
// Marker syntax:
//
//	@env:KEY_NAME
//
// Rules:
//   - Must be the entire cell value (full-cell replacement only).
//   - KEY_NAME: uppercase letters, digits, and underscores only.
//   - Partial interpolation (e.g. "user-@env:ID") is not supported in this version.
//
// Lookup order for each key:
//  1. values map (from seedmancer.yaml environments.<env>.values)
//  2. os.Getenv(KEY) — fallback for CI / secrets not committed to yaml
//  3. Hard error if neither is set.
package envmarker

import (
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// markerRe matches a full-cell @env: marker. Capture group 1 is the key name.
var markerRe = regexp.MustCompile(`^@env:([A-Z0-9_]+)$`)

// EnvironmentValues maps key names to their resolved string values.
// Comes from EnvConfig.Values in seedmancer.yaml.
type EnvironmentValues = map[string]string

// ResolvedMarker records metadata about a single marker that was replaced.
type ResolvedMarker struct {
	Key    string
	Value  string
	File   string
	Row    int
	Column string
}

// IsMarker reports whether s is a syntactically valid @env: marker.
func IsMarker(s string) bool {
	return markerRe.MatchString(s)
}

// ResolveValue resolves a single CSV cell value.
//
//   - If s is not a marker, returns (s, false, nil).
//   - If s is a marker and the key is found (yaml values or OS env), returns (resolved, true, nil).
//   - If s is a marker but the key is missing from both sources, returns ("", false, err).
//
// envName, file, row, and col are used only for error message context.
func ResolveValue(s string, values EnvironmentValues, envName, file string, row int, col string) (string, bool, error) {
	m := markerRe.FindStringSubmatch(s)
	if m == nil {
		return s, false, nil
	}
	key := m[1]

	if v, ok := values[key]; ok {
		return v, true, nil
	}
	if v := os.Getenv(key); v != "" {
		return v, true, nil
	}

	return "", false, missingValueError(key, envName, s, file, col)
}

// ResolveRecords resolves all @env: markers in a parsed CSV record set.
// row 0 is assumed to be the header and is never modified.
// Returns a new record slice (originals unchanged) and metadata for each replacement.
func ResolveRecords(records [][]string, values EnvironmentValues, envName, file string) ([][]string, []ResolvedMarker, error) {
	if len(records) == 0 {
		return records, nil, nil
	}
	header := records[0]
	out := make([][]string, len(records))
	out[0] = header

	var resolved []ResolvedMarker
	for i, row := range records[1:] {
		newRow := make([]string, len(row))
		for j, cell := range row {
			colName := ""
			if j < len(header) {
				colName = header[j]
			}
			v, replaced, err := ResolveValue(cell, values, envName, file, i+1, colName)
			if err != nil {
				return nil, nil, err
			}
			newRow[j] = v
			if replaced {
				resolved = append(resolved, ResolvedMarker{
					Key:    markerRe.FindStringSubmatch(cell)[1],
					Value:  v,
					File:   file,
					Row:    i + 1,
					Column: colName,
				})
			}
		}
		out[i+1] = newRow
	}
	return out, resolved, nil
}

// HasAnyMarker reports whether any cell in records (including header) contains
// a valid @env: marker.
func HasAnyMarker(records [][]string) bool {
	for _, row := range records {
		for _, cell := range row {
			if IsMarker(cell) {
				return true
			}
		}
	}
	return false
}

// HasAnyMarkerInFile opens a CSV file and returns true if any cell contains
// a valid @env: marker. It does not resolve any values. The file is closed
// before return.
func HasAnyMarkerInFile(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return false, fmt.Errorf("reading %s: %w", path, err)
	}
	return HasAnyMarker(records), nil
}

// ResolveCSVFile reads a CSV file, resolves all @env: markers, and returns the
// resolved records. The original file is never modified.
func ResolveCSVFile(path string, values EnvironmentValues, envName string) ([][]string, []ResolvedMarker, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("reading %s: %w", path, err)
	}
	return ResolveRecords(records, values, envName, path)
}

// WriteCSV writes records to path in standard CSV format.
func WriteCSV(path string, records [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.WriteAll(records); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

func missingValueError(key, envName, marker, file, col string) error {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Missing environment value: %s\n", key)
	if envName != "" {
		fmt.Fprintf(&sb, "\nEnvironment: %s", envName)
	}
	fmt.Fprintf(&sb, "\nMarker:      %s", marker)
	if file != "" {
		fmt.Fprintf(&sb, "\nFile:        %s", file)
	}
	if col != "" {
		fmt.Fprintf(&sb, "\nColumn:      %s", col)
	}
	sb.WriteString("\n\nAdd it to your seedmancer config:\n\n")
	if envName != "" && envName != "(ad-hoc)" {
		fmt.Fprintf(&sb, "  environments:\n    %s:\n      values:\n        %s: \"...\"\n", envName, key)
	} else {
		fmt.Fprintf(&sb, "  environments:\n    <env>:\n      values:\n        %s: \"...\"\n", key)
	}
	fmt.Fprintf(&sb, "\nOr export it as an environment variable before running seed:\n\n  export %s=\"...\"\n", key)
	return fmt.Errorf("%s", sb.String())
}
