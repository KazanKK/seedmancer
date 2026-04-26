package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// This file holds the helpers used by RunGenerateLocal's `inherit` flow:
// copying base CSVs into the new dataset, detecting which tables the script
// overwrote, walking the schema's foreign-key graph, and reducing descendant
// CSVs to header-only so the dataset is safe to seed without orphan FKs.
//
// copyFile is reused from export.go.

// fkChildIndex maps a parent table name to the list of child tables that
// reference it via at least one foreign key column.
type fkChildIndex map[string][]string

// buildFKChildIndex parses schema.json and returns a parent → children map.
// Errors are non-fatal: callers should treat an empty map as "no FK info".
func buildFKChildIndex(schemaJSONPath string) (fkChildIndex, error) {
	raw, err := os.ReadFile(schemaJSONPath)
	if err != nil {
		return nil, err
	}
	var parsed struct {
		Tables []generateTable `json:"tables"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	idx := fkChildIndex{}
	for _, t := range parsed.Tables {
		for _, c := range t.Columns {
			if c.ForeignKey == nil || c.ForeignKey.Table == "" {
				continue
			}
			parent := c.ForeignKey.Table
			child := t.Name
			if child == parent {
				// self-references aren't propagated; clearing the table itself
				// would defeat the purpose of inheritance.
				continue
			}
			idx[parent] = appendUnique(idx[parent], child)
		}
	}
	return idx, nil
}

func appendUnique(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}

// findFKDescendants returns every table reachable from `seeds` by following
// the parent → child edges in idx, transitively. The result includes the
// seed tables themselves so callers can decide what to do with each.
func findFKDescendants(idx fkChildIndex, seeds map[string]bool) map[string]bool {
	out := map[string]bool{}
	queue := make([]string, 0, len(seeds))
	for s := range seeds {
		out[s] = true
		queue = append(queue, s)
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, child := range idx[cur] {
			if !out[child] {
				out[child] = true
				queue = append(queue, child)
			}
		}
	}
	return out
}

// truncateCSVToHeader rewrites csvPath so it contains only its header row.
// If the file is empty or has only a header already, it's left as-is.
// Returns the table name (filename without .csv) and any error encountered.
func truncateCSVToHeader(csvPath string) error {
	in, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	r := csv.NewReader(in)
	header, err := r.Read()
	in.Close()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading %s: %w", filepath.Base(csvPath), err)
	}

	// Rewrite atomically via a temp file in the same directory.
	tmp := csvPath + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	w := csv.NewWriter(out)
	if err := w.Write(header); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, csvPath)
}

// sortedKeys returns the keys of a map[string]bool in sorted order so the
// output of RunGenerateLocal is deterministic.
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// trimCSVSuffix returns the table name for a given filename. Returns ""
// when the name doesn't end in .csv (case-insensitive).
func trimCSVSuffix(name string) string {
	if !strings.HasSuffix(strings.ToLower(name), ".csv") {
		return ""
	}
	return name[:len(name)-len(".csv")]
}
