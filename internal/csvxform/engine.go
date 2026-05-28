// Package csvxform applies a refresh plan to old CSV files, producing new
// CSVs that conform to the current database schema. All writes go to a
// temp directory first; the caller commits atomically by renaming.
package csvxform

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/KazanKK/seedmancer/internal/refreshplan"
)

// TableOps groups the operations that apply to one table, in plan order.
type TableOps struct {
	TableName string
	Ops       []refreshplan.Operation
}

// GroupByTable splits a plan's operations into per-table buckets, preserving
// original order within each table.
func GroupByTable(ops []refreshplan.Operation) []TableOps {
	seen := map[string]int{} // table -> index in result
	var result []TableOps
	for _, op := range ops {
		if op.Table == "" {
			continue
		}
		if idx, ok := seen[op.Table]; ok {
			result[idx].Ops = append(result[idx].Ops, op)
		} else {
			seen[op.Table] = len(result)
			result = append(result, TableOps{TableName: op.Table, Ops: []refreshplan.Operation{op}})
		}
	}
	return result
}

// Apply transforms every table in srcDir that has operations in the plan,
// writing results to dstDir. Tables with no operations are copied unchanged.
// srcDir and dstDir must already exist. The function is not atomic — the
// caller must wrap src/dst in a temp-dir commit pattern.
func Apply(plan refreshplan.Plan, srcDir, dstDir string, newSchemaJSON []byte) error {
	tableOpsMap := map[string][]refreshplan.Operation{}
	for _, tg := range GroupByTable(plan.Operations) {
		tableOpsMap[tg.TableName] = tg.Ops
	}

	newCols := parseNewSchemaCols(newSchemaJSON)
	schemaDefaults := parseSchemaDefaults(newSchemaJSON)

	// Walk all CSVs in srcDir.
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return fmt.Errorf("reading source dir: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".csv") {
			// Copy non-CSV files (schema sidecars, dataset.sql, etc.) unchanged.
			if !e.IsDir() {
				src := filepath.Join(srcDir, e.Name())
				dst := filepath.Join(dstDir, e.Name())
				if err := copyFile(src, dst); err != nil {
					return fmt.Errorf("copying %s: %w", e.Name(), err)
				}
			}
			continue
		}

		tableName := strings.TrimSuffix(e.Name(), ".csv")
		srcPath := filepath.Join(srcDir, e.Name())
		dstPath := filepath.Join(dstDir, e.Name())

		ops := tableOpsMap[tableName]
		colOrder := newCols[tableName]
		colDefaults := schemaDefaults[tableName]

		if err := transformCSV(srcPath, dstPath, tableName, ops, colOrder, colDefaults); err != nil {
			return fmt.Errorf("transforming %s: %w", tableName, err)
		}
	}

	// Some ops create entirely new rows in tables that may not have a CSV yet
	// (e.g. create_row for a parent table). Handle those tables.
	for tableName, ops := range tableOpsMap {
		srcPath := filepath.Join(srcDir, tableName+".csv")
		if _, err := os.Stat(srcPath); err == nil {
			continue // already handled above
		}
		colOrder := newCols[tableName]
		colDefaults := schemaDefaults[tableName]
		dstPath := filepath.Join(dstDir, tableName+".csv")
		if err := transformCSV("", dstPath, tableName, ops, colOrder, colDefaults); err != nil {
			return fmt.Errorf("creating %s: %w", tableName, err)
		}
	}

	return nil
}

// transformCSV reads srcPath (empty string = create new file), applies ops
// row-by-row, and writes the result to dstPath. colOrder controls the output
// header; if nil the existing header order is preserved. colDefaults maps
// column name to its DB default string, used by strategy=default.
func transformCSV(srcPath, dstPath, tableName string, ops []refreshplan.Operation, colOrder []string, colDefaults map[string]string) error {
	// Separate ops by kind for efficient dispatch.
	var (
		drops      = map[string]bool{}
		renames    = map[string]string{} // old -> new
		addCols    = map[string]refreshplan.Operation{}
		setCols    = map[string]refreshplan.Operation{}
		copyCols   = map[string]refreshplan.Operation{}
		createRows []map[string]string
	)
	for _, op := range ops {
		switch op.Op {
		case refreshplan.OpDropColumn:
			drops[op.Column] = true
		case refreshplan.OpRenameColumn:
			renames[op.FromColumn] = op.Column
		case refreshplan.OpAddColumn, refreshplan.OpGenerateUUID, refreshplan.OpGenerateTimestamp:
			addCols[op.Column] = op
		case refreshplan.OpSetConstant:
			setCols[op.Column] = op
		case refreshplan.OpCopyColumn:
			copyCols[op.Column] = op
		case refreshplan.OpFillForeignKey:
			setCols[op.Column] = op // fill_foreign_key works like set_constant if RefValue is provided
		case refreshplan.OpCreateRow:
			row := map[string]string{}
			for k, v := range op.Values {
				row[k] = rawToString(v)
			}
			createRows = append(createRows, row)
		}
	}

	// Open source CSV (or start with empty header if srcPath == "").
	var srcHeader []string
	var srcRows [][]string

	if srcPath != "" {
		f, err := os.Open(srcPath)
		if err != nil {
			return fmt.Errorf("opening source: %w", err)
		}
		defer f.Close()
		r := csv.NewReader(bufio.NewReader(f))
		r.FieldsPerRecord = -1
		r.LazyQuotes = true
		header, err := r.Read()
		if err != nil && err != io.EOF {
			return fmt.Errorf("reading header: %w", err)
		}
		srcHeader = header
		for {
			rec, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("reading row: %w", err)
			}
			srcRows = append(srcRows, rec)
		}
	}

	// Apply renames to source header.
	renamedHeader := make([]string, len(srcHeader))
	for i, col := range srcHeader {
		if newName, ok := renames[col]; ok {
			renamedHeader[i] = newName
		} else {
			renamedHeader[i] = col
		}
	}

	// Determine output columns.
	var outHeader []string
	if len(colOrder) > 0 {
		outHeader = colOrder
	} else {
		// Use renamed header, drop removed, add new.
		for _, col := range renamedHeader {
			if !drops[col] {
				outHeader = append(outHeader, col)
			}
		}
		for col := range addCols {
			if !contains(outHeader, col) {
				outHeader = append(outHeader, col)
			}
		}
	}

	// Write output CSV.
	dst, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("creating dest: %w", err)
	}
	defer dst.Close()

	w := csv.NewWriter(bufio.NewWriter(dst))
	defer w.Flush()

	if err := w.Write(outHeader); err != nil {
		return fmt.Errorf("writing header: %w", err)
	}

	// Process existing rows.
	for _, srcRow := range srcRows {
		// Build a map from renamed column name -> value.
		rowMap := map[string]string{}
		for i, col := range renamedHeader {
			if i < len(srcRow) {
				rowMap[col] = srcRow[i]
			}
		}

		// Apply set_constant / copy / add ops.
		for col, op := range setCols {
			rowMap[col] = op.ValueString()
		}
		for dstCol, op := range copyCols {
			rowMap[dstCol] = rowMap[op.FromColumn]
		}
		for col, op := range addCols {
			if _, exists := rowMap[col]; !exists {
				rowMap[col] = generateValue(op, colDefaults)
			}
		}

		out := rowToSlice(rowMap, outHeader, drops)
		if err := w.Write(out); err != nil {
			return fmt.Errorf("writing row: %w", err)
		}
	}

	// Append create_row rows.
	for _, rowMap := range createRows {
		out := rowToSlice(rowMap, outHeader, drops)
		if err := w.Write(out); err != nil {
			return fmt.Errorf("writing create_row: %w", err)
		}
	}

	return nil
}

func generateValue(op refreshplan.Operation, colDefaults map[string]string) string {
	switch op.Op {
	case refreshplan.OpGenerateUUID:
		return newUUID()
	case refreshplan.OpGenerateTimestamp:
		return time.Now().UTC().Format(time.RFC3339)
	}
	switch op.Strategy {
	case refreshplan.StrategyUUID:
		return newUUID()
	case refreshplan.StrategyTimestamp:
		return time.Now().UTC().Format(time.RFC3339)
	case refreshplan.StrategyDefault:
		if colDefaults != nil {
			if def, ok := colDefaults[op.Column]; ok {
				return def
			}
		}
		return ""
	case refreshplan.StrategyEmpty:
		return ""
	case refreshplan.StrategyConstant:
		return op.ValueString()
	}
	return op.ValueString()
}

func rowToSlice(rowMap map[string]string, outHeader []string, drops map[string]bool) []string {
	out := make([]string, 0, len(outHeader))
	for _, col := range outHeader {
		if drops[col] {
			continue
		}
		val, ok := rowMap[col]
		if !ok {
			val = ""
		}
		if val == "NULL" {
			val = ""
		}
		out = append(out, val)
	}
	return out
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func rawToString(v json.RawMessage) string {
	if len(v) == 0 || string(v) == "null" {
		return ""
	}
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		return s
	}
	return string(v)
}

// parseSchemaDefaults returns a map of table -> column -> DB default string.
// The default is the raw DB default value stringified (e.g. "" for DEFAULT '',
// "0" for DEFAULT 0). Missing or null defaults produce no entry in the map.
func parseSchemaDefaults(schemaJSON []byte) map[string]map[string]string {
	result := map[string]map[string]string{}
	if len(schemaJSON) == 0 {
		return result
	}
	var raw struct {
		Tables []struct {
			Name    string `json:"name"`
			Columns []struct {
				Name    string          `json:"name"`
				Default json.RawMessage `json:"default"`
			} `json:"columns"`
		} `json:"tables"`
	}
	if err := json.Unmarshal(schemaJSON, &raw); err != nil {
		return result
	}
	for _, t := range raw.Tables {
		colDefaults := map[string]string{}
		for _, c := range t.Columns {
			if len(c.Default) == 0 || string(c.Default) == "null" {
				continue
			}
			// Unwrap JSON string; keep numbers/booleans as their literal text.
			var s string
			if err := json.Unmarshal(c.Default, &s); err == nil {
				colDefaults[c.Name] = s
			} else {
				colDefaults[c.Name] = string(c.Default)
			}
		}
		if len(colDefaults) > 0 {
			result[t.Name] = colDefaults
		}
	}
	return result
}

// parseNewSchemaCols returns a map of table -> ordered column names from schema.json.
// Generated columns are excluded (they don't appear in CSVs).
func parseNewSchemaCols(schemaJSON []byte) map[string][]string {	result := map[string][]string{}
	if len(schemaJSON) == 0 {
		return result
	}
	var raw struct {
		Tables []struct {
			Name    string `json:"name"`
			Columns []struct {
				Name        string `json:"name"`
				IsGenerated *bool  `json:"isGenerated"`
			} `json:"columns"`
		} `json:"tables"`
	}
	if err := json.Unmarshal(schemaJSON, &raw); err != nil {
		return result
	}
	for _, t := range raw.Tables {
		var cols []string
		for _, c := range t.Columns {
			if c.IsGenerated != nil && *c.IsGenerated {
				continue
			}
			cols = append(cols, c.Name)
		}
		result[t.Name] = cols
	}
	return result
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

// newUUID generates a simple random UUID (v4) without external dependencies.
func newUUID() string {
	f, err := os.Open("/dev/urandom")
	if err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	defer f.Close()
	b := make([]byte, 16)
	if _, err := io.ReadFull(f, b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
