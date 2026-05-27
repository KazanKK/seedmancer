package csvxform_test

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/KazanKK/seedmancer/internal/csvxform"
	"github.com/KazanKK/seedmancer/internal/refreshplan"
)

func TestApply_dropColumn(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeCSV(t, filepath.Join(srcDir, "users.csv"), [][]string{
		{"id", "email", "fullName"},
		{"1", "a@b.com", "Alice"},
	})

	plan := refreshplan.Plan{
		Operations: []refreshplan.Operation{
			{Op: refreshplan.OpDropColumn, Table: "users", Column: "fullName"},
		},
	}

	newSchema := makeNewSchema("users", []string{"id", "email"})
	if err := csvxform.Apply(plan, srcDir, dstDir, newSchema); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	rows := readCSV(t, filepath.Join(dstDir, "users.csv"))
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (header+data), got %d", len(rows))
	}
	if len(rows[0]) != 2 {
		t.Fatalf("expected 2 columns after drop, got %v", rows[0])
	}
	for _, col := range rows[0] {
		if col == "fullName" {
			t.Error("fullName should have been dropped")
		}
	}
}

func TestApply_addColumn_constant(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeCSV(t, filepath.Join(srcDir, "users.csv"), [][]string{
		{"id", "email"},
		{"1", "a@b.com"},
	})

	plan := refreshplan.Plan{
		Operations: []refreshplan.Operation{
			{
				Op:       refreshplan.OpAddColumn,
				Table:    "users",
				Column:   "status",
				Strategy: refreshplan.StrategyConstant,
				Value:    refreshplan.StringValue("active"),
			},
		},
	}

	newSchema := makeNewSchema("users", []string{"id", "email", "status"})
	if err := csvxform.Apply(plan, srcDir, dstDir, newSchema); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	rows := readCSV(t, filepath.Join(dstDir, "users.csv"))
	if rows[1][2] != "active" {
		t.Errorf("expected status=active, got %q", rows[1][2])
	}
}

func TestApply_renameColumn(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeCSV(t, filepath.Join(srcDir, "users.csv"), [][]string{
		{"id", "fullName"},
		{"1", "Alice"},
	})

	plan := refreshplan.Plan{
		Operations: []refreshplan.Operation{
			{Op: refreshplan.OpRenameColumn, Table: "users", Column: "name", FromColumn: "fullName"},
		},
	}

	newSchema := makeNewSchema("users", []string{"id", "name"})
	if err := csvxform.Apply(plan, srcDir, dstDir, newSchema); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	rows := readCSV(t, filepath.Join(dstDir, "users.csv"))
	if rows[0][1] != "name" {
		t.Errorf("expected renamed header 'name', got %q", rows[0][1])
	}
	if rows[1][1] != "Alice" {
		t.Errorf("expected value Alice preserved, got %q", rows[1][1])
	}
}

func TestApply_createRow(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeCSV(t, filepath.Join(srcDir, "roles.csv"), [][]string{
		{"id", "name"},
	})

	plan := refreshplan.Plan{
		Operations: []refreshplan.Operation{
			{
				Op:    refreshplan.OpCreateRow,
				Table: "roles",
				Values: map[string]json.RawMessage{
					"id":   json.RawMessage(`"role_user"`),
					"name": json.RawMessage(`"user"`),
				},
			},
		},
	}

	newSchema := makeNewSchema("roles", []string{"id", "name"})
	if err := csvxform.Apply(plan, srcDir, dstDir, newSchema); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	rows := readCSV(t, filepath.Join(dstDir, "roles.csv"))
	if len(rows) != 2 { // header + 1 new row
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
	if rows[1][0] != "role_user" || rows[1][1] != "user" {
		t.Errorf("unexpected row: %v", rows[1])
	}
}

func TestApply_generateUUID(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeCSV(t, filepath.Join(srcDir, "items.csv"), [][]string{
		{"name"},
		{"Widget"},
	})

	plan := refreshplan.Plan{
		Operations: []refreshplan.Operation{
			{Op: refreshplan.OpGenerateUUID, Table: "items", Column: "id"},
		},
	}

	newSchema := makeNewSchema("items", []string{"id", "name"})
	if err := csvxform.Apply(plan, srcDir, dstDir, newSchema); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	rows := readCSV(t, filepath.Join(dstDir, "items.csv"))
	if len(rows) < 2 {
		t.Fatal("expected at least header + 1 data row")
	}
	// UUID should be non-empty
	if rows[1][0] == "" {
		t.Error("expected generated UUID, got empty string")
	}
}

func TestApply_noOps_copiesUnchanged(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	writeCSV(t, filepath.Join(srcDir, "orders.csv"), [][]string{
		{"id", "total"},
		{"1", "99.99"},
	})

	plan := refreshplan.Plan{Operations: nil}
	newSchema := makeNewSchema("orders", []string{"id", "total"})
	if err := csvxform.Apply(plan, srcDir, dstDir, newSchema); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	rows := readCSV(t, filepath.Join(dstDir, "orders.csv"))
	if rows[1][1] != "99.99" {
		t.Errorf("expected total=99.99 preserved, got %q", rows[1][1])
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func writeCSV(t *testing.T, path string, rows [][]string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	for _, row := range rows {
		if err := w.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
}

func readCSV(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func makeNewSchema(table string, cols []string) []byte {
	type colShape struct {
		Name string `json:"name"`
	}
	type tableShape struct {
		Name    string     `json:"name"`
		Columns []colShape `json:"columns"`
	}
	var cs []colShape
	for _, c := range cols {
		cs = append(cs, colShape{Name: c})
	}
	type schema struct {
		Tables []tableShape `json:"tables"`
	}
	out, _ := json.Marshal(schema{Tables: []tableShape{{Name: table, Columns: cs}}})
	return out
}
