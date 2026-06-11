package driftreport_test

import (
	"encoding/json"
	"testing"

	"github.com/KazanKK/seedmancer/internal/driftreport"
	"github.com/KazanKK/seedmancer/internal/schemadiff"
)

func TestClassifyNoDrift(t *testing.T) {
	report := driftreport.Build("billing/pro", "r001", "fp1", "fp1", nil, []byte(`{}`), []byte(`{}`))
	if report.HasDrift {
		t.Error("expected no drift")
	}
	if !report.AutoResolvable {
		t.Error("expected AutoResolvable=true when no changes (zero decision/breaking)")
	}
}

func TestClassifyNullableColumnAdded(t *testing.T) {
	nullable := true
	newSchema := makeSchema("users", []colDef{{name: "id", typ: "integer"}, {name: "nickname", typ: "text", nullable: &nullable}})
	oldSchema := makeSchema("users", []colDef{{name: "id", typ: "integer"}})

	changes := []schemadiff.Change{{
		Kind:   schemadiff.ColumnAdded,
		Table:  "users",
		Column: "nickname",
	}}

	report := driftreport.Build("s", "r001", "fp1", "fp2", changes, oldSchema, newSchema)
	if !report.HasDrift {
		t.Fatal("expected drift")
	}
	if len(report.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(report.Changes))
	}
	ch := report.Changes[0]
	if ch.Category != driftreport.Auto {
		t.Errorf("expected Auto, got %s (reason: %s)", ch.Category, ch.AutoReason)
	}
	if report.AutoResolvable != true {
		t.Error("expected AutoResolvable=true")
	}
}

func TestClassifyRequiredColumnNoDef(t *testing.T) {
	noNull := false
	newSchema := makeSchema("users", []colDef{
		{name: "id", typ: "integer"},
		{name: "roleId", typ: "integer", nullable: &noNull},
	})
	oldSchema := makeSchema("users", []colDef{{name: "id", typ: "integer"}})

	changes := []schemadiff.Change{{
		Kind:   schemadiff.ColumnAdded,
		Table:  "users",
		Column: "roleId",
	}}
	report := driftreport.Build("s", "r001", "fp1", "fp2", changes, oldSchema, newSchema)
	if report.Changes[0].Category != driftreport.Decision {
		t.Errorf("expected Decision, got %s", report.Changes[0].Category)
	}
	if !report.NeedsDecision {
		t.Error("expected NeedsDecision=true")
	}
}

func TestClassifyColumnRemoved(t *testing.T) {
	changes := []schemadiff.Change{{
		Kind:   schemadiff.ColumnRemoved,
		Table:  "users",
		Column: "fullName",
	}}
	report := driftreport.Build("s", "r001", "fp1", "fp2", changes, []byte(`{}`), []byte(`{}`))
	if report.Changes[0].Category != driftreport.Auto {
		t.Errorf("expected Auto for column removal, got %s", report.Changes[0].Category)
	}
}

func TestClassifyFKAdded(t *testing.T) {
	changes := []schemadiff.Change{{
		Kind:   schemadiff.ForeignKeyAdded,
		Table:  "orders",
		Column: "userId",
		Detail: "-> users.id",
	}}
	report := driftreport.Build("s", "r001", "fp1", "fp2", changes, []byte(`{}`), []byte(`{}`))
	if report.Changes[0].Category != driftreport.Decision {
		t.Errorf("expected Decision for FK added, got %s", report.Changes[0].Category)
	}
}

func TestRenameHeuristic(t *testing.T) {
	// Simulates: fullName removed, name added (same type), similar names.
	oldSchema := makeSchema("users", []colDef{
		{name: "id", typ: "integer"},
		{name: "fullName", typ: "text"},
	})
	newSchema := makeSchema("users", []colDef{
		{name: "id", typ: "integer"},
		{name: "name", typ: "text"},
	})
	changes := []schemadiff.Change{
		{Kind: schemadiff.ColumnRemoved, Table: "users", Column: "fullName"},
		{Kind: schemadiff.ColumnAdded, Table: "users", Column: "name"},
	}
	report := driftreport.Build("s", "r001", "fp1", "fp2", changes, oldSchema, newSchema)
	var addedChange *driftreport.AnnotatedChange
	for i := range report.Changes {
		if report.Changes[i].Kind == schemadiff.ColumnAdded {
			addedChange = &report.Changes[i]
		}
	}
	if addedChange == nil {
		t.Fatal("no ColumnAdded change found")
	}
	if addedChange.Category != driftreport.Likely {
		t.Errorf("expected Likely (rename heuristic) for added column 'name', got %s (reason: %s)", addedChange.Category, addedChange.AutoReason)
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

type colDef struct {
	name     string
	typ      string
	nullable *bool
	hasFK    bool
	fkTable  string
	fkCol    string
}

func makeSchema(table string, cols []colDef) []byte {
	type fkShape struct {
		Table  string `json:"table"`
		Column string `json:"column"`
	}
	type colShape struct {
		Name       string   `json:"name"`
		Type       string   `json:"type"`
		Nullable   *bool    `json:"nullable,omitempty"`
		ForeignKey *fkShape `json:"foreignKey,omitempty"`
	}
	type tableShape struct {
		Name    string     `json:"name"`
		Columns []colShape `json:"columns"`
	}
	type schemaShape struct {
		Tables []tableShape `json:"tables"`
	}

	var cs []colShape
	for _, c := range cols {
		col := colShape{Name: c.name, Type: c.typ, Nullable: c.nullable}
		if c.hasFK {
			col.ForeignKey = &fkShape{Table: c.fkTable, Column: c.fkCol}
		}
		cs = append(cs, col)
	}
	out, _ := json.Marshal(schemaShape{Tables: []tableShape{{Name: table, Columns: cs}}})
	return out
}
