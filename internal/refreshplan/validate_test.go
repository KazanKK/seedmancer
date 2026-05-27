package refreshplan_test

import (
	"encoding/json"
	"testing"

	"github.com/KazanKK/seedmancer/internal/refreshplan"
)

func TestValidate_valid(t *testing.T) {
	nullable := true
	oldSchema := makeSchema("users", []colShape{
		{Name: "id", Type: "integer"},
		{Name: "email", Type: "text"},
	})
	newSchema := makeSchema("users", []colShape{
		{Name: "id", Type: "integer"},
		{Name: "email", Type: "text"},
		{Name: "nickname", Type: "text", Nullable: &nullable},
	})

	plan := refreshplan.Plan{
		Scenario:     "s",
		BaseRevision: "r001",
		Operations: []refreshplan.Operation{
			{Op: refreshplan.OpAddColumn, Table: "users", Column: "nickname", Strategy: refreshplan.StrategyEmpty, Source: refreshplan.SourceAuto},
		},
	}

	errs := refreshplan.Validate(plan, oldSchema, newSchema)
	if len(errs) != 0 {
		t.Errorf("expected no errors, got: %v", errs)
	}
}

func TestValidate_missingTable(t *testing.T) {
	newSchema := makeSchema("users", []colShape{{Name: "id", Type: "integer"}})
	plan := refreshplan.Plan{
		Operations: []refreshplan.Operation{
			{Op: refreshplan.OpAddColumn, Table: "orders", Column: "total"},
		},
	}
	errs := refreshplan.Validate(plan, []byte(`{}`), newSchema)
	if len(errs) == 0 {
		t.Error("expected a validation error for missing table in new schema")
	}
}

func TestValidate_dropColumnNotInOldSchema(t *testing.T) {
	oldSchema := makeSchema("users", []colShape{{Name: "id", Type: "integer"}})
	plan := refreshplan.Plan{
		Operations: []refreshplan.Operation{
			{Op: refreshplan.OpDropColumn, Table: "users", Column: "email"},
		},
	}
	errs := refreshplan.Validate(plan, oldSchema, oldSchema)
	if len(errs) == 0 {
		t.Error("expected validation error: email not in old schema")
	}
}

func TestValidate_renameRequiresFromColumn(t *testing.T) {
	schema := makeSchema("users", []colShape{{Name: "id", Type: "integer"}})
	plan := refreshplan.Plan{
		Operations: []refreshplan.Operation{
			{Op: refreshplan.OpRenameColumn, Table: "users", Column: "name"},
			// FromColumn intentionally omitted
		},
	}
	errs := refreshplan.Validate(plan, schema, schema)
	if len(errs) == 0 {
		t.Error("expected validation error: fromColumn is required")
	}
}

func TestValidationSummary_empty(t *testing.T) {
	if s := refreshplan.ValidationSummary(nil); s != "" {
		t.Errorf("expected empty summary, got %q", s)
	}
}

func TestStringValue(t *testing.T) {
	v := refreshplan.StringValue("hello")
	var s string
	if err := json.Unmarshal(v, &s); err != nil || s != "hello" {
		t.Errorf("StringValue round-trip failed: got %q err %v", s, err)
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

type colShape struct {
	Name       string   `json:"name"`
	Type       string   `json:"type"`
	Nullable   *bool    `json:"nullable,omitempty"`
	ForeignKey *fkShape `json:"foreignKey,omitempty"`
}

type fkShape struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

func makeSchema(table string, cols []colShape) []byte {
	type tableShape struct {
		Name    string     `json:"name"`
		Columns []colShape `json:"columns"`
	}
	type schemaShape struct {
		Tables []tableShape `json:"tables"`
	}
	out, _ := json.Marshal(schemaShape{Tables: []tableShape{{Name: table, Columns: cols}}})
	return out
}
