// Package refreshplan defines the typed refresh-plan format shared by
// CLI, MCP tools, and backend AI. A plan describes the ordered set of
// operations that transforms old revision CSVs into CSVs that match the
// current database schema.
package refreshplan

import (
	"encoding/json"
	"fmt"
	"time"
)

// Op enumerates every supported operation kind.
type Op string

const (
	OpAddColumn         Op = "add_column"
	OpDropColumn        Op = "drop_column"
	OpRenameColumn      Op = "rename_column"
	OpSetConstant       Op = "set_constant"
	OpCopyColumn        Op = "copy_column"
	OpCreateRow         Op = "create_row"
	OpFillForeignKey    Op = "fill_foreign_key"
	OpGenerateUUID      Op = "generate_uuid"
	OpGenerateTimestamp Op = "generate_timestamp"
)

// Strategy for add_column / set_constant ops.
type Strategy string

const (
	StrategyConstant  Strategy = "constant"   // fixed value supplied by user/AI
	StrategyDefault   Strategy = "default"    // use the column's DB default
	StrategyUUID      Strategy = "uuid"       // generate a new UUID per row
	StrategyTimestamp Strategy = "timestamp"  // current time per row
	StrategyDerive    Strategy = "derive"     // copy from another column on the same row
	StrategyEmpty     Strategy = "empty"      // empty string / NULL
)

// Source records where a given operation's parameters came from.
type Source string

const (
	SourceAuto        Source = "auto"
	SourceRule        Source = "rule"
	SourceUser        Source = "user"
	SourceAI          Source = "ai"
	SourceSuggestion  Source = "suggestion"
)

// Operation is a single transformation step. The fields used depend on Op:
//
//	add_column          table, column, strategy, value (if strategy=constant), fromColumn (if strategy=derive)
//	drop_column         table, column
//	rename_column       table, column (new name), fromColumn (old name)
//	set_constant        table, column, value
//	copy_column         table, column (destination), fromColumn (source)
//	create_row          table, values
//	fill_foreign_key    table, column, refTable, refColumn
//	generate_uuid       table, column
//	generate_timestamp  table, column
type Operation struct {
	Op          Op                         `json:"op"`
	Table       string                     `json:"table"`
	Column      string                     `json:"column,omitempty"`
	FromColumn  string                     `json:"fromColumn,omitempty"`  // rename / derive source
	Strategy    Strategy                   `json:"strategy,omitempty"`
	Value       json.RawMessage            `json:"value,omitempty"`       // JSON scalar or string
	Values      map[string]json.RawMessage `json:"values,omitempty"`      // create_row payload
	RefTable    string                     `json:"refTable,omitempty"`    // fill_foreign_key
	RefColumn   string                     `json:"refColumn,omitempty"`   // fill_foreign_key
	Source      Source                     `json:"source,omitempty"`
	Reasoning   string                     `json:"reasoning,omitempty"`   // AI/user explanation
}

// Plan is the complete refresh specification for a scenario revision.
type Plan struct {
	Scenario                string      `json:"scenario"`
	BaseRevision            string      `json:"baseRevision"`
	TargetSchemaFingerprint string      `json:"targetSchemaFingerprint"`
	CreatedAt               time.Time   `json:"createdAt"`
	PlanSource              string      `json:"planSource"` // "auto" | "interactive" | "ai" | "file"
	Operations              []Operation `json:"operations"`
}

// ValueString returns the string representation of Value, or "" when nil/null.
func (op Operation) ValueString() string {
	if len(op.Value) == 0 || string(op.Value) == "null" {
		return ""
	}
	// If it's a JSON string, unquote it.
	var s string
	if err := json.Unmarshal(op.Value, &s); err == nil {
		return s
	}
	// Numeric / boolean / raw — return as-is.
	return string(op.Value)
}

// StringValue constructs a JSON-encoded string value.
func StringValue(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return b
}

// ValidationError is one problem found by Validate.
type ValidationError struct {
	OpIndex int
	Op      Op
	Table   string
	Column  string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("op[%d] %s %s.%s: %s", e.OpIndex, e.Op, e.Table, e.Column, e.Message)
}
