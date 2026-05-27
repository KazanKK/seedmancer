package refreshplan

import (
	"encoding/json"
	"fmt"
	"strings"
)

// schemaCol is the minimal view of a column needed for validation.
type schemaCol struct {
	Name        string
	Type        string
	Nullable    bool
	HasDefault  bool
	IsGenerated bool
	ForeignKey  *struct{ Table, Column string }
}

type schemaTable struct {
	Name    string
	Columns map[string]schemaCol
}

type schemaIndex map[string]schemaTable // key = table name

// Validate checks the plan's operations against the old and new schema JSON blobs.
// It returns a slice of ValidationErrors (never nil, but may be empty).
func Validate(p Plan, oldSchemaJSON, newSchemaJSON []byte) []ValidationError {
	newIdx := parseSchemaIndex(newSchemaJSON)
	oldIdx := parseSchemaIndex(oldSchemaJSON)

	var errs []ValidationError
	add := func(i int, op Operation, msg string) {
		errs = append(errs, ValidationError{OpIndex: i, Op: op.Op, Table: op.Table, Column: op.Column, Message: msg})
	}

	// Track which columns have been renamed in earlier ops so later ops see
	// the updated header.
	renames := map[string]map[string]string{} // table -> oldName -> newName

	for i, op := range p.Operations {
		switch op.Op {
		case OpDropColumn:
			if op.Table == "" {
				add(i, op, "table is required")
				break
			}
			if op.Column == "" {
				add(i, op, "column is required")
				break
			}
			if _, ok := oldIdx[op.Table]; !ok {
				// might be a newly added table — skip
				break
			}
			if _, ok := oldIdx[op.Table].Columns[op.Column]; !ok {
				add(i, op, fmt.Sprintf("column %q not found in old schema for table %q", op.Column, op.Table))
			}

		case OpAddColumn, OpGenerateUUID, OpGenerateTimestamp:
			if op.Table == "" || op.Column == "" {
				add(i, op, "table and column are required")
				break
			}
			tbl, ok := newIdx[op.Table]
			if !ok {
				add(i, op, fmt.Sprintf("table %q not found in new schema", op.Table))
				break
			}
			col, ok := tbl.Columns[op.Column]
			if !ok {
				add(i, op, fmt.Sprintf("column %q not found in new schema for table %q", op.Column, op.Table))
				break
			}
			if op.Op == OpAddColumn && op.Strategy == StrategyConstant && len(op.Value) == 0 {
				if !col.Nullable {
					add(i, op, "strategy=constant requires a value for NOT NULL column")
				}
			}

		case OpRenameColumn:
			if op.Table == "" || op.Column == "" || op.FromColumn == "" {
				add(i, op, "table, column (new name), and fromColumn (old name) are required")
				break
			}
			if _, ok := oldIdx[op.Table]; !ok {
				break
			}
			if _, ok := oldIdx[op.Table].Columns[op.FromColumn]; !ok {
				add(i, op, fmt.Sprintf("source column %q not found in old schema for table %q", op.FromColumn, op.Table))
			}
			if _, ok := newIdx[op.Table]; ok {
				if _, ok := newIdx[op.Table].Columns[op.Column]; !ok {
					add(i, op, fmt.Sprintf("target column %q not found in new schema for table %q", op.Column, op.Table))
				}
			}
			if renames[op.Table] == nil {
				renames[op.Table] = map[string]string{}
			}
			renames[op.Table][op.FromColumn] = op.Column

		case OpSetConstant:
			if op.Table == "" || op.Column == "" {
				add(i, op, "table and column are required")
				break
			}
			if len(op.Value) == 0 || string(op.Value) == "null" {
				// null/empty is only allowed on nullable columns
				if tbl, ok := newIdx[op.Table]; ok {
					if col, ok := tbl.Columns[op.Column]; ok {
						if !col.Nullable {
							add(i, op, "value is required for NOT NULL column")
						}
					}
				}
			}

		case OpCopyColumn:
			if op.Table == "" || op.Column == "" || op.FromColumn == "" {
				add(i, op, "table, column (destination), and fromColumn (source) are required")
			}

		case OpCreateRow:
			if op.Table == "" {
				add(i, op, "table is required")
				break
			}
			if len(op.Values) == 0 {
				add(i, op, "values map must not be empty")
				break
			}
			tbl, ok := newIdx[op.Table]
			if !ok {
				break
			}
			// Check required columns are present.
			for colName, col := range tbl.Columns {
				if col.IsGenerated || col.Nullable || col.HasDefault {
					continue
				}
				if _, hasVal := op.Values[colName]; !hasVal {
					add(i, op, fmt.Sprintf("required column %q missing from create_row values", colName))
				}
			}

		case OpFillForeignKey:
			if op.Table == "" || op.Column == "" || op.RefTable == "" || op.RefColumn == "" {
				add(i, op, "table, column, refTable, and refColumn are required")
				break
			}
			if tbl, ok := newIdx[op.Table]; ok {
				if col, ok := tbl.Columns[op.Column]; ok {
					if col.ForeignKey != nil {
						wantTable := strings.ToLower(col.ForeignKey.Table)
						gotTable := strings.ToLower(op.RefTable)
						if wantTable != gotTable {
							add(i, op, fmt.Sprintf("column FK references %s.%s but op specifies %s.%s",
								col.ForeignKey.Table, col.ForeignKey.Column, op.RefTable, op.RefColumn))
						}
					}
				}
			}

		default:
			add(i, op, fmt.Sprintf("unknown op %q", op.Op))
		}
	}

	return errs
}

// ValidationSummary converts a slice of ValidationErrors into a human-readable
// multi-line string, or returns "" when there are no errors.
func ValidationSummary(errs []ValidationError) string {
	if len(errs) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d validation error(s):\n", len(errs)))
	for _, e := range errs {
		sb.WriteString("  " + e.Error() + "\n")
	}
	return strings.TrimRight(sb.String(), "\n")
}

// rawSchemaForVal is the lenient shape we unmarshal schema.json into.
type rawSchemaForVal struct {
	Tables []rawTableForVal `json:"tables"`
}

type rawTableForVal struct {
	Name    string           `json:"name"`
	Columns []rawColForVal   `json:"columns"`
}

type rawColForVal struct {
	Name        string          `json:"name"`
	Type        string          `json:"type"`
	Nullable    *bool           `json:"nullable"`
	Default     json.RawMessage `json:"default"`
	IsGenerated *bool           `json:"isGenerated"`
	ForeignKey  *struct {
		Table  string `json:"table"`
		Column string `json:"column"`
	} `json:"foreignKey"`
}

func parseSchemaIndex(schemaJSON []byte) schemaIndex {
	idx := schemaIndex{}
	if len(schemaJSON) == 0 {
		return idx
	}
	var raw rawSchemaForVal
	if err := json.Unmarshal(schemaJSON, &raw); err != nil {
		return idx
	}
	for _, t := range raw.Tables {
		st := schemaTable{Name: t.Name, Columns: map[string]schemaCol{}}
		for _, c := range t.Columns {
			sc := schemaCol{
				Name:        c.Name,
				Type:        c.Type,
				Nullable:    c.Nullable != nil && *c.Nullable,
				HasDefault:  len(c.Default) > 0 && string(c.Default) != "null",
				IsGenerated: c.IsGenerated != nil && *c.IsGenerated,
			}
			if c.ForeignKey != nil {
				sc.ForeignKey = &struct{ Table, Column string }{
					Table:  c.ForeignKey.Table,
					Column: c.ForeignKey.Column,
				}
			}
			st.Columns[c.Name] = sc
		}
		idx[t.Name] = st
	}
	return idx
}
