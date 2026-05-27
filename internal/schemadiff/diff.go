// Package schemadiff compares two schema.json snapshots and reports the
// differences in a form suitable for `seedmancer check`. It deliberately
// stays lenient: missing fields are treated as "not set" so a hand-edited
// schema.json doesn't blow up the diff.
package schemadiff

import (
	"encoding/json"
	"fmt"
	"sort"
)

// ChangeKind enumerates the categories of difference we surface.
type ChangeKind string

const (
	TableAdded       ChangeKind = "table_added"
	TableRemoved     ChangeKind = "table_removed"
	ColumnAdded      ChangeKind = "column_added"
	ColumnRemoved    ChangeKind = "column_removed"
	ColumnChanged    ChangeKind = "column_changed"
	ForeignKeyAdded  ChangeKind = "fk_added"
	ForeignKeyRemoved ChangeKind = "fk_removed"
	ForeignKeyChanged ChangeKind = "fk_changed"
)

// Change is one entry in the diff. ColumnChange holds the before/after
// description for "changed" entries; the other kinds leave it empty.
type Change struct {
	Kind   ChangeKind
	Table  string
	Column string // empty for table-level changes
	Detail string // human-readable summary, e.g. "type changed varchar(255) -> text"
}

// String renders one change in the format used by `seedmancer check`.
func (c Change) String() string {
	switch c.Kind {
	case TableAdded:
		return "+ table " + c.Table + " added"
	case TableRemoved:
		return "- table " + c.Table + " removed"
	case ColumnAdded:
		if c.Detail != "" {
			return fmt.Sprintf("+ %s.%s added %s", c.Table, c.Column, c.Detail)
		}
		return fmt.Sprintf("+ %s.%s added", c.Table, c.Column)
	case ColumnRemoved:
		return fmt.Sprintf("- %s.%s removed", c.Table, c.Column)
	case ColumnChanged:
		return fmt.Sprintf("~ %s.%s %s", c.Table, c.Column, c.Detail)
	case ForeignKeyAdded:
		return fmt.Sprintf("+ %s.%s FK -> %s", c.Table, c.Column, c.Detail)
	case ForeignKeyRemoved:
		return fmt.Sprintf("- %s.%s FK removed", c.Table, c.Column)
	case ForeignKeyChanged:
		return fmt.Sprintf("~ %s.%s FK %s", c.Table, c.Column, c.Detail)
	}
	return fmt.Sprintf("? %s.%s %s", c.Table, c.Column, c.Detail)
}

// rawSchema is the lenient shape we accept. Defaults and ForeignKeys are
// kept as raw JSON so we can distinguish "field absent" from
// "field present and null" without exploding on weird inputs.
type rawSchema struct {
	Tables []rawTable `json:"tables"`
}

type rawTable struct {
	Name    string      `json:"name"`
	Columns []rawColumn `json:"columns"`
}

type rawColumn struct {
	Name     string          `json:"name"`
	Type     string          `json:"type"`
	Nullable *bool           `json:"nullable"`
	Default  json.RawMessage `json:"default"`
	IsPrimary *bool          `json:"isPrimary"`
	IsUnique  *bool          `json:"isUnique"`
	IsGenerated *bool        `json:"isGenerated"`
	ForeignKey *rawForeignKey `json:"foreignKey"`
}

type rawForeignKey struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

// Diff parses the two schema JSON blobs and returns a sorted list of
// changes. The returned changes are sorted by table then column so the
// output is deterministic across runs.
func Diff(oldJSON, newJSON []byte) ([]Change, error) {
	var oldS, newS rawSchema
	if err := json.Unmarshal(oldJSON, &oldS); err != nil {
		return nil, fmt.Errorf("parsing old schema: %w", err)
	}
	if err := json.Unmarshal(newJSON, &newS); err != nil {
		return nil, fmt.Errorf("parsing new schema: %w", err)
	}

	oldTables := indexTables(oldS.Tables)
	newTables := indexTables(newS.Tables)

	var out []Change
	for _, name := range sortedKeys(oldTables, newTables) {
		ot, oOK := oldTables[name]
		nt, nOK := newTables[name]
		switch {
		case !oOK && nOK:
			out = append(out, Change{Kind: TableAdded, Table: name})
		case oOK && !nOK:
			out = append(out, Change{Kind: TableRemoved, Table: name})
		default:
			out = append(out, diffTable(name, ot, nt)...)
		}
	}
	return out, nil
}

func diffTable(table string, oldT, newT rawTable) []Change {
	oldCols := indexColumns(oldT.Columns)
	newCols := indexColumns(newT.Columns)
	var out []Change
	for _, name := range sortedKeys(oldCols, newCols) {
		oc, oOK := oldCols[name]
		nc, nOK := newCols[name]
		switch {
		case !oOK && nOK:
			out = append(out, Change{
				Kind:   ColumnAdded,
				Table:  table,
				Column: name,
				Detail: describeColumn(nc),
			})
			if nc.ForeignKey != nil {
				out = append(out, Change{
					Kind:   ForeignKeyAdded,
					Table:  table,
					Column: name,
					Detail: nc.ForeignKey.Table + "." + nc.ForeignKey.Column,
				})
			}
		case oOK && !nOK:
			out = append(out, Change{
				Kind:   ColumnRemoved,
				Table:  table,
				Column: name,
			})
		default:
			if detail := diffColumn(oc, nc); detail != "" {
				out = append(out, Change{
					Kind:   ColumnChanged,
					Table:  table,
					Column: name,
					Detail: detail,
				})
			}
			if fkDetail := diffFK(oc.ForeignKey, nc.ForeignKey); fkDetail != "" {
				kind := ForeignKeyChanged
				if oc.ForeignKey == nil {
					kind = ForeignKeyAdded
				} else if nc.ForeignKey == nil {
					kind = ForeignKeyRemoved
				}
				out = append(out, Change{
					Kind:   kind,
					Table:  table,
					Column: name,
					Detail: fkDetail,
				})
			}
		}
	}
	return out
}

func diffColumn(oc, nc rawColumn) string {
	var bits []string
	if oc.Type != nc.Type {
		bits = append(bits, fmt.Sprintf("type changed %s -> %s", oc.Type, nc.Type))
	}
	if boolPtrVal(oc.Nullable) != boolPtrVal(nc.Nullable) {
		bits = append(bits, fmt.Sprintf("nullable %v -> %v",
			boolPtrVal(oc.Nullable), boolPtrVal(nc.Nullable)))
	}
	if !sameRaw(oc.Default, nc.Default) {
		bits = append(bits, fmt.Sprintf("default %s -> %s",
			rawDisplay(oc.Default), rawDisplay(nc.Default)))
	}
	if boolPtrVal(oc.IsPrimary) != boolPtrVal(nc.IsPrimary) {
		bits = append(bits, fmt.Sprintf("isPrimary %v -> %v",
			boolPtrVal(oc.IsPrimary), boolPtrVal(nc.IsPrimary)))
	}
	if boolPtrVal(oc.IsUnique) != boolPtrVal(nc.IsUnique) {
		bits = append(bits, fmt.Sprintf("isUnique %v -> %v",
			boolPtrVal(oc.IsUnique), boolPtrVal(nc.IsUnique)))
	}
	if boolPtrVal(oc.IsGenerated) != boolPtrVal(nc.IsGenerated) {
		bits = append(bits, fmt.Sprintf("isGenerated %v -> %v",
			boolPtrVal(oc.IsGenerated), boolPtrVal(nc.IsGenerated)))
	}
	if len(bits) == 0 {
		return ""
	}
	return joinComma(bits)
}

// diffFK returns a human-readable description when the FK target changed,
// or empty string when both are nil or equal.
func diffFK(old, new *rawForeignKey) string {
	oldRef := fkRef(old)
	newRef := fkRef(new)
	if oldRef == newRef {
		return ""
	}
	if old == nil {
		return "-> " + newRef
	}
	if new == nil {
		return "(removed)"
	}
	return oldRef + " -> " + newRef
}

func fkRef(fk *rawForeignKey) string {
	if fk == nil {
		return ""
	}
	return fk.Table + "." + fk.Column
}

// describeColumn renders the new-column detail line: type plus
// nullable/default/generated when interesting.
func describeColumn(c rawColumn) string {
	bits := []string{c.Type}
	bits = append(bits, fmt.Sprintf("nullable=%v", boolPtrVal(c.Nullable)))
	if len(c.Default) > 0 && string(c.Default) != "null" {
		bits = append(bits, "default="+rawDisplay(c.Default))
	}
	if boolPtrVal(c.IsGenerated) {
		bits = append(bits, "generated")
	}
	return joinSpace(bits)
}

func indexTables(ts []rawTable) map[string]rawTable {
	out := make(map[string]rawTable, len(ts))
	for _, t := range ts {
		out[t.Name] = t
	}
	return out
}

func indexColumns(cs []rawColumn) map[string]rawColumn {
	out := make(map[string]rawColumn, len(cs))
	for _, c := range cs {
		out[c.Name] = c
	}
	return out
}

func sortedKeys[V any](maps ...map[string]V) []string {
	seen := map[string]bool{}
	for _, m := range maps {
		for k := range m {
			seen[k] = true
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func boolPtrVal(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

func sameRaw(a, b json.RawMessage) bool {
	// Both empty / null collapse to "no default", matching the
	// fingerprinting helper's nullable-string handling.
	an := isNullOrEmpty(a)
	bn := isNullOrEmpty(b)
	if an && bn {
		return true
	}
	if an != bn {
		return false
	}
	return string(a) == string(b)
}

func isNullOrEmpty(r json.RawMessage) bool {
	s := string(r)
	return s == "" || s == "null"
}

func rawDisplay(r json.RawMessage) string {
	if isNullOrEmpty(r) {
		return "null"
	}
	return string(r)
}

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ", "
		}
		out += p
	}
	return out
}

func joinSpace(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += " "
		}
		out += p
	}
	return out
}
