// Package driftreport classifies schemadiff changes into four actionability
// categories and produces a structured Report that drives both interactive
// CLI prompts and MCP tool responses for `seedmancer refresh`.
package driftreport

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/KazanKK/seedmancer/internal/schemadiff"
)

// Category describes how automatically Seedmancer can resolve a change.
type Category string

const (
	// Auto — safe to apply without asking the user.
	// Examples: nullable column added, column with DB default added, column removed.
	Auto Category = "auto"

	// Likely — Seedmancer has a high-confidence suggestion but should confirm.
	// Examples: column rename heuristic, type widening, enum value added.
	Likely Category = "likely"

	// Decision — intent is ambiguous; user or AI must supply a strategy.
	// Examples: NOT NULL column added without default, required FK added.
	Decision Category = "decision"

	// Breaking — Seedmancer cannot safely transform the data.
	// Examples: PK changed, type incompatible narrowing, table removed with FK refs.
	Breaking Category = "breaking"
)

// AnnotatedChange wraps a schemadiff.Change with classification metadata.
type AnnotatedChange struct {
	schemadiff.Change
	Category   Category `json:"category"`
	AutoReason string   `json:"autoReason,omitempty"` // why Seedmancer classifies it Auto/Likely
	// Suggestion is a draft operation for Auto/Likely changes; nil otherwise.
	Suggestion *OpSuggestion `json:"suggestion,omitempty"`
}

// OpSuggestion is a lightweight hint pointing at the refresh-plan operation
// that would resolve this change. The full op is built by the classifier.
type OpSuggestion struct {
	Op       string `json:"op"`
	Strategy string `json:"strategy,omitempty"`
	Value    string `json:"value,omitempty"`
	Note     string `json:"note,omitempty"`
}

// Report is the full drift analysis for one scenario revision.
type Report struct {
	Scenario        string            `json:"scenario"`
	BaseRevision    string            `json:"baseRevision"`
	OldSchemaFP     string            `json:"oldSchemaFingerprint"`
	NewSchemaFP     string            `json:"newSchemaFingerprint"`
	Changes         []AnnotatedChange `json:"changes"`
	Counts          map[Category]int  `json:"counts"`
	HasDrift        bool              `json:"hasDrift"`
	AutoResolvable  bool              `json:"autoResolvable"`  // true when every change is Auto
	NeedsDecision   bool              `json:"needsDecision"`   // true when any change is Decision
	HasBreaking     bool              `json:"hasBreaking"`     // true when any change is Breaking
}

// rawSchemaForDrift is the lenient shape we decode schema.json into for
// classification purposes (needs nullable/default/FK/isGenerated fields).
type rawSchemaForDrift struct {
	Tables []rawTableForDrift `json:"tables"`
}

type rawTableForDrift struct {
	Name    string              `json:"name"`
	Columns []rawColumnForDrift `json:"columns"`
}

type rawColumnForDrift struct {
	Name        string          `json:"name"`
	Type        string          `json:"type"`
	Nullable    *bool           `json:"nullable"`
	Default     json.RawMessage `json:"default"`
	IsPrimary   *bool           `json:"isPrimary"`
	IsGenerated *bool           `json:"isGenerated"`
	ForeignKey  *rawFKForDrift  `json:"foreignKey"`
	Enum        string          `json:"enum,omitempty"`
}

type rawFKForDrift struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

// Build produces a classified drift Report from two schema.json blobs and
// the raw schemadiff.Change slice. It enriches each change with a category
// and optional suggestion.
func Build(scenario, baseRevision, oldFP, newFP string, rawChanges []schemadiff.Change, oldJSON, newJSON []byte) Report {
	var newSchema rawSchemaForDrift
	if err := json.Unmarshal(newJSON, &newSchema); err != nil {
		newSchema = rawSchemaForDrift{}
	}
	var oldSchema rawSchemaForDrift
	if err := json.Unmarshal(oldJSON, &oldSchema); err != nil {
		oldSchema = rawSchemaForDrift{}
	}

	newColIndex := buildColIndex(newSchema)
	oldColIndex := buildColIndex(oldSchema)

	// Rename heuristic: map drop+add pairs on the same table that share the
	// same type as likely-rename candidates. We build this first so the main
	// loop can tag the add-side as Likely instead of Auto.
	renameHints := detectRenames(rawChanges, oldColIndex, newColIndex)

	counts := map[Category]int{Auto: 0, Likely: 0, Decision: 0, Breaking: 0}
	annotated := make([]AnnotatedChange, 0, len(rawChanges))

	for _, ch := range rawChanges {
		ac := classify(ch, newColIndex, oldColIndex, renameHints)
		counts[ac.Category]++
		annotated = append(annotated, ac)
	}

	r := Report{
		Scenario:     scenario,
		BaseRevision: baseRevision,
		OldSchemaFP:  oldFP,
		NewSchemaFP:  newFP,
		Changes:      annotated,
		Counts:       counts,
		HasDrift:     len(annotated) > 0,
	}
	r.AutoResolvable = counts[Decision] == 0 && counts[Breaking] == 0 && counts[Likely] == 0
	r.NeedsDecision = counts[Decision] > 0
	r.HasBreaking = counts[Breaking] > 0
	return r
}

type colKey struct{ table, col string }

func buildColIndex(s rawSchemaForDrift) map[colKey]rawColumnForDrift {
	idx := map[colKey]rawColumnForDrift{}
	for _, t := range s.Tables {
		for _, c := range t.Columns {
			idx[colKey{t.Name, c.Name}] = c
		}
	}
	return idx
}

// renameCandidate pairs a removed column with a matching added column.
type renameCandidate struct {
	FromCol string
	ToCol   string
}

func detectRenames(changes []schemadiff.Change, oldIdx, newIdx map[colKey]rawColumnForDrift) map[colKey]renameCandidate {
	// Collect drops and adds per table.
	type colInfo struct {
		name string
		typ  string
	}
	drops := map[string][]colInfo{}
	adds := map[string][]colInfo{}
	for _, ch := range changes {
		switch ch.Kind {
		case schemadiff.ColumnRemoved:
			if c, ok := oldIdx[colKey{ch.Table, ch.Column}]; ok {
				drops[ch.Table] = append(drops[ch.Table], colInfo{ch.Column, c.Type})
			}
		case schemadiff.ColumnAdded:
			if c, ok := newIdx[colKey{ch.Table, ch.Column}]; ok {
				adds[ch.Table] = append(adds[ch.Table], colInfo{ch.Column, c.Type})
			}
		}
	}

	hints := map[colKey]renameCandidate{}
	for table, addedCols := range adds {
		droppedCols, ok := drops[table]
		if !ok {
			continue
		}
		for _, added := range addedCols {
			for _, dropped := range droppedCols {
				if dropped.typ == added.typ && nameSimilar(dropped.name, added.name) {
					// Tag the added column as a likely rename from the dropped one.
					hints[colKey{table, added.name}] = renameCandidate{FromCol: dropped.name, ToCol: added.name}
					break
				}
			}
		}
	}
	return hints
}

// nameSimilar is a heuristic: names are "similar" if one is a prefix/suffix of
// the other (case-insensitive) or if they share ≥60% of their characters via
// a simple length-based overlap test. This is intentionally permissive — the
// user is always shown the suggestion and can reject it.
func nameSimilar(a, b string) bool {
	a, b = strings.ToLower(a), strings.ToLower(b)
	if a == b {
		return true
	}
	if strings.HasPrefix(a, b) || strings.HasPrefix(b, a) ||
		strings.HasSuffix(a, b) || strings.HasSuffix(b, a) {
		return true
	}
	longer, shorter := len(a), len(b)
	if longer < shorter {
		longer, shorter = shorter, longer
	}
	if longer == 0 {
		return false
	}
	// simple overlap ratio
	return float64(shorter)/float64(longer) >= 0.6
}

func classify(ch schemadiff.Change, newIdx, oldIdx map[colKey]rawColumnForDrift, renameHints map[colKey]renameCandidate) AnnotatedChange {
	ac := AnnotatedChange{Change: ch}

	switch ch.Kind {
	case schemadiff.TableAdded:
		// A new table with no data required is Auto; if it might need rows it's Likely.
		ac.Category = Auto
		ac.AutoReason = "new table — no existing data affected"

	case schemadiff.TableRemoved:
		// Removing a table that may still be referenced elsewhere is Breaking.
		ac.Category = Breaking
		ac.AutoReason = "table removed — possible FK references from other tables"

	case schemadiff.ColumnRemoved:
		ac.Category = Auto
		ac.AutoReason = "column removed — drop from CSV"
		ac.Suggestion = &OpSuggestion{Op: "drop_column"}

	case schemadiff.ColumnAdded:
		col, ok := newIdx[colKey{ch.Table, ch.Column}]
		if !ok {
			ac.Category = Decision
			ac.AutoReason = "new column — schema metadata unavailable"
			break
		}
		if boolVal(col.IsGenerated) {
			// Generated columns never need CSV data.
			ac.Category = Auto
			ac.AutoReason = "generated column — no CSV value needed"
			break
		}
		// Check rename hint first.
		if hint, isRename := renameHints[colKey{ch.Table, ch.Column}]; isRename {
			ac.Category = Likely
			ac.AutoReason = fmt.Sprintf("possible rename from %s (same type)", hint.FromCol)
			ac.Suggestion = &OpSuggestion{Op: "rename_column", Value: hint.FromCol,
				Note: fmt.Sprintf("rename %s → %s", hint.FromCol, ch.Column)}
			break
		}
		hasDefault := len(col.Default) > 0 && string(col.Default) != "null"
		if boolVal(col.Nullable) {
			ac.Category = Auto
			ac.AutoReason = "nullable column — fill with empty"
			ac.Suggestion = &OpSuggestion{Op: "add_column", Strategy: "constant", Value: ""}
		} else if hasDefault {
			ac.Category = Auto
			ac.AutoReason = "column has a default — fill with default"
			ac.Suggestion = &OpSuggestion{Op: "add_column", Strategy: "default"}
		} else if col.ForeignKey != nil {
			ac.Category = Decision
			ac.AutoReason = fmt.Sprintf("required FK column references %s.%s — need a valid parent row", col.ForeignKey.Table, col.ForeignKey.Column)
		} else {
			ac.Category = Decision
			ac.AutoReason = "NOT NULL column without default — need a fill strategy"
		}

	case schemadiff.ColumnChanged:
		ac.Category = classifyColumnChanged(ch.Detail)

	case schemadiff.ForeignKeyAdded:
		// An FK was added to an existing column. If the column already has data
		// it must reference valid parent rows — this is a Decision.
		ac.Category = Decision
		ac.AutoReason = fmt.Sprintf("FK added to existing column referencing %s — existing rows may violate constraint", ch.Detail)

	case schemadiff.ForeignKeyRemoved:
		ac.Category = Auto
		ac.AutoReason = "FK removed — no CSV transformation needed"

	case schemadiff.ForeignKeyChanged:
		ac.Category = Likely
		ac.AutoReason = fmt.Sprintf("FK target changed to %s — existing references may need update", ch.Detail)

	default:
		ac.Category = Decision
	}

	return ac
}

// classifyColumnChanged categorises a column attribute change.
func classifyColumnChanged(detail string) Category {
	d := strings.ToLower(detail)

	// PK changes are breaking.
	if strings.Contains(d, "isprimary") {
		return Breaking
	}
	// Type changes: detect safe widenings vs narrowings.
	if strings.Contains(d, "type changed") {
		return classifyTypeChange(d)
	}
	// Nullable: becoming non-null is a Decision; becoming null is Auto.
	if strings.Contains(d, "nullable") {
		if strings.Contains(d, "true -> false") {
			return Decision
		}
		return Auto
	}
	// Default changes are generally safe.
	if strings.Contains(d, "default") {
		return Auto
	}
	// Generated flag change.
	if strings.Contains(d, "isgenerated") {
		return Likely
	}
	// Unique change: adding unique constraint may fail if data has duplicates.
	if strings.Contains(d, "isunique true") {
		return Decision
	}
	if strings.Contains(d, "isunique false") {
		return Auto
	}
	return Likely
}

// classifyTypeChange returns a category for a "type changed X -> Y" detail.
func classifyTypeChange(detail string) Category {
	// Safe widenings: varchar(n) -> text, int -> bigint, etc.
	widenings := []struct{ from, to string }{
		{"varchar", "text"},
		{"character varying", "text"},
		{"integer", "bigint"},
		{"int", "bigint"},
		{"float", "double"},
		{"real", "double precision"},
		{"timestamp without time zone", "timestamp with time zone"},
	}
	for _, w := range widenings {
		if strings.Contains(detail, w.from) && strings.Contains(detail, w.to) {
			return Likely
		}
	}
	return Breaking
}

func boolVal(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}
