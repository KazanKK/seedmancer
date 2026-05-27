package refreshplan

import (
	"time"

	"github.com/KazanKK/seedmancer/internal/driftreport"
	"github.com/KazanKK/seedmancer/internal/schemadiff"
)

// Classify converts a drift report into a draft Plan. Auto-category changes
// get fully-populated operations; Likely changes get suggestive operations
// marked as Source=suggestion (the user should confirm); Decision and
// Breaking changes produce stub operations with no strategy so the caller
// knows it needs to prompt or call AI.
func Classify(report driftreport.Report) Plan {
	p := Plan{
		Scenario:                report.Scenario,
		BaseRevision:            report.BaseRevision,
		TargetSchemaFingerprint: report.NewSchemaFP,
		CreatedAt:               time.Now().UTC(),
		PlanSource:              "auto",
	}

	for _, ch := range report.Changes {
		ops := buildOpsForChange(ch)
		p.Operations = append(p.Operations, ops...)
	}

	return p
}

func buildOpsForChange(ch driftreport.AnnotatedChange) []Operation {
	var ops []Operation

	switch ch.Kind {
	case schemadiff.ColumnRemoved:
		ops = append(ops, Operation{
			Op:        OpDropColumn,
			Table:     ch.Table,
			Column:    ch.Column,
			Source:    resolveSource(ch.Category),
			Reasoning: ch.AutoReason,
		})

	case schemadiff.ColumnAdded:
		if ch.Suggestion == nil {
			ops = append(ops, stubOp(ch))
			break
		}
		switch Op(ch.Suggestion.Op) {
		case OpAddColumn:
			op := Operation{
				Op:        OpAddColumn,
				Table:     ch.Table,
				Column:    ch.Column,
				Source:    resolveSource(ch.Category),
				Reasoning: ch.AutoReason,
			}
			if ch.Suggestion.Strategy != "" {
				op.Strategy = Strategy(ch.Suggestion.Strategy)
			}
			if ch.Suggestion.Value != "" {
				op.Value = StringValue(ch.Suggestion.Value)
			}
			ops = append(ops, op)
		case OpRenameColumn:
			ops = append(ops, Operation{
				Op:         OpRenameColumn,
				Table:      ch.Table,
				Column:     ch.Column,
				FromColumn: ch.Suggestion.Value,
				Source:     resolveSource(ch.Category),
				Reasoning:  ch.AutoReason,
			})
		default:
			ops = append(ops, stubOp(ch))
		}

	case schemadiff.ForeignKeyRemoved:
		// Nothing to do in the CSV — the FK constraint just disappears.
		// We add a no-op marker so the plan is complete.
		ops = append(ops, Operation{
			Op:        OpDropColumn, // represents "no transform needed"
			Table:     ch.Table,
			Column:    ch.Column,
			Source:    SourceAuto,
			Reasoning: "FK removed — no CSV transformation needed (column data unchanged)",
		})
		// Correction: don't emit a real drop_column for FK removal — there's
		// no op needed. Return empty.
		return nil

	case schemadiff.ForeignKeyAdded:
		ops = append(ops, Operation{
			Op:        OpFillForeignKey,
			Table:     ch.Table,
			Column:    ch.Column,
			RefTable:  extractRefTable(ch.Detail),
			RefColumn: extractRefColumn(ch.Detail),
			Source:    SourceAI, // needs AI or user to fill strategy
			Reasoning: ch.AutoReason,
		})

	case schemadiff.ColumnChanged:
		op := changeOp(ch)
		if op.Op != "" {
			ops = append(ops, op)
		}

	case schemadiff.TableRemoved:
		// No CSV operation can address a removed table.
		// The Breaking entry in the drift report is sufficient to block auto-apply.
		return nil
	}

	return ops
}

func changeOp(ch driftreport.AnnotatedChange) Operation {
	switch ch.Category {
	case driftreport.Auto:
		// Default / nullable changes don't require any CSV transformation.
		return Operation{}
	case driftreport.Likely, driftreport.Decision, driftreport.Breaking:
		return stubOp(ch)
	}
	return Operation{}
}

func stubOp(ch driftreport.AnnotatedChange) Operation {
	op := Operation{
		Table:     ch.Table,
		Column:    ch.Column,
		Reasoning: ch.AutoReason,
	}
	switch ch.Kind {
	case schemadiff.ColumnAdded:
		op.Op = OpAddColumn
	case schemadiff.ColumnRemoved:
		op.Op = OpDropColumn
	case schemadiff.ColumnChanged:
		op.Op = OpSetConstant
	case schemadiff.ForeignKeyAdded:
		op.Op = OpFillForeignKey
	default:
		op.Op = OpSetConstant
	}

	switch ch.Category {
	case driftreport.Decision:
		op.Source = "" // blank = "needs filling"
	case driftreport.Breaking:
		op.Source = "" // blank = "needs manual resolution"
	default:
		op.Source = resolveSource(ch.Category)
	}
	return op
}

func resolveSource(cat driftreport.Category) Source {
	switch cat {
	case driftreport.Auto:
		return SourceAuto
	case driftreport.Likely:
		return SourceSuggestion
	default:
		return "" // needs filling
	}
}

// extractRefTable parses "table.column" → "table".
func extractRefTable(detail string) string {
	if idx := indexOf(detail, '.'); idx >= 0 {
		return detail[:idx]
	}
	return detail
}

// extractRefColumn parses "table.column" → "column".
func extractRefColumn(detail string) string {
	// detail may be "-> table.column" (from ForeignKeyAdded)
	d := detail
	if len(d) > 3 && d[:3] == "-> " {
		d = d[3:]
	}
	if idx := indexOf(d, '.'); idx >= 0 {
		return d[idx+1:]
	}
	return ""
}

func indexOf(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
