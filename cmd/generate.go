package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	db "github.com/KazanKK/seedmancer/database"
	"github.com/KazanKK/seedmancer/internal/ui"

	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/urfave/cli/v2"
)

// ─── API schema types ─────────────────────────────────────────────────────────

type generateSchema struct {
	Enums  []generateEnum  `json:"enums,omitempty"`
	Tables []generateTable `json:"tables"`
}

type generateEnum struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

type generateTable struct {
	Name    string           `json:"name"`
	Columns []generateColumn `json:"columns"`
}

type generateColumn struct {
	Name          string              `json:"name"`
	Type          string              `json:"type"`
	Nullable      bool                `json:"nullable"`
	IsPrimary     bool                `json:"isPrimary"`
	IsUnique      bool                `json:"isUnique"`
	Default       string              `json:"default,omitempty"`
	ForeignKey    *generateForeignKey `json:"foreignKey,omitempty"`
	Enum          string              `json:"enum,omitempty"`
	AllowedValues []string            `json:"allowedValues,omitempty"`
}

type generateForeignKey struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

// ─── Command definition ───────────────────────────────────────────────────────

// GenerateCommand uses Seedmancer's AI service to create realistic test data
// for a scenario and snapshot it as a new revision.
func GenerateCommand() *cli.Command {
	return &cli.Command{
		Name:      "generate",
		Usage:     "Generate realistic AI test data into a new revision of a scenario",
		ArgsUsage: "<scenario>",
		Description: "Uses AI to produce realistic test data for every table in your\n" +
			"schema, then snapshots the result as a new revision under the scenario.\n\n" +
			"Examples:\n" +
			"  seedmancer generate baseline --prompt 'three realistic users'\n" +
			"  seedmancer generate qa/smoke --prompt 'a few orders with line items'\n" +
			"  seedmancer generate qa/smoke --inherit baseline --prompt 'add two orders'\n\n" +
			"NOTE: this overwrites data in the configured local env.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "prompt",
				Required: true,
				Usage:    "(required) Natural-language description of the data to generate",
			},
			&cli.StringFlag{
				Name:    "inherit",
				Aliases: []string{"b"},
				Usage:   "Seed this base scenario before generating (optional)",
			},
			&cli.StringFlag{
				Name:    "env",
				Aliases: []string{"e"},
				Usage:   "Named environment to generate data against (defaults to default_env)",
			},
			&cli.StringFlag{
				Name:  "db-url",
				Usage: "Ad-hoc database URL (overrides --env)",
			},
			&cli.StringFlag{
				Name:  "description",
				Usage: "Optional description stored on the new revision manifest",
			},
			&cli.StringFlag{
				Name:  "token",
				Usage: "API token (falls back to ~/.seedmancer/credentials, then SEEDMANCER_API_TOKEN)",
			},
		},
		Action: func(c *cli.Context) error {
			scenarioArg := strings.TrimSpace(c.Args().First())
			if scenarioArg == "" {
				return fmt.Errorf("usage: seedmancer generate <scenario>")
			}
			spinner := ui.StartSpinner("Generating test data…")
			out, err := RunGenerate(c.Context, GenerateInput{
				Prompt:      c.String("prompt"),
				Scenario:    scenarioArg,
				Inherit:     c.String("inherit"),
				Env:         c.String("env"),
				DBURL:       c.String("db-url"),
				Description: c.String("description"),
				Token:       c.String("token"),
			})
			if err != nil {
				spinner.Stop(false, "")
				return err
			}
			spinner.Stop(true, fmt.Sprintf("Created %s @ %s", out.Scenario, out.Revision))
			ui.KeyValue("Schema:  ", out.Schema)
			if len(out.Tables) > 0 {
				ui.KeyValue("Tables:  ", strings.Join(out.Tables, ", "))
			}
			ui.KeyValue("Run:     ", fmt.Sprintf("seedmancer seed %s", out.Scenario))
			return nil
		},
	}
}

// ─── Schema conversion ────────────────────────────────────────────────────────

func buildAPISchema(schemaJSON []byte, excludeTables []string) (generateSchema, error) {
	var raw struct {
		Enums  []db.EnumItem `json:"enums"`
		Tables []db.Table    `json:"tables"`
	}
	if err := json.Unmarshal(schemaJSON, &raw); err != nil {
		return generateSchema{}, fmt.Errorf("parsing schema.json: %v", err)
	}

	excluded := make(map[string]bool, len(excludeTables))
	for _, name := range excludeTables {
		excluded[name] = true
	}

	var apiEnums []generateEnum
	for _, e := range raw.Enums {
		apiEnums = append(apiEnums, generateEnum{Name: e.Name, Values: e.Values})
	}

	var apiTables []generateTable
	for _, t := range raw.Tables {
		if excluded[t.Name] {
			continue
		}
		var cols []generateColumn
		for _, c := range t.Columns {
			col := generateColumn{
				Name:          c.Name,
				Type:          c.Type,
				Nullable:      c.Nullable,
				IsPrimary:     c.IsPrimary,
				IsUnique:      c.IsUnique,
				Enum:          c.Enum,
				AllowedValues: c.AllowedValues,
			}
			if c.Default != nil {
				col.Default = fmt.Sprintf("%v", c.Default)
			}
			if c.ForeignKey != nil {
				col.ForeignKey = &generateForeignKey{
					Table:  c.ForeignKey.Table,
					Column: c.ForeignKey.Column,
				}
			}
			cols = append(cols, col)
		}
		apiTables = append(apiTables, generateTable{Name: t.Name, Columns: cols})
	}

	return generateSchema{Enums: apiEnums, Tables: apiTables}, nil
}

// reorderInsertsByFK parses a full SQL script returned by the AI and reorders
// INSERT statements so that parent tables (referenced by foreign keys) always
// come before child tables. TRUNCATE statements are kept at the top;
// UPDATE/DELETE statements are kept at the bottom after all INSERTs.
//
// Statements are split on semicolons (the standard SQL delimiter), which is
// robust against multi-row INSERTs that span blank lines.
// If the SQL has no FK dependencies or cannot be parsed, it is returned unchanged.
func reorderInsertsByFK(sql string, fkGraph map[string]map[string]struct{}) string {
	insertPattern := regexp.MustCompile(`(?i)^\s*INSERT\s+INTO\s+(?:"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))`)

	// Split into individual statements on semicolons, preserving each statement.
	stmts := splitStatements(sql)
	if len(stmts) == 0 {
		return sql
	}

	var preamble []string
	type namedStmt struct {
		table string
		text  string
	}
	var inserts []namedStmt
	var postamble []string

	for _, stmt := range stmts {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}
		upper := strings.ToUpper(trimmed)
		if m := insertPattern.FindStringSubmatch(trimmed); m != nil {
			table := m[1]
			if table == "" {
				table = m[2]
			}
			inserts = append(inserts, namedStmt{table: table, text: stmt})
		} else if strings.HasPrefix(upper, "UPDATE") || strings.HasPrefix(upper, "DELETE") {
			postamble = append(postamble, stmt)
		} else {
			preamble = append(preamble, stmt)
		}
	}

	if len(inserts) == 0 {
		return sql
	}

	// Build per-table statement list and an ordered table slice (first-seen order).
	seen := make(map[string]bool, len(inserts))
	var tables []string
	stmtMap := make(map[string][]string, len(inserts))
	for _, s := range inserts {
		stmtMap[s.table] = append(stmtMap[s.table], s.text)
		if !seen[s.table] {
			tables = append(tables, s.table)
			seen[s.table] = true
		}
	}

	sorted := topoSort(tables, fkGraph)

	var parts []string
	parts = append(parts, preamble...)
	for _, t := range sorted {
		parts = append(parts, stmtMap[t]...)
	}
	parts = append(parts, postamble...)

	return strings.Join(parts, "\n")
}

// splitStatements splits sql into individual semicolon-terminated statements,
// keeping the trailing semicolon with each statement. It skips over semicolons
// that appear inside single-quoted string literals or line comments.
func splitStatements(sql string) []string {
	var stmts []string
	var cur strings.Builder
	inSingleQuote := false
	inLineComment := false
	runes := []rune(sql)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		// Track line comments (-- ... newline).
		if !inSingleQuote && !inLineComment && i+1 < len(runes) && ch == '-' && runes[i+1] == '-' {
			inLineComment = true
		}
		if inLineComment && ch == '\n' {
			inLineComment = false
		}
		// Track single-quoted strings (escape '' is two single quotes).
		if !inLineComment && ch == '\'' {
			if inSingleQuote && i+1 < len(runes) && runes[i+1] == '\'' {
				// Escaped quote inside string — write both and skip ahead.
				cur.WriteRune(ch)
				i++
				cur.WriteRune(runes[i])
				continue
			}
			inSingleQuote = !inSingleQuote
		}
		cur.WriteRune(ch)
		if !inSingleQuote && !inLineComment && ch == ';' {
			stmts = append(stmts, cur.String())
			cur.Reset()
		}
	}
	if tail := strings.TrimSpace(cur.String()); tail != "" {
		stmts = append(stmts, cur.String())
	}
	return stmts
}

// keep sentinel references so the compiler does not complain about the
// scenario and filepath packages being imported for side-effects only.
var (
	_ = scenario.Normalize
	_ = filepath.Join
	_ = os.Stat
	_ = db.Table{}
)
