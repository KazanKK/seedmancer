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

// GenerateCommand runs an AI generation job for a scenario and stores
// the result as a new revision.
//
// The schema is resolved from --inherit (when given), the scenario's
// existing latest revision, or the current database (auto-exported).
func GenerateCommand() *cli.Command {
	return &cli.Command{
		Name:      "generate",
		Usage:     "Generate AI data into a new revision of a scenario",
		ArgsUsage: "<scenario>",
		Description: "Sends the scenario's schema + a natural-language prompt to\n" +
			"Seedmancer's AI generation service, then materialises the\n" +
			"resulting SQL as a new revision under the scenario.\n\n" +
			"The schema is resolved automatically: from --inherit if given,\n" +
			"from the scenario's existing revision, or from the current database.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "prompt",
				Required: true,
				Usage:    "(required) Natural-language description of the data to generate",
			},
			&cli.StringFlag{
				Name:    "inherit",
				Aliases: []string{"b"},
				Usage:   "Scenario whose latest revision provides the schema (optional)",
			},
			&cli.StringFlag{
				Name:    "env",
				Aliases: []string{"e"},
				Usage:   "Named environment to connect to when auto-exporting schema",
			},
			&cli.StringFlag{
				Name:  "db-url",
				Usage: "Ad-hoc database URL when auto-exporting schema (overrides --env)",
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
				return err
			}
			ui.Success("Generated revision: %s @ %s", out.Scenario, out.Revision)
			ui.KeyValue("Schema: ", out.Schema)
			ui.KeyValue("Path: ", out.Path)
			ui.KeyValue("Run: ", fmt.Sprintf("seedmancer seed %s", out.Scenario))
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
// INSERT blocks so that parent tables (referenced by foreign keys) always come
// before child tables. TRUNCATE statements are kept at the top; UPDATE/DELETE
// statements are kept at the bottom after all INSERTs.
//
// Blocks are split on blank lines, matching how the AI formats its output.
// If the SQL has no FK dependencies or cannot be parsed, it is returned unchanged.
func reorderInsertsByFK(sql string, fkGraph map[string]map[string]struct{}) string {
	insertPattern := regexp.MustCompile(`(?i)^\s*INSERT\s+INTO\s+"?([^"\s(]+)"?`)

	blocks := strings.Split(strings.TrimSpace(sql), "\n\n")

	var preamble []string // TRUNCATE and other pre-INSERT statements
	type namedBlock struct {
		table string
		text  string
	}
	var inserts []namedBlock
	var postamble []string // UPDATE/DELETE statements after INSERTs

	for _, block := range blocks {
		trimmed := strings.TrimSpace(block)
		if trimmed == "" {
			continue
		}
		upper := strings.ToUpper(trimmed)
		if m := insertPattern.FindStringSubmatch(trimmed); m != nil {
			inserts = append(inserts, namedBlock{table: m[1], text: trimmed})
		} else if strings.HasPrefix(upper, "UPDATE") || strings.HasPrefix(upper, "DELETE") {
			postamble = append(postamble, trimmed)
		} else {
			preamble = append(preamble, trimmed)
		}
	}

	if len(inserts) == 0 {
		return sql
	}

	// Build per-table block list and ordered table slice (preserving first-seen order).
	seen := make(map[string]bool, len(inserts))
	var tables []string
	blockMap := make(map[string][]string, len(inserts))
	for _, b := range inserts {
		blockMap[b.table] = append(blockMap[b.table], b.text)
		if !seen[b.table] {
			tables = append(tables, b.table)
			seen[b.table] = true
		}
	}

	sorted := topoSort(tables, fkGraph)

	var parts []string
	parts = append(parts, preamble...)
	for _, t := range sorted {
		parts = append(parts, blockMap[t]...)
	}
	parts = append(parts, postamble...)

	return strings.Join(parts, "\n\n")
}

// keep sentinel references so the compiler does not complain about the
// scenario and filepath packages being imported for side-effects only.
var (
	_ = scenario.Normalize
	_ = filepath.Join
	_ = os.Stat
	_ = db.Table{}
)
