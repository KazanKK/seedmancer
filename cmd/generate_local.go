package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/KazanKK/seedmancer/internal/ui"
	"github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
)

// GenerateLocalCommand runs an agent-written SQL block on top of an inherit
// base and snapshots the resulting state as a new revision under the named
// scenario. Pipeline:
//
//  1. Seed the inherit base into the configured local env (CSV → COPY,
//     same path as `seedmancer seed`).
//  2. Apply the user-provided SQL inside a single transaction. On failure
//     the env is left at the inherit baseline.
//  3. Export the resulting tables to CSV under a fresh rNNN revision.
//  4. Save the raw SQL alongside the CSVs as dataset.sql so agents can
//     retrieve it later with `seedmancer mcp` get_dataset_sql.
//
// Recommended invocation (SQL piped via stdin so it never touches disk):
//
//	seedmancer generate-local <scenario> --inherit <base-scenario> <<'EOF'
//	DELETE FROM order_items WHERE product_id IN (SELECT id FROM products);
//	DELETE FROM products;
//	INSERT INTO products (id, brand_id, name, price) VALUES (1, 1, 'P1', 9.99);
//	EOF
func GenerateLocalCommand() *cli.Command {
	return &cli.Command{
		Name:      "generate-local",
		Usage:     "Generate a scenario revision from a FULL, idempotent SQL script",
		ArgsUsage: "<scenario>",
		Description: "Seeds an inherit base into the configured local env, applies your SQL\n" +
			"on top of it, then exports the resulting tables back to CSV as a new\n" +
			"rNNN revision. The SQL is stored alongside the CSVs as dataset.sql.\n\n" +
			"The SQL MUST be a FULL, self-contained, idempotent script:\n" +
			"  - every populated table starts with TRUNCATE TABLE <t> RESTART IDENTITY\n" +
			"    CASCADE (or an unconditional DELETE FROM <t>) before its INSERTs,\n" +
			"  - running it twice produces the same DB state,\n" +
			"  - running it alone on an empty migrated schema reproduces the dataset.\n" +
			"Partial / delta scripts are rejected after export with a list of every\n" +
			"populated table missing a leading wipe.\n\n" +
			"Recommended: pipe the SQL via stdin so nothing is written to disk:\n\n" +
			"  seedmancer generate-local billing/pro --inherit basic <<'EOF'\n" +
			"  TRUNCATE TABLE order_items, products, brands\n" +
			"      RESTART IDENTITY CASCADE;\n" +
			"  INSERT INTO brands (id, name) VALUES (1, 'Acme');\n" +
			"  INSERT INTO products (id, brand_id, name, price) VALUES\n" +
			"    (1, 1, 'Product 1', 9.99),\n" +
			"    (2, 1, 'Product 2', 19.98);\n" +
			"  EOF\n\n" +
			"`--inherit` is REQUIRED — it specifies the base scenario whose\n" +
			"latest revision is seeded into the local env before your SQL runs\n" +
			"(safety net, not a data source the SQL relies on).\n" +
			"NOTE: this overwrites data in the configured local env.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "sql-file",
				Aliases: []string{"f"},
				Usage:   "Path to a SQL file (use \"-\" for stdin; omit to read stdin automatically). Project-relative paths are rejected.",
			},
			&cli.StringFlag{
				Name:    "inherit",
				Aliases: []string{"b"},
				Usage:   "REQUIRED. Base scenario whose latest revision is seeded before the SQL runs",
			},
			&cli.StringFlag{
				Name:    "env",
				Aliases: []string{"e"},
				Usage:   "Named environment to seed/export against (defaults to default_env)",
			},
			&cli.StringFlag{
				Name:  "db-url",
				Usage: "Ad-hoc target URL (mutually exclusive with --env)",
			},
			&cli.StringFlag{
				Name:  "description",
				Usage: "Optional description stored on the new revision manifest",
			},
		},
		Action: func(c *cli.Context) error {
			scenarioArg := strings.TrimSpace(c.Args().First())
			if scenarioArg == "" {
				return fmt.Errorf("usage: seedmancer generate-local <scenario>")
			}
			inherit := strings.TrimSpace(c.String("inherit"))
			if inherit == "" {
				return fmt.Errorf(
					"--inherit is required (run `seedmancer export <baseline>` first, then pass --inherit <baseline>)",
				)
			}

			sqlFile := strings.TrimSpace(c.String("sql-file"))
			var sqlBody []byte
			var err error
			switch {
			case sqlFile == "" || sqlFile == "-":
				sqlBody, err = io.ReadAll(os.Stdin)
				if err != nil {
					return fmt.Errorf("reading SQL from stdin: %w", err)
				}
			default:
				if err := rejectProjectRelativeSQLPath(sqlFile); err != nil {
					return err
				}
				sqlBody, err = os.ReadFile(sqlFile)
				if err != nil {
					return fmt.Errorf("reading SQL file %q: %w", sqlFile, err)
				}
			}
			if strings.TrimSpace(string(sqlBody)) == "" {
				return fmt.Errorf("SQL is empty (pass via stdin or --sql-file)")
			}

			out, err := RunGenerateLocal(context.Background(), GenerateLocalInput{
				SQL:         string(sqlBody),
				Scenario:    scenarioArg,
				Inherit:     inherit,
				Env:         strings.TrimSpace(c.String("env")),
				DBURL:       strings.TrimSpace(c.String("db-url")),
				Description: strings.TrimSpace(c.String("description")),
			})
			if err != nil {
				return err
			}

			fmt.Println()
			ui.Success("Generated revision: %s @ %s", out.Scenario, out.Revision)
			ui.KeyValue("Schema: ", out.Schema)
			ui.KeyValue("Tables: ", strings.Join(out.Tables, ", "))
			ui.KeyValue("Inherited from: ", fmt.Sprintf("%s @ %s", out.InheritedFrom, out.InheritedRevision))
			ui.KeyValue("Env used: ", out.Env)
			ui.KeyValue("SQL saved: ", out.SQLPath)
			ui.KeyValue("Run: ", fmt.Sprintf("seedmancer seed %s", out.Scenario))
			return nil
		},
	}
}

// rejectProjectRelativeSQLPath returns an error when sqlFile resolves to
// a path inside the project root (the directory containing seedmancer.yaml).
// This keeps generator SQL files out of the repository — they should live
// in /tmp or be piped via stdin.
func rejectProjectRelativeSQLPath(sqlFile string) error {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return nil
	}
	projectRoot, err := filepath.Abs(filepath.Dir(configPath))
	if err != nil {
		return nil
	}
	abs, err := filepath.Abs(sqlFile)
	if err != nil {
		return nil
	}
	rel, err := filepath.Rel(projectRoot, abs)
	if err != nil {
		return nil
	}
	if rel == "." || (!strings.HasPrefix(rel, "..") && !filepath.IsAbs(rel)) {
		return fmt.Errorf(
			"refusing to read --sql-file %q: path is inside the project (%s).\n"+
				"Generator SQL files are throwaway — pipe via stdin (heredoc) or place the file under /tmp.",
			sqlFile, projectRoot,
		)
	}
	return nil
}
