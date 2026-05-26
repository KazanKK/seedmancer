package cmd

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/KazanKK/seedmancer/internal/ui"

	"github.com/urfave/cli/v2"
)

// ExportCommand dumps the current database into a brand-new revision of
// the named scenario.
//
// On-disk layout after a successful export:
//
//	<storagePath>/schemas/<fp-short>/
//	  schema.json                   # source of truth for fingerprint
//	  *_func.sql / *_trigger.sql    # function/trigger sidecars
//
//	<storagePath>/scenarios/<scenario>/
//	  manifest.json                 # createdAt/updatedAt/latest/stable
//	  pointers.json                 # { latest, stable }
//	  revisions/<rNNN>/
//	    manifest.json               # rev id, schema fp, source, tables, ...
//	    data/<table>.csv            # CSV payload + service sidecars
//
// Every export creates a new immutable revision. There is no overwrite
// path — the previous revisions stay on disk untouched and `pointers.latest`
// flips to the freshly created one.
func ExportCommand() *cli.Command {
	return &cli.Command{
		Name:      "export",
		Usage:     "Export current database state as a new revision of a scenario",
		ArgsUsage: "<scenario>",
		Description: "Dumps the current database schema + data into a new revision of\n" +
			"the given scenario. Scenario names may be nested with `/`:\n\n" +
			"  seedmancer export basic\n" +
			"  seedmancer export billing/pro\n" +
			"  seedmancer export checkout/payment/failed\n\n" +
			"Each export creates a new revision (r001, r002, ...) under\n" +
			"<storagePath>/scenarios/<scenario>/revisions/. Previous revisions\n" +
			"are never overwritten; the scenario's `latest` pointer always\n" +
			"points to the most recent export.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "env",
				Aliases: []string{"e"},
				Usage:   "Named environment to export from (defaults to default_env in seedmancer.yaml)",
			},
			&cli.StringFlag{
				Name:  "db-url",
				Usage: "Source database URL (ad-hoc override; takes precedence over --env)",
			},
			&cli.StringFlag{
				Name:  "description",
				Usage: "Optional human-readable description stored in the revision manifest",
			},
		},
		Action: func(c *cli.Context) error {
			scenarioArg := strings.TrimSpace(c.Args().First())
			if scenarioArg == "" {
				return fmt.Errorf("usage: seedmancer export <scenario>")
			}

			out, err := RunExport(c.Context, ExportInput{
				Scenario:    scenarioArg,
				Env:         c.String("env"),
				DBURL:       c.String("db-url"),
				Description: c.String("description"),
			})
			if err != nil {
				return err
			}

			fmt.Println()
			ui.Success("Exported scenario: %s", out.Scenario)
			ui.KeyValue("Revision: ", out.Revision)
			ui.KeyValue("Schema fingerprint: ", out.SchemaShort)
			if len(out.Tables) > 0 {
				parts := make([]string, 0, len(out.Tables))
				for _, t := range out.Tables {
					parts = append(parts, fmt.Sprintf("%s(%d)", t, out.RowCounts[t]))
				}
				ui.KeyValue("Tables: ", strings.Join(parts, ", "))
			}
			ui.KeyValue("Latest now points to: ", out.Revision)
			return nil
		},
	}
}

// refreshSchemaFolder copies schema.json (plus any *_func.sql / *_trigger.sql
// sidecars) from the temp dump into the canonical schema folder. Existing
// files are overwritten so a fresh export always wins over stale sidecars.
func refreshSchemaFolder(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("reading temp schema dir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if name != "schema.json" &&
			!strings.HasSuffix(name, "_func.sql") &&
			!strings.HasSuffix(name, "_trigger.sql") {
			continue
		}
		if err := copyFile(filepath.Join(src, name), filepath.Join(dst, name)); err != nil {
			return fmt.Errorf("copying %s: %v", name, err)
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return nil
}

// normalizePostgresDSN is kept for tests (helpers_test.go exercises the
// same fixups) so Postgres URLs from --db-url and seedmancer.yaml
// always reach pgx in the form it expects.
func normalizePostgresDSN(dbURL string) (string, string, error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return "", "", fmt.Errorf("parsing database URL: %v", err)
	}
	scheme := u.Scheme
	if scheme == "postgresql" {
		dbURL = "postgres" + dbURL[len("postgresql"):]
		scheme = "postgres"
	}
	if scheme == "postgres" && !strings.Contains(dbURL, "sslmode=") {
		if strings.Contains(dbURL, "?") {
			dbURL += "&sslmode=disable"
		} else {
			dbURL += "?sslmode=disable"
		}
	}
	return dbURL, scheme, nil
}

// silence unused-import warning when no command body uses ctx directly
var _ = context.Background
