package cmd

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	db "github.com/KazanKK/seedmancer/database"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// ExportCommand dumps the current database into a fingerprint-keyed folder.
//
// Layout after a successful export:
//
//	<storagePath>/schemas/<fp-short>/
//	  ├── schema.json              # source of truth; fingerprint is SHA-256 of this
//	  ├── *_func.sql / *_trigger.sql  (schema-level sidecars)
//	  └── datasets/<name>/*.csv    # per-dataset payload
//
// The `<fp-short>` folder name is derived from the schema.json fingerprint — two
// exports against the same DB shape always land in the same folder, which keeps
// repeated syncs idempotent.
func ExportCommand() *cli.Command {
	return &cli.Command{
		Name:  "export",
		Usage: "Export current database schema and data as a dataset",
		Description: "Dumps the current database into a content-addressed folder under\n" +
			"<storagePath>/schemas/<fp-short>/. The folder name is derived from\n" +
			"the SHA-256 fingerprint of schema.json, so two exports with the\n" +
			"same shape always land in the same schema folder.\n\n" +
			"Layout after a successful export:\n" +
			"  <storagePath>/schemas/<fp-short>/\n" +
			"    schema.json                         (source of truth)\n" +
			"    *_func.sql / *_trigger.sql         (sidecars)\n" +
			"    datasets/<name>/*.csv               (per-dataset rows)",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "id",
				Usage: "Dataset id for the new dump (defaults to a YYYYMMDDHHMMSS timestamp)",
			},
			&cli.StringFlag{
				Name:    "env",
				Aliases: []string{"e"},
				Usage:   "Named environment to export from (defaults to default_env in seedmancer.yaml)",
			},
			&cli.StringFlag{
				Name:  "db-url",
				Usage: "Source database URL (ad-hoc override; takes precedence over --env)",
			},
			&cli.BoolFlag{
				Name:    "force",
				Aliases: []string{"y"},
				Usage:   "Overwrite an existing dataset without confirmation",
				Value:   false,
			},
		},
		Action: func(c *cli.Context) error {
			configPath, err := utils.FindConfigFile()
			if err != nil {
				return err
			}
			projectRoot := filepath.Dir(configPath)

			cfg, err := utils.LoadConfig(configPath)
			if err != nil {
				return err
			}

			target, err := resolveSingleDB(c, cfg)
			if err != nil {
				return err
			}
			dbURL := target.DatabaseURL
			ui.Step("using env: %s", target.Name)

		datasetName := strings.TrimSpace(c.String("id"))
		if datasetName == "" {
			datasetName = time.Now().UTC().Format("20060102150405")
			ui.Info("Auto-generated dataset id: %s", datasetName)
		}
			datasetName = utils.SanitizeDatasetSegment(datasetName)

			dbURL, scheme, err := normalizePostgresDSN(dbURL)
			if err != nil {
				return err
			}
			if scheme != "postgres" {
				return fmt.Errorf("unsupported database type: %s (only postgres is supported)", scheme)
			}

			pg := &db.PostgresManager{}
			if err := pg.ConnectWithDSN(dbURL); err != nil {
				return fmt.Errorf("connecting to database: %v", err)
			}

			// Phase 1: dump the schema into a temp folder so we can fingerprint
			// it before deciding which on-disk folder it belongs to.
			tmpSchema, err := os.MkdirTemp("", "seedmancer-schema-*")
			if err != nil {
				return fmt.Errorf("creating temp directory: %v", err)
			}
			defer os.RemoveAll(tmpSchema)

			sp := ui.StartSpinner("Exporting schema...")
			if err := pg.ExportSchema(tmpSchema); err != nil {
				sp.Stop(false, "Schema export failed")
				return fmt.Errorf("exporting schema: %v", err)
			}
			sp.Stop(true, "Schema exported")

			fingerprint, err := utils.FingerprintSchemaFile(filepath.Join(tmpSchema, "schema.json"))
			if err != nil {
				return fmt.Errorf("fingerprinting schema: %v", err)
			}
			fpShort := utils.FingerprintShort(fingerprint)

			// Phase 2: materialize the schema folder (or verify the existing one
			// still matches the fingerprint, then refresh its files so hand-edits
			// aren't possible drift sources).
			schemaDir := utils.SchemaDir(projectRoot, cfg.StoragePath, fpShort)
			if err := os.MkdirAll(schemaDir, 0755); err != nil {
				return fmt.Errorf("creating schema directory: %v", err)
			}
			if err := refreshSchemaFolder(tmpSchema, schemaDir); err != nil {
				return err
			}
			ui.KeyValue("Schema fingerprint: ", fpShort)
			ui.Debug("Schema folder: %s", schemaDir)

			// Phase 3: export CSVs into the dataset folder under this schema.
			datasetDir := utils.DatasetPath(projectRoot, cfg.StoragePath, fpShort, datasetName)
			if info, statErr := os.Stat(datasetDir); statErr == nil && info.IsDir() {
				ui.Warn("Dataset %q already exists at %s", datasetName, datasetDir)
				if !c.Bool("force") {
					if !ui.Confirm("Overwrite existing dataset?", false) {
						ui.Info("Export cancelled.")
						return nil
					}
				}
				if err := os.RemoveAll(datasetDir); err != nil {
					return fmt.Errorf("removing existing dataset directory: %v", err)
				}
			}
			if err := os.MkdirAll(datasetDir, 0755); err != nil {
				return fmt.Errorf("creating dataset directory: %v", err)
			}

		sp = ui.StartSpinner("Exporting table data...")
		if err := pg.ExportToCSV(datasetDir); err != nil {
			sp.Stop(false, "Data export failed")
			return fmt.Errorf("exporting data: %v", err)
		}
		sp.Stop(true, "Data exported")

		if err := utils.WriteDatasetMeta(datasetDir, utils.DatasetMeta{SourceEnv: target.Name}); err != nil {
			ui.Warn("could not write dataset metadata: %v", err)
		}

		fmt.Println()
			ui.Success("Export complete")
			ui.KeyValue("Env: ", target.Name)
			ui.KeyValue("Schema: ", fpShort)
			ui.KeyValue("Dataset: ", datasetName)
			ui.KeyValue("Path: ", datasetDir)
			fmt.Println()
			ui.Info("Next: `seedmancer sync --id %s`", datasetName)
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

// normalizePostgresDSN applies the fixups that every command needs:
//   - `postgresql://` → `postgres://` (pgx expects the latter)
//   - appends `sslmode=disable` when the URL is local and sslmode wasn't set
//
// Returns (dsn, scheme, err). scheme is the first URL scheme Seedmancer saw
// so callers can reject non-postgres URLs cleanly.
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
