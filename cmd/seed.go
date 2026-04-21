package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	db "github.com/KazanKK/seedmancer/database"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// SeedCommand restores a local dataset into the target database.
//
// `RestoreFromCSV` expects `schema.json` (plus any `*_func.sql` / `*_trigger.sql`
// sidecars) to sit next to the CSV files, but our on-disk layout keeps them
// one level up (in the schema folder) so multiple datasets can share a
// schema. We bridge that by materializing a temp directory that merges
// schema files + CSVs, then point `RestoreFromCSV` at it.
func SeedCommand() *cli.Command {
	return &cli.Command{
		Name:  "seed",
		Usage: "Restore a dataset into the database",
		Description: "Loads a local dataset's CSVs + schema sidecars back into the target\n" +
			"Postgres database. Tables are truncated and reloaded; functions and\n" +
			"triggers are replayed from their SQL sidecars.\n\n" +
			"The target database can come from --db-url, $SEEDMANCER_DATABASE_URL,\n" +
			"or the `database_url:` key in seedmancer.yaml.",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "dataset-id",
				Aliases:  []string{"d", "id"},
				Required: true,
				Usage:    "(required) Dataset id to restore (the name given at export/generate time)",
			},
			&cli.StringFlag{
				Name:    "db-url",
				Usage:   "Target database URL (overrides seedmancer.yaml)",
				EnvVars: []string{"SEEDMANCER_DATABASE_URL"},
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

			dbURL := resolveDatabaseURL(c.String("db-url"), cfg.DatabaseURL)
			if dbURL == "" {
				return fmt.Errorf("database URL required: set `database_url:` in seedmancer.yaml, or use --db-url / SEEDMANCER_DATABASE_URL")
			}

			datasetName := strings.TrimSpace(c.String("dataset-id"))

			schema, datasetDir, err := utils.FindLocalDataset(projectRoot, cfg.StoragePath, "", datasetName)
			if err != nil {
				return err
			}

			ui.Step("Seeding %s  (schema %s)", datasetName, schema.FingerprintShort)

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

			merged, cleanup, err := materializeRestoreDir(schema.Path, datasetDir)
			if err != nil {
				return err
			}
			defer cleanup()

			ui.Debug("Merged restore dir: %s", merged)

			sp := ui.StartSpinner("Importing dataset...")
			if err := pg.RestoreFromCSV(merged); err != nil {
				sp.Stop(false, "Import failed")
				return fmt.Errorf("importing dataset: %v", err)
			}
			sp.Stop(true, fmt.Sprintf("Imported %s", datasetName))
			return nil
		},
	}
}

// materializeRestoreDir builds a single flat temp directory containing the
// schema sidecars (schema.json + *.sql) symlinked in from `schemaDir` and the
// CSV/JSON files from `datasetDir`. The returned cleanup removes the temp dir.
// When symlinks fail (Windows, exotic filesystems) we fall back to copying.
func materializeRestoreDir(schemaDir, datasetDir string) (string, func(), error) {
	tmp, err := os.MkdirTemp("", "seedmancer-restore-*")
	if err != nil {
		return "", func() {}, fmt.Errorf("creating temp dir: %v", err)
	}
	cleanup := func() { _ = os.RemoveAll(tmp) }

	schemaFiles, err := utils.SchemaFiles(schemaDir)
	if err != nil {
		cleanup()
		return "", func() {}, err
	}
	dataFiles, err := utils.DatasetFiles(datasetDir)
	if err != nil {
		cleanup()
		return "", func() {}, err
	}
	if len(dataFiles) == 0 {
		cleanup()
		return "", func() {}, fmt.Errorf("no CSV or JSON files in %s", datasetDir)
	}

	for _, src := range append(append([]string{}, schemaFiles...), dataFiles...) {
		dst := filepath.Join(tmp, filepath.Base(src))
		if err := linkOrCopy(src, dst); err != nil {
			cleanup()
			return "", func() {}, fmt.Errorf("staging %s: %v", src, err)
		}
	}
	return tmp, cleanup, nil
}

func linkOrCopy(src, dst string) error {
	if err := os.Symlink(src, dst); err == nil {
		return nil
	}
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
	_, err = io.Copy(out, in)
	return err
}
