package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	db "github.com/KazanKK/seedmancer/database"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// SeedCommand restores a local dataset into one or more target databases.
//
// The multi-target use case is load-bearing: the primary reason for named
// environments is "apply the same dataset to local and staging with one
// command". Passing `--env local,staging` runs the restore sequentially
// against each target, with per-env banners, prod confirmations, and a
// summary at the end.
//
// Implementation note: `RestoreFromCSV` expects `schema.json` (plus any
// `*_func.sql` / `*_trigger.sql` sidecars) to sit next to the CSV files,
// but our on-disk layout keeps them one level up (in the schema folder) so
// multiple datasets can share a schema. We materialize a temp directory
// once, then reuse it for every target to avoid redundant I/O.
func SeedCommand() *cli.Command {
	return &cli.Command{
		Name:  "seed",
		Usage: "Restore a dataset into one or more environments",
		Description: "Loads a local dataset's CSVs + schema sidecars into each target\n" +
			"Postgres database. Tables are truncated and reloaded; functions\n" +
			"and triggers are replayed from their SQL sidecars.\n\n" +
			"Targets:\n" +
			"  --env local           single env\n" +
			"  --env local,staging   many envs, sequentially, same dataset\n" +
			"  (no --env)            the default_env in seedmancer.yaml\n\n" +
			"Ad-hoc override: --db-url <url> or $SEEDMANCER_DATABASE_URL point at\n" +
			"a single target without touching the config.",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "dataset-id",
				Aliases:  []string{"d", "id"},
				Required: true,
				Usage:    "(required) Dataset id to restore (the name given at export/generate time)",
			},
			&cli.StringFlag{
				Name:    "env",
				Aliases: []string{"e"},
				Usage:   "Comma-separated env names to seed into (e.g. local,staging)",
			},
			&cli.StringFlag{
				Name:  "db-url",
				Usage: "Single ad-hoc target URL (mutually exclusive with --env)",
			},
			&cli.BoolFlag{
				Name:    "yes",
				Aliases: []string{"y"},
				Usage:   "Skip confirmation prompts (including prod safety check)",
			},
			&cli.BoolFlag{
				Name:  "continue-on-error",
				Usage: "Keep seeding remaining envs after a failure (default: stop)",
			},
			&cli.BoolFlag{
				Name:  "dry-run",
				Usage: "Resolve envs and print what would run; make no DB changes",
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

			targets, err := resolveSeedTargets(c, cfg)
			if err != nil {
				return err
			}

			datasetName := strings.TrimSpace(c.String("dataset-id"))
			schema, datasetDir, err := utils.FindLocalDataset(projectRoot, cfg.StoragePath, "", datasetName)
			if err != nil {
				return err
			}

			targetNames := make([]string, len(targets))
			for i, t := range targets {
				targetNames[i] = t.Name
			}
			ui.Step("seed %s (schema %s) → %s", datasetName, schema.FingerprintShort, strings.Join(targetNames, ", "))

			if c.Bool("dry-run") {
				ui.Info("dry-run: no databases will be modified")
				for _, t := range targets {
					ui.KeyValue(fmt.Sprintf("  %-12s", t.Name), maskDatabaseURL(t.DatabaseURL))
				}
				return nil
			}

			// Build the merged restore dir ONCE and reuse across targets.
			// Each RestoreFromCSV call is read-only against this dir, so
			// this is both correct and a meaningful perf win when seeding
			// several envs.
			merged, cleanup, err := materializeRestoreDir(schema.Path, datasetDir)
			if err != nil {
				return err
			}
			defer cleanup()
			ui.Debug("Merged restore dir: %s", merged)

			results := make([]seedResult, 0, len(targets))
			for i, t := range targets {
				if i > 0 {
					fmt.Fprintln(os.Stderr)
				}
				res := seedOneEnv(t, merged, datasetName, c.Bool("yes"))
				results = append(results, res)
				if res.Err != nil && !c.Bool("continue-on-error") {
					// Fill remaining targets as "skipped" so the summary
					// tells the whole story instead of pretending the rest
					// succeeded or silently vanished.
					for _, rest := range targets[i+1:] {
						results = append(results, seedResult{Env: rest.Name, Skipped: true})
					}
					break
				}
			}

			fmt.Fprintln(os.Stderr)
			printSeedSummary(results)
			if anyFailed(results) {
				return fmt.Errorf("one or more environments failed to seed")
			}
			return nil
		},
	}
}

// seedResult captures the outcome of one target-env seed so the summary at
// the end of `seed --env local,staging` can tell the whole story even
// after a partial failure.
type seedResult struct {
	Env      string
	Err      error
	Duration time.Duration
	Skipped  bool // true when the user declined the prod prompt or --continue-on-error bailed
}

// seedOneEnv does the work of applying `merged` to a single database URL.
// Extracted so the Action loop stays readable and so tests can exercise
// the per-target orchestration (prompts, duration timing, error shaping)
// independent of the CLI framework.
func seedOneEnv(target utils.NamedEnv, mergedDir, datasetName string, skipConfirm bool) seedResult {
	start := time.Now()

	ui.Title(fmt.Sprintf("→ %s", target.Name))

	if isProdLike(target.Name) && !skipConfirm {
		if !ui.Confirm(fmt.Sprintf("Seed %q into %q?", datasetName, target.Name), false) {
			return seedResult{Env: target.Name, Skipped: true, Duration: time.Since(start)}
		}
	}

	dbURL, scheme, err := normalizePostgresDSN(target.DatabaseURL)
	if err != nil {
		return seedResult{Env: target.Name, Err: err, Duration: time.Since(start)}
	}
	if scheme != "postgres" {
		return seedResult{
			Env:      target.Name,
			Err:      fmt.Errorf("unsupported database type: %s (only postgres is supported)", scheme),
			Duration: time.Since(start),
		}
	}

	pg := &db.PostgresManager{}
	if err := pg.ConnectWithDSN(dbURL); err != nil {
		return seedResult{Env: target.Name, Err: fmt.Errorf("connecting: %v", err), Duration: time.Since(start)}
	}

	sp := ui.StartSpinner("Importing dataset...")
	if err := pg.RestoreFromCSV(mergedDir); err != nil {
		sp.Stop(false, fmt.Sprintf("Import failed (%s)", target.Name))
		return seedResult{Env: target.Name, Err: err, Duration: time.Since(start)}
	}
	sp.Stop(true, fmt.Sprintf("Seeded %s (%s)", target.Name, time.Since(start).Round(time.Millisecond)))
	return seedResult{Env: target.Name, Duration: time.Since(start)}
}

// printSeedSummary renders a table of target outcomes. Matters most after a
// partial failure or `--continue-on-error`, where the user needs a single
// place to see "what worked and what didn't" without re-reading the log.
func printSeedSummary(results []seedResult) {
	if len(results) <= 1 {
		return
	}
	ui.Title("Summary")
	ok, failed, skipped := 0, 0, 0
	for _, r := range results {
		switch {
		case r.Err != nil:
			failed++
			ui.KeyValue(fmt.Sprintf("  %-12s", r.Env), fmt.Sprintf("✗ failed  — %v", r.Err))
		case r.Skipped:
			skipped++
			ui.KeyValue(fmt.Sprintf("  %-12s", r.Env), "— skipped")
		default:
			ok++
			ui.KeyValue(fmt.Sprintf("  %-12s", r.Env), fmt.Sprintf("✓ ok  (%s)", r.Duration.Round(time.Millisecond)))
		}
	}
	ui.Info("%d ok, %d failed, %d skipped", ok, failed, skipped)
}

func anyFailed(results []seedResult) bool {
	for _, r := range results {
		if r.Err != nil {
			return true
		}
	}
	return false
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
