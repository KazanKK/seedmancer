package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	db "github.com/KazanKK/seedmancer/database"
	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// SeedCommand restores a revision of a scenario into one or more
// target databases.
//
// Revision resolution rules (highest priority first):
//  1. --revision rNNN
//  2. manifest.latest
func SeedCommand() *cli.Command {
	return &cli.Command{
		Name:      "seed",
		Usage:     "Restore a scenario revision into one or more environments",
		ArgsUsage: "<scenario>",
		Description: "Loads a scenario's CSVs + schema sidecars into each target Postgres\n" +
			"database. The chosen revision is resolved as follows:\n\n" +
			"  --revision rNNN  → exact revision\n" +
			"  (default)        → manifest.latest\n\n" +
			"Targets:\n" +
			"  --env local            single env\n" +
			"  --env local,staging    many envs sequentially\n" +
			"  (no --env)             the default_env in seedmancer.yaml\n\n" +
			"Schema safety: if the database's current schema fingerprint\n" +
			"differs from the revision's, the seed is blocked unless\n" +
			"--force is passed. Use `seedmancer check <scenario>` to see\n" +
			"the diff.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "env",
				Aliases: []string{"e"},
				Usage:   "Comma-separated env names to seed into (e.g. local,staging)",
			},
			&cli.StringFlag{
				Name:  "db-url",
				Usage: "Single ad-hoc target URL (mutually exclusive with --env)",
			},
			&cli.StringFlag{
				Name:    "revision",
				Aliases: []string{"r"},
				Usage:   "Specific revision id (e.g. r002); defaults to latest",
			},
			&cli.BoolFlag{
				Name:    "force",
				Aliases: []string{"f"},
				Usage:   "Seed even when the database schema fingerprint differs",
			},
			&cli.BoolFlag{
				Name:    "yes",
				Aliases: []string{"y"},
				Usage:   "Skip confirmation prompts",
			},
			&cli.BoolFlag{
				Name:  "continue-on-error",
				Usage: "Keep seeding remaining envs after a failure (default: stop)",
			},
		},
		Action: func(c *cli.Context) error {
			scenarioArg := strings.TrimSpace(c.Args().First())
			if scenarioArg == "" {
				return usageError(c, "missing required argument: <scenario>")
			}

			configPath, err := utils.FindConfigFile()
			if err != nil {
				return err
			}
			projectRoot := filepath.Dir(configPath)
			cfg, err := utils.LoadConfig(configPath)
			if err != nil {
				return err
			}

			scenarioPath, err := scenario.Normalize(scenarioArg)
			if err != nil {
				return err
			}

			targets, err := resolveSeedTargets(c, cfg)
			if err != nil {
				return err
			}

			rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, c.String("revision"))
			if err != nil {
				return err
			}

			ui.Step("seed %s @ %s (schema %s) → %s",
				rev.Scenario, rev.RevID,
				utils.FingerprintShort(rev.Manifest.SchemaFingerprint),
				strings.Join(targetNames(targets), ", "))

			schemaDir := scenario.SchemaStoreDir(projectRoot, cfg.StoragePath, utils.FingerprintShort(rev.Manifest.SchemaFingerprint))
			merged, cleanup, err := materializeRestoreDir(schemaDir, rev.DataDir)
			if err != nil {
				return err
			}
			defer cleanup()
			ui.Debug("Merged restore dir: %s", merged)

			// Fingerprint guard runs against each target separately so a
			// matching local env can succeed even if a sibling drifts.
			force := c.Bool("force")
			skipConfirm := c.Bool("yes")
			if !skipConfirm {
				for _, t := range targets {
					dest := targetDisplay(t)
					msg := fmt.Sprintf("Seed %q @ %s into %q?", rev.Scenario, rev.RevID, dest)
					if isProdLike(t.Name) {
						ui.Title(fmt.Sprintf("→ %s", dest))
					}
					if !ui.Confirm(msg, false) {
						ui.Info("Skipped.")
						return nil
					}
				}
			}

			results := make([]seedResult, 0, len(targets))
			for i, t := range targets {
				if i > 0 {
					fmt.Fprintln(os.Stderr)
				}
				if !force {
					if err := guardSchemaMatch(t, rev); err != nil {
						ui.Error("%v", err)
						results = append(results, seedResult{Env: targetDisplay(t), Err: err})
						if !c.Bool("continue-on-error") {
							for _, rest := range targets[i+1:] {
								results = append(results, seedResult{Env: rest.Name, Skipped: true})
							}
							break
						}
						continue
					}
				}
				res := seedOneEnv(t, merged, rev.RevID, rev.Scenario, true)
				results = append(results, res)
				if res.Err != nil && !c.Bool("continue-on-error") {
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

// guardSchemaMatch fingerprints the target database and compares with
// the revision's stored fingerprint. Returns nil when they match (or
// when the target's URL is empty, e.g. a misconfigured ad-hoc env).
//
// On mismatch returns the spec §10 message verbatim so users get the
// same diagnostic whether the failure came from CLI or MCP.
func guardSchemaMatch(t utils.NamedEnv, rev resolvedRevision) error {
	if strings.TrimSpace(t.DatabaseURL) == "" {
		return nil
	}
	currentFP, _, err := fingerprintCurrentDB(t)
	if err != nil {
		return fmt.Errorf("checking schema for %s: %w", targetDisplay(t), err)
	}
	if currentFP == rev.Manifest.SchemaFingerprint {
		return nil
	}
	return fmt.Errorf(
		"schema fingerprint mismatch on %s\n"+
			"  Scenario: %s\n"+
			"  Revision: %s\n"+
			"  Dataset schema: %s\n"+
			"  Current schema: %s\n\n"+
			"The database schema has changed — seeding may fail.\n"+
			"Update the revision to match the current schema:\n"+
			"  seedmancer refresh %s\n"+
			"\n"+
			"Or inspect what changed:\n"+
			"  seedmancer check %s\n"+
			"\n"+
			"Or force seed anyway:\n"+
			"  seedmancer seed %s --force",
		targetDisplay(t), rev.Scenario, rev.RevID,
		utils.FingerprintShort(rev.Manifest.SchemaFingerprint),
		utils.FingerprintShort(currentFP),
		rev.Scenario,
		rev.Scenario, rev.Scenario,
	)
}

func targetNames(targets []utils.NamedEnv) []string {
	out := make([]string, len(targets))
	for i, t := range targets {
		out[i] = t.Name
	}
	return out
}

// seedResult captures the outcome of one target-env seed so the
// summary at the end of `seed --env local,staging` can tell the whole
// story even after a partial failure.
type seedResult struct {
	Env      string
	Err      error
	Duration time.Duration
	Skipped  bool
}

// seedOneEnv applies merged into a single database URL.
func seedOneEnv(target utils.NamedEnv, mergedDir, revID, scenarioPath string, skipConfirm bool) seedResult {
	start := time.Now()

	ui.Title(fmt.Sprintf("→ %s", targetDisplay(target)))

	if !skipConfirm {
		dest := targetDisplay(target)
		msg := fmt.Sprintf("Seed %q @ %s into %q?", scenarioPath, revID, dest)
		if !ui.Confirm(msg, false) {
			return seedResult{Env: targetDisplay(target), Skipped: true, Duration: time.Since(start)}
		}
	}

	// Resolve @env:KEY markers into a per-env temp dir so each env gets its
	// own substituted copies without mutating the shared mergedDir or the
	// original revision CSVs.
	restoreDir, cleanupResolved, err := resolveMarkersDir(mergedDir, target.Values, target.Name)
	if err != nil {
		return seedResult{Env: targetDisplay(target), Err: err, Duration: time.Since(start)}
	}
	defer cleanupResolved()

	manager, normalizedURL, err := db.NewManager(target.DatabaseURL)
	if err != nil {
		return seedResult{Env: targetDisplay(target), Err: err, Duration: time.Since(start)}
	}
	if err := manager.ConnectWithDSN(normalizedURL); err != nil {
		return seedResult{Env: targetDisplay(target), Err: fmt.Errorf("connecting: %v", err), Duration: time.Since(start)}
	}

	sp := ui.StartSpinner("Importing dataset...")
	if err := manager.RestoreFromCSV(restoreDir); err != nil {
		sp.Stop(false, fmt.Sprintf("Import failed (%s)", targetDisplay(target)))
		ui.Error("%v", err)
		return seedResult{Env: targetDisplay(target), Err: err, Duration: time.Since(start)}
	}
	sp.Stop(true, fmt.Sprintf("Seeded %s (%s)", targetDisplay(target), time.Since(start).Round(time.Millisecond)))
	return seedResult{Env: targetDisplay(target), Duration: time.Since(start)}
}

// printSeedSummary renders a table of target outcomes.
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

// materializeRestoreDir builds a single flat temp directory containing
// the schema sidecars (schema.json + *.sql) symlinked in from
// schemaDir and the CSV/JSON files from dataDir. The returned cleanup
// removes the temp dir. When symlinks fail (Windows, exotic
// filesystems) we fall back to copying.
func materializeRestoreDir(schemaDir, dataDir string) (string, func(), error) {
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
	dataFiles, err := utils.DatasetFiles(dataDir)
	if err != nil {
		cleanup()
		return "", func() {}, err
	}
	if len(dataFiles) == 0 {
		cleanup()
		return "", func() {}, fmt.Errorf("no CSV or JSON files in %s", dataDir)
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

// silence unused-import warning
var _ = context.Background
