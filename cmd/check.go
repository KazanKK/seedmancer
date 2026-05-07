package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/schemadiff"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// CheckCommand compares a scenario's stored schema with the current
// database schema. Prints a structured diff so users can decide whether
// they need to re-export.
func CheckCommand() *cli.Command {
	return &cli.Command{
		Name:      "check",
		Usage:     "Compare a scenario revision schema with the current database",
		ArgsUsage: "<scenario>",
		Description: "Loads the schema fingerprint stored on the chosen revision and\n" +
			"compares it with the schema of the live database. Reports added /\n" +
			"removed / changed tables and columns so you know whether seeding\n" +
			"the dataset is still safe.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "env",
				Aliases: []string{"e"},
				Usage:   "Named environment to inspect (defaults to default_env)",
			},
			&cli.StringFlag{
				Name:  "db-url",
				Usage: "Ad-hoc database URL (takes precedence over env)",
			},
			&cli.StringFlag{
				Name:    "revision",
				Aliases: []string{"r"},
				Usage:   "Specific revision id to compare (defaults to latest)",
			},
			&cli.BoolFlag{
				Name:  "stable",
				Usage: "Use the scenario's stable revision",
			},
			&cli.BoolFlag{
				Name:  "json",
				Usage: "Emit JSON for CI/CD pipelines",
			},
		},
		Action: func(c *cli.Context) error {
			scenarioArg := strings.TrimSpace(c.Args().First())
			if scenarioArg == "" {
				return fmt.Errorf("usage: seedmancer check <scenario>")
			}
			out, err := RunCheck(c.Context, CheckInput{
				Scenario:  scenarioArg,
				Revision:  c.String("revision"),
				UseStable: c.Bool("stable"),
				Env:       c.String("env"),
				DBURL:     c.String("db-url"),
			})
			if err != nil {
				return err
			}
			if c.Bool("json") {
				return outputJSON(out)
			}
			ui.Title(fmt.Sprintf("%s @ %s", out.Scenario, out.Revision))
			ui.KeyValue("Status: ", out.Status)
			ui.KeyValue("Dataset schema: ", out.DatasetSchema)
			ui.KeyValue("Current schema: ", out.CurrentSchema)
			if len(out.Changes) == 0 {
				ui.Info("Schemas match — safe to seed.")
				return nil
			}
			fmt.Println()
			ui.Info("Changes:")
			for _, c := range out.Changes {
				ui.KeyValue("  ", c)
			}
			fmt.Println()
			ui.Warn("Recommendation: create a new revision by exporting again:")
			ui.Info("  seedmancer export %s", out.Scenario)
			return nil
		},
	}
}

// CheckInput is the structured input for RunCheck.
type CheckInput struct {
	Scenario  string `json:"scenario" jsonschema:"Scenario path"`
	Revision  string `json:"revision,omitempty" jsonschema:"Specific revision id (defaults to latest)"`
	UseStable bool   `json:"useStable,omitempty" jsonschema:"Use the scenario's stable revision"`
	Env       string `json:"env,omitempty" jsonschema:"Named environment to inspect (defaults to default_env)"`
	DBURL     string `json:"dbUrl,omitempty" jsonschema:"Ad-hoc database URL (takes precedence over env)"`
}

// CheckOutput is the structured response for RunCheck. Status is "ok" when
// fingerprints match, "outdated" when they differ, "noschema" when the
// stored schema.json is missing.
type CheckOutput struct {
	Scenario      string   `json:"scenario"`
	Revision      string   `json:"revision"`
	Status        string   `json:"status"`
	DatasetSchema string   `json:"datasetSchema"`
	CurrentSchema string   `json:"currentSchema"`
	Changes       []string `json:"changes,omitempty"`
}

// RunCheck does the heavy lifting for the `check` command.
func RunCheck(_ context.Context, in CheckInput) (CheckOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return CheckOutput{}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return CheckOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)

	scenarioPath, err := scenario.Normalize(in.Scenario)
	if err != nil {
		return CheckOutput{}, err
	}
	rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, in.Revision, in.UseStable)
	if err != nil {
		return CheckOutput{}, err
	}

	target, err := pickExportTarget(cfg, in.Env, in.DBURL)
	if err != nil {
		return CheckOutput{}, err
	}

	currentFP, currentJSON, err := fingerprintCurrentDB(target)
	if err != nil {
		return CheckOutput{}, err
	}

	out := CheckOutput{
		Scenario:      scenarioPath,
		Revision:      rev.RevID,
		DatasetSchema: utils.FingerprintShort(rev.Manifest.SchemaFingerprint),
		CurrentSchema: utils.FingerprintShort(currentFP),
	}
	if currentFP == rev.Manifest.SchemaFingerprint {
		out.Status = "ok"
		return out, nil
	}

	out.Status = "outdated"

	storedJSONPath := scenario.SchemaJSONPath(projectRoot, cfg.StoragePath, utils.FingerprintShort(rev.Manifest.SchemaFingerprint))
	storedJSON, err := os.ReadFile(storedJSONPath)
	if err != nil {
		// Schemas are content-addressed; missing JSON means it was
		// pruned. Still useful to know the fingerprints differ.
		out.Status = "noschema"
		return out, nil
	}
	changes, err := schemadiff.Diff(storedJSON, currentJSON)
	if err != nil {
		return out, fmt.Errorf("computing diff: %w", err)
	}
	for _, c := range changes {
		out.Changes = append(out.Changes, c.String())
	}
	return out, nil
}
