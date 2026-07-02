package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/KazanKK/seedmancer/internal/contract"
	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/schemahistory"
	"github.com/KazanKK/seedmancer/internal/schemadiff"
	"github.com/KazanKK/seedmancer/internal/ui"
	"github.com/KazanKK/seedmancer/internal/usage"
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
				Name:  "json",
				Usage: "Emit JSON for CI/CD pipelines",
			},
		},
		Action: func(c *cli.Context) error {
			scenarioArg := strings.TrimSpace(c.Args().First())
			if scenarioArg == "" {
				return usageError(c, "missing required argument: <scenario>")
			}
			out, err := RunCheck(c.Context, CheckInput{
				Scenario: scenarioArg,
				Revision: c.String("revision"),
				Env:      c.String("env"),
				DBURL:    c.String("db-url"),
			})
			if err != nil {
				return err
			}
			if c.Bool("json") {
				return outputJSON(out)
			}
			ui.Title(fmt.Sprintf("%s @ %s", out.Scenario, out.Revision))
			if out.Purpose != "" {
				ui.KeyValue("Purpose: ", out.Purpose)
			}
			ui.KeyValue("Status: ", out.Status)
			ui.KeyValue("Dataset schema: ", out.DatasetSchema)
			ui.KeyValue("Current schema: ", out.CurrentSchema)
			if out.BehindStr != "" {
				ui.KeyValue("Behind: ", out.BehindStr)
			}
			if len(out.UsedBy) > 0 {
				fmt.Println()
				ui.Info("Used by:")
				for _, r := range out.UsedBy {
					label := fmt.Sprintf("%s > %s", r.File, r.Title)
					if r.Project != "" {
						label += fmt.Sprintf(" [%s]", r.Project)
					}
					ui.KeyValue("  - ", label)
				}
			}
			if len(out.Changes) == 0 {
				fmt.Println()
				ui.Info("Schemas match — safe to seed.")
				return nil
			}
			fmt.Println()
			ui.Info("Changes:")
			for _, c := range out.Changes {
				ui.KeyValue("  ", c)
			}
			if out.Drift != "" && out.Drift != "-" {
				fmt.Println()
				ui.KeyValue("Drift summary: ", out.Drift)
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
	Scenario string `json:"scenario" jsonschema:"Scenario path"`
	Revision string `json:"revision,omitempty" jsonschema:"Specific revision id (defaults to latest)"`
	Env      string `json:"env,omitempty" jsonschema:"Named environment to inspect (defaults to default_env)"`
	DBURL    string `json:"dbUrl,omitempty" jsonschema:"Ad-hoc database URL (takes precedence over env)"`
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
	// Behind is the number of schema versions the dataset is behind the
	// current DB schema. Zero means matched or unknown.
	Behind    int    `json:"behind,omitempty"`
	BehindStr string `json:"behindStr,omitempty"` // "3 schemas", "unknown", or ""
	Drift     string `json:"drift,omitempty"`     // compact diff summary
	// Purpose is the state's contract purpose, when a contract.yaml exists.
	Purpose string `json:"purpose,omitempty"`
	// UsedBy lists the Playwright tests that have seeded this state, from
	// local usage tracking. Empty when no usage has been recorded.
	UsedBy []usage.Ref `json:"usedBy,omitempty"`
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
	rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, in.Revision)
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

	tryUpdateSchemaHistory(projectRoot, cfg.StoragePath, currentFP)

	out := CheckOutput{
		Scenario:      scenarioPath,
		Revision:      rev.RevID,
		DatasetSchema: utils.FingerprintShort(rev.Manifest.SchemaFingerprint),
		CurrentSchema: utils.FingerprintShort(currentFP),
	}
	// Attach local metadata: contract purpose + which tests use this state.
	// Both are best-effort and independent of schema status.
	annotateCheckMeta(&out, projectRoot, cfg.StoragePath, scenarioPath)

	if currentFP == rev.Manifest.SchemaFingerprint {
		out.Status = "ok"
		return out, nil
	}

	out.Status = "outdated"

	// Populate behind/drift using schema history.
	histPath := utils.SchemaHistoryPath(projectRoot, cfg.StoragePath)
	if hist, herr := schemahistory.LoadSchemaHistory(histPath); herr == nil {
		if n, ok := schemahistory.VersionsBehind(hist, rev.Manifest.SchemaFingerprint, currentFP); ok {
			out.Behind = n
			out.BehindStr = pluralSchemas(n)
		} else {
			out.BehindStr = "unknown"
		}
	}

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

	if d, derr := schemahistory.SummarizeSchemaDiff(storedJSON, currentJSON); derr == nil {
		out.Drift = d.String()
	}

	return out, nil
}

// annotateCheckMeta loads the state's contract purpose and recorded test
// usage. Both are local project metadata and best-effort — any failure leaves
// the corresponding field empty without surfacing an error to the user.
func annotateCheckMeta(out *CheckOutput, projectRoot, storagePath, scenarioPath string) {
	scenarioDir := scenario.ScenarioDir(projectRoot, storagePath, scenarioPath)
	if c, ok, err := contract.Load(scenarioDir); err == nil && ok && c != nil {
		out.Purpose = c.Purpose
	}
	if agg, err := usage.Load(projectRoot, storagePath); err == nil {
		if su := agg.States[scenarioPath]; su != nil {
			out.UsedBy = su.UsedBy
		}
	}
}

// pluralSchemas returns "N schema" or "N schemas".
func pluralSchemas(n int) string {
	if n == 1 {
		return "1 schema"
	}
	return fmt.Sprintf("%d schemas", n)
}
