package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	db "github.com/KazanKK/seedmancer/database"
	"github.com/KazanKK/seedmancer/internal/csvxform"
	"github.com/KazanKK/seedmancer/internal/driftreport"
	"github.com/KazanKK/seedmancer/internal/refreshplan"
	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/schemadiff"
	"github.com/KazanKK/seedmancer/internal/ui"
	"github.com/KazanKK/seedmancer/internal/utils"
)

// ─── CLI ─────────────────────────────────────────────────────────────────────

func RefreshCommand() *cli.Command {
	return &cli.Command{
		Name:      "refresh",
		Usage:     "Update a scenario revision to match the current database schema",
		ArgsUsage: "<scenario>",
		Description: "Detects schema drift between a stored revision and the live database,\n" +
			"then creates a new revision whose CSVs conform to the current schema.\n" +
			"The original revision is never modified.\n\n" +
			"Examples:\n" +
			"  seedmancer refresh billing/pro\n" +
			"  seedmancer refresh billing/pro --plan          # preview only\n" +
			"  seedmancer refresh billing/pro --plan-file refresh-plan.json\n" +
			"  seedmancer refresh billing/pro --yes           # auto-apply safe changes only\n" +
			"  seedmancer refresh --all                       # refresh every outdated scenario",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "env", Aliases: []string{"e"}, Usage: "Named environment to connect to"},
			&cli.StringFlag{Name: "db-url", Usage: "Ad-hoc database URL (overrides --env)"},
			&cli.StringFlag{Name: "revision", Aliases: []string{"r"}, Usage: "Base revision to refresh from (defaults to latest)"},
			&cli.BoolFlag{Name: "stable", Usage: "Use the stable revision as base"},
			&cli.BoolFlag{Name: "plan", Usage: "Print the plan and exit without applying"},
			&cli.StringFlag{Name: "plan-file", Usage: "Path to an existing refresh-plan.json to apply"},
			&cli.BoolFlag{Name: "yes", Usage: "Non-interactive: apply Auto changes only; fail if Decision/Breaking remain"},
			&cli.BoolFlag{Name: "ai", Usage: "Send unresolved drift to Seedmancer backend AI for planning (requires Pro)"},
			&cli.BoolFlag{Name: "all", Usage: "Refresh every scenario that has schema drift"},
			&cli.BoolFlag{Name: "json", Usage: "Output JSON"},
		},
		Action: func(c *cli.Context) error {
			if c.Bool("all") {
				return runRefreshAll(c)
			}
			scenarioArg := strings.TrimSpace(c.Args().First())
			if scenarioArg == "" {
				return fmt.Errorf("usage: seedmancer refresh <scenario>")
			}
			return runRefreshOne(c, scenarioArg)
		},
	}
}

func runRefreshOne(c *cli.Context, scenarioArg string) error {
	planFile := c.String("plan-file")

	out, err := RunRefresh(c.Context, RefreshInput{
		Scenario:  scenarioArg,
		Revision:  c.String("revision"),
		UseStable: c.Bool("stable"),
		Env:       c.String("env"),
		DBURL:     c.String("db-url"),
		PlanOnly:  c.Bool("plan"),
		PlanFile:  planFile,
		Yes:       c.Bool("yes"),
		AI:        c.Bool("ai"),
	})
	if err != nil {
		return err
	}

	if c.Bool("json") {
		return outputJSON(out)
	}

	// Human-readable output.
	ui.Title(fmt.Sprintf("refresh %s @ %s", out.Scenario, out.BaseRevision))
	printDriftSummary(out.DriftReport)

	if out.PlanOnly {
		printPlan(out.Plan)
		return nil
	}

	if out.NewRevision == "" {
		ui.Info("No new revision created (nothing to apply).")
		return nil
	}

	ui.Success("Created %s %s", out.Scenario, out.NewRevision)
	ui.Info("Run:  seedmancer seed %s", out.Scenario)
	return nil
}

func runRefreshAll(c *cli.Context) error {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return err
	}
	projectRoot := filepath.Dir(configPath)

	scenarios, _, err := scenario.WalkScenarios(projectRoot, cfg.StoragePath)
	if err != nil {
		return fmt.Errorf("listing scenarios: %w", err)
	}
	if len(scenarios) == 0 {
		ui.Info("No scenarios found.")
		return nil
	}

	var succeeded, skipped, failed []string
	for _, sc := range scenarios {
		out, err := RunRefresh(c.Context, RefreshInput{
			Scenario: sc,
			Env:      c.String("env"),
			DBURL:    c.String("db-url"),
			Yes:      true,
		})
		if err != nil {
			ui.Warn("%s: %v", sc, err)
			failed = append(failed, sc)
			continue
		}
		if !out.DriftReport.HasDrift {
			skipped = append(skipped, sc)
			continue
		}
		if out.NewRevision == "" {
			ui.Warn("%s: drift detected but requires manual intervention", sc)
			failed = append(failed, sc)
			continue
		}
		ui.Success("%s → %s", sc, out.NewRevision)
		succeeded = append(succeeded, sc)
	}

	fmt.Println()
	ui.Info("Refreshed: %d  Skipped (no drift): %d  Failed: %d", len(succeeded), len(skipped), len(failed))
	if len(failed) > 0 {
		return fmt.Errorf("%d scenario(s) need manual attention: %s", len(failed), strings.Join(failed, ", "))
	}
	return nil
}

func printDriftSummary(r driftreport.Report) {
	if !r.HasDrift {
		ui.Success("No schema drift — revision is up to date.")
		return
	}
	ui.Info("Schema drift detected:")
	for cat, n := range r.Counts {
		if n == 0 {
			continue
		}
		symbol := "~"
		switch cat {
		case driftreport.Auto:
			symbol = "✓"
		case driftreport.Likely:
			symbol = "?"
		case driftreport.Decision:
			symbol = "!"
		case driftreport.Breaking:
			symbol = "✗"
		}
		ui.Info("  %s %s: %d change(s)", symbol, cat, n)
	}
}

func printPlan(p refreshplan.Plan) {
	if len(p.Operations) == 0 {
		ui.Info("Plan: (empty — nothing to do)")
		return
	}
	ui.Info("Plan:")
	for i, op := range p.Operations {
		line := fmt.Sprintf("  [%d] %s %s", i+1, op.Op, op.Table)
		if op.Column != "" {
			line += "." + op.Column
		}
		if op.Strategy != "" {
			line += " strategy=" + string(op.Strategy)
		}
		v := op.ValueString()
		if v != "" {
			line += fmt.Sprintf(" value=%q", v)
		}
		if op.Source != "" {
			line += " [" + string(op.Source) + "]"
		}
		ui.Info("%s", line)
	}
}

// ─── Runners ─────────────────────────────────────────────────────────────────

// CheckStateSchemaInput is the input for the MCP tool check_state_schema.
type CheckStateSchemaInput struct {
	Scenario  string `json:"scenario"`
	Revision  string `json:"revision,omitempty"`
	UseStable bool   `json:"useStable,omitempty"`
	Env       string `json:"env,omitempty"`
	DBURL     string `json:"dbUrl,omitempty"`
}

// CheckStateSchemaOutput is the output for check_state_schema — a full
// structured drift report rather than the flat string-list of RunCheck.
type CheckStateSchemaOutput struct {
	driftreport.Report
}

// RunCheckStateSchema returns a structured drift report for a scenario.
func RunCheckStateSchema(_ context.Context, in CheckStateSchemaInput) (CheckStateSchemaOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return CheckStateSchemaOutput{}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return CheckStateSchemaOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)

	scenarioPath, err := scenario.Normalize(in.Scenario)
	if err != nil {
		return CheckStateSchemaOutput{}, err
	}
	rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, in.Revision, in.UseStable)
	if err != nil {
		return CheckStateSchemaOutput{}, err
	}
	target, err := pickExportTarget(cfg, in.Env, in.DBURL)
	if err != nil {
		return CheckStateSchemaOutput{}, err
	}

	currentFP, currentJSON, err := fingerprintCurrentDB(target)
	if err != nil {
		return CheckStateSchemaOutput{}, err
	}

	oldFP := rev.Manifest.SchemaFingerprint
	if currentFP == oldFP {
		return CheckStateSchemaOutput{Report: driftreport.Report{
			Scenario:     scenarioPath,
			BaseRevision: rev.RevID,
			OldSchemaFP:  oldFP,
			NewSchemaFP:  currentFP,
			HasDrift:     false,
		}}, nil
	}

	storedJSONPath := scenario.SchemaJSONPath(projectRoot, cfg.StoragePath, utils.FingerprintShort(oldFP))
	storedJSON, err := os.ReadFile(storedJSONPath)
	if err != nil {
		return CheckStateSchemaOutput{}, fmt.Errorf("stored schema.json not found for fingerprint %s (pruned?): %w", utils.FingerprintShort(oldFP), err)
	}

	changes, err := schemadiff.Diff(storedJSON, currentJSON)
	if err != nil {
		return CheckStateSchemaOutput{}, fmt.Errorf("computing diff: %w", err)
	}

	report := driftreport.Build(scenarioPath, rev.RevID, oldFP, currentFP, changes, storedJSON, currentJSON)
	return CheckStateSchemaOutput{Report: report}, nil
}

// CreateRefreshPlanInput is the input for the MCP tool create_refresh_plan.
type CreateRefreshPlanInput struct {
	Scenario  string `json:"scenario"`
	Revision  string `json:"revision,omitempty"`
	UseStable bool   `json:"useStable,omitempty"`
	Env       string `json:"env,omitempty"`
	DBURL     string `json:"dbUrl,omitempty"`
	// Operations overrides the auto-classified plan. If supplied the classifier
	// is skipped and these operations are validated and returned.
	Operations []refreshplan.Operation `json:"operations,omitempty"`
}

type CreateRefreshPlanOutput struct {
	Plan             refreshplan.Plan            `json:"plan"`
	DriftReport      driftreport.Report          `json:"driftReport"`
	ValidationErrors []refreshplan.ValidationError `json:"validationErrors,omitempty"`
}

// RunCreateRefreshPlan derives a refresh plan from the drift report. When
// Operations is provided the classifier is skipped and those ops are used.
func RunCreateRefreshPlan(ctx context.Context, in CreateRefreshPlanInput) (CreateRefreshPlanOutput, error) {
	csIn := CheckStateSchemaInput{
		Scenario:  in.Scenario,
		Revision:  in.Revision,
		UseStable: in.UseStable,
		Env:       in.Env,
		DBURL:     in.DBURL,
	}
	csOut, err := RunCheckStateSchema(ctx, csIn)
	if err != nil {
		return CreateRefreshPlanOutput{}, err
	}

	configPath, err := utils.FindConfigFile()
	if err != nil {
		return CreateRefreshPlanOutput{}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return CreateRefreshPlanOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)

	scenarioPath, _ := scenario.Normalize(in.Scenario)
	rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, in.Revision, in.UseStable)
	if err != nil {
		return CreateRefreshPlanOutput{}, err
	}

	storedJSONPath := scenario.SchemaJSONPath(projectRoot, cfg.StoragePath, utils.FingerprintShort(rev.Manifest.SchemaFingerprint))
	oldSchemaJSON, _ := os.ReadFile(storedJSONPath)

	target, err := pickExportTarget(cfg, in.Env, in.DBURL)
	if err != nil {
		return CreateRefreshPlanOutput{}, err
	}
	_, newSchemaJSON, err := fingerprintCurrentDB(target)
	if err != nil {
		return CreateRefreshPlanOutput{}, err
	}

	var plan refreshplan.Plan
	if len(in.Operations) > 0 {
		plan = refreshplan.Plan{
			Scenario:                scenarioPath,
			BaseRevision:            rev.RevID,
			TargetSchemaFingerprint: csOut.NewSchemaFP,
			CreatedAt:               time.Now().UTC(),
			PlanSource:              "user",
			Operations:              in.Operations,
		}
	} else {
		plan = refreshplan.Classify(csOut.Report)
		// Apply saved rules from config before returning.
		plan = applySavedRules(plan, cfg.Refresh.Rules)
	}

	validationErrors := refreshplan.Validate(plan, oldSchemaJSON, newSchemaJSON)
	return CreateRefreshPlanOutput{
		Plan:             plan,
		DriftReport:      csOut.Report,
		ValidationErrors: validationErrors,
	}, nil
}

// ValidateRefreshPlanInput is the input for validate_refresh_plan.
type ValidateRefreshPlanInput struct {
	Scenario  string                  `json:"scenario"`
	Revision  string                  `json:"revision,omitempty"`
	UseStable bool                    `json:"useStable,omitempty"`
	Env       string                  `json:"env,omitempty"`
	DBURL     string                  `json:"dbUrl,omitempty"`
	Plan      refreshplan.Plan        `json:"plan"`
}

type ValidateRefreshPlanOutput struct {
	Valid            bool                        `json:"valid"`
	Errors           []refreshplan.ValidationError `json:"errors,omitempty"`
	ErrorSummary     string                      `json:"errorSummary,omitempty"`
}

func RunValidateRefreshPlan(ctx context.Context, in ValidateRefreshPlanInput) (ValidateRefreshPlanOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return ValidateRefreshPlanOutput{}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return ValidateRefreshPlanOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)

	scenarioPath, _ := scenario.Normalize(in.Scenario)
	rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, in.Revision, in.UseStable)
	if err != nil {
		return ValidateRefreshPlanOutput{}, err
	}

	storedJSONPath := scenario.SchemaJSONPath(projectRoot, cfg.StoragePath, utils.FingerprintShort(rev.Manifest.SchemaFingerprint))
	oldSchemaJSON, _ := os.ReadFile(storedJSONPath)

	target, err := pickExportTarget(cfg, in.Env, in.DBURL)
	if err != nil {
		return ValidateRefreshPlanOutput{}, err
	}
	_, newSchemaJSON, err := fingerprintCurrentDB(target)
	if err != nil {
		return ValidateRefreshPlanOutput{}, err
	}

	errs := refreshplan.Validate(in.Plan, oldSchemaJSON, newSchemaJSON)
	return ValidateRefreshPlanOutput{
		Valid:        len(errs) == 0,
		Errors:       errs,
		ErrorSummary: refreshplan.ValidationSummary(errs),
	}, nil
}

// ApplyRefreshPlanInput is the input for apply_refresh_plan.
type ApplyRefreshPlanInput struct {
	Scenario  string               `json:"scenario"`
	Revision  string               `json:"revision,omitempty"`
	UseStable bool                 `json:"useStable,omitempty"`
	Env       string               `json:"env,omitempty"`
	DBURL     string               `json:"dbUrl,omitempty"`
	Plan      refreshplan.Plan     `json:"plan"`
}

type ApplyRefreshPlanOutput struct {
	Scenario     string `json:"scenario"`
	BaseRevision string `json:"baseRevision"`
	NewRevision  string `json:"newRevision"`
	Schema       string `json:"schema"`
	DataDir      string `json:"dataDir"`
}

// RunApplyRefreshPlan transforms the base revision's CSVs using the plan,
// writes the result as a new revision, and advances pointers.latest.
func RunApplyRefreshPlan(ctx context.Context, in ApplyRefreshPlanInput) (ApplyRefreshPlanOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return ApplyRefreshPlanOutput{}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return ApplyRefreshPlanOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)

	scenarioPath, err := scenario.Normalize(in.Scenario)
	if err != nil {
		return ApplyRefreshPlanOutput{}, err
	}

	rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, in.Revision, in.UseStable)
	if err != nil {
		return ApplyRefreshPlanOutput{}, err
	}

	// Obtain current schema JSON for column ordering.
	target, err := pickExportTarget(cfg, in.Env, in.DBURL)
	if err != nil {
		return ApplyRefreshPlanOutput{}, err
	}
	currentFP, currentSchemaJSON, err := fingerprintCurrentDB(target)
	if err != nil {
		return ApplyRefreshPlanOutput{}, err
	}

	// Determine next revision id.
	scenarioDir := scenario.ScenarioDir(projectRoot, cfg.StoragePath, scenarioPath)
	newRevID, err := scenario.NextRevisionID(scenarioDir)
	if err != nil {
		return ApplyRefreshPlanOutput{}, fmt.Errorf("computing next revision id: %w", err)
	}

	newRevDir := scenario.RevisionDir(projectRoot, cfg.StoragePath, scenarioPath, newRevID)
	newDataDir := filepath.Join(newRevDir, "data")

	// Prepare a temp directory for the transformation so a crash doesn't
	// leave a half-written revision.
	tmpDir, err := os.MkdirTemp("", "seedmancer-refresh-*")
	if err != nil {
		return ApplyRefreshPlanOutput{}, fmt.Errorf("creating temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// Apply the plan to the base revision's data directory.
	if err := csvxform.Apply(in.Plan, rev.DataDir, tmpDir, currentSchemaJSON); err != nil {
		return ApplyRefreshPlanOutput{}, fmt.Errorf("applying refresh plan: %w", err)
	}

	// Commit: move temp data into the new revision directory.
	if err := os.MkdirAll(newDataDir, 0755); err != nil {
		return ApplyRefreshPlanOutput{}, fmt.Errorf("creating revision dir: %w", err)
	}

	// Write plan JSON sidecar first (before data so any failure leaves the
	// old revision intact).
	planJSON, err := json.MarshalIndent(in.Plan, "", "  ")
	if err != nil {
		return ApplyRefreshPlanOutput{}, err
	}
	if err := os.WriteFile(filepath.Join(newRevDir, "refresh-plan.json"), planJSON, 0644); err != nil {
		return ApplyRefreshPlanOutput{}, fmt.Errorf("writing refresh-plan.json: %w", err)
	}

	// Copy transformed CSVs from tmpDir to newDataDir.
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		return ApplyRefreshPlanOutput{}, err
	}
	for _, e := range entries {
		src := filepath.Join(tmpDir, e.Name())
		dst := filepath.Join(newDataDir, e.Name())
		if err := copyFilePath(src, dst); err != nil {
			return ApplyRefreshPlanOutput{}, fmt.Errorf("copying %s: %w", e.Name(), err)
		}
	}

	// Preserve dataset.sql from base revision (AI reference — never deleted).
	if srcSQL := DatasetSQLPath(rev.RevDir); fileExists(srcSQL) {
		if err := copyFilePath(srcSQL, DatasetSQLPath(newRevDir)); err != nil {
			return ApplyRefreshPlanOutput{}, fmt.Errorf("copying dataset.sql: %w", err)
		}
	}

	// Write new schema to the schema store.
	fpShort := utils.FingerprintShort(currentFP)
	schemaDir := scenario.SchemaStoreDir(projectRoot, cfg.StoragePath, fpShort)
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		return ApplyRefreshPlanOutput{}, err
	}
	schemaJSONPath := filepath.Join(schemaDir, "schema.json")
	if !fileExists(schemaJSONPath) {
		if err := os.WriteFile(schemaJSONPath, currentSchemaJSON, 0644); err != nil {
			return ApplyRefreshPlanOutput{}, err
		}
	}

	// Count rows for manifest.
	tables, rowCounts, err := listCSVTablesAndRowCounts(newDataDir)
	if err != nil {
		return ApplyRefreshPlanOutput{}, err
	}

	// Write revision manifest.
	revManifest := scenario.RevisionManifest{
		Scenario:          scenarioPath,
		Revision:          newRevID,
		SchemaFingerprint: currentFP,
		CreatedAt:         time.Now().UTC(),
		Source:            "refresh",
		Tables:            tables,
		RowCounts:         rowCounts,
		Services:          []string{"postgres"},
	}
	if err := scenario.WriteRevisionManifest(newRevDir, revManifest); err != nil {
		return ApplyRefreshPlanOutput{}, err
	}

	// Also export function/trigger sidecars so the new revision's schema
	// store is self-contained.
	if err := exportSchemaToStore(target, schemaDir); err != nil {
		// Non-fatal — schema.json is the critical part.
		ui.Debug("refresh: could not export schema sidecars: %v", err)
	}

	// Update scenario manifest and pointers.
	scenarioManifest, _ := scenario.ReadManifest(scenarioDir)
	if scenarioManifest.Scenario == "" {
		scenarioManifest = scenario.Manifest{Scenario: scenarioPath, CreatedAt: time.Now().UTC()}
	}
	scenarioManifest.UpdatedAt = time.Now().UTC()
	scenarioManifest.LatestRevision = newRevID
	if err := scenario.WriteManifest(scenarioDir, scenarioManifest); err != nil {
		return ApplyRefreshPlanOutput{}, err
	}

	pointers, _ := scenario.ReadPointers(scenarioDir)
	pointers.Latest = newRevID
	if err := scenario.WritePointers(scenarioDir, pointers); err != nil {
		return ApplyRefreshPlanOutput{}, err
	}

	return ApplyRefreshPlanOutput{
		Scenario:     scenarioPath,
		BaseRevision: rev.RevID,
		NewRevision:  newRevID,
		Schema:       fpShort,
		DataDir:      newDataDir,
	}, nil
}

// RefreshInput is the combined input for the top-level RunRefresh function.
type RefreshInput struct {
	Scenario  string `json:"scenario"`
	Revision  string `json:"revision,omitempty"`
	UseStable bool   `json:"useStable,omitempty"`
	Env       string `json:"env,omitempty"`
	DBURL     string `json:"dbUrl,omitempty"`
	PlanOnly  bool   `json:"planOnly,omitempty"`
	PlanFile  string `json:"planFile,omitempty"`
	Yes       bool   `json:"yes,omitempty"` // non-interactive
	AI        bool   `json:"ai,omitempty"`  // call backend AI planner (Pro)
}

type RefreshOutput struct {
	Scenario     string             `json:"scenario"`
	BaseRevision string             `json:"baseRevision"`
	NewRevision  string             `json:"newRevision,omitempty"`
	Schema       string             `json:"schema,omitempty"`
	DriftReport  driftreport.Report `json:"driftReport"`
	Plan         refreshplan.Plan   `json:"plan"`
	PlanOnly     bool               `json:"planOnly,omitempty"`
}

// RunRefresh orchestrates the full refresh flow:
//
//	check → classify → (optional prompt/AI) → validate → apply
func RunRefresh(ctx context.Context, in RefreshInput) (RefreshOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return RefreshOutput{}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return RefreshOutput{}, err
	}

	// 1. Check for drift.
	csOut, err := RunCheckStateSchema(ctx, CheckStateSchemaInput{
		Scenario:  in.Scenario,
		Revision:  in.Revision,
		UseStable: in.UseStable,
		Env:       in.Env,
		DBURL:     in.DBURL,
	})
	if err != nil {
		return RefreshOutput{}, err
	}

	out := RefreshOutput{
		Scenario:     csOut.Scenario,
		BaseRevision: csOut.BaseRevision,
		DriftReport:  csOut.Report,
		PlanOnly:     in.PlanOnly,
	}

	if !csOut.HasDrift {
		return out, nil
	}

	// 2. Build the plan — either from file, AI, classifier, or interactive prompt.
	var plan refreshplan.Plan
	if in.PlanFile != "" {
		plan, err = loadPlanFile(in.PlanFile)
		if err != nil {
			return RefreshOutput{}, err
		}
	} else {
		// Start with auto-classification.
		projectRoot := filepath.Dir(configPath)
		scenarioPath, _ := scenario.Normalize(in.Scenario)
		rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, in.Revision, in.UseStable)
		if err != nil {
			return RefreshOutput{}, err
		}
		oldFP := rev.Manifest.SchemaFingerprint
		storedJSONPath := scenario.SchemaJSONPath(projectRoot, cfg.StoragePath, utils.FingerprintShort(oldFP))
		oldSchemaJSON, _ := os.ReadFile(storedJSONPath)

		target, err := pickExportTarget(cfg, in.Env, in.DBURL)
		if err != nil {
			return RefreshOutput{}, err
		}
		_, newSchemaJSON, err := fingerprintCurrentDB(target)
		if err != nil {
			return RefreshOutput{}, err
		}

		plan = refreshplan.Classify(csOut.Report)
		plan = applySavedRules(plan, cfg.Refresh.Rules)

		hasUnresolved := hasUnresolvedOps(plan)

		if hasUnresolved && in.AI {
			plan, err = callAIPlanner(ctx, csOut.Report, oldSchemaJSON, newSchemaJSON, cfg)
			if err != nil {
				ui.Warn("AI planner failed: %v — falling back to interactive", err)
			}
		}

		if hasUnresolved && !in.Yes && !in.AI {
			plan, err = interactiveResolve(plan, csOut.Report)
			if err != nil {
				return RefreshOutput{}, err
			}
		}

		if in.Yes && hasUnresolvedOps(plan) {
			return RefreshOutput{}, fmt.Errorf(
				"schema drift in %s requires manual decisions (Decision/Breaking changes present).\n"+
					"Run without --yes to resolve interactively:\n  seedmancer refresh %s",
				in.Scenario, in.Scenario,
			)
		}

		// Validate the final plan.
		errs := refreshplan.Validate(plan, oldSchemaJSON, newSchemaJSON)
		if len(errs) > 0 {
			return RefreshOutput{}, fmt.Errorf("plan validation failed:\n%s", refreshplan.ValidationSummary(errs))
		}
	}

	out.Plan = plan

	if in.PlanOnly {
		return out, nil
	}

	// 3. Apply the plan.
	applyOut, err := RunApplyRefreshPlan(ctx, ApplyRefreshPlanInput{
		Scenario:  in.Scenario,
		Revision:  in.Revision,
		UseStable: in.UseStable,
		Env:       in.Env,
		DBURL:     in.DBURL,
		Plan:      plan,
	})
	if err != nil {
		return RefreshOutput{}, err
	}

	out.NewRevision = applyOut.NewRevision
	out.Schema = applyOut.Schema
	return out, nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func loadPlanFile(path string) (refreshplan.Plan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return refreshplan.Plan{}, fmt.Errorf("reading plan file %s: %w", path, err)
	}
	var plan refreshplan.Plan
	if err := json.Unmarshal(data, &plan); err != nil {
		return refreshplan.Plan{}, fmt.Errorf("parsing plan file %s: %w", path, err)
	}
	return plan, nil
}

// applySavedRules replaces placeholder operations in a plan with resolved
// operations from the user's saved rules in seedmancer.yaml.
func applySavedRules(plan refreshplan.Plan, rules map[string]utils.RefreshRule) refreshplan.Plan {
	if len(rules) == 0 {
		return plan
	}
	for i, op := range plan.Operations {
		if op.Source != "" {
			continue // already resolved
		}
		key := op.Table + "." + op.Column
		rule, ok := rules[key]
		if !ok {
			continue
		}
		plan.Operations[i].Strategy = refreshplan.Strategy(rule.Strategy)
		if rule.Value != "" {
			plan.Operations[i].Value = refreshplan.StringValue(rule.Value)
		}
		if rule.FromColumn != "" {
			plan.Operations[i].FromColumn = rule.FromColumn
		}
		plan.Operations[i].Source = refreshplan.SourceRule
		plan.Operations[i].Reasoning = "applied saved rule from seedmancer.yaml"
	}
	return plan
}

// hasUnresolvedOps returns true if any operation has an empty Source
// (meaning it still needs a user/AI decision).
func hasUnresolvedOps(plan refreshplan.Plan) bool {
	for _, op := range plan.Operations {
		if op.Source == "" && op.Op != "" {
			return true
		}
	}
	return false
}

// interactiveResolve prompts the user for each unresolved operation.
func interactiveResolve(plan refreshplan.Plan, report driftreport.Report) (refreshplan.Plan, error) {
	_ = report // may be used for context in future

	scanner := bufio.NewScanner(os.Stdin)
	for i, op := range plan.Operations {
		if op.Source != "" || op.Op == "" {
			continue
		}

		fmt.Fprintf(os.Stderr, "\n")
		ui.Warn("Unresolved: %s %s.%s", op.Op, op.Table, op.Column)
		if op.Reasoning != "" {
			ui.Info("  Reason: %s", op.Reasoning)
		}

		ui.Info("  Strategies: constant / empty / uuid / timestamp / derive / skip")
		fmt.Fprintf(os.Stderr, "  Strategy [constant]: ")
		scanner.Scan()
		strategy := strings.TrimSpace(scanner.Text())
		if strategy == "" {
			strategy = "constant"
		}
		if strategy == "skip" {
			plan.Operations[i].Source = "skip"
			continue
		}

		plan.Operations[i].Strategy = refreshplan.Strategy(strategy)
		plan.Operations[i].Source = refreshplan.SourceUser

		if strategy == "constant" || strategy == "derive" {
			prompt := "  Value"
			if strategy == "derive" {
				prompt = "  From column"
			}
			fmt.Fprintf(os.Stderr, "%s: ", prompt)
			scanner.Scan()
			val := strings.TrimSpace(scanner.Text())
			if strategy == "derive" {
				plan.Operations[i].FromColumn = val
			} else {
				plan.Operations[i].Value = refreshplan.StringValue(val)
			}
		}
	}
	return plan, nil
}

// callAIPlanner posts the drift context to the Seedmancer backend and
// retrieves a complete refresh plan. Requires a valid API token (Pro).
func callAIPlanner(ctx context.Context, report driftreport.Report, oldSchemaJSON, newSchemaJSON []byte, cfg utils.Config) (refreshplan.Plan, error) {
	token, err := utils.ResolveAPIToken("")
	if err != nil {
		return refreshplan.Plan{}, fmt.Errorf("AI planner requires a Seedmancer Pro account — run `seedmancer login` first: %w", err)
	}

	// Redact sensitive columns from schema hints before sending.
	oldSchemaJSON = redactSchemaJSON(oldSchemaJSON, cfg.Refresh.RedactColumns)
	newSchemaJSON = redactSchemaJSON(newSchemaJSON, cfg.Refresh.RedactColumns)

	// Build the request body.
	type aiRequest struct {
		Report    driftreport.Report `json:"driftReport"`
		OldSchema json.RawMessage    `json:"oldSchema"`
		NewSchema json.RawMessage    `json:"newSchema"`
	}
	reqBody, err := json.Marshal(aiRequest{
		Report:    report,
		OldSchema: oldSchemaJSON,
		NewSchema: newSchemaJSON,
	})
	if err != nil {
		return refreshplan.Plan{}, err
	}

	baseURL := utils.GetBaseURL()
	endpoint := fmt.Sprintf("%s/v1.0/refresh-plan", baseURL)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return refreshplan.Plan{}, fmt.Errorf("building AI planner request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return refreshplan.Plan{}, fmt.Errorf("AI planner request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		return refreshplan.Plan{}, fmt.Errorf("AI refresh planning requires a Seedmancer Pro subscription")
	}
	if resp.StatusCode == http.StatusServiceUnavailable {
		return refreshplan.Plan{}, fmt.Errorf("AI backend is temporarily unavailable — please try again later")
	}
	if resp.StatusCode != http.StatusOK {
		type apiErr struct {
			Errors []struct {
				Message string `json:"message"`
			} `json:"errors"`
		}
		var aerr apiErr
		if decErr := json.NewDecoder(resp.Body).Decode(&aerr); decErr == nil && len(aerr.Errors) > 0 {
			return refreshplan.Plan{}, fmt.Errorf("AI planner error: %s", aerr.Errors[0].Message)
		}
		return refreshplan.Plan{}, fmt.Errorf("AI planner returned HTTP %d", resp.StatusCode)
	}

	// The backend returns { operations: [...] }; we fill Plan metadata locally.
	type aiResponse struct {
		Operations []refreshplan.Operation `json:"operations"`
	}
	var aiResp aiResponse
	if err := json.NewDecoder(resp.Body).Decode(&aiResp); err != nil {
		return refreshplan.Plan{}, fmt.Errorf("decoding AI planner response: %w", err)
	}

	plan := refreshplan.Plan{
		PlanSource: "ai",
		Operations: aiResp.Operations,
	}
	return plan, nil
}

// redactSchemaJSON strips sensitive column names from schema JSON before it
// leaves the machine. It only removes columns whose names match the builtin
// blocklist or the user's extra patterns.
func redactSchemaJSON(schemaJSON []byte, extras []string) []byte {
	sensitive := []string{"password", "token", "secret", "apikey", "apitoken",
		"accesstoken", "refreshtoken", "privatekey", "secretkey", "credential"}
	for _, e := range extras {
		sensitive = append(sensitive, strings.ToLower(e))
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(schemaJSON, &raw); err != nil {
		return schemaJSON
	}

	tables, _ := raw["tables"].([]interface{})
	for _, t := range tables {
		tbl, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		cols, _ := tbl["columns"].([]interface{})
		for j, c := range cols {
			col, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			name, _ := col["name"].(string)
			lower := strings.ToLower(name)
			for _, s := range sensitive {
				if strings.Contains(lower, s) {
					col["name"] = "[redacted]"
					cols[j] = col
					break
				}
			}
		}
	}

	out, err := json.Marshal(raw)
	if err != nil {
		return schemaJSON
	}
	return out
}

// exportSchemaToStore exports schema sidecars (functions/triggers) to the
// schema store directory. Non-fatal; errors are logged via ui.Debug.
func exportSchemaToStore(target utils.NamedEnv, schemaDir string) error {
	manager, normalizedURL, err := db.NewManager(target.DatabaseURL)
	if err != nil {
		return err
	}
	if err := manager.ConnectWithDSN(normalizedURL); err != nil {
		return err
	}
	return manager.ExportSchema(schemaDir)
}

// copyFilePath copies src to dst, creating the parent directory if needed.
func copyFilePath(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
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
	buf := make([]byte, 32*1024)
	for {
		nr, er := in.Read(buf)
		if nr > 0 {
			if _, ew := out.Write(buf[:nr]); ew != nil {
				return ew
			}
		}
		if er != nil {
			if er.Error() == "EOF" {
				return nil
			}
			return er
		}
	}
}
