package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	db "github.com/KazanKK/seedmancer/database"
	"github.com/KazanKK/seedmancer/internal/driftreport"
	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/schemadiff"
	"github.com/KazanKK/seedmancer/internal/ui"
	"github.com/KazanKK/seedmancer/internal/utils"
)

// ─── CLI ─────────────────────────────────────────────────────────────────────

func RefreshCommand() *cli.Command {
	return &cli.Command{
		Name:      "refresh",
		Usage:     "Update a scenario revision to match the current database schema using AI",
		ArgsUsage: "<scenario>",
		Description: "Detects schema drift between a stored revision and the live database,\n" +
			"then uses AI to adapt the existing data to the new schema and creates a\n" +
			"new revision. The original revision is never modified.\n\n" +
			"Examples:\n" +
			"  seedmancer refresh billing/pro\n" +
			"  seedmancer refresh billing/pro --yes           # skip confirmation\n" +
			"  seedmancer refresh billing/pro --prompt 'keep user emails realistic'",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "env", Aliases: []string{"e"}, Usage: "Named environment to connect to"},
			&cli.StringFlag{Name: "db-url", Usage: "Ad-hoc database URL (overrides --env)"},
			&cli.StringFlag{Name: "revision", Aliases: []string{"r"}, Usage: "Base revision to refresh from (defaults to latest)"},
			&cli.BoolFlag{Name: "yes", Usage: "Non-interactive: skip confirmation prompt"},
			&cli.StringFlag{Name: "prompt", Usage: "Extra context passed to the AI (e.g. 'keep user names realistic')"},
		},
		Action: func(c *cli.Context) error {
			scenarioArg := strings.TrimSpace(c.Args().First())
			if scenarioArg == "" {
				return usageError(c, "missing required argument: <scenario>")
			}
			return runRefreshOne(c, scenarioArg)
		},
	}
}

func runRefreshOne(c *cli.Context, scenarioArg string) error {
	spinner := ui.StartSpinner(fmt.Sprintf("Checking schema drift for %s…", scenarioArg))
	out, err := RunRefresh(c.Context, RefreshInput{
		Scenario: scenarioArg,
		Revision: c.String("revision"),
		Env:      c.String("env"),
		DBURL:    c.String("db-url"),
		Prompt:   c.String("prompt"),
		Yes:      c.Bool("yes"),
	})
	if err != nil {
		spinner.Stop(false, "")
		if errors.Is(err, utils.ErrNoTables) {
			ui.Warn("Database appears to have no tables — skipping refresh for %q.\nMake sure your migrations have been applied before running refresh.", scenarioArg)
			return nil
		}
		return err
	}
	spinner.Stop(true, fmt.Sprintf("refresh %s @ %s", out.Scenario, out.BaseRevision))

	printDriftSummary(out.DriftReport)

	if !out.DriftReport.HasDrift {
		if out.DriftReport.OldSchemaFP != out.DriftReport.NewSchemaFP {
			if err := syncRevisionFingerprint(out.ProjectRoot, out.StoragePath, out.DriftReport, out.NewSchemaJSON); err != nil {
				ui.Warn("could not update fingerprint: %v", err)
			} else {
				ui.Success("Fingerprint synced (%s → %s)", utils.FingerprintShort(out.DriftReport.OldSchemaFP), utils.FingerprintShort(out.DriftReport.NewSchemaFP))
			}
		}
		return nil
	}

	printSchemaBeforeAfter(out.OldSchemaJSON, out.NewSchemaJSON, out.BaseRevision)

	if !c.Bool("yes") {
		if !confirmApply(out.Scenario) {
			ui.Info("Aborted.")
			return nil
		}
	}

	spinner = ui.StartSpinner("Generating refreshed data with AI…")
	genResult, err := runGenerateRefreshSQL(c.Context, applyAIRefreshInput{
		Scenario:      scenarioArg,
		Revision:      c.String("revision"),
		Env:           c.String("env"),
		DBURL:         c.String("db-url"),
		Prompt:        c.String("prompt"),
		DriftReport:   out.DriftReport,
		NewSchemaJSON: out.NewSchemaJSON,
	})
	if err != nil {
		spinner.Stop(false, "")
		return err
	}
	spinner.Stop(true, "Test data generated")

	printSQLSummary(genResult.existingSQL, genResult.generatedSQL)

	if !c.Bool("yes") {
		if !confirmApplySQL() {
			ui.Info("Aborted. No changes applied.")
			return nil
		}
	}

	spinner = ui.StartSpinner("Applying refreshed data…")
	applyOut, err := runApplyGeneratedSQL(c.Context, genResult)
	if err != nil {
		spinner.Stop(false, "")
		return err
	}
	spinner.Stop(true, fmt.Sprintf("Created %s %s", out.Scenario, applyOut.NewRevision))
	ui.Info("Run:  seedmancer seed %s", out.Scenario)
	return nil
}

func printDriftSummary(r driftreport.Report) {
	if !r.HasDrift {
		ui.Success("No schema drift — revision is up to date.")
		return
	}

	// One compact headline: "Schema drift  ✗ 1 breaking  · ! 4 decision  · ✓ 7 auto"
	type badge struct {
		cat driftreport.Category
		sym string
	}
	order := []badge{
		{driftreport.Breaking, "✗"},
		{driftreport.Decision, "!"},
		{driftreport.Likely, "?"},
		{driftreport.Auto, "✓"},
	}
	var parts []string
	for _, b := range order {
		n := r.Counts[b.cat]
		if n == 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s %d %s", b.sym, n, b.cat))
	}
	fmt.Fprintf(os.Stderr, "\n  Schema drift  %s\n", strings.Join(parts, "  · "))
}

// printSchemaBeforeAfter renders a per-row arrow-style schema diff with ANSI colors.
// Each row shows:  <bold name>  <dim cols>  →  <bold name>  <dim cols>  <tag>
// The → arrows are vertically aligned by padding the before-side to a fixed width.
func printSchemaBeforeAfter(oldJSON, newJSON []byte, baseRev string) {
	if len(oldJSON) == 0 || len(newJSON) == 0 {
		return
	}
	var oldSchema, newSchema utils.SchemaJSON
	if err := json.Unmarshal(oldJSON, &oldSchema); err != nil {
		return
	}
	if err := json.Unmarshal(newJSON, &newSchema); err != nil {
		return
	}

	// Build lookup maps.
	oldByName := make(map[string]utils.SchemaTable, len(oldSchema.Tables))
	for _, t := range oldSchema.Tables {
		oldByName[t.Name] = t
	}
	newByName := make(map[string]utils.SchemaTable, len(newSchema.Tables))
	for _, t := range newSchema.Tables {
		newByName[t.Name] = t
	}

	// Union of all table names: old tables first, then new-only tables.
	seen := make(map[string]struct{})
	var allNames []string
	for _, t := range oldSchema.Tables {
		seen[t.Name] = struct{}{}
		allNames = append(allNames, t.Name)
	}
	for _, t := range newSchema.Tables {
		if _, ok := seen[t.Name]; !ok {
			allNames = append(allNames, t.Name)
		}
	}
	if len(allNames) == 0 {
		return
	}

	// colList builds a space-separated list of column names, truncated to maxW
	// visible characters (no ANSI) with … if too long.
	const colMaxW = 38
	colList := func(t utils.SchemaTable) string {
		if len(t.Columns) == 0 {
			return ""
		}
		var names []string
		for _, c := range t.Columns {
			names = append(names, c.Name)
		}
		s := strings.Join(names, "  ")
		if len(s) > colMaxW {
			s = s[:colMaxW-1] + "…"
		}
		return s
	}

	// Compute the max width of the "before" side (visible chars, no ANSI) so
	// we can pad every row to align the → arrows in a single column.
	// before-side = 2-space indent + name + 2 gap + cols
	nameW := 0
	for _, n := range allNames {
		if len(n) > nameW {
			nameW = len(n)
		}
	}
	// Measure max before-side visible width across all rows.
	beforeW := 0
	for _, name := range allNames {
		oldT, inOld := oldByName[name]
		var visW int
		if inOld {
			cols := colList(oldT)
			if cols != "" {
				visW = nameW + 2 + len(cols)
			} else {
				visW = nameW
			}
		} else {
			visW = 0 // new-only row: before side is blank
		}
		if visW > beforeW {
			beforeW = visW
		}
	}
	const minBeforeW = 20
	if beforeW < minBeforeW {
		beforeW = minBeforeW
	}

	// Title.
	title := fmt.Sprintf("Schema changes (%s → live):", baseRev)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s\n", ui.Bold(title))
	fmt.Fprintf(os.Stderr, "  %s\n", ui.Dim(strings.Repeat("─", len(title))))

	for _, name := range allNames {
		oldT, inOld := oldByName[name]
		newT, inNew := newByName[name]

		// Build the before-side string (plain, for padding) and after-side.
		var beforePlain string // used only for length measurement
		var beforeFormatted string
		var afterFormatted string

		switch {
		case inOld && inNew:
			cols := colList(oldT)
			if cols != "" {
				beforePlain = fmt.Sprintf("%-*s  %s", nameW, name, cols)
				beforeFormatted = fmt.Sprintf("%s  %s", ui.Bold(fmt.Sprintf("%-*s", nameW, name)), ui.Dim(cols))
			} else {
				beforePlain = fmt.Sprintf("%-*s", nameW, name)
				beforeFormatted = ui.Bold(fmt.Sprintf("%-*s", nameW, name))
			}

			newCols := colList(newT)
			oldCount, newCount := len(oldT.Columns), len(newT.Columns)
			var tag string
			switch {
			case newCount > oldCount:
				tag = "  " + ui.Green(fmt.Sprintf("[+%d col]", newCount-oldCount))
			case newCount < oldCount:
				tag = "  " + ui.Red(fmt.Sprintf("[-%d col]", oldCount-newCount))
			}
			if newCols != "" {
				afterFormatted = fmt.Sprintf("%s  %s%s", ui.Bold(fmt.Sprintf("%-*s", nameW, name)), ui.Dim(newCols), tag)
			} else {
				afterFormatted = ui.Bold(fmt.Sprintf("%-*s", nameW, name)) + tag
			}

		case inOld && !inNew:
			cols := colList(oldT)
			if cols != "" {
				beforePlain = fmt.Sprintf("%-*s  %s", nameW, name, cols)
				beforeFormatted = fmt.Sprintf("%s  %s", ui.Bold(fmt.Sprintf("%-*s", nameW, name)), ui.Dim(cols))
			} else {
				beforePlain = fmt.Sprintf("%-*s", nameW, name)
				beforeFormatted = ui.Bold(fmt.Sprintf("%-*s", nameW, name))
			}
			afterFormatted = ui.Red("(removed)")

		case !inOld && inNew:
			beforePlain = strings.Repeat(" ", nameW)
			beforeFormatted = strings.Repeat(" ", nameW)
			newCols := colList(newT)
			tag := "  " + ui.Green("[new]")
			if newCols != "" {
				afterFormatted = fmt.Sprintf("%s  %s%s", ui.Bold(fmt.Sprintf("%-*s", nameW, name)), ui.Dim(newCols), tag)
			} else {
				afterFormatted = ui.Bold(fmt.Sprintf("%-*s", nameW, name)) + tag
			}
		}

		// Pad before side so arrows align.
		pad := beforeW - len(beforePlain)
		if pad < 0 {
			pad = 0
		}
		fmt.Fprintf(os.Stderr, "  %s%s  %s  %s\n",
			beforeFormatted,
			strings.Repeat(" ", pad),
			ui.Dim("→"),
			afterFormatted,
		)
	}
	fmt.Fprintln(os.Stderr)
}

func confirmApply(scenarioName string) bool {
	fmt.Fprintf(os.Stderr, "\nRefresh %q with AI? [y/N]: ", scenarioName)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	ans := strings.TrimSpace(strings.ToLower(scanner.Text()))
	return ans == "y" || ans == "yes"
}

func confirmApplySQL() bool {
	fmt.Fprintf(os.Stderr, "Apply refreshed test data? [y/N]: ")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	ans := strings.TrimSpace(strings.ToLower(scanner.Text()))
	return ans == "y" || ans == "yes"
}

// ─── Runners ─────────────────────────────────────────────────────────────────

// CheckStateSchemaInput is the input for the MCP tool check_state_schema.
type CheckStateSchemaInput struct {
	Scenario string `json:"scenario"`
	Revision string `json:"revision,omitempty"`
	Env      string `json:"env,omitempty"`
	DBURL    string `json:"dbUrl,omitempty"`
}

// CheckStateSchemaOutput is the output for check_state_schema.
type CheckStateSchemaOutput struct {
	driftreport.Report
	// NewSchemaJSON is the current DB schema — included so callers can pass
	// it straight to apply_ai_refresh without a second DB round-trip.
	NewSchemaJSON []byte `json:"-"`
	// OldSchemaJSON is the stored schema from the last snapshot — used for
	// before/after display.
	OldSchemaJSON []byte `json:"-"`
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
	rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, in.Revision)
	if err != nil {
		return CheckStateSchemaOutput{}, err
	}
	target, err := pickExportTarget(cfg, in.Env, in.DBURL)
	if err != nil {
		return CheckStateSchemaOutput{}, err
	}

	// rawFP is always the full-schema fingerprint — this is what gets stored in
	// revision manifests and what seed/check use for comparison.
	rawFP, currentJSON, err := fingerprintCurrentDB(target)
	if err != nil {
		return CheckStateSchemaOutput{}, err
	}

	oldFP := rev.Manifest.SchemaFingerprint
	if rawFP == oldFP {
		return CheckStateSchemaOutput{
			Report: driftreport.Report{
				Scenario:     scenarioPath,
				BaseRevision: rev.RevID,
				OldSchemaFP:  oldFP,
				NewSchemaFP:  rawFP,
				HasDrift:     false,
			},
			NewSchemaJSON: currentJSON,
		}, nil
	}

	storedJSONPath := scenario.SchemaJSONPath(projectRoot, cfg.StoragePath, utils.FingerprintShort(oldFP))
	storedJSON, err := os.ReadFile(storedJSONPath)
	if err != nil {
		return CheckStateSchemaOutput{}, fmt.Errorf("stored schema.json not found for fingerprint %s (pruned?): %w", utils.FingerprintShort(oldFP), err)
	}

	// Strip excluded tables from both sides for display/diff only.
	// The raw FP is preserved above; stripping is purely cosmetic so that
	// framework-managed tables (e.g. _prisma_migrations) never appear in the
	// drift output or the before/after visual.
	diffCurrentJSON, err := stripExcludedTables(currentJSON, cfg.ExcludeTables)
	if err != nil {
		return CheckStateSchemaOutput{}, fmt.Errorf("filtering excluded tables from current schema: %w", err)
	}
	diffStoredJSON, err := stripExcludedTables(storedJSON, cfg.ExcludeTables)
	if err != nil {
		return CheckStateSchemaOutput{}, fmt.Errorf("filtering excluded tables from stored schema: %w", err)
	}

	changes, err := schemadiff.Diff(diffStoredJSON, diffCurrentJSON)
	if err != nil {
		return CheckStateSchemaOutput{}, fmt.Errorf("computing diff: %w", err)
	}

	// Pass rawFP as NewSchemaFP so it flows through to the revision manifest.
	report := driftreport.Build(scenarioPath, rev.RevID, oldFP, rawFP, changes, diffStoredJSON, diffCurrentJSON)
	return CheckStateSchemaOutput{Report: report, NewSchemaJSON: diffCurrentJSON, OldSchemaJSON: diffStoredJSON}, nil
}

// RefreshInput is the input for the drift-check phase of RunRefresh.
type RefreshInput struct {
	Scenario string `json:"scenario"`
	Revision string `json:"revision,omitempty"`
	Env      string `json:"env,omitempty"`
	DBURL    string `json:"dbUrl,omitempty"`
	Prompt   string `json:"prompt,omitempty"`
	Yes      bool   `json:"yes,omitempty"`
}

// RefreshOutput is returned after the drift-check phase.
type RefreshOutput struct {
	Scenario      string             `json:"scenario"`
	BaseRevision  string             `json:"baseRevision"`
	NewRevision   string             `json:"newRevision,omitempty"`
	Schema        string             `json:"schema,omitempty"`
	DriftReport   driftreport.Report `json:"driftReport"`
	OldSchemaJSON []byte             `json:"-"`
	NewSchemaJSON []byte             `json:"-"`
	// ProjectRoot and StoragePath are populated so callers (e.g. runRefreshOne)
	// can perform follow-up writes like syncing a stale fingerprint.
	ProjectRoot string `json:"-"`
	StoragePath string `json:"-"`
}

// RunRefresh performs the drift-check phase and returns a RefreshOutput.
// When called via MCP, follow up with RunApplyAIRefresh to complete the refresh.
func RunRefresh(ctx context.Context, in RefreshInput) (RefreshOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return RefreshOutput{}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return RefreshOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)

	csOut, err := RunCheckStateSchema(ctx, CheckStateSchemaInput{
		Scenario: in.Scenario,
		Revision: in.Revision,
		Env:      in.Env,
		DBURL:    in.DBURL,
	})
	if err != nil {
		return RefreshOutput{}, err
	}
	return RefreshOutput{
		Scenario:      csOut.Scenario,
		BaseRevision:  csOut.BaseRevision,
		DriftReport:   csOut.Report,
		OldSchemaJSON: csOut.OldSchemaJSON,
		NewSchemaJSON: csOut.NewSchemaJSON,
		ProjectRoot:   projectRoot,
		StoragePath:   cfg.StoragePath,
	}, nil
}

// ApplyAIRefreshInput is the input for the apply phase of an AI refresh.
type ApplyAIRefreshInput struct {
	Scenario string `json:"scenario"`
	Revision string `json:"revision,omitempty"`
	Env      string `json:"env,omitempty"`
	DBURL    string `json:"dbUrl,omitempty"`
	Prompt   string `json:"prompt,omitempty"`
	Token    string `json:"token,omitempty"`
}

// ApplyAIRefreshOutput is the result of the apply phase.
type ApplyAIRefreshOutput struct {
	Scenario     string `json:"scenario"`
	BaseRevision string `json:"baseRevision"`
	NewRevision  string `json:"newRevision"`
	Schema       string `json:"schema"`
	DataDir      string `json:"dataDir"`
}

// applyAIRefreshInput is the internal full input (includes pre-computed drift).
type applyAIRefreshInput struct {
	Scenario      string
	Revision      string
	Env           string
	DBURL         string
	Prompt        string
	Token         string
	DriftReport   driftreport.Report
	NewSchemaJSON []byte
}

// RunApplyAIRefresh is the MCP-callable variant that re-runs drift detection
// internally. Use runApplyAIRefresh directly from CLI to avoid a second
// DB round-trip.
func RunApplyAIRefresh(ctx context.Context, in ApplyAIRefreshInput) (ApplyAIRefreshOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return ApplyAIRefreshOutput{}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return ApplyAIRefreshOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)

	csOut, err := RunCheckStateSchema(ctx, CheckStateSchemaInput{
		Scenario: in.Scenario,
		Revision: in.Revision,
		Env:      in.Env,
		DBURL:    in.DBURL,
	})
	if err != nil {
		return ApplyAIRefreshOutput{}, err
	}
	if !csOut.HasDrift {
		if csOut.OldSchemaFP != csOut.NewSchemaFP {
			if syncErr := syncRevisionFingerprint(projectRoot, cfg.StoragePath, csOut.Report, csOut.NewSchemaJSON); syncErr != nil {
				return ApplyAIRefreshOutput{}, fmt.Errorf("syncing fingerprint: %w", syncErr)
			}
		}
		return ApplyAIRefreshOutput{}, fmt.Errorf("no schema drift detected for %s — nothing to refresh", in.Scenario)
	}
	return runApplyAIRefresh(ctx, applyAIRefreshInput{
		Scenario:      in.Scenario,
		Revision:      in.Revision,
		Env:           in.Env,
		DBURL:         in.DBURL,
		Prompt:        in.Prompt,
		Token:         in.Token,
		DriftReport:   csOut.Report,
		NewSchemaJSON: csOut.NewSchemaJSON,
	})
}

// generateRefreshResult carries all state produced by runGenerateRefreshSQL
// so that runApplyGeneratedSQL can continue without re-reading config or the DB.
type generateRefreshResult struct {
	generatedSQL  string
	existingSQL   string
	scenarioDir   string
	projectRoot   string
	scenarioPath  string
	cfg           utils.Config
	rev           resolvedRevision
	target        utils.NamedEnv
	newSchemaJSON []byte
	driftReport   driftreport.Report
}

// runGenerateRefreshSQL covers steps 1–4: resolve revision, load existing SQL,
// build schema, call the AI backend, and save refresh-draft.sql.
func runGenerateRefreshSQL(ctx context.Context, in applyAIRefreshInput) (generateRefreshResult, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return generateRefreshResult{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return generateRefreshResult{}, err
	}

	scenarioPath, err := scenario.Normalize(in.Scenario)
	if err != nil {
		return generateRefreshResult{}, err
	}

	rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, in.Revision)
	if err != nil {
		return generateRefreshResult{}, err
	}

	// ── 1. Load existing SQL from the revision's dataset.sql ──────────────────
	existingSQL, err := loadExistingSQL(rev.RevDir, scenarioPath, rev.RevID)
	if err != nil {
		return generateRefreshResult{}, err
	}

	// ── 2. Build schema diff text ─────────────────────────────────────────────
	schemaDiff := buildSchemaDiffText(in.DriftReport)

	// ── 3. Build API schema from new schema JSON ──────────────────────────────
	apiSchema, err := buildAPISchema(in.NewSchemaJSON, cfg.ExcludeTables)
	if err != nil {
		return generateRefreshResult{}, fmt.Errorf("parsing new schema: %w", err)
	}

	// ── 4. Call /generate-refresh-sql backend ─────────────────────────────────
	token, err := utils.ResolveAPIToken(in.Token)
	if err != nil {
		return generateRefreshResult{}, fmt.Errorf("AI refresh requires a Seedmancer account — run `seedmancer login` first: %w", err)
	}

	// Send the scenario's saved purpose so the adapted data keeps its
	// original intent; --prompt rides along as per-run extra instructions.
	scenarioDir := scenario.ScenarioDir(projectRoot, cfg.StoragePath, scenarioPath)
	var purpose string
	if m, mErr := scenario.ReadManifest(scenarioDir); mErr == nil {
		purpose = strings.TrimSpace(m.Prompt)
	}

	generatedSQL, err := callGenerateRefreshSQL(ctx, token, apiSchema, schemaDiff, existingSQL, purpose, in.Prompt)
	if err != nil {
		return generateRefreshResult{}, err
	}

	// Save generated SQL as a draft immediately so it is inspectable even if
	// execution fails. Overwritten on every refresh attempt.
	draftPath := filepath.Join(scenarioDir, "refresh-draft.sql")
	_ = os.WriteFile(draftPath, []byte(generatedSQL), 0644)

	target, err := pickExportTarget(cfg, in.Env, in.DBURL)
	if err != nil {
		return generateRefreshResult{}, err
	}

	return generateRefreshResult{
		generatedSQL:  generatedSQL,
		existingSQL:   existingSQL,
		scenarioDir:   scenarioDir,
		projectRoot:   projectRoot,
		scenarioPath:  scenarioPath,
		cfg:           cfg,
		rev:           rev,
		target:        target,
		newSchemaJSON: in.NewSchemaJSON,
		driftReport:   in.DriftReport,
	}, nil
}

// runApplyGeneratedSQL covers steps 5–7: connect to DB, execute SQL, export
// CSVs, and write the new revision manifest + pointers.
func runApplyGeneratedSQL(ctx context.Context, r generateRefreshResult) (ApplyAIRefreshOutput, error) {
	manager, normalizedURL, err := db.NewManager(r.target.DatabaseURL)
	if err != nil {
		return ApplyAIRefreshOutput{}, err
	}
	if err := manager.ConnectWithDSN(normalizedURL); err != nil {
		return ApplyAIRefreshOutput{}, fmt.Errorf("connecting to database: %w", err)
	}

	if err := manager.ExecSQL(r.generatedSQL); err != nil {
		return ApplyAIRefreshOutput{}, fmt.Errorf("executing AI-generated SQL: %w", err)
	}

	// ── 6. Export updated tables to CSV as a new revision ─────────────────────
	newRevID, err := scenario.NextRevisionID(r.scenarioDir)
	if err != nil {
		return ApplyAIRefreshOutput{}, fmt.Errorf("computing next revision id: %w", err)
	}

	newRevDir := scenario.RevisionDir(r.projectRoot, r.cfg.StoragePath, r.scenarioPath, newRevID)
	newDataDir := filepath.Join(newRevDir, "data")
	if err := os.MkdirAll(newDataDir, 0755); err != nil {
		return ApplyAIRefreshOutput{}, fmt.Errorf("creating revision dir: %w", err)
	}

	if err := manager.ExportToCSV(newDataDir); err != nil {
		return ApplyAIRefreshOutput{}, fmt.Errorf("exporting refreshed data: %w", err)
	}

	// ── 7. Save dataset.sql, schema store, manifest, pointers ─────────────────
	currentFP := r.driftReport.NewSchemaFP
	fpShort := utils.FingerprintShort(currentFP)
	schemaDir := scenario.SchemaStoreDir(r.projectRoot, r.cfg.StoragePath, fpShort)
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		return ApplyAIRefreshOutput{}, err
	}
	schemaJSONPath := filepath.Join(schemaDir, "schema.json")
	if !fileExists(schemaJSONPath) {
		if err := os.WriteFile(schemaJSONPath, r.newSchemaJSON, 0644); err != nil {
			return ApplyAIRefreshOutput{}, err
		}
	}

	if err := exportSchemaToStore(r.target, schemaDir); err != nil {
		ui.Debug("refresh: could not export schema sidecars: %v", err)
	}

	if err := os.WriteFile(DatasetSQLPath(newRevDir), []byte(r.generatedSQL), 0644); err != nil {
		return ApplyAIRefreshOutput{}, fmt.Errorf("writing dataset.sql: %w", err)
	}

	tables, rowCounts, err := listCSVTablesAndRowCounts(newDataDir)
	if err != nil {
		return ApplyAIRefreshOutput{}, err
	}

	revManifest := scenario.RevisionManifest{
		Scenario:          r.scenarioPath,
		Revision:          newRevID,
		SchemaFingerprint: currentFP,
		CreatedAt:         time.Now().UTC(),
		Source:            "refresh",
		Tables:            tables,
		RowCounts:         rowCounts,
		Services:          []string{"postgres"},
	}
	if err := scenario.WriteRevisionManifest(newRevDir, revManifest); err != nil {
		return ApplyAIRefreshOutput{}, err
	}

	scenarioManifest, _ := scenario.ReadManifest(r.scenarioDir)
	if scenarioManifest.Scenario == "" {
		scenarioManifest = scenario.Manifest{Scenario: r.scenarioPath, CreatedAt: time.Now().UTC()}
	}
	scenarioManifest.UpdatedAt = time.Now().UTC()
	scenarioManifest.Latest = newRevID
	_ = scenario.WriteManifest(r.scenarioDir, scenarioManifest)

	return ApplyAIRefreshOutput{
		Scenario:     r.scenarioPath,
		BaseRevision: r.rev.RevID,
		NewRevision:  newRevID,
		Schema:       fpShort,
		DataDir:      newDataDir,
	}, nil
}

// runApplyAIRefresh is the internal implementation — a thin wrapper used by
// RunApplyAIRefresh (MCP) that calls both phases without interactive prompts.
func runApplyAIRefresh(ctx context.Context, in applyAIRefreshInput) (ApplyAIRefreshOutput, error) {
	genResult, err := runGenerateRefreshSQL(ctx, in)
	if err != nil {
		return ApplyAIRefreshOutput{}, err
	}
	return runApplyGeneratedSQL(ctx, genResult)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// loadExistingSQL returns the revision's dataset.sql contents. Revisions
// without dataset.sql (created by export/pull) cannot be refreshed — sending
// every CSV row to the AI would be prohibitively expensive.
func loadExistingSQL(revDir, scenarioPath, revID string) (string, error) {
	sqlPath := DatasetSQLPath(revDir)
	if !fileExists(sqlPath) {
		return "", fmt.Errorf(
			"revision %s of %s has no generation history — refresh works on revisions created by `seedmancer generate` or a previous refresh.\n"+
				"After migrating your database, run `seedmancer export` to snapshot it again, or `seedmancer generate` to create AI-managed test data",
			revID, scenarioPath)
	}
	data, err := os.ReadFile(sqlPath)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// buildFKGraph parses a schema JSON blob and returns a map from each table
// name to the set of table names it directly references via foreign keys.
func buildFKGraph(schemaJSON []byte) map[string]map[string]struct{} {
	var raw struct {
		Tables []struct {
			Name    string `json:"name"`
			Columns []struct {
				Nullable   bool `json:"nullable"`
				ForeignKey *struct {
					Table string `json:"table"`
				} `json:"foreignKey"`
			} `json:"columns"`
		} `json:"tables"`
	}
	if err := json.Unmarshal(schemaJSON, &raw); err != nil {
		return nil
	}
	graph := make(map[string]map[string]struct{}, len(raw.Tables))
	for _, t := range raw.Tables {
		graph[t.Name] = make(map[string]struct{})
		for _, c := range t.Columns {
			// Only include non-nullable FK edges. Nullable FKs (e.g. the
			// back-reference from scenarios.latest_revision_id to
			// scenario_revisions.id) create circular dependencies that break
			// topological sort; they can always be inserted as NULL first and
			// updated after the child rows exist.
			if c.ForeignKey != nil && c.ForeignKey.Table != "" && c.ForeignKey.Table != t.Name && !c.Nullable {
				graph[t.Name][c.ForeignKey.Table] = struct{}{}
			}
		}
	}
	return graph
}

// topoSort returns tables sorted so that each table appears after all tables it
// references via foreign keys (Kahn's algorithm). Tables not present in fkGraph
// are treated as having no dependencies. Any remaining tables (cycles or unknown
// refs) are appended at the end in their original order.
func topoSort(tables []string, fkGraph map[string]map[string]struct{}) []string {
	// Build in-degree map restricted to the tables present in this CSV set.
	present := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		present[t] = struct{}{}
	}

	inDegree := make(map[string]int, len(tables))
	for _, t := range tables {
		inDegree[t] = 0
	}
	for _, t := range tables {
		refs, ok := fkGraph[t]
		if !ok {
			continue
		}
		for dep := range refs {
			if _, inSet := present[dep]; inSet {
				inDegree[t]++
			}
		}
	}

	// Seed queue with tables that have no in-set dependencies.
	queue := make([]string, 0, len(tables))
	for _, t := range tables {
		if inDegree[t] == 0 {
			queue = append(queue, t)
		}
	}

	result := make([]string, 0, len(tables))
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		result = append(result, cur)
		// Reduce in-degree of tables that depend on cur.
		for _, t := range tables {
			refs := fkGraph[t]
			if _, refsCur := refs[cur]; refsCur {
				if _, inSet := present[cur]; inSet {
					inDegree[t]--
					if inDegree[t] == 0 {
						queue = append(queue, t)
					}
				}
			}
		}
	}

	// Append any remaining (cycle or missing from fkGraph) in original order.
	inResult := make(map[string]struct{}, len(result))
	for _, t := range result {
		inResult[t] = struct{}{}
	}
	for _, t := range tables {
		if _, seen := inResult[t]; !seen {
			result = append(result, t)
		}
	}
	return result
}

// buildSchemaDiffText produces a compact human-readable list of changes.
func buildSchemaDiffText(report driftreport.Report) string {
	if !report.HasDrift {
		return "(no changes)"
	}
	lines := make([]string, 0, len(report.Changes))
	for _, ch := range report.Changes {
		lines = append(lines, ch.String())
	}
	return strings.Join(lines, "\n")
}

// ─── SQL preview helpers ──────────────────────────────────────────────────────

// tableSQLStats holds the columns and row count extracted from an INSERT block.
// syncRevisionFingerprint patches an existing revision manifest's SchemaFingerprint
// to newFP and writes the corresponding schema.json into the schema store.
// It is called when refresh detects no structural drift but the stored fingerprint
// differs from the current one (non-deterministic export ordering, algorithm change, etc.).
func syncRevisionFingerprint(projectRoot, storagePath string, report driftreport.Report, newSchemaJSON []byte) error {
	revDir := scenario.RevisionDir(projectRoot, storagePath, report.Scenario, report.BaseRevision)
	m, err := scenario.ReadRevisionManifest(revDir)
	if err != nil {
		return fmt.Errorf("reading revision manifest: %w", err)
	}
	m.SchemaFingerprint = report.NewSchemaFP
	if err := scenario.WriteRevisionManifest(revDir, m); err != nil {
		return fmt.Errorf("writing revision manifest: %w", err)
	}

	newFPShort := utils.FingerprintShort(report.NewSchemaFP)
	schemaPath := scenario.SchemaJSONPath(projectRoot, storagePath, newFPShort)
	if _, statErr := os.Stat(schemaPath); os.IsNotExist(statErr) {
		if err := os.MkdirAll(filepath.Dir(schemaPath), 0755); err != nil {
			return fmt.Errorf("creating schema store dir: %w", err)
		}
		if err := os.WriteFile(schemaPath, newSchemaJSON, 0644); err != nil {
			return fmt.Errorf("writing schema.json: %w", err)
		}
	}
	return nil
}

type tableSQLStats struct {
	Columns  []string
	RowCount int
}

// insertTableRe matches: INSERT INTO "tableName" (col, ...)
var insertTableRe = regexp.MustCompile(`(?i)INSERT\s+INTO\s+"([^"]+)"\s*\(([^)]+)\)`)

// parseSQLStats scans INSERT INTO blocks in sql and returns per-table stats.
func parseSQLStats(sql string) map[string]tableSQLStats {
	result := make(map[string]tableSQLStats)
	matches := insertTableRe.FindAllStringSubmatchIndex(sql, -1)
	for i, m := range matches {
		tableName := sql[m[2]:m[3]]
		colStr := sql[m[4]:m[5]]

		// Parse column names: strip quotes and trim spaces.
		var cols []string
		for _, c := range strings.Split(colStr, ",") {
			c = strings.TrimSpace(c)
			c = strings.Trim(c, `"`)
			if c != "" {
				cols = append(cols, c)
			}
		}

		// Count rows: find the VALUES block — everything until the next INSERT or end.
		bodyStart := m[1]
		bodyEnd := len(sql)
		if i+1 < len(matches) {
			bodyEnd = matches[i+1][0]
		}
		body := sql[bodyStart:bodyEnd]
		rowCount := countValueRows(body)

		result[tableName] = tableSQLStats{Columns: cols, RowCount: rowCount}
	}
	return result
}

// countValueRows counts the number of top-level value tuples in an INSERT body.
// It increments the counter each time it encounters '(' at depth 0 after VALUES.
func countValueRows(body string) int {
	valIdx := strings.Index(strings.ToUpper(body), "VALUES")
	if valIdx < 0 {
		return 0
	}
	s := body[valIdx+6:]
	count := 0
	depth := 0
	for _, ch := range s {
		switch ch {
		case '(':
			if depth == 0 {
				count++
			}
			depth++
		case ')':
			depth--
		}
	}
	return count
}

// printSQLSummary compares old and new INSERT stats and prints a compact table.
func printSQLSummary(oldSQL, newSQL string) {
	old := parseSQLStats(oldSQL)
	nw := parseSQLStats(newSQL)

	// Collect all table names in new-SQL order (insertion order matters for display).
	seen := make(map[string]struct{})
	var tables []string
	for _, m := range insertTableRe.FindAllStringSubmatch(newSQL, -1) {
		name := m[1]
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			tables = append(tables, name)
		}
	}
	// Append tables only in old SQL (removed).
	for name := range old {
		if _, ok := seen[name]; !ok {
			tables = append(tables, name)
		}
	}

	if len(tables) == 0 {
		return
	}

	// Calculate column width for table name column.
	maxLen := 0
	for _, t := range tables {
		if len(t) > maxLen {
			maxLen = len(t)
		}
	}

	fmt.Fprintln(os.Stderr, "\nTest data summary:")
	for _, name := range tables {
		ns, inNew := nw[name]
		os_, inOld := old[name]

		pad := strings.Repeat(" ", maxLen-len(name)+2)

		switch {
		case inNew && !inOld:
			rows := rowWord(ns.RowCount)
			fmt.Fprintf(os.Stderr, "  + %s%s%s   (new table)\n", name, pad, rows)

		case !inNew && inOld:
			fmt.Fprintf(os.Stderr, "  - %s%s        (removed)\n", name, pad)

		case inNew && inOld:
			rows := rowWord(ns.RowCount)
			colNote := columnNote(os_.Columns, ns.Columns)
			if colNote == "" {
				fmt.Fprintf(os.Stderr, "    %s%s%s\n", name, pad, rows)
			} else {
				fmt.Fprintf(os.Stderr, "  ~ %s%s%s   %s\n", name, pad, rows, colNote)
			}
		}
	}
	fmt.Fprintln(os.Stderr)
}

func rowWord(n int) string {
	if n == 1 {
		return "1 row "
	}
	return fmt.Sprintf("%d rows", n)
}

func columnNote(old, nw []string) string {
	oldSet := make(map[string]struct{}, len(old))
	for _, c := range old {
		oldSet[c] = struct{}{}
	}
	newSet := make(map[string]struct{}, len(nw))
	for _, c := range nw {
		newSet[c] = struct{}{}
	}
	var added, removed []string
	for _, c := range nw {
		if _, ok := oldSet[c]; !ok {
			added = append(added, c)
		}
	}
	for _, c := range old {
		if _, ok := newSet[c]; !ok {
			removed = append(removed, c)
		}
	}
	if len(added) == 0 && len(removed) == 0 {
		return ""
	}
	var parts []string
	for _, c := range added {
		parts = append(parts, "+"+c)
	}
	for _, c := range removed {
		parts = append(parts, "-"+c)
	}
	return "(cols: " + strings.Join(parts, ", ") + ")"
}

// refreshSQLRequest is the body sent to /generate-refresh-sql.
type refreshSQLRequest struct {
	Schema      generateSchema `json:"schema"`
	SchemaDiff  string         `json:"schemaDiff"`
	ExistingSQL string         `json:"existingSql"`
	Purpose     string         `json:"purpose,omitempty"`
	Prompt      string         `json:"prompt,omitempty"`
}

// refreshSQLResponse is the response from /generate-refresh-sql.
type refreshSQLResponse struct {
	SQL string `json:"sql"`
}

// callGenerateRefreshSQL posts to the backend and returns the AI SQL.
// purpose is the scenario's saved intent; prompt carries per-run extra
// instructions from --prompt.
func callGenerateRefreshSQL(ctx context.Context, token string, schema generateSchema, schemaDiff, existingSQL, purpose, prompt string) (string, error) {
	reqBody, err := json.Marshal(refreshSQLRequest{
		Schema:      schema,
		SchemaDiff:  schemaDiff,
		ExistingSQL: existingSQL,
		Purpose:     purpose,
		Prompt:      prompt,
	})
	if err != nil {
		return "", err
	}

	baseURL := utils.GetBaseURL()
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/generate-refresh-sql", bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("calling /generate-refresh-sql: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized {
		return "", utils.ErrInvalidAPIToken
	}
	if resp.StatusCode == http.StatusPaymentRequired {
		return "", fmt.Errorf("AI refresh requires a Seedmancer Pro subscription")
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("generate-refresh-sql failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var result refreshSQLResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("parsing response: %w", err)
	}
	if strings.TrimSpace(result.SQL) == "" {
		return "", fmt.Errorf("backend returned empty SQL")
	}
	return result.SQL, nil
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
