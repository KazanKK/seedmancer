package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"
)

// HistoryCommand prints every revision of a scenario, newest first.
func HistoryCommand() *cli.Command {
	return &cli.Command{
		Name:      "history",
		Usage:     "List revisions of a scenario, newest first",
		ArgsUsage: "<scenario>",
		Description: "Shows every revision under a scenario along with its pointer\n" +
			"status (latest / stable), schema fingerprint, when it was created,\n" +
			"and the services that were snapshotted with it.",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "json",
				Usage: "Emit JSON for CI/CD pipelines",
			},
			&cli.StringFlag{
				Name:    "env",
				Aliases: []string{"e"},
				Usage:   "Named environment to connect to (see seedmancer env)",
			},
			&cli.StringFlag{
				Name:  "db-url",
				Usage: "Ad-hoc database URL (overrides --env)",
			},
		},
		Action: func(c *cli.Context) error {
			scenarioArg := strings.TrimSpace(c.Args().First())
			if scenarioArg == "" {
				return fmt.Errorf("usage: seedmancer history <scenario>")
			}
			out, err := RunHistory(c.Context, HistoryInput{Scenario: scenarioArg})
			if err != nil {
				return err
			}

			// Always attempt to fingerprint the live DB. Silently skip the DB
			// column if the database is not configured or unreachable.
			out.Revisions, _ = annotateHistoryWithDBStatus(out.Revisions, c.String("env"), c.String("db-url"))

			if c.Bool("json") {
				return outputJSON(out)
			}
			ui.Title(fmt.Sprintf("Scenario: %s", out.Scenario))
			if len(out.Revisions) == 0 {
				ui.Info("No revisions yet. Run `seedmancer export %s` to create one.", out.Scenario)
				return nil
			}
			renderHistoryTable(out)
			for path, msg := range out.BadRevisions {
				ui.Warn("revision %q is unreadable: %s", path, msg)
			}
			return nil
		},
	}
}

func renderHistoryTable(out HistoryOutput) {
	// Determine whether the DB column is populated.
	showDB := false
	for _, r := range out.Revisions {
		if r.DB != "" {
			showDB = true
			break
		}
	}

	table := tablewriter.NewWriter(os.Stdout)
	headers := []string{"Revision", "Pointer", "Schema", "Created", "Services", "Description"}
	if showDB {
		headers = []string{"Revision", "Pointer", "Schema", "DB", "Created", "Services", "Description"}
	}
	table.SetHeader(headers)
	table.SetBorder(false)
	table.SetColumnSeparator("  ")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, r := range out.Revisions {
		if showDB {
			dbCell := "—"
			switch r.DB {
			case "matched":
				dbCell = ui.Green("matched")
			case "outdated":
				dbCell = ui.Yellow("outdated")
			}
			table.Append([]string{
				r.Revision,
				defaultDash(r.Pointer),
				defaultDash(r.Schema),
				dbCell,
				defaultDash(r.Created),
				defaultDash(r.Services),
				defaultDash(r.Description),
			})
		} else {
			table.Append([]string{
				r.Revision,
				defaultDash(r.Pointer),
				defaultDash(r.Schema),
				defaultDash(r.Created),
				defaultDash(r.Services),
				defaultDash(r.Description),
			})
		}
	}
	table.Render()
}

// annotateHistoryWithDBStatus fingerprints the live DB and marks revisions
// whose schema fingerprint matches with DB = "matched".
func annotateHistoryWithDBStatus(revisions []HistoryRevision, env, dbURL string) ([]HistoryRevision, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return revisions, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return revisions, err
	}
	target, err := pickExportTarget(cfg, env, dbURL)
	if err != nil {
		return revisions, err
	}
	// Use the raw fingerprint (before any stripping) because stored revision
	// fingerprints were also computed over the full schema at export time.
	liveFP, _, err := fingerprintCurrentDB(target)
	if err != nil {
		return revisions, err
	}
	liveShort := utils.FingerprintShort(liveFP)
	for i := range revisions {
		if revisions[i].Schema == liveShort {
			revisions[i].DB = "matched"
		} else {
			revisions[i].DB = "outdated"
		}
	}
	return revisions, nil
}

// HistoryInput is the structured input for RunHistory.
type HistoryInput struct {
	Scenario string `json:"scenario" jsonschema:"Scenario path to inspect"`
}

// HistoryRevision is a single row in HistoryOutput.Revisions.
type HistoryRevision struct {
	Revision    string `json:"revision"`
	Pointer     string `json:"pointer,omitempty"`
	Schema      string `json:"schema,omitempty"`
	// DB is set when --check is passed: "matched" or empty.
	DB          string `json:"db,omitempty"`
	Created     string `json:"created,omitempty"`
	Source      string `json:"source,omitempty"`
	Services    string `json:"services,omitempty"`
	Description string `json:"description,omitempty"`
	// HasSQL is true when a dataset.sql sidecar is stored alongside the
	// revision's CSVs (i.e. the revision was created via generate_dataset_local).
	// Agents can use this to decide whether get_dataset_sql will succeed.
	HasSQL bool `json:"hasSql,omitempty"`
}

// HistoryOutput is the structured response for RunHistory.
type HistoryOutput struct {
	Scenario     string            `json:"scenario"`
	Revisions    []HistoryRevision `json:"revisions"`
	BadRevisions map[string]string `json:"badRevisions,omitempty"`
}

// RunHistory loads every revision under a scenario and renders pointer
// metadata. Newest first; corrupt revision manifests are reported via
// BadRevisions instead of aborting the command so users can still see
// the rest.
func RunHistory(_ context.Context, in HistoryInput) (HistoryOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return HistoryOutput{}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return HistoryOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	scenarioPath, err := scenario.Normalize(in.Scenario)
	if err != nil {
		return HistoryOutput{}, err
	}
	scenarioDir := scenario.ScenarioDir(projectRoot, cfg.StoragePath, scenarioPath)
	if _, err := os.Stat(scenarioDir); os.IsNotExist(err) {
		return HistoryOutput{}, fmt.Errorf("scenario %q does not exist", scenarioPath)
	}
	scManifest, _ := scenario.ReadManifest(scenarioDir)
	revs, err := scenario.ListRevisions(scenarioDir)
	if err != nil {
		return HistoryOutput{}, err
	}

	out := HistoryOutput{Scenario: scenarioPath, Revisions: []HistoryRevision{}}
	bad := map[string]string{}
	for _, r := range revs {
		revDir := scenario.RevisionDir(projectRoot, cfg.StoragePath, scenarioPath, r.ID)
		manifest, err := scenario.ReadRevisionManifest(revDir)
		if err != nil {
			bad[r.ID] = err.Error()
			continue
		}
		row := HistoryRevision{
			Revision:    r.ID,
			Pointer:     pointerLabel(r.ID, scManifest.Latest),
			Schema:      utils.FingerprintShort(manifest.SchemaFingerprint),
			Created:     formatExportTime(manifest.CreatedAt),
			Source:      manifest.Source,
			Services:    strings.Join(manifest.Services, ","),
			Description: manifest.Description,
		}
		if _, err := os.Stat(DatasetSQLPath(revDir)); err == nil {
			row.HasSQL = true
		}
		out.Revisions = append(out.Revisions, row)
	}
	// Newest first.
	sort.Slice(out.Revisions, func(i, j int) bool {
		ni, _ := scenario.ParseRevisionID(out.Revisions[i].Revision)
		nj, _ := scenario.ParseRevisionID(out.Revisions[j].Revision)
		return ni > nj
	})
	if len(bad) > 0 {
		out.BadRevisions = bad
	}
	return out, nil
}
