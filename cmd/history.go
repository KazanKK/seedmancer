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
			"and the service connectors that were snapshotted with it.",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "json",
				Usage: "Emit JSON for CI/CD pipelines",
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
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Revision", "Pointer", "Schema", "Created", "Services", "Description"})
	table.SetBorder(false)
	table.SetColumnSeparator("  ")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, r := range out.Revisions {
		table.Append([]string{
			r.Revision,
			defaultDash(r.Pointer),
			defaultDash(r.Schema),
			defaultDash(r.Created),
			defaultDash(r.Services),
			defaultDash(r.Description),
		})
	}
	table.Render()
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
	Created     string `json:"created,omitempty"`
	Source      string `json:"source,omitempty"`
	Services    string `json:"services,omitempty"`
	Description string `json:"description,omitempty"`
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
	pointers, _ := scenario.ReadPointers(scenarioDir)
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
			Pointer:     pointerLabel(r.ID, pointers),
			Schema:      utils.FingerprintShort(manifest.SchemaFingerprint),
			Created:     formatExportTime(manifest.CreatedAt),
			Source:      manifest.Source,
			Services:    strings.Join(manifest.Services, ","),
			Description: manifest.Description,
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
