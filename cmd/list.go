package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/schemahistory"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"
)

// listEntry is one row in the list output. Each row corresponds to a
// scenario that has at least one revision (or a manifest, in the case
// of an empty scenario).
type listEntry struct {
	Scenario string `json:"scenario"`
	Latest   string `json:"latest,omitempty"`
	Schema   string `json:"schema,omitempty"`
	// DB is populated when the live DB can be reached: "current" or "outdated".
	DB string `json:"db,omitempty"`
	// Drift is a compact diff summary between the scenario schema and the current
	// DB schema, e.g. "+1 tbl +3 cols".
	Drift    string `json:"drift,omitempty"`
	Updated  string `json:"updated,omitempty"`
	Services string `json:"services,omitempty"`
}

// ListCommand prints every scenario known on disk, grouped by name with
// its latest/stable revision pointers and schema fingerprint.
func ListCommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List scenarios and their pointers",
		Description: "Walks <storagePath>/scenarios/** and prints a table with one row\n" +
			"per scenario: latest revision, schema fingerprint,\n" +
			"updated time, and the services snapshotted with the\n" +
			"latest revision.",
		ArgsUsage: " ",
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
			entries, badManifests, err := collectListEntries()
			if err != nil {
				return err
			}

			// Always attempt to fingerprint the live DB. Silently skip the DB
			// column if the database is not configured or unreachable.
			entries, _ = annotateEntriesWithDBStatus(entries, c.String("env"), c.String("db-url"))

			if c.Bool("json") {
				return outputJSON(struct {
					Scenarios []listEntry            `json:"scenarios"`
					BadPaths  map[string]string      `json:"badPaths,omitempty"`
				}{Scenarios: entries, BadPaths: stringifyBadPaths(badManifests)})
			}
			ui.Title("Scenarios")
			if len(entries) == 0 {
				ui.Info("No scenarios yet. Run `seedmancer export <scenario>` to create one.")
				return nil
			}
			renderScenarioTable(entries)
			for path, err := range badManifests {
				ui.Warn("scenario %q has a corrupt manifest: %v", path, err)
			}
			return nil
		},
	}
}

// collectListEntries walks the scenarios root and returns one row per
// scenario plus any unreadable manifests so the caller can warn the user.
func collectListEntries() ([]listEntry, map[string]error, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return nil, nil, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return nil, nil, err
	}
	projectRoot := filepath.Dir(configPath)
	paths, badManifests, err := scenario.WalkScenarios(projectRoot, cfg.StoragePath)
	if err != nil {
		return nil, nil, err
	}
	out := make([]listEntry, 0, len(paths))
	for _, p := range paths {
		entry, entryErr := buildListEntry(projectRoot, cfg.StoragePath, p)
		if entryErr != nil {
			badManifests[p] = entryErr
			continue
		}
		out = append(out, entry)
	}
	return out, badManifests, nil
}

func buildListEntry(projectRoot, storagePath, scenarioPath string) (listEntry, error) {
	scenarioDir := scenario.ScenarioDir(projectRoot, storagePath, scenarioPath)
	manifest, err := scenario.ReadManifest(scenarioDir)
	if err != nil {
		return listEntry{}, err
	}
	entry := listEntry{
		Scenario: scenarioPath,
		Latest:   manifest.Latest,
	}
	if !manifest.UpdatedAt.IsZero() {
		entry.Updated = utils.HumanizeAgo(manifest.UpdatedAt)
	}
	if manifest.Latest != "" {
		revDir := scenario.RevisionDir(projectRoot, storagePath, scenarioPath, manifest.Latest)
		if rev, err := scenario.ReadRevisionManifest(revDir); err == nil {
			entry.Schema = utils.FingerprintShort(rev.SchemaFingerprint)
			entry.Services = strings.Join(rev.Services, ",")
		}
	}
	return entry, nil
}

func renderScenarioTable(entries []listEntry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Scenario < entries[j].Scenario })

	// Determine whether the DB column is populated.
	showDB := false
	for _, e := range entries {
		if e.DB != "" {
			showDB = true
			break
		}
	}

	table := tablewriter.NewWriter(os.Stdout)
	var headers []string
	if showDB {
		headers = []string{"Scenario", "Schema Status", "Drift", "Updated"}
	} else {
		headers = []string{"Scenario", "Schema", "Updated"}
	}
	table.SetHeader(headers)
	table.SetBorder(false)
	table.SetColumnSeparator("  ")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, e := range entries {
		if showDB {
			dbCell := "—"
			switch e.DB {
			case "current":
				dbCell = ui.Green("current")
			case "outdated":
				dbCell = ui.Yellow("outdated")
			}
			table.Append([]string{
				e.Scenario,
				dbCell,
				defaultDash(e.Drift),
				defaultDash(e.Updated),
			})
		} else {
			table.Append([]string{
				e.Scenario,
				defaultDash(e.Schema),
				defaultDash(e.Updated),
			})
		}
	}
	table.Render()
}

// annotateEntriesWithDBStatus fingerprints the live DB and fills the DB and
// Drift fields of each entry. The live DB is the single source of truth:
// a scenario is either "current" (schema matches live DB) or "outdated".
func annotateEntriesWithDBStatus(entries []listEntry, env, dbURL string) ([]listEntry, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return entries, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return entries, err
	}
	projectRoot := filepath.Dir(configPath)

	target, err := pickExportTarget(cfg, env, dbURL)
	if err != nil {
		return entries, err
	}
	liveFP, liveJSON, err := fingerprintCurrentDB(target)
	if err != nil {
		return entries, err
	}

	tryUpdateSchemaHistory(projectRoot, cfg.StoragePath, liveFP)
	reportLiveSchema(liveFP)

	liveShort := utils.FingerprintShort(liveFP)

	for i := range entries {
		if entries[i].Schema == "" {
			continue
		}
		if entries[i].Schema == liveShort {
			entries[i].DB = "current"
			continue
		}
		entries[i].DB = "outdated"

		// Compute drift between stored schema.json and current live schema.
		storedJSONPath := utils.SchemaJSONPath(projectRoot, cfg.StoragePath, entries[i].Schema)
		if storedJSON, rerr := os.ReadFile(storedJSONPath); rerr == nil {
			if d, derr := schemahistory.SummarizeSchemaDiff(storedJSON, liveJSON); derr == nil {
				entries[i].Drift = d.String()
			}
		}
	}
	return entries, nil
}

func defaultDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "—"
	}
	return s
}

func stringifyBadPaths(in map[string]error) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v.Error()
	}
	return out
}

func outputJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// listLocalEntries kept for the MCP runner; returns the same scenarios as
// the table view.
func listLocalEntries() ([]listEntry, error) {
	entries, _, err := collectListEntries()
	return entries, err
}

// silence unused-import warning when `fmt` isn't needed by stub paths.
var _ = fmt.Sprintf











