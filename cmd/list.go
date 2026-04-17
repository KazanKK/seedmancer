package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"
)

// listEntry is one row in the rendered table — a dataset scoped under a
// schema's fingerprint-short label. `SchemaLabel` is either the user's
// custom display name or the fingerprint short id used on disk.
type listEntry struct {
	SchemaLabel      string `json:"schemaLabel"`
	SchemaShort      string `json:"schemaShort"`
	Dataset          string `json:"dataset"`
	FileCount        int    `json:"fileCount,omitempty"`
	LastUpdated      string `json:"lastUpdated,omitempty"`
}

type listOutput struct {
	Local  []listEntry `json:"local,omitempty"`
	Remote []listEntry `json:"remote,omitempty"`
}

func ListCommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List schemas and datasets (local, remote, or both)",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "token",
				Usage:   "API token (or set SEEDMANCER_API_TOKEN env var)",
				EnvVars: []string{"SEEDMANCER_API_TOKEN"},
			},
			&cli.BoolFlag{
				Name:  "local",
				Usage: "List only local datasets",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "remote",
				Usage: "List only remote datasets",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "json",
				Usage: "Output as JSON (for CI/CD and scripting)",
				Value: false,
			},
		},
		Action: func(c *cli.Context) error {
			localOnly := c.Bool("local")
			remoteOnly := c.Bool("remote")
			jsonMode := c.Bool("json")

			// No flags → show both.
			if !localOnly && !remoteOnly {
				localOnly = true
				remoteOnly = true
			}

			var out listOutput

			if localOnly {
				entries, err := listLocalEntries()
				if err != nil {
					if jsonMode {
						out.Local = []listEntry{}
					} else {
						ui.Title("Local")
						ui.Warn("%v", err)
					}
				} else {
					if jsonMode {
						out.Local = entries
					} else {
						ui.Title("Local")
						if len(entries) == 0 {
							ui.Info("No local schemas found. Run `seedmancer export` first.")
						} else {
							renderTable(entries)
						}
					}
				}
			}

			if remoteOnly {
				token, tokenErr := utils.ResolveAPIToken(c.String("token"))
				if tokenErr != nil {
					if !jsonMode {
						ui.Title("Remote")
						ui.Warn("API token required to list remote schemas.")
						ui.Info("Use --token flag or set SEEDMANCER_API_TOKEN environment variable.")
					}
					if jsonMode {
						return outputJSON(out)
					}
					if localOnly {
						return nil
					}
					return tokenErr
				}

				entries, err := listRemoteEntries(token)
				if err != nil {
					return err
				}

				if jsonMode {
					out.Remote = entries
				} else {
					ui.Title("Remote")
					if len(entries) == 0 {
						ui.Info("No remote schemas found.")
					} else {
						renderTable(entries)
					}
				}
			}

			if jsonMode {
				return outputJSON(out)
			}
			return nil
		},
	}
}

func listLocalEntries() ([]listEntry, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return nil, err
	}

	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return nil, err
	}

	schemas, err := utils.ListLocalSchemas(projectRoot, cfg.StoragePath)
	if err != nil {
		return nil, err
	}

	var entries []listEntry
	for _, s := range schemas {
		// `SchemaLabel` on disk is always the fp-short — display names live on
		// the server. A schema folder with no datasets still shows up with an
		// em-dash so users notice it exists.
		if len(s.Datasets) == 0 {
			entries = append(entries, listEntry{
				SchemaLabel: s.FingerprintShort,
				SchemaShort: s.FingerprintShort,
				Dataset:     "—",
			})
			continue
		}
		for _, d := range s.Datasets {
			entries = append(entries, listEntry{
				SchemaLabel: s.FingerprintShort,
				SchemaShort: s.FingerprintShort,
				Dataset:     d,
			})
		}
	}
	return entries, nil
}

func listRemoteEntries(token string) ([]listEntry, error) {
	baseURL := utils.GetBaseURL()
	reqURL := fmt.Sprintf("%s/v1.0/datasets", baseURL)
	ui.Debug("GET %s", reqURL)

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("unauthorized: please check your API token")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}

	var dsResp datasetListResponse
	if err := json.Unmarshal(body, &dsResp); err != nil {
		return nil, fmt.Errorf("parsing response JSON: %v", err)
	}

	var entries []listEntry
	for _, ds := range dsResp.Datasets {
		e := listEntry{
			Dataset:     ds.Name,
			FileCount:   ds.FileCount,
			LastUpdated: ds.UpdatedAt,
		}
		if ds.Schema != nil {
			e.SchemaShort = ds.Schema.FingerprintShort
			if ds.Schema.DisplayName != nil && *ds.Schema.DisplayName != "" {
				e.SchemaLabel = *ds.Schema.DisplayName
			} else {
				e.SchemaLabel = ds.Schema.FingerprintShort
			}
		} else {
			e.SchemaLabel = "(orphan)"
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func renderTable(entries []listEntry) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Schema", "Short ID", "Dataset"})
	table.SetBorder(false)
	table.SetColumnSeparator("  ")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, e := range entries {
		table.Append([]string{e.SchemaLabel, e.SchemaShort, e.Dataset})
	}
	table.Render()
}

func outputJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
