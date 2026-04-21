package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"
)

// listEntry is one row in the rendered table — a dataset scoped under a
// schema's 12-char fingerprint prefix. Display names (if any) are the
// domain of `seedmancer schemas list`; `seedmancer list` uses the
// fingerprint everywhere so local + remote columns line up.
//
// UpdatedAt is the machine-readable ISO-8601 timestamp (preserved for JSON
// consumers); Updated is the humanized relative string shown in the TTY
// table. updatedAtTime is an internal-only parsed timestamp used for
// sorting; it's excluded from JSON output.
type listEntry struct {
	Schema        string    `json:"schema"`
	Dataset       string    `json:"dataset"`
	FileCount     int       `json:"fileCount,omitempty"`
	UpdatedAt     string    `json:"updatedAt,omitempty"`
	Updated       string    `json:"-"`
	updatedAtTime time.Time `json:"-"`
}

type listOutput struct {
	Local  []listEntry `json:"local,omitempty"`
	Remote []listEntry `json:"remote,omitempty"`
}

func ListCommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List datasets, grouped by schema fingerprint",
		Description: "Shows one row per dataset with its schema fingerprint and last-updated\n" +
			"time, newest first. By default both local and remote datasets are\n" +
			"shown — pass --local or --remote to scope to one side.\n\n" +
			"To see schema-level details (display names, sizes, table counts)\n" +
			"use `seedmancer schemas list` instead.",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "token",
				Usage: "API token (falls back to ~/.seedmancer/credentials, then SEEDMANCER_API_TOKEN)",
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
				Usage: "Emit result as JSON for CI/CD pipelines",
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
					if jsonMode {
						return outputJSON(out)
					}
					// "seedmancer list" (no flags) runs both sides; show local
					// results + an inline login hint so the user keeps working
					// offline instead of a hard failure.
					if localOnly {
						ui.Title("Remote")
						ui.PrintLoginHint()
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
		// em-dash so users notice it exists. Datasets come pre-sorted by mtime
		// DESC from ListLocalSchemas, so they render newest-first under each
		// schema.
		if len(s.Datasets) == 0 {
			entries = append(entries, listEntry{
				Schema:        s.FingerprintShort,
				Dataset:       "—",
				UpdatedAt:     s.UpdatedAt.Format(time.RFC3339),
				Updated:       utils.HumanizeAgo(s.UpdatedAt),
				updatedAtTime: s.UpdatedAt,
			})
			continue
		}
		for _, d := range s.Datasets {
			entries = append(entries, listEntry{
				Schema:        s.FingerprintShort,
				Dataset:       d.Name,
				UpdatedAt:     d.UpdatedAt.Format(time.RFC3339),
				Updated:       utils.HumanizeAgo(d.UpdatedAt),
				updatedAtTime: d.UpdatedAt,
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
		return nil, utils.ErrInvalidAPIToken
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
		// Server-provided ISO-8601 strings → parse once so we can sort, then
		// keep the original string for JSON consumers. Unparseable timestamps
		// fall back to the zero time, which sorts to the bottom.
		t, _ := time.Parse(time.RFC3339, ds.UpdatedAt)
		e := listEntry{
			Dataset:       ds.Name,
			FileCount:     ds.FileCount,
			UpdatedAt:     ds.UpdatedAt,
			Updated:       utils.HumanizeAgo(t),
			updatedAtTime: t,
		}
		if ds.Schema != nil {
			e.Schema = ds.Schema.FingerprintShort
		} else {
			e.Schema = "(orphan)"
		}
		entries = append(entries, e)
	}
	// Newest first — the whole point of this command is "what did I just do?".
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].updatedAtTime.Equal(entries[j].updatedAtTime) {
			return entries[i].Dataset < entries[j].Dataset
		}
		return entries[i].updatedAtTime.After(entries[j].updatedAtTime)
	})
	return entries, nil
}

func renderTable(entries []listEntry) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Schema ID", "Dataset ID", "Updated"})
	table.SetBorder(false)
	table.SetColumnSeparator("  ")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, e := range entries {
		table.Append([]string{e.Schema, e.Dataset, e.Updated})
	}
	table.Render()
}

func outputJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
