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

type datasetsResponse struct {
	Datasets []datasetItem `json:"datasets"`
}

type datasetItem struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Description  *string        `json:"description"`
	DatabaseName string         `json:"databaseName"`
	VersionName  string         `json:"versionName"`
	FileCount    int            `json:"fileCount"`
	TotalSize    int64          `json:"totalSize"`
	Files        []datasetFile  `json:"files"`
	CreatedAt    string         `json:"createdAt"`
	UpdatedAt    string         `json:"updatedAt"`
}

type datasetFile struct {
	Name       string `json:"name"`
	Size       int    `json:"size"`
	Rows       int    `json:"rows"`
	HasContent bool   `json:"hasContent,omitempty"`
}

type listEntry struct {
	Database string `json:"database"`
	Version  string `json:"version"`
}

type listOutput struct {
	Local  []listEntry `json:"local,omitempty"`
	Remote []listEntry `json:"remote,omitempty"`
}

func ListCommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List databases and versions (local, remote, or both)",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "token",
				Usage:   "API token (or set SEEDMANCER_API_TOKEN env var)",
				EnvVars: []string{"SEEDMANCER_API_TOKEN"},
			},
			&cli.BoolFlag{
				Name:  "local",
				Usage: "List only local databases and versions",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "remote",
				Usage: "List only remote databases and versions",
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

			if !localOnly && !remoteOnly {
				localOnly = true
				remoteOnly = true
			}

			var out listOutput

			if localOnly {
				entries, err := listLocal()
				if err != nil && !remoteOnly {
					if jsonMode {
						out.Local = []listEntry{}
					} else {
						ui.Title("Local")
						ui.Info("No local databases found.")
					}
				} else if err != nil {
					// remote-only with no config is fine — skip local silently
				} else {
					if jsonMode {
						out.Local = entries
					} else {
						ui.Title("Local")
						if len(entries) == 0 {
							ui.Info("No local databases found.")
						} else {
							renderTable(entries)
						}
					}
				}
			}

			if remoteOnly {
				token, tokenErr := utils.ResolveAPIToken(c.String("token"))
				if tokenErr != nil {
					if localOnly && !jsonMode {
						ui.Title("Remote")
						ui.Warn("API token required to list remote databases.")
						ui.Info("Use --token flag or set SEEDMANCER_API_TOKEN environment variable.")
						return nil
					}
					if jsonMode && localOnly {
						return outputJSON(out)
					}
					return tokenErr
				}

				entries, err := listRemote(token)
				if err != nil {
					return err
				}

				if jsonMode {
					out.Remote = entries
				} else {
					ui.Title("Remote")
					if len(entries) == 0 {
						ui.Info("No remote databases found.")
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

func listLocal() ([]listEntry, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return nil, err
	}

	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return nil, err
	}

	databasesDir := filepath.Join(projectRoot, cfg.StoragePath, "databases")
	if _, err := os.Stat(databasesDir); os.IsNotExist(err) {
		return nil, nil
	}

	dbEntries, err := os.ReadDir(databasesDir)
	if err != nil {
		return nil, fmt.Errorf("reading databases directory: %v", err)
	}

	var entries []listEntry
	for _, dbEntry := range dbEntries {
		if !dbEntry.IsDir() {
			continue
		}
		dbName := dbEntry.Name()
		versionEntries, err := os.ReadDir(filepath.Join(databasesDir, dbName))
		if err != nil {
			continue
		}
		for _, vEntry := range versionEntries {
			if vEntry.IsDir() {
				entries = append(entries, listEntry{Database: dbName, Version: vEntry.Name()})
			}
		}
	}
	return entries, nil
}

func listRemote(token string) ([]listEntry, error) {
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

	var dsResp datasetsResponse
	if err := json.Unmarshal(body, &dsResp); err != nil {
		return nil, fmt.Errorf("parsing response JSON: %v", err)
	}

	var entries []listEntry
	for _, ds := range dsResp.Datasets {
		entries = append(entries, listEntry{Database: ds.DatabaseName, Version: ds.VersionName})
	}
	return entries, nil
}

func renderTable(entries []listEntry) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Database", "Version"})
	table.SetBorder(false)
	table.SetColumnSeparator("  ")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, e := range entries {
		table.Append([]string{e.Database, e.Version})
	}
	table.Render()
}

func outputJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
