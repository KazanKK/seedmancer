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
	"gopkg.in/yaml.v3"
)

type ListResponse struct {
	Databases []struct {
		ID               string   `json:"id"`
		Name             string   `json:"name"`
		Tables           []string `json:"tables"`
		Enums            []string `json:"enums"`
		TestDataVersions []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"testDataVersions"`
	} `json:"databases"`
}

func ListCommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List databases and versions",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "token",
				Required: false,
				Usage:    "API token (or set SEEDMANCER_API_TOKEN env var)",
				EnvVars:  []string{"SEEDMANCER_API_TOKEN"},
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
		},
		Action: func(c *cli.Context) error {
			configPath, err := utils.FindConfigFile()
			if err != nil {
				return fmt.Errorf("finding config file: %v", err)
			}

			projectRoot := filepath.Dir(configPath)
			data, err := os.ReadFile(configPath)
			if err != nil {
				return fmt.Errorf("reading config file: %v", err)
			}

			var config struct {
				StoragePath string `yaml:"storage_path"`
			}
			if err := yaml.Unmarshal(data, &config); err != nil {
				return fmt.Errorf("parsing config file: %v", err)
			}

			token := c.String("token")
			localOnly := c.Bool("local")
			remoteOnly := c.Bool("remote")

			if !localOnly && !remoteOnly {
				localOnly = true
				remoteOnly = true
			}

			if localOnly {
				ui.Title("Local")

				databasesDir := filepath.Join(projectRoot, config.StoragePath, "databases")
				if _, err := os.Stat(databasesDir); os.IsNotExist(err) {
					ui.Info("No local databases found.")
				} else {
					entries, err := os.ReadDir(databasesDir)
					if err != nil {
						return fmt.Errorf("reading databases directory: %v", err)
					}

					if len(entries) == 0 {
						ui.Info("No local databases found.")
					} else {
						table := tablewriter.NewWriter(os.Stdout)
						table.SetHeader([]string{"Database", "Version"})
						table.SetBorder(false)
						table.SetColumnSeparator("  ")
						table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
						table.SetAlignment(tablewriter.ALIGN_LEFT)

						for _, entry := range entries {
							if entry.IsDir() {
								dbName := entry.Name()
								versionsDir := filepath.Join(databasesDir, dbName)

								versionEntries, err := os.ReadDir(versionsDir)
								if err != nil {
									return fmt.Errorf("reading versions directory: %v", err)
								}

								for _, versionEntry := range versionEntries {
									if versionEntry.IsDir() {
										table.Append([]string{dbName, versionEntry.Name()})
									}
								}
							}
						}

						table.Render()
					}
				}
			}

			if remoteOnly {
				if token == "" {
					if localOnly {
						ui.Title("Remote")
						ui.Warn("API token required to list remote databases.")
						ui.Info("Use --token flag or set SEEDMANCER_API_TOKEN environment variable.")
						return nil
					}
					return fmt.Errorf("API token required to list remote databases")
				}

				ui.Title("Remote")

				baseURL := utils.GetBaseURL()
				url := fmt.Sprintf("%s/v1.0/databases/testdata/list", baseURL)
				ui.Debug("GET %s", url)

				req, err := http.NewRequest("GET", url, nil)
				if err != nil {
					return fmt.Errorf("creating request: %v", err)
				}

				req.Header.Set("Authorization", utils.BearerAPIToken(token))

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return fmt.Errorf("making request: %v", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode == http.StatusUnauthorized {
					return fmt.Errorf("unauthorized: please check your API token")
				}

				if resp.StatusCode != http.StatusOK {
					body, _ := io.ReadAll(resp.Body)
					return fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
				}

				body, err := io.ReadAll(resp.Body)
				if err != nil {
					return fmt.Errorf("reading response body: %v", err)
				}

				var listResp ListResponse
				if err := json.Unmarshal(body, &listResp); err != nil {
					return fmt.Errorf("parsing response JSON: %v", err)
				}

				if len(listResp.Databases) == 0 {
					ui.Info("No remote databases found.")
				} else {
					table := tablewriter.NewWriter(os.Stdout)
					table.SetHeader([]string{"Database", "Version"})
					table.SetBorder(false)
					table.SetColumnSeparator("  ")
					table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
					table.SetAlignment(tablewriter.ALIGN_LEFT)

					for _, db := range listResp.Databases {
						if len(db.TestDataVersions) == 0 {
							table.Append([]string{db.Name, "(no versions)"})
						} else {
							for _, version := range db.TestDataVersions {
								table.Append([]string{db.Name, version.Name})
							}
						}
					}

					table.Render()
				}
			}

			return nil
		},
	}
}
