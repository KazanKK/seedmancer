package cmd

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

type fetchResult struct {
	Database string   `json:"database"`
	Version  string   `json:"version"`
	Output   string   `json:"output"`
	Files    []string `json:"files"`
}

func FetchCommand() *cli.Command {
	return &cli.Command{
		Name:  "fetch",
		Usage: "Fetch seed data from the cloud (CI/CD friendly)",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "database-name",
				Required: true,
				Usage:    "Database name",
			},
			&cli.StringFlag{
				Name:     "version",
				Required: true,
				Usage:    "Version name",
			},
			&cli.StringFlag{
				Name:    "token",
				Usage:   "API token (or set SEEDMANCER_API_TOKEN env var)",
				EnvVars: []string{"SEEDMANCER_API_TOKEN"},
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "Output directory (overrides storage_path from config; no config file needed)",
			},
			&cli.BoolFlag{
				Name:  "json",
				Usage: "Output result as JSON (for CI/CD and scripting)",
				Value: false,
			},
		},
		Action: func(c *cli.Context) error {
			databaseName := c.String("database-name")
			version := c.String("version")
			jsonMode := c.Bool("json")

			token, err := utils.ResolveAPIToken(c.String("token"))
			if err != nil {
				return err
			}

			outputDir, err := resolveFetchOutputDir(c.String("output"), databaseName, version)
			if err != nil {
				return err
			}

			if _, err := os.Stat(outputDir); err == nil {
				if err := os.RemoveAll(outputDir); err != nil {
					return fmt.Errorf("removing existing directory: %v", err)
				}
			}
			if err := os.MkdirAll(outputDir, 0755); err != nil {
				return fmt.Errorf("creating output directory: %v", err)
			}

			baseURL := utils.GetBaseURL()

			sp := ui.StartSpinner(fmt.Sprintf("Fetching %s/%s...", databaseName, version))

			datasetID, err := findDatasetID(baseURL, token, databaseName, version)
			if err != nil {
				sp.Stop(false, "Fetch failed")
				return err
			}

			downloadURL, err := getDownloadURL(baseURL, token, datasetID)
			if err != nil {
				sp.Stop(false, "Fetch failed")
				return err
			}

			extracted, err := downloadAndExtractZip(downloadURL, outputDir)
			if err != nil {
				sp.Stop(false, "Fetch failed")
				return err
			}

			sp.Stop(true, fmt.Sprintf("Fetched %s/%s → %s (%d files)", databaseName, version, outputDir, len(extracted)))

			if jsonMode {
				return outputJSON(fetchResult{
					Database: databaseName,
					Version:  version,
					Output:   outputDir,
					Files:    extracted,
				})
			}

			return nil
		},
	}
}

func findDatasetID(baseURL, token, databaseName, version string) (string, error) {
	reqURL := fmt.Sprintf("%s/v1.0/datasets", baseURL)
	ui.Debug("GET %s", reqURL)

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return "", fmt.Errorf("unauthorized: please check your API token")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading response body: %v", err)
	}

	var dsResp datasetsResponse
	if err := json.Unmarshal(body, &dsResp); err != nil {
		return "", fmt.Errorf("parsing response JSON: %v", err)
	}

	for _, ds := range dsResp.Datasets {
		if ds.DatabaseName == databaseName && ds.VersionName == version {
			return ds.ID, nil
		}
	}

	return "", fmt.Errorf("dataset not found: %s/%s\n  Run 'seedmancer list --remote' to see available datasets", databaseName, version)
}

func getDownloadURL(baseURL, token, datasetID string) (string, error) {
	reqURL := fmt.Sprintf("%s/v1.0/datasets/%s/download", baseURL, datasetID)
	ui.Debug("GET %s", reqURL)

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("download request failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	var result struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("parsing download response: %v", err)
	}
	if result.URL == "" {
		return "", fmt.Errorf("server returned empty download URL")
	}
	return result.URL, nil
}

func downloadAndExtractZip(downloadURL, outputDir string) ([]string, error) {
	ui.Debug("Downloading zip...")

	resp, err := http.Get(downloadURL)
	if err != nil {
		return nil, fmt.Errorf("downloading zip: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download failed with status: %d", resp.StatusCode)
	}

	tmpFile, err := os.CreateTemp("", "seedmancer-*.zip")
	if err != nil {
		return nil, fmt.Errorf("creating temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		return nil, fmt.Errorf("saving zip file: %v", err)
	}

	zipReader, err := zip.OpenReader(tmpFile.Name())
	if err != nil {
		return nil, fmt.Errorf("opening zip file: %v", err)
	}
	defer zipReader.Close()

	var extracted []string
	for _, file := range zipReader.File {
		if file.FileInfo().IsDir() {
			continue
		}

		rc, err := file.Open()
		if err != nil {
			return nil, fmt.Errorf("opening file in zip: %v", err)
		}

		destPath := filepath.Join(outputDir, filepath.Base(file.Name))
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			rc.Close()
			return nil, fmt.Errorf("creating directories: %v", err)
		}

		outFile, err := os.Create(destPath)
		if err != nil {
			rc.Close()
			return nil, fmt.Errorf("creating output file: %v", err)
		}

		if _, err := io.Copy(outFile, rc); err != nil {
			outFile.Close()
			rc.Close()
			return nil, fmt.Errorf("extracting file: %v", err)
		}

		outFile.Close()
		rc.Close()

		extracted = append(extracted, filepath.Base(file.Name))
		ui.Debug("Extracted: %s", filepath.Base(file.Name))
	}

	return extracted, nil
}

func resolveFetchOutputDir(outputFlag, databaseName, version string) (string, error) {
	if outputFlag != "" {
		abs, err := filepath.Abs(outputFlag)
		if err != nil {
			return "", fmt.Errorf("resolving output path: %v", err)
		}
		return abs, nil
	}

	configPath, err := utils.FindConfigFile()
	if err != nil {
		return "", fmt.Errorf(
			"no config file found and --output not specified.\n" +
				"  Use --output <dir> to set the destination directly (no config needed),\n" +
				"  or run 'seedmancer init' to create a seedmancer.yaml",
		)
	}

	projectRoot := filepath.Dir(configPath)
	storagePath, err := utils.ReadConfig(configPath)
	if err != nil {
		return "", fmt.Errorf("reading config: %v", err)
	}
	return filepath.Join(projectRoot, storagePath, "databases", databaseName, version), nil
}
