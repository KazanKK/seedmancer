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

func FetchCommand() *cli.Command {
	return &cli.Command{
		Name:  "fetch",
		Usage: "Fetch database schema and test data from API endpoint using database name",
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
				Name:     "token",
				Required: true,
				Usage:    "API token for authentication",
				EnvVars:  []string{"SEEDMANCER_API_TOKEN"},
			},
		},
		Action: func(c *cli.Context) error {
			configPath, err := utils.FindConfigFile()
			if err != nil {
				return fmt.Errorf("finding config file: %v", err)
			}

			projectRoot := filepath.Dir(configPath)
			storagePath, err := utils.ReadConfig(configPath)
			if err != nil {
				return fmt.Errorf("reading config: %v", err)
			}

			databaseName := c.String("database-name")
			version := c.String("version")
			token := c.String("token")

			outputDir := filepath.Join(projectRoot, storagePath, "databases", databaseName, version)

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
			if err := fetchData(baseURL, databaseName, version, outputDir, token); err != nil {
				sp.Stop(false, "Fetch failed")
				if err.Error() == "unauthorized: please check your API token" {
					ui.Error("Authentication failed")
					ui.Info("Set a valid API token using --token flag")
					ui.Info("  or set the SEEDMANCER_API_TOKEN environment variable")
				}
				return err
			}
			sp.Stop(true, fmt.Sprintf("Fetched %s/%s → %s", databaseName, version, outputDir))
			return nil
		},
	}
}

func fetchData(baseURL, databaseName, version, outputDir, token string) error {
	reqURL := fmt.Sprintf("%s/v1.0/databases/testdata/fetch?database_name=%s&version_name=%s",
		baseURL, databaseName, version)
	ui.Debug("GET %s", reqURL)

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return fmt.Errorf("creating request: %v", err)
	}

	req.Header.Add("Authorization", utils.BearerAPIToken(token))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("fetching data from API: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized: please check your API token")
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("creating target directory: %v", err)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "application/zip" {
		return extractZip(resp.Body, outputDir)
	}

	var response struct {
		URL string `json:"url"`
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}

	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("parsing API response: %v", err)
	}

	if response.URL == "" {
		return fmt.Errorf("invalid response format: missing URL")
	}

	ui.Debug("Downloading from S3...")
	s3Resp, err := http.Get(response.URL)
	if err != nil {
		return fmt.Errorf("downloading from S3: %v", err)
	}
	defer s3Resp.Body.Close()

	if s3Resp.StatusCode != http.StatusOK {
		return fmt.Errorf("S3 download failed with status: %d", s3Resp.StatusCode)
	}

	return extractZip(s3Resp.Body, outputDir)
}

func extractZip(reader io.Reader, targetDir string) error {
	tmpFile, err := os.CreateTemp("", "database-*.zip")
	if err != nil {
		return fmt.Errorf("creating temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, reader); err != nil {
		return fmt.Errorf("saving zip file: %v", err)
	}

	zipReader, err := zip.OpenReader(tmpFile.Name())
	if err != nil {
		return fmt.Errorf("opening zip file: %v", err)
	}
	defer zipReader.Close()

	for _, file := range zipReader.File {
		rc, err := file.Open()
		if err != nil {
			return fmt.Errorf("opening file in zip: %v", err)
		}

		path := filepath.Join(targetDir, file.Name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			rc.Close()
			return fmt.Errorf("creating directories: %v", err)
		}

		outFile, err := os.Create(path)
		if err != nil {
			rc.Close()
			return fmt.Errorf("creating output file: %v", err)
		}

		if _, err := io.Copy(outFile, rc); err != nil {
			outFile.Close()
			rc.Close()
			return fmt.Errorf("extracting file: %v", err)
		}

		outFile.Close()
		rc.Close()
		ui.Debug("Extracted: %s", file.Name)
	}

	return nil
}
