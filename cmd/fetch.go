package cmd

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

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
			&cli.StringFlag{
				Name:    "type",
				Value:   "postgres",
				Usage:   "Database type (postgres or mysql)",
				EnvVars: []string{"SEEDMANCER_DB_TYPE"},
			},
		},
		Action: func(c *cli.Context) error {
			// Find config file to get storage path and project root
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
			dbType := c.String("type")

			// Create output directory structure
			outputDir := filepath.Join(projectRoot, storagePath, "databases", databaseName, version)
			
			// Remove existing directory if it exists
			if _, err := os.Stat(outputDir); err == nil {
				if err := os.RemoveAll(outputDir); err != nil {
					return fmt.Errorf("removing existing directory: %v", err)
				}
			}

			// Create fresh directory
			if err := os.MkdirAll(outputDir, 0755); err != nil {
				return fmt.Errorf("creating output directory: %v", err)
			}

			baseURL := utils.GetBaseURL()
			
			if err := fetchData(baseURL, databaseName, version, outputDir, token, dbType); err != nil {
				if err.Error() == "unauthorized: please check your API token" {
					fmt.Println("\n❌ Authentication failed!")
					fmt.Println("\nPlease ensure you have:")
					fmt.Println("1. Set a valid API token using the --token flag")
					fmt.Println("   OR")
					fmt.Println("2. Set the SEEDMANCER_API_TOKEN environment variable")
					fmt.Println("\nExample:")
					fmt.Println("  seedmancer fetch --database-name <name> --version <version> --token <your-token>")
					fmt.Println("  # OR")
					fmt.Println("  export SEEDMANCER_API_TOKEN=<your-token>")
					fmt.Println("  seedmancer fetch --database-name <name> --version <version>")
				}
				return err
			}

			fmt.Printf("\n✅ Fetched test data version '%s' to: %s\n", version, outputDir)
			return nil
		},
	}
}

// fetchData downloads and extracts database schema and test data
func fetchData(baseURL, databaseName, version, outputDir, token, dbType string) error {
	// Create request
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v1.0/databases/testdata/fetch?database_name=%s&version_name=%s&db_type=%s", 
		baseURL, databaseName, version, dbType), nil)
	if err != nil {
		return fmt.Errorf("creating request: %v", err)
	}

	// Add authorization header
	req.Header.Add("Authorization", "cli_"+token)

	// Send request
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

	// Create target directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("creating target directory: %v", err)
	}

	// Check Content-Type header
	contentType := resp.Header.Get("Content-Type")
	if contentType == "application/zip" {
		// Direct zip file download
		return extractZip(resp.Body, outputDir)
	}

	// Try to parse JSON response for S3 URL
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

	// Download from S3 URL
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

// extractZip extracts a zip file to the target directory
func extractZip(reader io.Reader, targetDir string) error {
	// Create temporary file for zip
	tmpFile, err := os.CreateTemp("", "database-*.zip")
	if err != nil {
		return fmt.Errorf("creating temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Copy download to temporary file
	if _, err := io.Copy(tmpFile, reader); err != nil {
		return fmt.Errorf("saving zip file: %v", err)
	}

	// Extract zip file
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
		fmt.Printf("Extracted: %s\n", path)
	}

	return nil
}