package cmd

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

func SyncCommand() *cli.Command {
	return &cli.Command{
		Name:  "sync",
		Usage: "Sync local test data to cloud storage",
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
				Required: false,
				Usage:    "API token (or set SEEDMANCER_API_TOKEN env var)",
				EnvVars:  []string{"SEEDMANCER_API_TOKEN"},
			},
		},
		Action: func(c *cli.Context) error {
			// Find config file to get storage path and project root
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

			databaseName := c.String("database-name")
			version := c.String("version")
			token := c.String("token")

			if token == "" {
				return fmt.Errorf("API token is required. Set --token flag or SEEDMANCER_API_TOKEN env var")
			}

			// Check if version exists locally
			versionPath := filepath.Join(projectRoot, config.StoragePath, "databases", databaseName, version)
			unversionedPath := filepath.Join(projectRoot, config.StoragePath, "databases", databaseName, "unversioned")

			if _, err := os.Stat(versionPath); err != nil {
				if os.IsNotExist(err) {
					// Check if unversioned exists
					if _, err := os.Stat(unversionedPath); err == nil {
						fmt.Printf("Version '%s' not found.\n", version)
						fmt.Printf("An unversioned database exists. Use the unversioned database instead? (y/N): ")

						var response string
						fmt.Scanln(&response)
						if strings.ToLower(response) == "y" {
							if err := os.Rename(unversionedPath, versionPath); err != nil {
								return fmt.Errorf("renaming unversioned database: %v", err)
							}
							fmt.Printf("Renamed unversioned database to '%s'.\n", version)
						} else {
							return fmt.Errorf("sync canceled. Please specify an existing test data version")
						}
					} else {
						return fmt.Errorf("test data version '%s' not found and no unversioned data exists", version)
					}
				} else {
					return fmt.Errorf("accessing version directory: %v", err)
				}
			}

			// Find all CSV files in the version directory
			entries, err := os.ReadDir(versionPath)
			if err != nil {
				return fmt.Errorf("reading version directory: %v", err)
			}

			var files []string
			for _, entry := range entries {
				if !entry.IsDir() && strings.HasSuffix(strings.ToLower(entry.Name()), ".csv") {
					files = append(files, filepath.Join(versionPath, entry.Name()))
				}
			}

			if len(files) == 0 {
				return fmt.Errorf("no CSV files found in version directory")
			}

			fmt.Printf("Found %d CSV files to sync\n", len(files))
			fmt.Println("Compressing files...")
			
			zipData, err := compressFiles(files)
			if err != nil {
				return fmt.Errorf("compressing files: %v", err)
			}

			zipSize := float64(zipData.Len()) / 1024 / 1024 // Size in MB
			fmt.Printf("Compressed file size: %.2f MB\n", zipSize)

			if zipSize <= 10 {
				// For files under 10MB, use direct API upload
				fmt.Println("Using direct API upload for small file...")
				baseURL := utils.GetBaseURL()
				url := fmt.Sprintf("%s/api/v1.0/databases/testdata/sync/db?database_name=%s&version_name=%s", 
					baseURL, databaseName, version)

				// Create a new buffer with the zip data
				zipBuffer := bytes.NewBuffer(zipData.Bytes())
				
				req, err := http.NewRequest("POST", url, zipBuffer)
				if err != nil {
					return fmt.Errorf("creating request: %v", err)
				}

				req.Header.Set("Content-Type", "application/zip")
				req.Header.Set("Authorization", fmt.Sprintf("cli_%s", token))
				req.ContentLength = int64(zipBuffer.Len())

				fmt.Printf("Uploading zip file (%d bytes)...\n", zipBuffer.Len())

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return fmt.Errorf("uploading file: %v", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					body, _ := io.ReadAll(resp.Body)
					return fmt.Errorf("upload failed: %s - %s", resp.Status, string(body))
				}
			} else {
				// For larger files, use the existing S3 presigned URL method
				fmt.Println("Using S3 presigned URL for large file...")
				if err := uploadZipFileWithAuth(zipData, databaseName, version, token); err != nil {
					return fmt.Errorf("uploading zip: %v", err)
				}
			}

			fmt.Println("\n✅ Sync complete!")
			return nil
		},
	}
}

func compressFiles(files []string) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	zipWriter := zip.NewWriter(buf)
	defer zipWriter.Close()

	for _, file := range files {
		// Open the file
		fileToZip, err := os.Open(file)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", file, err)
		}
		defer fileToZip.Close()

		// Get file info
		info, err := fileToZip.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to get file info %s: %v", file, err)
		}

		// Create zip header
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return nil, fmt.Errorf("failed to create header %s: %v", file, err)
		}

		// Use base name for files
		header.Name = filepath.Base(file)
		// Use best compression
		header.Method = zip.Deflate

		// Create writer for this file in zip
		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return nil, fmt.Errorf("failed to create zip entry %s: %v", file, err)
		}

		// Copy file content to zip
		if _, err := io.Copy(writer, fileToZip); err != nil {
			return nil, fmt.Errorf("failed to write file to zip %s: %v", file, err)
		}
	}

	if err := zipWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close zip writer: %v", err)
	}

	return buf, nil
}

func getPresignedURLWithAuth(baseURL, databaseName, testDataVersionName, fileName, token string) (string, error) {
	url := fmt.Sprintf("%s/api/v1.0/databases/testdata/sync?database_name=%s&version_name=%s", baseURL, databaseName, testDataVersionName)
	
	fmt.Printf("Requesting presigned URL from: %s\n", url) // Debug log

	payload := map[string]string{
		"fileName":    fileName,
		"contentType": "application/zip",
	}
	
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshaling payload: %v", err)
	}

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return "", fmt.Errorf("creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("cli_%s", token))

	fmt.Println("Sending request with headers:", req.Header) // Debug log

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
	}

	var result struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("parsing response: %v, body: %s", err, string(body))
	}

	if result.URL == "" {
		return "", fmt.Errorf("received empty upload URL from API")
	}

	fmt.Printf("Received presigned URL: %s\n", result.URL) // Debug log

	return result.URL, nil
}

func uploadZipFileWithAuth(zipData *bytes.Buffer, databaseName, testDataVersionName, token string) error {
	if zipData.Len() == 0 {
		return fmt.Errorf("zip file is empty")
	}

	baseURL := utils.GetBaseURL()
	if baseURL == "" {
		return fmt.Errorf("base URL is empty")
	}

	fileName := fmt.Sprintf("%s/%s/testdata.zip", databaseName, testDataVersionName)
	
	// Get presigned URL with auth
	presignedURL, err := getPresignedURLWithAuth(baseURL, databaseName, testDataVersionName, fileName, token)
	if err != nil {
		return fmt.Errorf("getting presigned URL: %v", err)
	}

	if presignedURL == "" {
		return fmt.Errorf("received empty presigned URL")
	}

	// Upload zip file
	req, err := http.NewRequest("PUT", presignedURL, bytes.NewReader(zipData.Bytes()))
	if err != nil {
		return fmt.Errorf("creating upload request: %v", err)
	}

	req.Header.Set("Content-Type", "application/zip")
	req.ContentLength = int64(zipData.Len())

	fmt.Printf("Uploading to: %s\n", presignedURL) // Debug log

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("making upload request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed with status %s: %s", resp.Status, string(body))
	}

	fmt.Println("Successfully uploaded zip file")
	return nil
}