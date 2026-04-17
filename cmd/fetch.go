package cmd

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// fetchResult is the --json shape emitted after a successful fetch.
type fetchResult struct {
	Dataset          string   `json:"dataset"`
	SchemaID         string   `json:"schemaId"`
	SchemaShort      string   `json:"schemaShort"`
	SchemaFingerprint string  `json:"schemaFingerprint"`
	SchemaDisplayName string  `json:"schemaDisplayName,omitempty"`
	Output           string   `json:"output"`
	Files            []string `json:"files"`
}

// datasetAPI mirrors the /v1.0/datasets response shape. Only the fields we
// actually consume are decoded; everything else is ignored by encoding/json.
type datasetAPI struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	FileCount int             `json:"fileCount"`
	TotalSize int64           `json:"totalSize"`
	CreatedAt string          `json:"createdAt"`
	UpdatedAt string          `json:"updatedAt"`
	Schema    *schemaRefShort `json:"schema"`
}

type schemaRefShort struct {
	ID               string `json:"id"`
	DisplayName      *string `json:"displayName"`
	Fingerprint      string `json:"fingerprint"`
	FingerprintShort string `json:"fingerprintShort"`
	IsLegacy         bool   `json:"isLegacy"`
}

type datasetListResponse struct {
	Datasets []datasetAPI `json:"datasets"`
}

func FetchCommand() *cli.Command {
	return &cli.Command{
		Name:  "fetch",
		Usage: "Download a dataset from the cloud into local storage",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "name",
				Required: true,
				Usage:    "Dataset name to fetch",
			},
			&cli.StringFlag{
				Name:  "schema",
				Usage: "Fingerprint prefix (required if the dataset name exists under multiple schemas)",
			},
			&cli.StringFlag{
				Name:    "token",
				Usage:   "API token (or set SEEDMANCER_API_TOKEN env var)",
				EnvVars: []string{"SEEDMANCER_API_TOKEN"},
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "Output directory (overrides the default <storagePath>/schemas/<fp-short>/datasets/<name> layout)",
			},
			&cli.BoolFlag{
				Name:  "json",
				Usage: "Output result as JSON (for CI/CD and scripting)",
				Value: false,
			},
		},
		Action: func(c *cli.Context) error {
			datasetName := strings.TrimSpace(c.String("name"))
			if datasetName == "" {
				return fmt.Errorf("--name is required")
			}
			schemaPrefix := strings.TrimSpace(c.String("schema"))
			outputFlag := strings.TrimSpace(c.String("output"))
			jsonMode := c.Bool("json")

			token, err := utils.ResolveAPIToken(c.String("token"))
			if err != nil {
				return err
			}
			baseURL := utils.GetBaseURL()

			// Ask the server for every dataset matching the name (and optional
			// schema prefix filter). This gives us both the dataset id for
			// downloading and the schema fingerprint for the local folder.
			match, err := findRemoteDataset(baseURL, token, datasetName, schemaPrefix)
			if err != nil {
				return err
			}

			outputDir, err := resolveFetchOutput(outputFlag, match, datasetName)
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

			label := displayLabelForSchema(match.Schema)
			sp := ui.StartSpinner(fmt.Sprintf("Fetching %s  (schema %s)...", datasetName, label))

			downloadURL, err := getDownloadURL(baseURL, token, match.ID)
			if err != nil {
				sp.Stop(false, "Fetch failed")
				return err
			}

			extracted, err := downloadAndExtractZip(downloadURL, outputDir)
			if err != nil {
				sp.Stop(false, "Fetch failed")
				return err
			}
			sp.Stop(true, fmt.Sprintf("Fetched %s → %s (%d files)", datasetName, outputDir, len(extracted)))

			if jsonMode {
				res := fetchResult{
					Dataset:           datasetName,
					Output:            outputDir,
					Files:             extracted,
					SchemaID:          match.Schema.ID,
					SchemaShort:       match.Schema.FingerprintShort,
					SchemaFingerprint: match.Schema.Fingerprint,
				}
				if match.Schema.DisplayName != nil {
					res.SchemaDisplayName = *match.Schema.DisplayName
				}
				return outputJSON(res)
			}

			return nil
		},
	}
}

// findRemoteDataset looks up a dataset by name, optionally narrowed to a
// specific schema fingerprint prefix. Returns the resolved dataset metadata
// (including schema info) or an error explaining why the match is unclear.
func findRemoteDataset(baseURL, token, datasetName, schemaPrefix string) (datasetAPI, error) {
	q := url.Values{}
	if schemaPrefix != "" {
		q.Set("schema", schemaPrefix)
	}
	reqURL := fmt.Sprintf("%s/v1.0/datasets", baseURL)
	if encoded := q.Encode(); encoded != "" {
		reqURL += "?" + encoded
	}
	ui.Debug("GET %s", reqURL)

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return datasetAPI{}, fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return datasetAPI{}, fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return datasetAPI{}, fmt.Errorf("reading response body: %v", err)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return datasetAPI{}, fmt.Errorf("unauthorized: please check your API token")
	}
	if resp.StatusCode != http.StatusOK {
		return datasetAPI{}, fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
	}

	var dsResp datasetListResponse
	if err := json.Unmarshal(body, &dsResp); err != nil {
		return datasetAPI{}, fmt.Errorf("parsing response JSON: %v", err)
	}

	var hits []datasetAPI
	for _, d := range dsResp.Datasets {
		if d.Name == datasetName {
			hits = append(hits, d)
		}
	}

	switch len(hits) {
	case 0:
		if schemaPrefix != "" {
			return datasetAPI{}, fmt.Errorf(
				"no remote dataset named %q under schema prefix %q\n  Run 'seedmancer list --remote' to see available datasets",
				datasetName, schemaPrefix,
			)
		}
		return datasetAPI{}, fmt.Errorf(
			"no remote dataset named %q\n  Run 'seedmancer list --remote' to see available datasets",
			datasetName,
		)
	case 1:
		return hits[0], nil
	default:
		var fps []string
		for _, h := range hits {
			if h.Schema != nil {
				fps = append(fps, h.Schema.FingerprintShort)
			}
		}
		return datasetAPI{}, fmt.Errorf(
			"dataset name %q exists under multiple schemas (%s) — pass --schema <fp-prefix> to pick one",
			datasetName, strings.Join(fps, ", "),
		)
	}
}

// resolveFetchOutput picks the destination directory for extracted CSVs.
//
// Priority:
//  1. --output <dir>            -> used verbatim (no config needed)
//  2. seedmancer.yaml present   -> <storagePath>/schemas/<fp-short>/datasets/<name>
//  3. no config, no --output    -> error with actionable hint
func resolveFetchOutput(outputFlag string, match datasetAPI, datasetName string) (string, error) {
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
			"no seedmancer.yaml found and --output not specified.\n" +
				"  Use --output <dir> to set the destination directly,\n" +
				"  or run 'seedmancer init' to create a seedmancer.yaml",
		)
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return "", fmt.Errorf("reading config: %v", err)
	}

	if match.Schema == nil {
		return "", fmt.Errorf(
			"remote dataset %q has no attached schema — pass --output <dir> to write CSVs somewhere explicit",
			datasetName,
		)
	}

	return utils.DatasetPath(projectRoot, cfg.StoragePath, match.Schema.FingerprintShort, datasetName), nil
}

func displayLabelForSchema(s *schemaRefShort) string {
	if s == nil {
		return "(orphan)"
	}
	if s.DisplayName != nil && *s.DisplayName != "" {
		return fmt.Sprintf("%s [%s]", *s.DisplayName, s.FingerprintShort)
	}
	return s.FingerprintShort
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
