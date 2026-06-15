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
	"time"

	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// datasetAPI mirrors the /v1.0/datasets response shape. Only the fields we
// actually consume are decoded; everything else is ignored by encoding/json.
type datasetAPI struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Prompt    string          `json:"prompt"`
	FileCount int             `json:"fileCount"`
	TotalSize int64           `json:"totalSize"`
	CreatedAt string          `json:"createdAt"`
	UpdatedAt string          `json:"updatedAt"`
	Schema    *schemaRefShort `json:"schema"`
}

type schemaRefShort struct {
	ID               string  `json:"id"`
	DisplayName      *string `json:"displayName"`
	Fingerprint      string  `json:"fingerprint"`
	FingerprintShort string  `json:"fingerprintShort"`
	IsLegacy         bool    `json:"isLegacy"`
}

type datasetListResponse struct {
	Datasets []datasetAPI `json:"datasets"`
}

// PullCommand downloads the cloud dataset whose name matches the
// scenario path and lands it as a fresh local revision under that
// scenario.
func PullCommand() *cli.Command {
	return &cli.Command{
		Name:      "pull",
		Aliases:   []string{"fetch"},
		Usage:     "Download a scenario from the cloud as a new local revision",
		ArgsUsage: "[scenario]",
		Description: "Looks up the cloud dataset whose name matches <scenario>, downloads\n" +
			"it, and writes the result as a new revision under the local\n" +
			"scenario. Pointers.latest advances to the new revision so\n" +
			"`seedmancer seed <scenario>` picks it up immediately.\n\n" +
			"With --all, every locally-known scenario is pulled. Scenarios whose\n" +
			"local latest already matches the cloud are skipped (diff-only).",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "all",
				Usage: "Pull every locally-known scenario, skipping those already up to date",
			},
			&cli.StringFlag{
				Name:  "token",
				Usage: "API token (falls back to SEEDMANCER_API_TOKEN, then ~/.seedmancer/credentials)",
			},
		},
		Action: func(c *cli.Context) error {
			scenarioArg := strings.TrimSpace(c.Args().First())
			useAll := c.Bool("all")

			if useAll && scenarioArg != "" {
				return fmt.Errorf("cannot combine --all with a scenario name")
			}
			if !useAll && scenarioArg == "" {
				return usageError(c, "missing required argument: <scenario> (or use --all)")
			}

			token, err := utils.ResolveAPIToken(c.String("token"))
			if err != nil {
				return err
			}

			if useAll {
				configPath, cfgErr := utils.FindConfigFile()
				if cfgErr != nil {
					return cfgErr
				}
				cfg, cfgErr := utils.LoadConfig(configPath)
				if cfgErr != nil {
					return cfgErr
				}
				projectRoot := filepath.Dir(configPath)

				paths, badManifests, walkErr := scenario.WalkScenarios(projectRoot, cfg.StoragePath)
				if walkErr != nil {
					return walkErr
				}
				for path, manifestErr := range badManifests {
					ui.Warn("skipping scenario %q (unreadable manifest): %v", path, manifestErr)
				}
				if len(paths) == 0 {
					return fmt.Errorf("no local scenarios found — run `seedmancer export <scenario>` first")
				}

				var pulled, upToDate, failed int
				for _, sp := range paths {
					out, fetchErr := RunFetch(c.Context, FetchInput{Scenario: sp, Token: token})
					if fetchErr != nil {
						ui.Warn("  fail  %s: %v", sp, fetchErr)
						failed++
						continue
					}
					if out.UpToDate {
						ui.Info("  ok    %s @ %s  (already up to date)", sp, out.Revision)
						upToDate++
					} else {
						ui.Success("  pull  %s → %s", sp, out.Revision)
						pulled++
					}
				}
				ui.Info("pulled %d, up to date %d, failed %d", pulled, upToDate, failed)
				return nil
			}

			start := time.Now()
			out, err := RunFetch(c.Context, FetchInput{
				Scenario: scenarioArg,
				Token:    token,
			})
			if err != nil {
				return err
			}
			elapsed := time.Since(start)
			if out.UpToDate {
				ui.Success("Already up to date — %s @ %s matches the cloud (%s)",
					out.Scenario, out.Revision, formatDuration(elapsed))
				ui.KeyValue("Path: ", out.Path)
				return nil
			}
			ui.Success("Pulled %s @ %s (%s in %s)",
				out.Scenario, out.Revision, formatBytes(out.BytesDownloaded), formatDuration(elapsed))
			ui.KeyValue("Schema: ", out.SchemaShort)
			ui.KeyValue("Files: ", fmt.Sprintf("%d", len(out.Files)))
			ui.KeyValue("Path: ", out.Path)
			return nil
		},
	}
}

// findRemoteDataset looks up a dataset by name. Returns the resolved
// dataset metadata or a friendly error.
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
		return datasetAPI{}, utils.ErrInvalidAPIToken
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
		return datasetAPI{}, fmt.Errorf(
			"no remote scenario named %q\n  Run `seedmancer push %s` first or check the spelling",
			datasetName, datasetName,
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
			"scenario %q exists under multiple schemas (%s) — rename the duplicates in the dashboard so ids are unique",
			datasetName, strings.Join(fps, ", "),
		)
	}
}

// listRemoteDatasets fetches every scenario on the connected cloud API and
// indexes them by scenario path (dataset name). Used by push --all to decide
// which local scenarios still need uploading.
func listRemoteDatasets(baseURL, token string) (map[string]datasetAPI, error) {
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, utils.ErrInvalidAPIToken
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
	}

	var dsResp datasetListResponse
	if err := json.Unmarshal(body, &dsResp); err != nil {
		return nil, fmt.Errorf("parsing response JSON: %v", err)
	}

	out := make(map[string]datasetAPI, len(dsResp.Datasets))
	for _, d := range dsResp.Datasets {
		out[d.Name] = d
	}
	return out, nil
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

// liftSchemaSidecars moves schema-level files (schema.json plus any
// *_func.sql / *_trigger.sql) from a freshly-extracted revision data
// folder up into the shared schema folder. Two scenarios that share a
// schema produce byte-identical sidecars (fingerprint is derived from
// schema.json), so we overwrite unconditionally rather than sniffing
// for changes. Returns the number of files moved.
func liftSchemaSidecars(dataDir, schemaDir string) (int, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return 0, fmt.Errorf("reading %s: %v", dataDir, err)
	}
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		return 0, fmt.Errorf("creating %s: %v", schemaDir, err)
	}
	var moved int
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !utils.IsSchemaSidecarName(name) {
			continue
		}
		src := filepath.Join(dataDir, name)
		dst := filepath.Join(schemaDir, name)
		if err := os.Rename(src, dst); err != nil {
			if err := copyFile(src, dst); err != nil {
				return moved, err
			}
			if err := os.Remove(src); err != nil {
				return moved, err
			}
		}
		moved++
	}
	return moved, nil
}

func downloadAndExtractZip(downloadURL, outputDir string) ([]string, int64, error) {
	ui.Debug("Downloading zip...")

	resp, err := http.Get(downloadURL)
	if err != nil {
		return nil, 0, fmt.Errorf("downloading zip: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("download failed with status: %d", resp.StatusCode)
	}

	tmpFile, err := os.CreateTemp("", "seedmancer-*.zip")
	if err != nil {
		return nil, 0, fmt.Errorf("creating temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	downloadedBytes, err := io.Copy(tmpFile, resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("saving zip file: %v", err)
	}

	zipReader, err := zip.OpenReader(tmpFile.Name())
	if err != nil {
		return nil, 0, fmt.Errorf("opening zip file: %v", err)
	}
	defer zipReader.Close()

	var extracted []string
	for _, file := range zipReader.File {
		if file.FileInfo().IsDir() {
			continue
		}

		rc, err := file.Open()
		if err != nil {
			return nil, 0, fmt.Errorf("opening file in zip: %v", err)
		}

		destPath := filepath.Join(outputDir, filepath.Base(file.Name))
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			rc.Close()
			return nil, 0, fmt.Errorf("creating directories: %v", err)
		}

		outFile, err := os.Create(destPath)
		if err != nil {
			rc.Close()
			return nil, 0, fmt.Errorf("creating output file: %v", err)
		}

		if _, err := io.Copy(outFile, rc); err != nil {
			outFile.Close()
			rc.Close()
			return nil, 0, fmt.Errorf("extracting file: %v", err)
		}

		outFile.Close()
		rc.Close()

		extracted = append(extracted, filepath.Base(file.Name))
		ui.Debug("Extracted: %s", filepath.Base(file.Name))
	}

	return extracted, downloadedBytes, nil
}
