package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	utils "github.com/KazanKK/seedmancer/internal/utils"
)

// These helpers mirror the HTTP flows used by the CLI sync/fetch/generate
// actions, but without any spinners, stdout logging, or interactive
// prompts. They exist to back the Run* functions that the MCP server
// calls. Keeping them here instead of inline in runners.go keeps that
// file focused on result shaping.

type syncUploadResult struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	SchemaID         string `json:"schemaId"`
	Fingerprint      string `json:"fingerprint"`
	FingerprintShort string `json:"fingerprintShort"`
	SchemaCreated    bool   `json:"schemaCreated"`
	Updated          bool   `json:"updated"`
	FileCount        int    `json:"fileCount"`
}

// syncDatasetUpload zips the schema sidecars + dataset CSVs and POSTs
// them to /v1.0/datasets/sync. It is the quiet counterpart to syncOne.
func syncDatasetUpload(ctx context.Context, token string, schema utils.LocalSchema, datasetDir, datasetName, baseURL string) (syncUploadResult, error) {
	schemaFiles, err := utils.SchemaFiles(schema.Path)
	if err != nil {
		return syncUploadResult{}, err
	}
	dataFiles, err := utils.DatasetFiles(datasetDir)
	if err != nil {
		return syncUploadResult{}, err
	}
	if len(dataFiles) == 0 {
		return syncUploadResult{}, fmt.Errorf("no CSV or JSON files in %s", datasetDir)
	}

	entries := make([]string, 0, len(schemaFiles)+len(dataFiles))
	entries = append(entries, schemaFiles...)
	entries = append(entries, dataFiles...)

	zipData, err := compressFiles(entries)
	if err != nil {
		return syncUploadResult{}, fmt.Errorf("compressing files: %v", err)
	}

	q := url.Values{}
	q.Set("name", datasetName)
	apiURL := fmt.Sprintf("%s/v1.0/datasets/sync?%s", baseURL, q.Encode())

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewReader(zipData.Bytes()))
	if err != nil {
		return syncUploadResult{}, fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/zip")
	req.Header.Set("Authorization", utils.BearerAPIToken(token))
	req.ContentLength = int64(zipData.Len())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return syncUploadResult{}, fmt.Errorf("uploading: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized {
		return syncUploadResult{}, utils.ErrInvalidAPIToken
	}
	if resp.StatusCode == http.StatusPaymentRequired {
		return syncUploadResult{}, formatLimitError(body)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return syncUploadResult{}, fmt.Errorf("server responded %s: %s", resp.Status, string(body))
	}

	var result syncUploadResult
	if err := json.Unmarshal(body, &result); err != nil {
		return syncUploadResult{}, fmt.Errorf("parsing sync response: %v", err)
	}
	return result, nil
}

type fetchDownloadResult struct {
	Match       datasetAPI
	OutputDir   string
	Files       []string
	LiftedCount int
}

// fetchDatasetDownload resolves a remote dataset by name, downloads the
// zipped bundle into the default schema-first layout rooted at
// projectRoot/storagePath, and lifts the schema sidecars up one level so
// the on-disk layout matches what `list --local` expects.
func fetchDatasetDownload(ctx context.Context, baseURL, token, projectRoot, storagePath, datasetName string) (fetchDownloadResult, error) {
	match, err := findRemoteDataset(baseURL, token, datasetName, "")
	if err != nil {
		return fetchDownloadResult{}, err
	}

	if match.Schema == nil || match.Schema.FingerprintShort == "" {
		return fetchDownloadResult{}, fmt.Errorf("remote dataset %q is missing schema metadata", datasetName)
	}

	schemaDir := filepath.Join(projectRoot, storagePath, match.Schema.FingerprintShort)
	outputDir := filepath.Join(schemaDir, "datasets", datasetName)

	if _, err := os.Stat(outputDir); err == nil {
		if err := os.RemoveAll(outputDir); err != nil {
			return fetchDownloadResult{}, fmt.Errorf("removing existing directory: %v", err)
		}
	}
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fetchDownloadResult{}, fmt.Errorf("creating output directory: %v", err)
	}

	downloadURL, err := getDownloadURL(baseURL, token, match.ID)
	if err != nil {
		return fetchDownloadResult{}, err
	}

	extracted, err := downloadAndExtractZip(downloadURL, outputDir)
	if err != nil {
		return fetchDownloadResult{}, err
	}

	lifted, err := liftSchemaSidecars(outputDir, schemaDir)
	if err != nil {
		return fetchDownloadResult{}, fmt.Errorf("placing schema files: %v", err)
	}

	// Silence the ctx arg warning while reserving it for future use (e.g.
	// the download helper eventually honoring ctx.Done()).
	_ = ctx

	return fetchDownloadResult{
		Match:       match,
		OutputDir:   outputDir,
		Files:       extracted,
		LiftedCount: lifted,
	}, nil
}

// fetchGenerateJobStatus is a thin, context-aware wrapper around the
// /generation-status endpoint used by the CLI's pollJobUntilDone.
func fetchGenerateJobStatus(ctx context.Context, baseURL, token, jobID string) (*generateStatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", baseURL+"/generation-status?id="+jobID, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling API: %v", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status API error (HTTP %d): %s", resp.StatusCode, string(respBytes))
	}

	var status generateStatusResponse
	if err := json.Unmarshal(respBytes, &status); err != nil {
		return nil, fmt.Errorf("parsing status response: %v", err)
	}
	return &status, nil
}

// downloadGenerateArtifacts downloads every completed job file into
// datasetDir and returns the list of written paths.
func downloadGenerateArtifacts(ctx context.Context, files []generateFileEntry, datasetDir string) ([]string, error) {
	if err := os.MkdirAll(datasetDir, 0755); err != nil {
		return nil, fmt.Errorf("creating dataset dir: %v", err)
	}

	client := &http.Client{Timeout: 5 * time.Minute}
	written := make([]string, 0, len(files))
	for _, f := range files {
		if f.FileURL == "" {
			continue
		}
		name := filepath.Base(f.Path)
		if name == "" || name == "." || name == "/" {
			name = f.Table + ".csv"
		}
		destPath := filepath.Join(datasetDir, name)

		req, err := http.NewRequestWithContext(ctx, "GET", f.FileURL, nil)
		if err != nil {
			return written, fmt.Errorf("creating download request: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return written, fmt.Errorf("downloading %s: %v", name, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return written, fmt.Errorf("download %s failed (HTTP %d)", name, resp.StatusCode)
		}

		out, err := os.Create(destPath)
		if err != nil {
			resp.Body.Close()
			return written, fmt.Errorf("creating %s: %v", destPath, err)
		}
		if _, err := io.Copy(out, resp.Body); err != nil {
			out.Close()
			resp.Body.Close()
			return written, fmt.Errorf("writing %s: %v", destPath, err)
		}
		out.Close()
		resp.Body.Close()

		written = append(written, destPath)
	}
	return written, nil
}
