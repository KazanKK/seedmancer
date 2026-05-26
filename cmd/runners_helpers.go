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
	"strings"

	utils "github.com/KazanKK/seedmancer/internal/utils"
)

// These helpers mirror the HTTP flows used by the CLI push/pull actions,
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
	Revision         string `json:"revision,omitempty"`
}

// uploadURLResponse is returned by POST /v1.0/datasets/sync/upload-url.
type uploadURLResponse struct {
	UploadURL string `json:"uploadUrl"`
	Path      string `json:"path"`
}

// syncUploadPresigned uploads zipData via the three-step presigned URL flow:
//  1. POST /v1.0/datasets/sync/upload-url  → receive { uploadUrl, path }
//  2. PUT  uploadUrl                       → stream bytes directly to storage
//  3. POST /v1.0/datasets/sync/confirm     → process ZIP + register dataset
//
// This bypasses the Vercel function body-size limit (≈4.5 MB) so datasets
// of any size can be synced.
func syncUploadPresigned(ctx context.Context, token, baseURL, datasetName, revisionLabel string, zipData *bytes.Buffer) (syncUploadResult, error) {
	// Step 1: request a presigned upload URL.
	q1 := url.Values{}
	q1.Set("name", datasetName)
	if strings.TrimSpace(revisionLabel) != "" {
		q1.Set("revision", strings.TrimSpace(revisionLabel))
	}
	uploadURLEndpoint := fmt.Sprintf("%s/v1.0/datasets/sync/upload-url?%s", baseURL, q1.Encode())

	req1, err := http.NewRequestWithContext(ctx, "POST", uploadURLEndpoint, nil)
	if err != nil {
		return syncUploadResult{}, fmt.Errorf("creating upload-url request: %v", err)
	}
	req1.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		return syncUploadResult{}, fmt.Errorf("requesting upload URL: %v", err)
	}
	defer resp1.Body.Close()

	body1, _ := io.ReadAll(resp1.Body)
	if resp1.StatusCode == http.StatusUnauthorized {
		return syncUploadResult{}, utils.ErrInvalidAPIToken
	}
	if resp1.StatusCode == http.StatusPaymentRequired {
		return syncUploadResult{}, formatLimitError(body1)
	}
	if resp1.StatusCode != http.StatusOK {
		return syncUploadResult{}, fmt.Errorf("server responded %s: %s", resp1.Status, string(body1))
	}

	var uploadURLResp uploadURLResponse
	if err := json.Unmarshal(body1, &uploadURLResp); err != nil || uploadURLResp.UploadURL == "" {
		return syncUploadResult{}, fmt.Errorf("parsing upload-url response: %v", err)
	}

	// Step 2: PUT the zip directly to storage (presigned URL — no auth header).
	req2, err := http.NewRequestWithContext(ctx, "PUT", uploadURLResp.UploadURL, bytes.NewReader(zipData.Bytes()))
	if err != nil {
		return syncUploadResult{}, fmt.Errorf("creating storage PUT request: %v", err)
	}
	req2.Header.Set("Content-Type", "application/zip")
	req2.ContentLength = int64(zipData.Len())

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		return syncUploadResult{}, fmt.Errorf("uploading to storage: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK && resp2.StatusCode != http.StatusCreated {
		return syncUploadResult{}, fmt.Errorf("storage upload failed (HTTP %d)", resp2.StatusCode)
	}

	// Step 3: confirm — server parses the ZIP, resolves schema, and records the revision.
	q3 := url.Values{}
	q3.Set("name", datasetName)
	q3.Set("path", uploadURLResp.Path)
	if strings.TrimSpace(revisionLabel) != "" {
		q3.Set("revision", strings.TrimSpace(revisionLabel))
	}
	confirmEndpoint := fmt.Sprintf("%s/v1.0/datasets/sync/confirm?%s", baseURL, q3.Encode())

	req3, err := http.NewRequestWithContext(ctx, "POST", confirmEndpoint, nil)
	if err != nil {
		return syncUploadResult{}, fmt.Errorf("creating confirm request: %v", err)
	}
	req3.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		return syncUploadResult{}, fmt.Errorf("confirming upload: %v", err)
	}
	defer resp3.Body.Close()

	body3, _ := io.ReadAll(resp3.Body)
	if resp3.StatusCode == http.StatusUnauthorized {
		return syncUploadResult{}, utils.ErrInvalidAPIToken
	}
	if resp3.StatusCode == http.StatusPaymentRequired {
		return syncUploadResult{}, formatLimitError(body3)
	}
	if resp3.StatusCode != http.StatusOK && resp3.StatusCode != http.StatusCreated {
		return syncUploadResult{}, fmt.Errorf("server responded %s: %s", resp3.Status, string(body3))
	}

	var result syncUploadResult
	if err := json.Unmarshal(body3, &result); err != nil {
		return syncUploadResult{}, fmt.Errorf("parsing confirm response: %v", err)
	}
	return result, nil
}

// syncDatasetUpload zips the schema sidecars + dataset CSVs and uploads
// them via the presigned URL flow. It is the quiet counterpart to syncOne.
func syncDatasetUpload(ctx context.Context, token string, schema utils.LocalSchema, datasetDir, datasetName, revisionLabel, baseURL string) (syncUploadResult, error) {
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

	return syncUploadPresigned(ctx, token, baseURL, datasetName, revisionLabel, zipData)
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
