package cmd

import (
	"archive/zip"
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

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// PushCommand uploads a local dataset to the cloud.
//
// The server identifies the target schema from the fingerprint of the
// zip's `schema.json`, so we never send a schema name — it's fully
// derived. Dataset ids must be unique across local schemas; if the same
// dataset id exists under two schemas, rename one via `seedmancer schemas
// rename` first.
func PushCommand() *cli.Command {
	return &cli.Command{
		Name:    "push",
		Aliases: []string{"sync"},
		Usage:   "Upload a single local dataset to the cloud",
		Description: "Zips the schema sidecars + CSVs for one local dataset and uploads\n" +
			"them to your Seedmancer cloud account. The target schema is derived\n" +
			"from schema.json's fingerprint — no need to pass a schema id.",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "dataset-id",
				Aliases:  []string{"d", "id"},
				Required: true,
				Usage:    "(required) Dataset id to upload (the name given at export/generate time)",
			},
			&cli.StringFlag{
				Name:  "token",
				Usage: "API token (falls back to ~/.seedmancer/credentials, then SEEDMANCER_API_TOKEN)",
			},
		},
		Action: func(c *cli.Context) error {
			configPath, err := utils.FindConfigFile()
			if err != nil {
				return err
			}
			projectRoot := filepath.Dir(configPath)
			cfg, err := utils.LoadConfig(configPath)
			if err != nil {
				return err
			}

			token, err := utils.ResolveAPIToken(c.String("token"))
			if err != nil {
				return err
			}

			datasetName := strings.TrimSpace(c.String("dataset-id"))

			schema, datasetDir, err := utils.FindLocalDataset(projectRoot, cfg.StoragePath, "", datasetName)
			if err != nil {
				return err
			}

			ui.Step("%s / %s  (schema %s)", datasetName, filepath.Base(schema.Path), schema.FingerprintShort)

			baseURL := utils.GetBaseURL()
			return syncOne(schema, datasetDir, datasetName, baseURL, token)
		},
	}
}

func syncOne(schema utils.LocalSchema, datasetDir, datasetName, baseURL, token string) error {
	schemaFiles, err := utils.SchemaFiles(schema.Path)
	if err != nil {
		return err
	}
	dataFiles, err := utils.DatasetFiles(datasetDir)
	if err != nil {
		return err
	}
	if len(dataFiles) == 0 {
		return fmt.Errorf("no CSV or JSON files in %s", datasetDir)
	}

	// Zip entries: schema sidecars + data CSVs, all flat at the root so the
	// server sees `schema.json` and `<table>.csv` as siblings.
	entries := make([]string, 0, len(schemaFiles)+len(dataFiles))
	entries = append(entries, schemaFiles...)
	entries = append(entries, dataFiles...)

	sp := ui.StartSpinner("Compressing...")
	zipData, err := compressFiles(entries)
	if err != nil {
		sp.Stop(false, "Compression failed")
		return fmt.Errorf("compressing files: %v", err)
	}
	sp.Stop(true, fmt.Sprintf("Compressed (%.1f MB)", float64(zipData.Len())/1024/1024))

	ctx := context.Background()

	sp = ui.StartSpinner("Uploading...")
	ui.Debug("POST %s/v1.0/datasets/sync/upload-url?name=%s", baseURL, datasetName)
	uploadURLResp, err := requestUploadURL(ctx, token, baseURL, datasetName)
	if err != nil {
		sp.Stop(false, "Upload failed")
		return err
	}

	if err := putToStorage(ctx, uploadURLResp.UploadURL, zipData); err != nil {
		sp.Stop(false, "Upload failed")
		return err
	}
	sp.Stop(true, "Uploaded")

	sp = ui.StartSpinner("Processing...")
	result, err := confirmUpload(ctx, token, baseURL, datasetName, uploadURLResp.Path)
	if err != nil {
		sp.Stop(false, "Processing failed")
		return err
	}
	sp.Stop(true, "Done")

	verb := "Uploaded"
	if result.Updated {
		verb = "Updated"
	}
	ui.Success("%s dataset %q", verb, result.Name)
	ui.KeyValue("  Schema: ", fmt.Sprintf("%s%s", result.FingerprintShort, newSchemaBadge(result.SchemaCreated)))
	ui.KeyValue("  Dataset ID: ", result.ID)
	ui.KeyValue("  Files: ", fmt.Sprintf("%d", result.FileCount))
	fmt.Println()
	ui.Info("View it at https://seedmancer.dev/dashboard/schemas")
	return nil
}

// requestUploadURL calls POST /v1.0/datasets/sync/upload-url and returns
// the presigned storage URL and staging path.
func requestUploadURL(ctx context.Context, token, baseURL, datasetName string) (uploadURLResponse, error) {
	q := url.Values{}
	q.Set("name", datasetName)
	endpoint := fmt.Sprintf("%s/v1.0/datasets/sync/upload-url?%s", baseURL, q.Encode())

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, nil)
	if err != nil {
		return uploadURLResponse{}, fmt.Errorf("creating upload-url request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return uploadURLResponse{}, fmt.Errorf("requesting upload URL: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized {
		return uploadURLResponse{}, utils.ErrInvalidAPIToken
	}
	if resp.StatusCode == http.StatusPaymentRequired {
		return uploadURLResponse{}, formatLimitError(body)
	}
	if resp.StatusCode != http.StatusOK {
		return uploadURLResponse{}, fmt.Errorf("server responded %s: %s", resp.Status, string(body))
	}

	var result uploadURLResponse
	if err := json.Unmarshal(body, &result); err != nil || result.UploadURL == "" {
		return uploadURLResponse{}, fmt.Errorf("parsing upload-url response: %v", err)
	}
	return result, nil
}

// putToStorage PUTs the zip bytes directly to the presigned storage URL.
func putToStorage(ctx context.Context, signedURL string, zipData *bytes.Buffer) error {
	req, err := http.NewRequestWithContext(ctx, "PUT", signedURL, bytes.NewReader(zipData.Bytes()))
	if err != nil {
		return fmt.Errorf("creating storage PUT request: %v", err)
	}
	req.Header.Set("Content-Type", "application/zip")
	req.ContentLength = int64(zipData.Len())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("uploading to storage: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("storage upload failed (HTTP %d)", resp.StatusCode)
	}
	return nil
}

// confirmUpload calls POST /v1.0/datasets/sync/confirm and returns the result.
func confirmUpload(ctx context.Context, token, baseURL, datasetName, stagingPath string) (syncUploadResult, error) {
	q := url.Values{}
	q.Set("name", datasetName)
	q.Set("path", stagingPath)
	endpoint := fmt.Sprintf("%s/v1.0/datasets/sync/confirm?%s", baseURL, q.Encode())

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, nil)
	if err != nil {
		return syncUploadResult{}, fmt.Errorf("creating confirm request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return syncUploadResult{}, fmt.Errorf("confirming upload: %v", err)
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
		return syncUploadResult{}, fmt.Errorf("parsing confirm response: %v", err)
	}
	return result, nil
}

func newSchemaBadge(created bool) string {
	if created {
		return "  (new)"
	}
	return ""
}

func compressFiles(files []string) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	zipWriter := zip.NewWriter(buf)

	for _, file := range files {
		if err := addFileToZip(zipWriter, file); err != nil {
			_ = zipWriter.Close()
			return nil, err
		}
	}

	if err := zipWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close zip writer: %v", err)
	}
	return buf, nil
}

func addFileToZip(zw *zip.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening %s: %v", path, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %v", path, err)
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("zip header %s: %v", path, err)
	}
	header.Name = filepath.Base(path)
	header.Method = zip.Deflate

	writer, err := zw.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("zip entry %s: %v", path, err)
	}
	if _, err := io.Copy(writer, f); err != nil {
		return fmt.Errorf("writing %s: %v", path, err)
	}
	return nil
}
