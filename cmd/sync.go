package cmd

import (
	"archive/zip"
	"bytes"
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

// SyncCommand uploads a local dataset to the cloud.
//
// The server identifies the target schema from the fingerprint of the
// zip's `schema.json`, so we never send a schema name — it's fully
// derived. Dataset ids must be unique across local schemas; if the same
// dataset id exists under two schemas, rename one via `seedmancer schemas
// rename` first.
func SyncCommand() *cli.Command {
	return &cli.Command{
		Name:  "sync",
		Usage: "Upload a single local dataset to the cloud",
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

	sp = ui.StartSpinner("Uploading...")
	q := url.Values{}
	q.Set("name", datasetName)
	apiURL := fmt.Sprintf("%s/v1.0/datasets/sync?%s", baseURL, q.Encode())
	ui.Debug("POST %s", apiURL)

	req, err := http.NewRequest("POST", apiURL, bytes.NewReader(zipData.Bytes()))
	if err != nil {
		sp.Stop(false, "Upload failed")
		return fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/zip")
	req.Header.Set("Authorization", utils.BearerAPIToken(token))
	req.ContentLength = int64(zipData.Len())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		sp.Stop(false, "Upload failed")
		return fmt.Errorf("uploading: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized {
		sp.Stop(false, "Upload failed")
		return utils.ErrInvalidAPIToken
	}
	if resp.StatusCode == http.StatusPaymentRequired {
		sp.Stop(false, "Upload blocked")
		return formatLimitError(body)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		sp.Stop(false, "Upload failed")
		return fmt.Errorf("server responded %s: %s", resp.Status, string(body))
	}
	sp.Stop(true, "Uploaded")

	var result struct {
		ID               string `json:"id"`
		Name             string `json:"name"`
		SchemaID         string `json:"schemaId"`
		Fingerprint      string `json:"fingerprint"`
		FingerprintShort string `json:"fingerprintShort"`
		SchemaCreated    bool   `json:"schemaCreated"`
		Updated          bool   `json:"updated"`
		FileCount        int    `json:"fileCount"`
	}
	if err := json.Unmarshal(body, &result); err != nil || result.ID == "" {
		// Non-fatal — the server returned 2xx, just print what we got.
		ui.Success("Sync complete!")
		return nil
	}

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
