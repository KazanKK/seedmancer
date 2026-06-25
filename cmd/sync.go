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
	"time"

	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// PushCommand uploads the latest (or selected) revision of a scenario to the cloud.
// The scenario path is the dataset name; the local revision id (rNNN) is sent so
// the server stores an immutable row under that label.
func PushCommand() *cli.Command {
	return &cli.Command{
		Name:      "push",
		Aliases:   []string{"sync"},
		Usage:     "Upload a scenario's latest revision to the cloud",
		ArgsUsage: "[scenario]",
		Description: "Zips the schema sidecars + the chosen revision's CSVs and uploads\n" +
			"them to your Seedmancer cloud account. The scenario path is the cloud name;\n" +
			"the revision label (e.g. r002) is preserved on the server.\n\n" +
			"With --all, only scenarios missing from the connected cloud API or whose\n" +
			"local stamp no longer matches the cloud are pushed (diff-only). To re-push\n" +
			"a scenario already in sync, use `seedmancer push <scenario>` directly.",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "all",
				Usage: "Push every local scenario (latest revision each)",
			},
			&cli.StringFlag{
				Name:  "token",
				Usage: "API token (falls back to SEEDMANCER_API_TOKEN env var, then ~/.seedmancer/credentials)",
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

			scenarioArg := strings.TrimSpace(c.Args().First())
			useAll := c.Bool("all")
			if useAll && scenarioArg != "" {
				return fmt.Errorf("cannot combine --all with a scenario name")
			}
			if !useAll && scenarioArg == "" {
				return usageError(c, "missing scenario — pass a scenario path or use --all")
			}

			baseURL := utils.GetBaseURL()

			if useAll {
				paths, badManifests, walkErr := scenario.WalkScenarios(projectRoot, cfg.StoragePath)
				if walkErr != nil {
					return walkErr
				}
				for path, manifestErr := range badManifests {
					ui.Warn("skipping scenario %q (unreadable manifest): %v", path, manifestErr)
				}
			if len(paths) == 0 {
				return fmt.Errorf("no scenarios to push — run `seedmancer export <scenario>` first")
			}
			cloudDatasets, err := listRemoteDatasets(baseURL, token)
			if err != nil {
				return fmt.Errorf("listing cloud datasets: %w", err)
			}
			var pushed, skipped int
			for _, scenarioPath := range paths {
				rev, revErr := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, "")
				if revErr != nil {
					return fmt.Errorf("push %s: %w", scenarioPath, revErr)
				}
				if cloudDS, ok := cloudDatasets[scenarioPath]; ok && isPushUpToDate(rev.Manifest, cloudDS) {
					ui.Info("  skip  %s @ %s  (already in cloud)", scenarioPath, rev.RevID)
					skipped++
					continue
				}
				schemaDir := scenario.SchemaStoreDir(projectRoot, cfg.StoragePath, utils.FingerprintShort(rev.Manifest.SchemaFingerprint))
				ui.Step("%s @ %s  (schema %s)", scenarioPath, rev.RevID, utils.FingerprintShort(rev.Manifest.SchemaFingerprint))
				if err := syncOne(schemaDir, rev.DataDir, scenarioPath, rev.RevID, baseURL, token, scenarioPrompt(projectRoot, cfg.StoragePath, scenarioPath)); err != nil {
					return fmt.Errorf("push %s: %w", scenarioPath, err)
				}
				pushed++
			}
			ui.Info("pushed %d, skipped %d (already in cloud)", pushed, skipped)
			return nil
			}

			scenarioPath, err := scenario.Normalize(scenarioArg)
			if err != nil {
				return err
			}
			rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, "")
			if err != nil {
				return err
			}
			schemaDir := scenario.SchemaStoreDir(projectRoot, cfg.StoragePath, utils.FingerprintShort(rev.Manifest.SchemaFingerprint))
			ui.Step("%s @ %s  (schema %s)", scenarioPath, rev.RevID, utils.FingerprintShort(rev.Manifest.SchemaFingerprint))
			return syncOne(schemaDir, rev.DataDir, scenarioPath, rev.RevID, baseURL, token, scenarioPrompt(projectRoot, cfg.StoragePath, scenarioPath))
		},
	}
}

// isPushUpToDate reports whether the local revision stamp matches the cloud's
// latest revision for the same scenario path. Mirrors the pull-side check in
// RunFetch so push --all only skips scenarios the connected API confirms are
// already in sync — a stale local remoteId alone is not enough.
func isPushUpToDate(rm scenario.RevisionManifest, cloud datasetAPI) bool {
	return rm.RemoteID != "" &&
		rm.RemoteID == cloud.ID &&
		rm.RemoteUpdatedAt != "" &&
		rm.RemoteUpdatedAt == cloud.UpdatedAt
}

// scenarioPrompt returns the saved purpose from the scenario manifest, or ""
// when the scenario has none.
func scenarioPrompt(projectRoot, storagePath, scenarioPath string) string {
	m, err := scenario.ReadManifest(scenario.ScenarioDir(projectRoot, storagePath, scenarioPath))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(m.Prompt)
}

// syncOne uploads schema sidecars + revision CSVs for a single scenario.
// revisionID is sent as `revision=rNNN` so the cloud stores under that label.
// prompt, when non-empty, is synced to the cloud scenario after the upload.
func syncOne(schemaDir, dataDir, datasetName, revisionID, baseURL, token, prompt string) error {
	start := time.Now()
	schemaFiles, err := utils.SchemaFiles(schemaDir)
	if err != nil {
		return err
	}
	dataFiles, err := utils.DatasetFiles(dataDir)
	if err != nil {
		return err
	}
	if len(dataFiles) == 0 {
		return fmt.Errorf("no CSV or JSON files in %s", dataDir)
	}

	entries := make([]string, 0, len(schemaFiles)+len(dataFiles)+1)
	entries = append(entries, schemaFiles...)
	entries = append(entries, dataFiles...)
	// Bundle the agent-written SQL sidecar (if present) so a round-trip
	// pull preserves the source of truth, not just the materialised CSVs.
	revDir := filepath.Dir(dataDir)
	if sqlPath := DatasetSQLPath(revDir); fileExists(sqlPath) {
		entries = append(entries, sqlPath)
	}

	sp := ui.StartSpinner("Compressing...")
	zipData, err := compressFiles(entries)
	if err != nil {
		sp.Stop(false, "Compression failed")
		return fmt.Errorf("compressing files: %v", err)
	}
	sp.Stop(true, fmt.Sprintf("Compressed (%s)", formatBytes(int64(zipData.Len()))))

	ctx := context.Background()

	sp = ui.StartSpinner("Uploading...")
	ui.Debug("POST %s/v1.0/datasets/sync/upload-url?name=%s", baseURL, datasetName)
	uploadURLResp, err := requestUploadURL(ctx, token, baseURL, datasetName, revisionID)
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
	result, err := confirmUpload(ctx, token, baseURL, datasetName, uploadURLResp.Path, revisionID)
	if err != nil {
		sp.Stop(false, "Processing failed")
		return err
	}
	sp.Stop(true, "Done")

	// Sync the scenario's saved purpose. Best-effort: the data upload
	// already succeeded and the prompt re-syncs on the next push.
	if strings.TrimSpace(prompt) != "" && result.ID != "" {
		if pErr := pushScenarioPrompt(ctx, token, baseURL, result.ID, strings.TrimSpace(prompt)); pErr != nil {
			ui.Warn("could not sync the scenario purpose: %v", pErr)
		}
	}

	// Stamp the local revision with the cloud revision it now mirrors so a
	// subsequent `seedmancer pull` can skip the download. Best-effort.
	stampRemoteRevision(revDir, baseURL, token, datasetName, result.FingerprintShort)

	verb := "Uploaded"
	if result.Updated {
		verb = "Updated"
	}
	ui.Success("%s scenario %q (%s in %s)", verb, result.Name,
		formatBytes(int64(zipData.Len())), formatDuration(time.Since(start)))
	ui.KeyValue("  Schema: ", fmt.Sprintf("%s%s", result.FingerprintShort, newSchemaBadge(result.SchemaCreated)))
	ui.KeyValue("  ID:     ", result.ID)
	ui.KeyValue("  Files:  ", fmt.Sprintf("%d", result.FileCount))
	fmt.Println()
	ui.Info("View it at https://seedmancer.dev/dashboard/schemas")
	return nil
}

// stampRemoteRevision records the cloud's latest revision id/updatedAt on a
// local revision manifest after a successful push. `seedmancer pull` uses
// the stamp to detect "already up to date" without downloading. Failures
// are swallowed — the stamp is an optimisation, not a correctness need.
func stampRemoteRevision(revDir, baseURL, token, datasetName, schemaPrefix string) {
	match, err := findRemoteDataset(baseURL, token, datasetName, schemaPrefix)
	if err != nil || match.ID == "" || match.UpdatedAt == "" {
		return
	}
	rm, err := scenario.ReadRevisionManifest(revDir)
	if err != nil {
		return
	}
	rm.RemoteID = match.ID
	rm.RemoteUpdatedAt = match.UpdatedAt
	_ = scenario.WriteRevisionManifest(revDir, rm)
}

// requestUploadURL calls POST /v1.0/datasets/sync/upload-url and returns
// the presigned storage URL and staging path.
func requestUploadURL(ctx context.Context, token, baseURL, datasetName, revisionID string) (uploadURLResponse, error) {
	q := url.Values{}
	q.Set("name", datasetName)
	if strings.TrimSpace(revisionID) != "" {
		q.Set("revision", strings.TrimSpace(revisionID))
	}
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
func confirmUpload(ctx context.Context, token, baseURL, datasetName, stagingPath, revisionID string) (syncUploadResult, error) {
	q := url.Values{}
	q.Set("name", datasetName)
	q.Set("path", stagingPath)
	if strings.TrimSpace(revisionID) != "" {
		q.Set("revision", strings.TrimSpace(revisionID))
	}
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
