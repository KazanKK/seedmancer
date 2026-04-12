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

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

func SyncCommand() *cli.Command {
	return &cli.Command{
		Name:  "sync",
		Usage: "Sync local test data to cloud storage (defaults to all databases and versions)",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "database-name",
				Usage: "Database name (omit to sync all databases)",
			},
			&cli.StringFlag{
				Name:  "version",
				Usage: "Version name (omit to sync all versions)",
			},
			&cli.StringFlag{
				Name:    "token",
				Usage:   "API token (or set SEEDMANCER_API_TOKEN env var)",
				EnvVars: []string{"SEEDMANCER_API_TOKEN"},
			},
		},
		Action: func(c *cli.Context) error {
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

			token := c.String("token")
			if token == "" {
				return fmt.Errorf("API token is required. Set --token flag or SEEDMANCER_API_TOKEN env var")
			}

			basesDir := filepath.Join(projectRoot, config.StoragePath, "databases")

			targets, err := discoverSyncTargets(basesDir, c.String("database-name"), c.String("version"))
			if err != nil {
				return err
			}

			if len(targets) == 0 {
				return fmt.Errorf("no data found to sync")
			}

			if len(targets) > 1 {
				ui.Step("Syncing %d dataset(s)", len(targets))
			}

			baseURL := utils.GetBaseURL()
			synced := 0

			for _, t := range targets {
				if err := syncOne(t, baseURL, token); err != nil {
					ui.Error("%s / %s: %v", t.databaseName, t.versionName, err)
					continue
				}
				synced++
			}

			if synced == 0 {
				return fmt.Errorf("all syncs failed")
			}

			ui.Success("Sync complete! %d/%d dataset(s) uploaded", synced, len(targets))
			ui.Info("View your data at https://seedmancer.dev/dashboard/datasets")
			return nil
		},
	}
}

type syncTarget struct {
	databaseName string
	versionName  string
	path         string
}

func discoverSyncTargets(basesDir, dbFlag, versionFlag string) ([]syncTarget, error) {
	if dbFlag != "" && versionFlag != "" {
		versionPath := filepath.Join(basesDir, dbFlag, versionFlag)
		if _, err := os.Stat(versionPath); err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("version '%s/%s' not found at %s", dbFlag, versionFlag, versionPath)
			}
			return nil, fmt.Errorf("accessing version directory: %v", err)
		}
		return []syncTarget{{databaseName: dbFlag, versionName: versionFlag, path: versionPath}}, nil
	}

	var databases []string
	if dbFlag != "" {
		databases = []string{dbFlag}
	} else {
		entries, err := os.ReadDir(basesDir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("no databases directory found at %s", basesDir)
			}
			return nil, fmt.Errorf("reading databases directory: %v", err)
		}
		for _, e := range entries {
			if e.IsDir() {
				databases = append(databases, e.Name())
			}
		}
	}

	var targets []syncTarget
	for _, db := range databases {
		dbPath := filepath.Join(basesDir, db)

		if versionFlag != "" {
			vp := filepath.Join(dbPath, versionFlag)
			if _, err := os.Stat(vp); err == nil {
				targets = append(targets, syncTarget{databaseName: db, versionName: versionFlag, path: vp})
			}
			continue
		}

		versions, err := os.ReadDir(dbPath)
		if err != nil {
			continue
		}
		for _, v := range versions {
			if !v.IsDir() {
				continue
			}
			targets = append(targets, syncTarget{databaseName: db, versionName: v.Name(), path: filepath.Join(dbPath, v.Name())})
		}
	}

	return targets, nil
}

func syncOne(t syncTarget, baseURL, token string) error {
	entries, err := os.ReadDir(t.path)
	if err != nil {
		return fmt.Errorf("reading directory: %v", err)
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		lower := strings.ToLower(entry.Name())
		if strings.HasSuffix(lower, ".csv") || strings.HasSuffix(lower, ".json") {
			files = append(files, filepath.Join(t.path, entry.Name()))
		}
	}

	if len(files) == 0 {
		ui.Warn("%s / %s: no CSV or JSON files, skipping", t.databaseName, t.versionName)
		return nil
	}

	ui.Step("%s / %s — %d file(s)", t.databaseName, t.versionName, len(files))

	sp := ui.StartSpinner("Compressing...")
	zipData, err := compressFiles(files)
	if err != nil {
		sp.Stop(false, "Compression failed")
		return fmt.Errorf("compressing files: %v", err)
	}
	sp.Stop(true, fmt.Sprintf("Compressed (%.1f MB)", float64(zipData.Len())/1024/1024))

	sp = ui.StartSpinner("Uploading...")
	apiURL := fmt.Sprintf("%s/v1.0/datasets/sync?database_name=%s&version_name=%s",
		baseURL, t.databaseName, t.versionName)
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
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		sp.Stop(false, "Upload failed")
		return fmt.Errorf("server responded %s: %s", resp.Status, string(body))
	}
	sp.Stop(true, "Uploaded")

	var result struct {
		ID        string `json:"id"`
		FileCount int    `json:"fileCount"`
	}
	if err := json.Unmarshal(body, &result); err == nil && result.ID != "" {
		ui.Info("  ID: %s  |  Files: %d", result.ID, result.FileCount)
	}

	return nil
}

func compressFiles(files []string) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	zipWriter := zip.NewWriter(buf)
	defer zipWriter.Close()

	for _, file := range files {
		fileToZip, err := os.Open(file)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", file, err)
		}
		defer fileToZip.Close()

		info, err := fileToZip.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to get file info %s: %v", file, err)
		}

		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return nil, fmt.Errorf("failed to create header %s: %v", file, err)
		}

		header.Name = filepath.Base(file)
		header.Method = zip.Deflate

		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return nil, fmt.Errorf("failed to create zip entry %s: %v", file, err)
		}

		if _, err := io.Copy(writer, fileToZip); err != nil {
			return nil, fmt.Errorf("failed to write file to zip %s: %v", file, err)
		}
	}

	if err := zipWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close zip writer: %v", err)
	}

	return buf, nil
}
