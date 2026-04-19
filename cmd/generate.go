package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	db "github.com/KazanKK/seedmancer/database"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// ─── API types ────────────────────────────────────────────────────────────────

type generateJobRequest struct {
	Schema      generateSchema `json:"schema"`
	DatasetName string         `json:"datasetName,omitempty"`
	Prompt      string         `json:"prompt,omitempty"`
}

type generateSchema struct {
	Enums  []generateEnum  `json:"enums,omitempty"`
	Tables []generateTable `json:"tables"`
}

type generateEnum struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

type generateTable struct {
	Name    string           `json:"name"`
	Columns []generateColumn `json:"columns"`
}

type generateColumn struct {
	Name       string              `json:"name"`
	Type       string              `json:"type"`
	Nullable   bool                `json:"nullable"`
	IsPrimary  bool                `json:"isPrimary"`
	IsUnique   bool                `json:"isUnique"`
	Default    string              `json:"default,omitempty"`
	ForeignKey *generateForeignKey `json:"foreignKey,omitempty"`
	Enum       string              `json:"enum,omitempty"`
}

type generateForeignKey struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

type generateJobResponse struct {
	JobID string `json:"jobId"`
}

type generateFileEntry struct {
	Table   string `json:"table"`
	Path    string `json:"path"`
	FileURL string `json:"fileUrl"`
}

type generateStatusResponse struct {
	ID           string              `json:"id"`
	Status       string              `json:"status"`
	RowCount     int                 `json:"rowCount"`
	Files        []generateFileEntry `json:"files"`
	ErrorMessage *string             `json:"errorMessage"`
}

// ─── Command definition ───────────────────────────────────────────────────────

// GenerateCommand runs an AI generation job against an existing local schema.
//
// Schema source resolution:
//  1. --from <dataset>             → use the schema folder containing that dataset
//  2. --schema <fp-prefix>         → pick a schema folder directly by fingerprint
//  3. (neither)                    → auto-select if exactly one local schema exists
//
// The resulting CSVs land under the schema's `datasets/<--id>/` folder so the
// layout stays consistent with `export` output.
func GenerateCommand() *cli.Command {
	return &cli.Command{
		Name:  "generate",
		Usage: "Generate realistic CSV data via AI into a new dataset",
		Description: "Sends a local schema + a natural-language prompt to Seedmancer's\n" +
			"AI generation service, then streams the resulting CSVs into a new\n" +
			"dataset folder that sits alongside `seedmancer export` output.\n\n" +
			"Schema source resolution:\n" +
			"  1. --from <dataset>    use the schema folder containing that dataset\n" +
			"  2. --schema <fp-prefix> pick a schema folder by fingerprint\n" +
			"  3. (neither)           auto-select if exactly one local schema exists",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "prompt",
				Required: true,
				Usage:    "(required) Natural-language description of the data to generate",
			},
			&cli.StringFlag{
				Name:    "schema",
				Aliases: []string{"s"},
				Usage:   "Schema fingerprint prefix to generate against (defaults to the sole local schema)",
			},
			&cli.StringFlag{
				Name:  "from",
				Usage: "Existing dataset id to derive the schema from (wins over --schema)",
			},
			&cli.StringFlag{
				Name:  "id",
				Usage: "New dataset id (defaults to a YYYYMMDDHHMMSS timestamp)",
			},
			&cli.StringFlag{
				Name:    "token",
				Usage:   "API token (falls back to SEEDMANCER_API_TOKEN; cached after first use)",
				EnvVars: []string{"SEEDMANCER_API_TOKEN"},
			},
			&cli.StringFlag{
				Name:    "api-url",
				Usage:   "Seedmancer API base URL (overrides SEEDMANCER_API_URL and api_url in config)",
				EnvVars: []string{"SEEDMANCER_API_URL"},
			},
		},
		Action: func(c *cli.Context) error {
			return runGenerate(c)
		},
	}
}

func runGenerate(c *cli.Context) error {
	prompt := c.String("prompt")

	apiToken, err := resolveAndStoreAPIToken(c.String("token"))
	if err != nil {
		return err
	}

	configPath, err := utils.FindConfigFile()
	if err != nil {
		return err
	}
	projectRoot := filepath.Dir(configPath)

	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("reading config: %v", err)
	}

	schemaPrefix := strings.TrimSpace(c.String("schema"))
	fromDataset := strings.TrimSpace(c.String("from"))

	var sourceSchema utils.LocalSchema
	if fromDataset != "" {
		s, _, err := utils.FindLocalDataset(projectRoot, cfg.StoragePath, schemaPrefix, fromDataset)
		if err != nil {
			return fmt.Errorf("locating source schema from --from %q: %v", fromDataset, err)
		}
		sourceSchema = s
	} else {
		s, err := utils.ResolveLocalSchema(projectRoot, cfg.StoragePath, schemaPrefix)
		if err != nil {
			return err
		}
		sourceSchema = s
	}

	schemaBytes, err := os.ReadFile(sourceSchema.SchemaJSONPath)
	if err != nil {
		return fmt.Errorf("reading %s: %v\nRun 'seedmancer export' first to create schema.json.", sourceSchema.SchemaJSONPath, err)
	}
	ui.Info("Using schema: %s  (%s)", sourceSchema.FingerprintShort, sourceSchema.SchemaJSONPath)

	datasetName := strings.TrimSpace(c.String("id"))
	if datasetName == "" {
		datasetName = time.Now().UTC().Format("20060102150405")
		ui.Info("Auto-generated dataset id: %s", datasetName)
	}
	datasetName = utils.SanitizeDatasetSegment(datasetName)

	outputDir := utils.DatasetPath(projectRoot, cfg.StoragePath, sourceSchema.FingerprintShort, datasetName)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("creating dataset directory: %v", err)
	}

	sp := ui.StartSpinner("Submitting AI generation job...")
	apiSchema, err := buildAPISchema(schemaBytes)
	if err != nil {
		sp.Stop(false, "Schema conversion failed")
		return fmt.Errorf("building API schema: %v", err)
	}

	baseURL := c.String("api-url")
	if baseURL == "" {
		baseURL = utils.GetBaseURL()
	}
	baseURL = strings.TrimRight(baseURL, "/")
	ui.Debug("API endpoint: %s", baseURL)

	jobID, err := submitGenerateJob(baseURL, apiToken, apiSchema, prompt, datasetName)
	if err != nil {
		sp.Stop(false, "Job submission failed")
		return fmt.Errorf("submitting generation job: %v", err)
	}
	sp.Stop(true, fmt.Sprintf("Job submitted: %s", jobID))

	files, err := pollJobUntilDone(baseURL, apiToken, jobID)
	if err != nil {
		return fmt.Errorf("generation job failed: %v", err)
	}

	ui.Step("Downloading CSV files...")
	var csvNames []string
	for _, f := range files {
		dest := filepath.Join(outputDir, f.Table+".csv")
		if err := downloadFile(f.FileURL, dest); err != nil {
			return fmt.Errorf("downloading %s.csv: %v", f.Table, err)
		}
		ui.Success("%s.csv", f.Table)
		csvNames = append(csvNames, f.Table+".csv")
	}

	fmt.Println()
	ui.Success("Generated dataset → %s", outputDir)
	ui.Info("%d CSV file(s): %s", len(csvNames), strings.Join(csvNames, ", "))
	ui.Info("Run 'seedmancer seed --id %s' to import locally,", datasetName)
	ui.Info("or 'seedmancer sync --id %s' to upload.", datasetName)
	return nil
}

// ─── Schema conversion ────────────────────────────────────────────────────────

func buildAPISchema(schemaJSON []byte) (generateSchema, error) {
	var raw struct {
		Enums  []db.EnumItem `json:"enums"`
		Tables []db.Table    `json:"tables"`
	}
	if err := json.Unmarshal(schemaJSON, &raw); err != nil {
		return generateSchema{}, fmt.Errorf("parsing schema.json: %v", err)
	}

	var apiEnums []generateEnum
	for _, e := range raw.Enums {
		apiEnums = append(apiEnums, generateEnum{Name: e.Name, Values: e.Values})
	}

	var apiTables []generateTable
	for _, t := range raw.Tables {
		var cols []generateColumn
		for _, c := range t.Columns {
			col := generateColumn{
				Name:      c.Name,
				Type:      c.Type,
				Nullable:  c.Nullable,
				IsPrimary: c.IsPrimary,
				IsUnique:  c.IsUnique,
				Enum:      c.Enum,
			}
			if c.Default != nil {
				col.Default = fmt.Sprintf("%v", c.Default)
			}
			if c.ForeignKey != nil {
				col.ForeignKey = &generateForeignKey{
					Table:  c.ForeignKey.Table,
					Column: c.ForeignKey.Column,
				}
			}
			cols = append(cols, col)
		}
		apiTables = append(apiTables, generateTable{Name: t.Name, Columns: cols})
	}

	return generateSchema{Enums: apiEnums, Tables: apiTables}, nil
}

// ─── API helpers ──────────────────────────────────────────────────────────────

func submitGenerateJob(baseURL, token string, schema generateSchema, prompt, datasetName string) (string, error) {
	body := generateJobRequest{
		Schema:      schema,
		DatasetName: datasetName,
		Prompt:      prompt,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("marshalling request: %v", err)
	}

	req, err := http.NewRequest("POST", baseURL+"/generate-data", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("calling API: %v", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading response: %v", err)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return "", fmt.Errorf("authentication failed — check your API token (--token / SEEDMANCER_API_TOKEN)")
	}
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(respBytes))
	}

	var jobResp generateJobResponse
	if err := json.Unmarshal(respBytes, &jobResp); err != nil {
		return "", fmt.Errorf("parsing job response: %v", err)
	}
	if jobResp.JobID == "" {
		return "", fmt.Errorf("API returned empty job ID")
	}
	return jobResp.JobID, nil
}

var statusLabels = map[string]string{
	"pending":           "Queued, waiting to start...",
	"processing":        "Processing...",
	"generating_script": "Analyzing schema...",
	"executing":         "Generating data...",
	"uploading":         "Finalizing files...",
}

func pollJobUntilDone(baseURL, token, jobID string) ([]generateFileEntry, error) {
	const (
		pollInterval = 3 * time.Second
		timeout      = 10 * time.Minute
	)

	client := &http.Client{Timeout: 15 * time.Second}
	deadline := time.Now().Add(timeout)
	start := time.Now()
	lastStatus := ""

	sp := ui.StartSpinner("Waiting for worker...")

	for time.Now().Before(deadline) {
		status, err := fetchJobStatus(client, baseURL, token, jobID)
		if err != nil {
			sp.Stop(false, "Polling failed")
			return nil, fmt.Errorf("polling status: %v", err)
		}

		elapsed := time.Since(start).Truncate(time.Second)

		switch status.Status {
		case "completed":
			sp.Stop(true, fmt.Sprintf("Generated %d file(s) in %s", len(status.Files), elapsed))
			return status.Files, nil
		case "error":
			msg := "unknown error"
			if status.ErrorMessage != nil {
				msg = *status.ErrorMessage
			}
			sp.Stop(false, fmt.Sprintf("Generation failed (%s)", elapsed))
			return nil, fmt.Errorf("job failed: %s", msg)
		default:
			if status.Status != lastStatus {
				lastStatus = status.Status
				label, ok := statusLabels[status.Status]
				if !ok {
					label = fmt.Sprintf("Status: %s", status.Status)
				}
				sp.Stop(true, label)
				sp = ui.StartSpinner(fmt.Sprintf("%s (%s)", label, elapsed))
			} else {
				sp.UpdateMessage(fmt.Sprintf("%s (%s)", statusLabels[status.Status], elapsed))
			}
		}

		time.Sleep(pollInterval)
	}

	sp.Stop(false, "Timed out")
	return nil, fmt.Errorf("timed out after %v waiting for job %s", timeout, jobID)
}

func fetchJobStatus(client *http.Client, baseURL, token, jobID string) (*generateStatusResponse, error) {
	req, err := http.NewRequest("GET", baseURL+"/generation-status?id="+jobID, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

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

func downloadFile(fileURL, destPath string) error {
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Get(fileURL)
	if err != nil {
		return fmt.Errorf("downloading file: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download HTTP %d", resp.StatusCode)
	}

	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("creating file: %v", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return fmt.Errorf("writing file: %v", err)
	}
	return nil
}

// ─── Token helpers ────────────────────────────────────────────────────────────

func resolveAndStoreAPIToken(flagValue string) (string, error) {
	if flagValue != "" {
		if err := utils.SaveAPIToken(flagValue); err != nil {
			ui.Warn("Could not persist API token: %v", err)
		}
		return flagValue, nil
	}

	homeDir, err := os.UserHomeDir()
	if err == nil {
		globalCfg, cfgErr := utils.LoadConfig(filepath.Join(homeDir, ".seedmancer", "config.yaml"))
		if cfgErr == nil && globalCfg.APIToken != "" {
			return globalCfg.APIToken, nil
		}
	}

	if configPath, cfgErr := utils.FindConfigFile(); cfgErr == nil {
		if cfg, loadErr := utils.LoadConfig(configPath); loadErr == nil && cfg.APIToken != "" {
			return cfg.APIToken, nil
		}
	}

	return "", fmt.Errorf(
		"Seedmancer API token required.\n" +
			"  Use --token flag or set SEEDMANCER_API_TOKEN environment variable.\n" +
			"  Get your token at: https://seedmancer.dev/dashboard/settings",
	)
}
