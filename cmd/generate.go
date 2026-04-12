package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	Schema generateSchema `json:"schema"`
	Prompt string         `json:"prompt,omitempty"`
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

func GenerateCommand() *cli.Command {
	return &cli.Command{
		Name:  "generate",
		Usage: "Generate realistic seed data via AI and save as CSV files",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "prompt",
				Required: true,
				Usage:    "Natural language description of the data to generate",
			},
			&cli.StringFlag{
				Name:    "token",
				Usage:   "Seedmancer API token (saved locally for future use)",
				EnvVars: []string{"SEEDMANCER_API_TOKEN"},
			},
			&cli.StringFlag{
				Name:    "db-url",
				Usage:   "PostgreSQL connection URL",
				EnvVars: []string{"SEEDMANCER_DATABASE_URL"},
			},
			&cli.StringFlag{
				Name:  "database-name",
				Usage: "Database name used for the output directory (overrides database_name in seedmancer.yaml)",
			},
			&cli.StringFlag{
				Name:  "version-name",
				Usage: "Version directory name (optional; auto-generates YYYYMMDDHHMMSS_<database-name> if omitted)",
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
		return fmt.Errorf("finding config file: %v", err)
	}
	projectRoot := filepath.Dir(configPath)

	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("reading config: %v", err)
	}

	databaseName := strings.TrimSpace(c.String("database-name"))
	if databaseName == "" {
		databaseName = cfg.DatabaseName
	}
	if databaseName == "" {
		return fmt.Errorf("database name required: use --database-name or set database_name in seedmancer.yaml")
	}

	versionName := strings.TrimSpace(c.String("version-name"))
	if versionName == "" {
		versionName = utils.DefaultVersionName(databaseName)
		ui.Info("Auto-generated version: %s", versionName)
	}

	dbURL := c.String("db-url")
	if dbURL == "" {
		dbURL = cfg.DatabaseURL
	}
	if dbURL == "" {
		return fmt.Errorf("database URL required: set database_url in seedmancer.yaml, or use --db-url / SEEDMANCER_DATABASE_URL")
	}

	u, err := url.Parse(dbURL)
	if err != nil {
		return fmt.Errorf("parsing database URL: %v", err)
	}
	if u.Scheme == "postgresql" {
		dbURL = "postgres" + dbURL[len("postgresql"):]
		u.Scheme = "postgres"
	}
	if u.Scheme != "postgres" {
		return fmt.Errorf("unsupported database type: %s (only postgres is supported)", u.Scheme)
	}
	if !strings.Contains(dbURL, "sslmode=") {
		if strings.Contains(dbURL, "?") {
			dbURL += "&sslmode=disable"
		} else {
			dbURL += "?sslmode=disable"
		}
	}

	outputDir := utils.GetVersionPath(projectRoot, cfg.StoragePath, databaseName, versionName)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("creating output directory: %v", err)
	}
	ui.Debug("Output directory: %s", outputDir)

	// Step 1: Export schema
	sp := ui.StartSpinner("Exporting database schema...")
	pg := &db.PostgresManager{}
	if err := pg.ConnectWithDSN(dbURL); err != nil {
		sp.Stop(false, "Failed to connect")
		return fmt.Errorf("connecting to database: %v", err)
	}

	if err := pg.ExportSchema(outputDir); err != nil {
		sp.Stop(false, "Schema export failed")
		return fmt.Errorf("exporting schema: %v", err)
	}
	schemaPath := filepath.Join(outputDir, "schema.json")
	sp.Stop(true, "Schema exported")

	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("reading schema.json: %v", err)
	}

	// Step 2: Submit generation job
	sp = ui.StartSpinner("Submitting AI generation job...")
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

	jobID, err := submitGenerateJob(baseURL, apiToken, apiSchema, prompt)
	if err != nil {
		sp.Stop(false, "Job submission failed")
		return fmt.Errorf("submitting generation job: %v", err)
	}
	sp.Stop(true, fmt.Sprintf("Job submitted: %s", jobID))

	// Step 3: Poll until done
	sp = ui.StartSpinner("Generating data with AI...")
	files, err := pollJobUntilDone(baseURL, apiToken, jobID)
	if err != nil {
		sp.Stop(false, "Generation failed")
		return fmt.Errorf("generation job failed: %v", err)
	}
	sp.Stop(true, fmt.Sprintf("Generated %d file(s)", len(files)))

	// Download CSVs
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
	ui.Success("Generated data stored in: %s", outputDir)
	ui.Info("schema.json + %d CSV file(s): %s", len(csvNames), strings.Join(csvNames, ", "))
	ui.Info("Run 'seedmancer seed --database-name %s --version-name %s' to import.", databaseName, versionName)
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

func submitGenerateJob(baseURL, token string, schema generateSchema, prompt string) (string, error) {
	body := generateJobRequest{
		Schema: schema,
		Prompt: prompt,
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

	client := &http.Client{Timeout: 30 * time.Second}
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

func pollJobUntilDone(baseURL, token, jobID string) ([]generateFileEntry, error) {
	const (
		pollInterval = 5 * time.Second
		timeout      = 10 * time.Minute
	)

	client := &http.Client{Timeout: 15 * time.Second}
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		status, err := fetchJobStatus(client, baseURL, token, jobID)
		if err != nil {
			return nil, fmt.Errorf("polling status: %v", err)
		}

		switch status.Status {
		case "completed":
			return status.Files, nil
		case "error":
			msg := "unknown error"
			if status.ErrorMessage != nil {
				msg = *status.ErrorMessage
			}
			return nil, fmt.Errorf("job failed: %s", msg)
		default:
			ui.Debug("Job status: %s", status.Status)
		}

		time.Sleep(pollInterval)
	}

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
