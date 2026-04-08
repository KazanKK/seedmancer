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
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// ─── API types ────────────────────────────────────────────────────────────────

type generateJobRequest struct {
	Schema generateSchema `json:"schema"`
	Prompt string         `json:"prompt,omitempty"`
}

type generateSchema struct {
	Enums  []generateEnum   `json:"enums,omitempty"`
	Tables []generateTable  `json:"tables"`
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
	RowCount     int                 `json:"rowCount"` // legacy field from job row
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

	// ── Resolve API token ──────────────────────────────────────────────────────
	apiToken, err := resolveAndStoreAPIToken(c.String("token"))
	if err != nil {
		return err
	}

	// ── Resolve config ─────────────────────────────────────────────────────────
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
		fmt.Printf("Using auto-generated version name: %s\n", versionName)
	}

	// ── Resolve DB URL ─────────────────────────────────────────────────────────
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

	// ── Output directory ───────────────────────────────────────────────────────
	outputDir := utils.GetVersionPath(projectRoot, cfg.StoragePath, databaseName, versionName)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("creating output directory: %v", err)
	}
	fmt.Printf("Output directory: %s\n", outputDir)

	// ── Step 1: Export schema from the live database ───────────────────────────
	fmt.Println("[1/3] Exporting database schema...")

	pg := &db.PostgresManager{}
	if err := pg.ConnectWithDSN(dbURL); err != nil {
		return fmt.Errorf("connecting to database: %v", err)
	}

	if err := pg.ExportSchema(outputDir); err != nil {
		return fmt.Errorf("exporting schema: %v", err)
	}
	schemaPath := filepath.Join(outputDir, "schema.json")
	fmt.Printf("Schema saved: %s\n", schemaPath)

	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("reading schema.json: %v", err)
	}

	// ── Step 2: Submit generation job to the Seedmancer API ───────────────────
	fmt.Println("[2/3] Submitting AI generation job...")

	apiSchema, err := buildAPISchema(schemaBytes)
	if err != nil {
		return fmt.Errorf("building API schema: %v", err)
	}

	baseURL := c.String("api-url")
	if baseURL == "" {
		baseURL = utils.GetBaseURL()
	}
	baseURL = strings.TrimRight(baseURL, "/")
	fmt.Printf("API endpoint: %s\n", baseURL)
	jobID, err := submitGenerateJob(baseURL, apiToken, apiSchema, prompt)
	if err != nil {
		return fmt.Errorf("submitting generation job: %v", err)
	}
	fmt.Printf("Job submitted: %s\n", jobID)

	// ── Step 3: Poll until the job completes ───────────────────────────────────
	fmt.Println("[3/3] Waiting for AI to generate data...")

	files, err := pollJobUntilDone(baseURL, apiToken, jobID)
	if err != nil {
		return fmt.Errorf("generation job failed: %v", err)
	}

	// ── Download each CSV ──────────────────────────────────────────────────────
	fmt.Printf("\nDownloading %d CSV file(s)...\n", len(files))
	var csvNames []string
	for _, f := range files {
		dest := filepath.Join(outputDir, f.Table+".csv")
		if err := downloadFile(f.FileURL, dest); err != nil {
			return fmt.Errorf("downloading %s.csv: %v", f.Table, err)
		}
		fmt.Printf("  ✓ %s.csv\n", f.Table)
		csvNames = append(csvNames, f.Table+".csv")
	}

	fmt.Printf("\n✅ Generated data stored in: %s\n", outputDir)
	fmt.Printf("   schema.json + %d CSV file(s): %s\n", len(csvNames), strings.Join(csvNames, ", "))
	fmt.Printf("Run 'seedmancer seed --database-name %s --version-name %s' to import into PostgreSQL.\n", databaseName, versionName)
	return nil
}

// ─── Schema conversion ────────────────────────────────────────────────────────

// buildAPISchema converts the CLI's exported schema.json into the shape the
// Seedmancer API expects (only enums and tables; functions/triggers are omitted).
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

	req, err := http.NewRequest("POST", baseURL+"/api/generate-data", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "cli_"+token)

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

// pollJobUntilDone polls the generation-status endpoint every 5 seconds until
// the job completes or fails, with a 10-minute overall timeout.
func pollJobUntilDone(baseURL, token, jobID string) ([]generateFileEntry, error) {
	const (
		pollInterval = 5 * time.Second
		timeout      = 10 * time.Minute
	)

	client := &http.Client{Timeout: 15 * time.Second}
	deadline := time.Now().Add(timeout)
	dots := 0

	for time.Now().Before(deadline) {
		status, err := fetchJobStatus(client, baseURL, token, jobID)
		if err != nil {
			return nil, fmt.Errorf("polling status: %v", err)
		}

		switch status.Status {
		case "completed":
			fmt.Println(" done!")
			return status.Files, nil
		case "error":
			msg := "unknown error"
			if status.ErrorMessage != nil {
				msg = *status.ErrorMessage
			}
			return nil, fmt.Errorf("job failed: %s", msg)
		default:
			// pending / processing — keep waiting
			dots++
			if dots%6 == 0 {
				fmt.Printf("\r  Still working... (%s) ", status.Status)
			} else {
				fmt.Print(".")
			}
		}

		time.Sleep(pollInterval)
	}

	return nil, fmt.Errorf("timed out after %v waiting for job %s", timeout, jobID)
}

func fetchJobStatus(client *http.Client, baseURL, token, jobID string) (*generateStatusResponse, error) {
	req, err := http.NewRequest("GET", baseURL+"/api/generation-status?id="+jobID, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", "cli_"+token)

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

// downloadFile downloads a URL to a local file path.
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

// resolveAndStoreAPIToken returns the API token to use, persisting it to the
// global config when supplied via the flag so subsequent runs need not repeat it.
func resolveAndStoreAPIToken(flagValue string) (string, error) {
	if flagValue != "" {
		if err := utils.SaveAPIToken(flagValue); err != nil {
			fmt.Printf("Warning: could not persist API token: %v\n", err)
		}
		return flagValue, nil
	}

	// Fall back to the global ~/.seedmancer/config.yaml.
	homeDir, err := os.UserHomeDir()
	if err == nil {
		globalCfg, cfgErr := utils.LoadConfig(filepath.Join(homeDir, ".seedmancer", "config.yaml"))
		if cfgErr == nil && globalCfg.APIToken != "" {
			return globalCfg.APIToken, nil
		}
	}

	// Fall back to the nearest project seedmancer.yaml.
	if configPath, cfgErr := utils.FindConfigFile(); cfgErr == nil {
		if cfg, loadErr := utils.LoadConfig(configPath); loadErr == nil && cfg.APIToken != "" {
			return cfg.APIToken, nil
		}
	}

	return "", fmt.Errorf(
		"Seedmancer API token required.\n" +
			"  Use --token flag or set SEEDMANCER_API_TOKEN environment variable.\n" +
			"  Get your token at: https://seedmancer.com/dashboard",
	)
}
