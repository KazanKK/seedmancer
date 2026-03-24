package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	db "github.com/KazanKK/seedmancer/database"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

const (
	openAIEndpoint = "https://api.openai.com/v1/chat/completions"
	tmpScriptName  = "_tmp_seedmancer_gen.go"
)

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIRequest struct {
	Model       string          `json:"model"`
	Messages    []openAIMessage `json:"messages"`
	Temperature float64         `json:"temperature"`
}

type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func GenerateCommand() *cli.Command {
	return &cli.Command{
		Name:  "generate",
		Usage: "Generate realistic seed data using AI and save as CSV files",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "prompt",
				Required: true,
				Usage:    "Natural language description of the data to generate",
			},
			&cli.StringFlag{
				Name:    "api-key",
				Usage:   "OpenAI API key (stored locally for future use)",
				EnvVars: []string{"OPENAI_API_KEY"},
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
		},
		Action: func(c *cli.Context) error {
			return runGenerate(c)
		},
	}
}

func runGenerate(c *cli.Context) error {
	prompt := c.String("prompt")

	// Resolve and optionally persist the OpenAI API key.
	apiKey, err := resolveAndStoreAPIKey(c.String("api-key"))
	if err != nil {
		return err
	}

	// Resolve config for storage path and database name.
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

	// Resolve database URL (same pattern as export command).
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

	// Output directory mirrors what `export` produces:
	//   <projectRoot>/<storagePath>/databases/<databaseName>/<versionName>/
	outputDir := utils.GetVersionPath(projectRoot, cfg.StoragePath, databaseName, versionName)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("creating output directory: %v", err)
	}
	fmt.Printf("Output directory: %s\n", outputDir)

	// ── Step 1: Export schema from the live database ─────────────────────────
	fmt.Println("[1/2] Exporting database schema...")

	pg := &db.PostgresManager{}
	if err := pg.ConnectWithDSN(dbURL); err != nil {
		return fmt.Errorf("connecting to database: %v", err)
	}

	schemaPath := filepath.Join(outputDir, "schema.json")
	if err := pg.ExportSchema(schemaPath); err != nil {
		return fmt.Errorf("exporting schema: %v", err)
	}
	fmt.Printf("Schema saved: %s\n", schemaPath)

	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("reading schema.json: %v", err)
	}
	schemaJSON := string(schemaBytes)

	// ── Step 2: Generate Go script using the confirmed schema ───────────────
	fmt.Println("[2/2] Generating Go data script via OpenAI...")

	goCode, err := callOpenAI(apiKey, prompt, schemaJSON)
	if err != nil {
		return fmt.Errorf("generating Go script: %v", err)
	}

	tmpScriptPath := filepath.Join(outputDir, tmpScriptName)
	if err := os.WriteFile(tmpScriptPath, []byte(goCode), 0644); err != nil {
		return fmt.Errorf("saving temporary script: %v", err)
	}
	fmt.Printf("Go script saved: %s\n", tmpScriptPath)

	// ── Step 3: Execute the script — CSVs must land in outputDir ────────────
	fmt.Println("Generating CSV data...")
	scriptCmd := exec.Command("go", "run", tmpScriptPath, outputDir)
	scriptCmd.Stdout = os.Stdout
	scriptCmd.Stderr = os.Stderr
	if err := scriptCmd.Run(); err != nil {
		return fmt.Errorf("executing generated script: %v\nReview %s to diagnose issues", err, tmpScriptPath)
	}

	// Verify CSV files were actually written to the output directory.
	csvFiles, err := findCSVFiles(outputDir)
	if err != nil {
		return fmt.Errorf("reading output directory: %v", err)
	}
	if len(csvFiles) == 0 {
		return fmt.Errorf(
			"generated script produced no CSV files in %s\n"+
				"The script may have written files to a different location — review %s",
			outputDir, tmpScriptPath,
		)
	}

	fmt.Printf("\n✅ Generated data stored in: %s\n", outputDir)
	fmt.Printf("   schema.json + %d CSV file(s): %s\n", len(csvFiles), strings.Join(csvFiles, ", "))
	fmt.Printf("Run 'seedmancer seed --database-name %s --version-name %s' to import into PostgreSQL.\n", databaseName, versionName)
	return nil
}

// findCSVFiles returns the base names of all .csv files in dir.
func findCSVFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".csv") {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// resolveAndStoreAPIKey returns the API key to use, persisting it to the
// global config when supplied via the flag so subsequent runs need not repeat it.
func resolveAndStoreAPIKey(flagValue string) (string, error) {
	if flagValue != "" {
		if err := utils.SaveOpenAIKey(flagValue); err != nil {
			fmt.Printf("Warning: could not persist API key: %v\n", err)
		}
		return flagValue, nil
	}

	// Fall back to the global ~/.seedmancer/config.yaml.
	homeDir, err := os.UserHomeDir()
	if err == nil {
		globalCfg, cfgErr := utils.LoadConfig(filepath.Join(homeDir, ".seedmancer", "config.yaml"))
		if cfgErr == nil && globalCfg.OpenAIAPIKey != "" {
			return globalCfg.OpenAIAPIKey, nil
		}
	}

	// Fall back to the nearest project seedmancer.yaml.
	if configPath, cfgErr := utils.FindConfigFile(); cfgErr == nil {
		if cfg, loadErr := utils.LoadConfig(configPath); loadErr == nil && cfg.OpenAIAPIKey != "" {
			return cfg.OpenAIAPIKey, nil
		}
	}

	return "", fmt.Errorf(
		"OpenAI API key required: use --api-key flag or set OPENAI_API_KEY environment variable\n" +
			"  Example: seedmancer generate --prompt \"...\" --api-key sk-...",
	)
}

// callOpenAI asks OpenAI to generate a Go data-generation script that strictly
// follows the already-confirmed schemaJSON.
func callOpenAI(apiKey, userPrompt string, schemaJSON string) (string, error) {
	reqBody := openAIRequest{
		Model: "gpt-4o",
		Messages: []openAIMessage{
			{Role: "system", Content: buildCodeSystemPrompt()},
			{Role: "user", Content: buildCodeUserPrompt(userPrompt, schemaJSON)},
		},
		Temperature: 0.2,
	}
	raw, err := doOpenAIRequest(apiKey, reqBody)
	if err != nil {
		return "", err
	}
	code := extractGoCode(raw)
	if code == "" {
		return "", fmt.Errorf("OpenAI returned empty code")
	}
	return code, nil
}

// doOpenAIRequest executes a single chat-completions call and returns the raw
// text content of the first choice.
func doOpenAIRequest(apiKey string, reqBody openAIRequest) (string, error) {
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshalling request: %v", err)
	}

	req, err := http.NewRequest("POST", openAIEndpoint, bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("creating HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("calling OpenAI API: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading API response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("OpenAI API error (HTTP %d): %s", resp.StatusCode, string(body))
	}

	var aiResp openAIResponse
	if err := json.Unmarshal(body, &aiResp); err != nil {
		return "", fmt.Errorf("parsing API response: %v", err)
	}
	if aiResp.Error != nil {
		return "", fmt.Errorf("OpenAI error: %s", aiResp.Error.Message)
	}
	if len(aiResp.Choices) == 0 {
		return "", fmt.Errorf("empty response from OpenAI")
	}
	return aiResp.Choices[0].Message.Content, nil
}

// extractGoCode strips optional markdown code fences from a Go source response.
func extractGoCode(content string) string {
	content = strings.TrimSpace(content)
	if strings.HasPrefix(content, "```") {
		lines := strings.Split(content, "\n")
		lines = lines[1:] // drop the opening ```go / ``` line
		if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "```" {
			lines = lines[:len(lines)-1]
		}
		content = strings.Join(lines, "\n")
	}
	return strings.TrimSpace(content)
}

func buildCodeSystemPrompt() string {
	return `You are an expert Go developer specializing in generating realistic database seed data.
You write clean, self-contained Go programs that use ONLY the Go standard library.
Always return ONLY valid Go source code — no markdown fences, no explanations, no commentary.`
}

func buildCodeUserPrompt(userDesc string, schemaJSON string) string {
	return fmt.Sprintf(`Generate a self-contained Go program (package main, stdlib only) that creates realistic seed data.

User description: "%s"

Use this exact schema to generate the seed data:
%s

Requirements:

1. Read output directory from os.Args[1].

2. For each table in the schema (in the order they appear), create <output_dir>/<tablename>.csv:
   - First row is the header — column names in the EXACT order they appear in the schema
   - Subsequent rows are data values; use empty string for NULL
   - Booleans  : "true" or "false"
   - Timestamps: RFC3339 e.g. "2024-01-15T10:30:00Z"

3. Foreign keys:
   - Generate all parent rows first; store their IDs in a slice
   - For each child row pick a random parent ID from that slice

4. Realistic data:
   - Seed math/rand with 42 for reproducibility
   - Values should look real (varied names, plausible emails, realistic amounts)
   - Do NOT use placeholder values like "user1", "email2"

5. Performance:
   - Use bufio.NewWriter for CSV writing
   - Pre-allocate slices where size is known
   - Stream rows; do not accumulate all rows in memory before writing

Return ONLY the Go source code. No markdown fences, no explanations.
`, userDesc, schemaJSON)
}
