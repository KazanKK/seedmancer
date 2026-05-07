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

	"github.com/KazanKK/seedmancer/internal/scenario"
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

// GenerateCommand runs an AI generation job for a scenario and stores
// the result as a new revision.
//
// The schema is resolved from --inherit (when given) or from the
// scenario's existing latest revision; one of those must exist so we
// know which schema to send to the cloud service.
func GenerateCommand() *cli.Command {
	return &cli.Command{
		Name:      "generate",
		Usage:     "Generate AI data into a new revision of a scenario",
		ArgsUsage: "<scenario>",
		Description: "Sends the scenario's schema + a natural-language prompt to\n" +
			"Seedmancer's AI generation service, then streams the resulting\n" +
			"CSVs into a new revision under the scenario.\n\n" +
			"Use --inherit to pin the schema to an existing scenario when the\n" +
			"target scenario doesn't have a revision yet.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "prompt",
				Required: true,
				Usage:    "(required) Natural-language description of the data to generate",
			},
			&cli.StringFlag{
				Name:    "inherit",
				Aliases: []string{"b"},
				Usage:   "Scenario whose latest revision provides the schema fingerprint",
			},
			&cli.StringFlag{
				Name:  "description",
				Usage: "Optional description stored on the new revision manifest",
			},
			&cli.StringFlag{
				Name:  "token",
				Usage: "API token (falls back to ~/.seedmancer/credentials, then SEEDMANCER_API_TOKEN)",
			},
		},
		Action: func(c *cli.Context) error {
			scenarioArg := strings.TrimSpace(c.Args().First())
			if scenarioArg == "" {
				return fmt.Errorf("usage: seedmancer generate <scenario>")
			}
			out, err := RunGenerate(c.Context, GenerateInput{
				Prompt:      c.String("prompt"),
				Scenario:    scenarioArg,
				Inherit:     c.String("inherit"),
				Description: c.String("description"),
				Token:       c.String("token"),
			})
			if err != nil {
				return err
			}
			ui.Success("Generated revision: %s @ %s", out.Scenario, out.Revision)
			ui.KeyValue("Schema: ", out.Schema)
			ui.KeyValue("Path: ", out.Path)
			ui.KeyValue("Run: ", fmt.Sprintf("seedmancer seed %s", out.Scenario))
			return nil
		},
	}
}

// keep these helpers exported via package — they back submit/poll for
// future scenarios where the CLI bypasses the runner. We don't call
// them from any *new* path right now, but the package-internal generate
// runner does.
var _ = submitGenerateJob
var _ = pollJobUntilDone
var _ = downloadFile
var _ = buildAPISchema
var _ = scenario.Normalize
var _ = time.Now
var _ = filepath.Join
var _ = os.Stat
var _ = db.Table{}

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
		return "", utils.ErrInvalidAPIToken
	}
	if resp.StatusCode == http.StatusPaymentRequired {
		return "", formatLimitError(respBytes)
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
