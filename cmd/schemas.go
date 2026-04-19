package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"
)

// schemaSummary mirrors a single row returned by GET /v1.0/schemas. The field
// set matches the fingerprint-first model — no more revision counts, just a
// nullable display name and the fingerprint.
type schemaSummary struct {
	ID               string  `json:"id"`
	DisplayName      *string `json:"displayName"`
	Description      *string `json:"description"`
	Fingerprint      string  `json:"fingerprint"`
	FingerprintShort string  `json:"fingerprintShort"`
	IsLegacy         bool    `json:"isLegacy"`
	TableCount       int     `json:"tableCount"`
	DatasetCount     int     `json:"datasetCount"`
	TotalSize        int64   `json:"totalSize"`
	LastSyncedAt     *string `json:"lastSyncedAt"`
	CreatedAt        string  `json:"createdAt"`
	UpdatedAt        string  `json:"updatedAt"`
}

type schemasResponse struct {
	Schemas []schemaSummary `json:"schemas"`
}

func SchemasCommand() *cli.Command {
	return &cli.Command{
		Name:            "schemas",
		Aliases:         []string{"schema"},
		Usage:           "Inspect and manage server-side schemas",
		HideHelpCommand: true,
		Description: "Schemas are auto-created every time you `seedmancer sync` a fresh\n" +
			"schema.json. This command group lets you inspect them, give them\n" +
			"human-friendly display names, or delete ones you no longer need.",
		Subcommands: []*cli.Command{
			{
				Name:  "list",
				Usage: "List all schemas in your Seedmancer account",
				Description: "Shows every schema the server knows about, sorted by last-synced\n" +
					"time (newest first). The LABEL column is the custom display name\n" +
					"if set via `seedmancer schemas rename`, otherwise the fingerprint.",
				ArgsUsage: " ",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "token",
						Usage:   "API token (falls back to SEEDMANCER_API_TOKEN)",
						EnvVars: []string{"SEEDMANCER_API_TOKEN"},
					},
					&cli.BoolFlag{
						Name:  "json",
						Usage: "Emit result as JSON for CI/CD pipelines",
						Value: false,
					},
				},
				Action: runSchemasList,
			},
			{
				Name:      "rename",
				Usage:     "Set or clear the display name for a schema",
				ArgsUsage: "<fp-prefix-or-id> <new-name>",
				Description: "Gives a schema a human-friendly label (shown in the dashboard and\n" +
					"`seedmancer schemas list`). Pass an empty string (\"\") or --clear\n" +
					"to remove the custom name and fall back to the fingerprint short id.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "token",
						Usage:   "API token (falls back to SEEDMANCER_API_TOKEN)",
						EnvVars: []string{"SEEDMANCER_API_TOKEN"},
					},
					&cli.BoolFlag{
						Name:  "clear",
						Usage: "Clear the display name (falls back to fingerprint short id)",
						Value: false,
					},
				},
				Action: runSchemasRename,
			},
			{
				Name:      "rm",
				Usage:     "Delete a schema (orphans any attached datasets)",
				ArgsUsage: "<fp-prefix-or-id>",
				Description: "Permanently deletes a schema from the server. Any datasets attached\n" +
					"to it become orphans — they remain downloadable by id but lose their\n" +
					"schema grouping. Prompts for confirmation unless --force is passed.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "token",
						Usage:   "API token (falls back to SEEDMANCER_API_TOKEN)",
						EnvVars: []string{"SEEDMANCER_API_TOKEN"},
					},
					&cli.BoolFlag{
						Name:    "force",
						Aliases: []string{"y"},
						Usage:   "Skip the confirmation prompt",
						Value:   false,
					},
				},
				Action: runSchemasRm,
			},
		},
	}
}

func runSchemasList(c *cli.Context) error {
	token, err := utils.ResolveAPIToken(c.String("token"))
	if err != nil {
		return err
	}

	baseURL := utils.GetBaseURL()
	reqURL := fmt.Sprintf("%s/v1.0/schemas", baseURL)
	ui.Debug("GET %s", reqURL)

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized: please check your API token")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
	}

	var sr schemasResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		return fmt.Errorf("parsing response JSON: %v", err)
	}

	// Sort newest first — the schema you most recently synced is the one
	// you care about. Prefer lastSyncedAt (actual activity), fall back to
	// updatedAt for never-synced schemas.
	sort.SliceStable(sr.Schemas, func(i, j int) bool {
		ti := schemaRecency(sr.Schemas[i])
		tj := schemaRecency(sr.Schemas[j])
		if ti.Equal(tj) {
			return sr.Schemas[i].FingerprintShort < sr.Schemas[j].FingerprintShort
		}
		return ti.After(tj)
	})

	if c.Bool("json") {
		return outputJSON(sr)
	}

	if len(sr.Schemas) == 0 {
		ui.Info("No schemas found. Run `seedmancer sync` after an export to create one.")
		return nil
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Label", "Fingerprint", "Tables", "Datasets", "Size", "Last synced"})
	table.SetBorder(false)
	table.SetColumnSeparator("  ")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, s := range sr.Schemas {
		label := s.FingerprintShort
		if s.DisplayName != nil && strings.TrimSpace(*s.DisplayName) != "" {
			label = *s.DisplayName
		}
		if s.IsLegacy {
			label += "  (legacy)"
		}
		table.Append([]string{
			label,
			s.FingerprintShort,
			fmt.Sprintf("%d", s.TableCount),
			fmt.Sprintf("%d", s.DatasetCount),
			formatBytes(s.TotalSize),
			utils.HumanizeAgo(schemaRecency(s)),
		})
	}
	table.Render()
	return nil
}

// schemaRecency returns the best timestamp for "last activity": lastSyncedAt
// when present (real user-driven activity), else updatedAt (includes pure
// metadata edits like `schemas rename`). Unparseable strings return the zero
// time, which sorts to the bottom.
func schemaRecency(s schemaSummary) time.Time {
	if s.LastSyncedAt != nil && *s.LastSyncedAt != "" {
		if t, err := time.Parse(time.RFC3339, *s.LastSyncedAt); err == nil {
			return t
		}
	}
	if t, err := time.Parse(time.RFC3339, s.UpdatedAt); err == nil {
		return t
	}
	return time.Time{}
}

// resolveRemoteSchemaID turns a user-supplied reference (UUID or fingerprint
// prefix ≥ 4 chars) into the canonical schema UUID via GET /v1.0/schemas/:id,
// which the server resolves leniently.
func resolveRemoteSchemaID(baseURL, token, ref string) (schemaSummary, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return schemaSummary{}, fmt.Errorf("schema reference (id or fingerprint prefix) is required")
	}
	reqURL := fmt.Sprintf("%s/v1.0/schemas/%s", baseURL, ref)
	ui.Debug("GET %s", reqURL)

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return schemaSummary{}, fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return schemaSummary{}, fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return schemaSummary{}, fmt.Errorf("reading response body: %v", err)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return schemaSummary{}, fmt.Errorf("unauthorized: please check your API token")
	}
	if resp.StatusCode == http.StatusNotFound {
		return schemaSummary{}, fmt.Errorf("schema %q not found", ref)
	}
	if resp.StatusCode != http.StatusOK {
		return schemaSummary{}, fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
	}

	// Envelope: { schema: {...}, datasets: [...] }
	var env struct {
		Schema schemaSummary `json:"schema"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return schemaSummary{}, fmt.Errorf("parsing response JSON: %v", err)
	}
	if env.Schema.ID == "" {
		return schemaSummary{}, fmt.Errorf("server returned no schema for %q", ref)
	}
	return env.Schema, nil
}

func runSchemasRename(c *cli.Context) error {
	args := c.Args()
	if args.Len() < 1 {
		return fmt.Errorf("usage: seedmancer schemas rename <fp-prefix-or-id> <new-name>")
	}
	ref := args.Get(0)

	var newName string
	clear := c.Bool("clear")
	if clear {
		if args.Len() > 1 {
			return fmt.Errorf("--clear and a new-name argument are mutually exclusive")
		}
	} else {
		if args.Len() < 2 {
			return fmt.Errorf("missing new name — pass `\"\"` or --clear to remove the custom label")
		}
		newName = strings.TrimSpace(args.Get(1))
	}

	token, err := utils.ResolveAPIToken(c.String("token"))
	if err != nil {
		return err
	}
	baseURL := utils.GetBaseURL()

	schema, err := resolveRemoteSchemaID(baseURL, token, ref)
	if err != nil {
		return err
	}

	payload := map[string]interface{}{}
	if clear || newName == "" {
		payload["displayName"] = nil
	} else {
		payload["displayName"] = newName
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshalling request: %v", err)
	}

	reqURL := fmt.Sprintf("%s/v1.0/schemas/%s", baseURL, schema.ID)
	req, err := http.NewRequest("PATCH", reqURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rename failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	if clear || newName == "" {
		ui.Success("Cleared display name for schema %s (falls back to fingerprint)", schema.FingerprintShort)
	} else {
		ui.Success("Renamed schema %s → %q", schema.FingerprintShort, newName)
	}
	return nil
}

func runSchemasRm(c *cli.Context) error {
	args := c.Args()
	if args.Len() < 1 {
		return fmt.Errorf("usage: seedmancer schemas rm <fp-prefix-or-id>")
	}
	ref := args.Get(0)

	token, err := utils.ResolveAPIToken(c.String("token"))
	if err != nil {
		return err
	}
	baseURL := utils.GetBaseURL()

	schema, err := resolveRemoteSchemaID(baseURL, token, ref)
	if err != nil {
		return err
	}

	label := schema.FingerprintShort
	if schema.DisplayName != nil && strings.TrimSpace(*schema.DisplayName) != "" {
		label = fmt.Sprintf("%s (%s)", *schema.DisplayName, schema.FingerprintShort)
	}
	ui.Warn("Deleting schema %s  —  %d dataset(s) will become orphaned",
		label, schema.DatasetCount)
	if !c.Bool("force") {
		if !ui.Confirm("Proceed?", false) {
			ui.Info("Cancelled.")
			return nil
		}
	}

	reqURL := fmt.Sprintf("%s/v1.0/schemas/%s", baseURL, schema.ID)
	req, err := http.NewRequest("DELETE", reqURL, nil)
	if err != nil {
		return fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("delete failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	ui.Success("Deleted schema %s", label)
	return nil
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for n2 := n / unit; n2 >= unit; n2 /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}
