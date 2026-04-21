package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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

// schemasJSONOutput is the envelope written when `schemas list --json` runs.
// Local and remote are separate blocks so consumers can key off origin
// without poking at heuristic fields.
type schemasJSONOutput struct {
	Local  []localSchemaJSON `json:"local"`
	Remote []schemaSummary   `json:"remote"`
}

type localSchemaJSON struct {
	Fingerprint      string `json:"fingerprint"`
	FingerprintShort string `json:"fingerprintShort"`
	DisplayName      string `json:"displayName,omitempty"`
	DatasetCount     int    `json:"datasetCount"`
	UpdatedAt        string `json:"updatedAt"`
}

func SchemasCommand() *cli.Command {
	return &cli.Command{
		Name:            "schemas",
		Aliases:         []string{"schema"},
		Usage:           "Inspect and manage schemas (local + cloud)",
		HideHelpCommand: true,
		Description: "Schemas live both on disk (under .seedmancer/schemas/<fp-short>/) and\n" +
			"in your Seedmancer cloud account. This command group lets you\n" +
			"inspect them, give them human-friendly display names, or delete\n" +
			"ones you no longer need. Flags --local / --remote scope each\n" +
			"subcommand; by default they act on both sides when possible.",
		Subcommands: []*cli.Command{
			{
				Name:  "list",
				Usage: "List schemas (local + remote, or scope with --local / --remote)",
				Description: "Shows every schema known locally and on the server, sorted by last\n" +
					"activity (newest first). The LABEL column is the display name if\n" +
					"set via `seedmancer schemas rename`, otherwise the fingerprint.\n\n" +
					"Use --local for an offline, token-free view; use --remote to skip\n" +
					"the local walk. The default merges both sides.",
				ArgsUsage: " ",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "token",
						Usage: "API token (falls back to ~/.seedmancer/credentials, then SEEDMANCER_API_TOKEN)",
					},
					&cli.BoolFlag{
						Name:  "local",
						Usage: "Show only schemas on disk",
						Value: false,
					},
					&cli.BoolFlag{
						Name:  "remote",
						Usage: "Show only schemas stored in your Seedmancer cloud account",
						Value: false,
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
				Description: "Gives a schema a human-friendly label. Pass --local to edit only\n" +
					"the on-disk meta.yaml sidecar, --remote to update the server, or\n" +
					"neither to apply the change in both places when the schema exists\n" +
					"on both sides.\n\n" +
					"Pass an empty string (\"\") or --clear to remove the custom name.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "token",
						Usage: "API token (falls back to ~/.seedmancer/credentials, then SEEDMANCER_API_TOKEN)",
					},
					&cli.BoolFlag{
						Name:  "local",
						Usage: "Only rename the local schema (no server call)",
						Value: false,
					},
					&cli.BoolFlag{
						Name:  "remote",
						Usage: "Only rename the remote schema (skip local)",
						Value: false,
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
				Usage:     "Delete a schema locally and/or from the cloud",
				ArgsUsage: "<fp-prefix-or-id>",
				Description: "--local deletes the on-disk schema folder (schema.json, meta.yaml,\n" +
					"and every dataset it holds). --remote deletes the schema in your\n" +
					"cloud account; remote-attached datasets become orphans.\n\n" +
					"With neither flag, both sides are removed when present.\n" +
					"Prompts for confirmation unless --force is passed.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "token",
						Usage: "API token (falls back to ~/.seedmancer/credentials, then SEEDMANCER_API_TOKEN)",
					},
					&cli.BoolFlag{
						Name:  "local",
						Usage: "Only delete the local schema folder",
						Value: false,
					},
					&cli.BoolFlag{
						Name:  "remote",
						Usage: "Only delete the remote schema record",
						Value: false,
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

// ─── list ─────────────────────────────────────────────────────────────────────

func runSchemasList(c *cli.Context) error {
	localFlag := c.Bool("local")
	remoteFlag := c.Bool("remote")
	jsonMode := c.Bool("json")

	if !localFlag && !remoteFlag {
		localFlag = true
		remoteFlag = true
	}

	var localSchemas []utils.LocalSchema
	if localFlag {
		ls, err := listLocalSchemasForCmd()
		if err != nil && !jsonMode {
			ui.Title("Local")
			ui.Warn("%v", err)
		}
		localSchemas = ls
	}

	var remoteSchemas []schemaSummary
	remoteReachable := true
	if remoteFlag {
		token, tokenErr := utils.ResolveAPIToken(c.String("token"))
		if tokenErr != nil {
			remoteReachable = false
			if jsonMode {
				// JSON output must still succeed — return local-only.
				if !localFlag {
					return tokenErr
				}
			} else if !localFlag {
				return tokenErr
			} else {
				ui.Title("Remote")
				ui.PrintLoginHint()
			}
		} else {
			rs, err := fetchRemoteSchemas(token)
			if err != nil {
				return err
			}
			remoteSchemas = rs
		}
	}

	if jsonMode {
		out := schemasJSONOutput{Local: []localSchemaJSON{}, Remote: remoteSchemas}
		for _, s := range localSchemas {
			updated := ""
			if !s.UpdatedAt.IsZero() {
				updated = s.UpdatedAt.UTC().Format(time.RFC3339)
			}
			out.Local = append(out.Local, localSchemaJSON{
				Fingerprint:      s.Fingerprint,
				FingerprintShort: s.FingerprintShort,
				DisplayName:      s.DisplayName,
				DatasetCount:     len(s.Datasets),
				UpdatedAt:        updated,
			})
		}
		return outputJSON(out)
	}

	if localFlag {
		ui.Title("Local")
		if len(localSchemas) == 0 {
			ui.Info("No local schemas yet. Run `seedmancer export` to create one.")
		} else {
			renderLocalSchemaTable(localSchemas)
		}
	}

	if remoteFlag && remoteReachable {
		ui.Title("Remote")
		if len(remoteSchemas) == 0 {
			ui.Info("No remote schemas yet. Run `seedmancer sync` after an export to create one.")
		} else {
			renderRemoteSchemaTable(remoteSchemas)
		}
	}
	return nil
}

// listLocalSchemasForCmd reads local schemas from the closest seedmancer.yaml.
// Missing config is treated as "no local schemas yet" so running `schemas
// list --local` from outside a project still prints a useful message.
func listLocalSchemasForCmd() ([]utils.LocalSchema, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return nil, nil
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading config: %v", err)
	}
	return utils.ListLocalSchemas(projectRoot, cfg.StoragePath)
}

func renderLocalSchemaTable(schemas []utils.LocalSchema) {
	sort.SliceStable(schemas, func(i, j int) bool {
		if schemas[i].UpdatedAt.Equal(schemas[j].UpdatedAt) {
			return schemas[i].FingerprintShort < schemas[j].FingerprintShort
		}
		return schemas[i].UpdatedAt.After(schemas[j].UpdatedAt)
	})
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Label", "Fingerprint", "Datasets", "Updated"})
	table.SetBorder(false)
	table.SetColumnSeparator("  ")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, s := range schemas {
		label := s.FingerprintShort
		if s.DisplayName != "" {
			label = s.DisplayName
		}
		table.Append([]string{
			label,
			s.FingerprintShort,
			fmt.Sprintf("%d", len(s.Datasets)),
			utils.HumanizeAgo(s.UpdatedAt),
		})
	}
	table.Render()
}

func renderRemoteSchemaTable(schemas []schemaSummary) {
	// Sort newest first — the schema you most recently synced is the one
	// you care about. Prefer lastSyncedAt (actual activity), fall back to
	// updatedAt for never-synced schemas.
	sort.SliceStable(schemas, func(i, j int) bool {
		ti := schemaRecency(schemas[i])
		tj := schemaRecency(schemas[j])
		if ti.Equal(tj) {
			return schemas[i].FingerprintShort < schemas[j].FingerprintShort
		}
		return ti.After(tj)
	})

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Label", "Fingerprint", "Tables", "Datasets", "Size", "Last synced"})
	table.SetBorder(false)
	table.SetColumnSeparator("  ")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, s := range schemas {
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
}

func fetchRemoteSchemas(token string) ([]schemaSummary, error) {
	baseURL := utils.GetBaseURL()
	reqURL := fmt.Sprintf("%s/v1.0/schemas", baseURL)
	ui.Debug("GET %s", reqURL)

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, utils.ErrInvalidAPIToken
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
	}

	var sr schemasResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		return nil, fmt.Errorf("parsing response JSON: %v", err)
	}
	return sr.Schemas, nil
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

// ─── rename ───────────────────────────────────────────────────────────────────

func runSchemasRename(c *cli.Context) error {
	args := c.Args()
	if args.Len() < 1 {
		return fmt.Errorf("usage: seedmancer schemas rename <fp-prefix-or-id> <new-name>")
	}
	ref := args.Get(0)

	clear := c.Bool("clear")
	var newName string
	if clear {
		if args.Len() > 1 {
			return fmt.Errorf("--clear and a new-name argument are mutually exclusive")
		}
	} else {
		if args.Len() < 2 {
			return fmt.Errorf("missing new name — pass `\"\"` or --clear to remove the custom label")
		}
		newName = strings.TrimSpace(args.Get(1))
		if newName == "" {
			clear = true
		}
	}

	scope, err := resolveScope(c)
	if err != nil {
		return err
	}

	var touched bool

	if scope.local {
		ok, err := renameLocalSchema(ref, newName, clear)
		if err != nil {
			return err
		}
		if ok {
			touched = true
		} else if scope.explicit {
			return fmt.Errorf("no local schema matching %q — check `seedmancer schemas list --local`", ref)
		}
	}

	if scope.remote {
		if err := renameRemoteSchema(c.String("token"), ref, newName, clear); err != nil {
			if errors.Is(err, utils.ErrMissingAPIToken) && !scope.explicit {
				// Local-side worked; server update silently skipped.
				if touched {
					ui.Warn("Skipped remote rename — not signed in. Run `seedmancer login` to sync.")
				}
			} else {
				return err
			}
		} else {
			touched = true
		}
	}

	if !touched {
		return fmt.Errorf("no schema matching %q on either side", ref)
	}
	return nil
}

func renameLocalSchema(ref, newName string, clear bool) (bool, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return false, nil
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return false, fmt.Errorf("reading config: %v", err)
	}
	schema, err := utils.ResolveLocalSchema(projectRoot, cfg.StoragePath, ref)
	if err != nil {
		return false, nil
	}
	meta, err := utils.LoadLocalSchemaMeta(schema.Path)
	if err != nil {
		return false, err
	}
	if clear {
		meta.DisplayName = ""
	} else {
		meta.DisplayName = newName
	}
	if err := utils.SaveLocalSchemaMeta(schema.Path, meta); err != nil {
		return false, err
	}
	if clear {
		ui.Success("Cleared local display name for schema %s", schema.FingerprintShort)
	} else {
		ui.Success("Renamed local schema %s → %q", schema.FingerprintShort, newName)
	}
	return true, nil
}

func renameRemoteSchema(tokenFlag, ref, newName string, clear bool) error {
	token, err := utils.ResolveAPIToken(tokenFlag)
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
	if resp.StatusCode == http.StatusUnauthorized {
		return utils.ErrInvalidAPIToken
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rename failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	if clear || newName == "" {
		ui.Success("Cleared remote display name for schema %s (falls back to fingerprint)", schema.FingerprintShort)
	} else {
		ui.Success("Renamed remote schema %s → %q", schema.FingerprintShort, newName)
	}
	return nil
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
		return schemaSummary{}, utils.ErrInvalidAPIToken
	}
	if resp.StatusCode == http.StatusNotFound {
		return schemaSummary{}, fmt.Errorf("schema %q not found", ref)
	}
	if resp.StatusCode != http.StatusOK {
		return schemaSummary{}, fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
	}

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

// ─── rm ───────────────────────────────────────────────────────────────────────

func runSchemasRm(c *cli.Context) error {
	args := c.Args()
	if args.Len() < 1 {
		return fmt.Errorf("usage: seedmancer schemas rm <fp-prefix-or-id>")
	}
	ref := args.Get(0)

	scope, err := resolveScope(c)
	if err != nil {
		return err
	}

	var touched bool

	if scope.local {
		ok, err := rmLocalSchema(ref, c.Bool("force"))
		if err != nil {
			return err
		}
		if ok {
			touched = true
		} else if scope.explicit {
			return fmt.Errorf("no local schema matching %q — check `seedmancer schemas list --local`", ref)
		}
	}

	if scope.remote {
		if err := rmRemoteSchema(c.String("token"), ref, c.Bool("force")); err != nil {
			if errors.Is(err, utils.ErrMissingAPIToken) && !scope.explicit {
				if touched {
					ui.Warn("Skipped remote delete — not signed in. Run `seedmancer login` if you also need to remove the cloud copy.")
				}
			} else {
				return err
			}
		} else {
			touched = true
		}
	}

	if !touched {
		return fmt.Errorf("no schema matching %q on either side", ref)
	}
	return nil
}

func rmLocalSchema(ref string, force bool) (bool, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return false, nil
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return false, fmt.Errorf("reading config: %v", err)
	}
	schema, err := utils.ResolveLocalSchema(projectRoot, cfg.StoragePath, ref)
	if err != nil {
		return false, nil
	}

	label := schema.FingerprintShort
	if schema.DisplayName != "" {
		label = fmt.Sprintf("%s (%s)", schema.DisplayName, schema.FingerprintShort)
	}
	ui.Warn("Deleting local schema %s  —  %d dataset(s) will be removed from disk",
		label, len(schema.Datasets))
	if !force {
		if !ui.Confirm("Proceed?", false) {
			ui.Info("Cancelled.")
			return false, nil
		}
	}

	if err := os.RemoveAll(schema.Path); err != nil {
		return false, fmt.Errorf("removing %s: %v", schema.Path, err)
	}
	ui.Success("Deleted local schema %s", label)
	return true, nil
}

func rmRemoteSchema(tokenFlag, ref string, force bool) error {
	token, err := utils.ResolveAPIToken(tokenFlag)
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
	ui.Warn("Deleting remote schema %s  —  %d dataset(s) will become orphaned",
		label, schema.DatasetCount)
	if !force {
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
	if resp.StatusCode == http.StatusUnauthorized {
		return utils.ErrInvalidAPIToken
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("delete failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	ui.Success("Deleted remote schema %s", label)
	return nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// cmdScope normalises the --local / --remote flag pair into explicit booleans.
// `explicit` tells callers whether the user asked for one specific side (true)
// or the default "both" behaviour (false), so they can downgrade "not found
// on this side" into a silent skip when running in default mode.
type cmdScope struct {
	local    bool
	remote   bool
	explicit bool
}

func resolveScope(c *cli.Context) (cmdScope, error) {
	local := c.Bool("local")
	remote := c.Bool("remote")
	if local && remote {
		return cmdScope{}, fmt.Errorf("--local and --remote are mutually exclusive — omit both to target both sides")
	}
	if !local && !remote {
		return cmdScope{local: true, remote: true, explicit: false}, nil
	}
	return cmdScope{local: local, remote: remote, explicit: true}, nil
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
