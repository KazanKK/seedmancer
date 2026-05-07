package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	db "github.com/KazanKK/seedmancer/database"
	"github.com/KazanKK/seedmancer/internal/gointerp"
	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/schemadiff"
	svc "github.com/KazanKK/seedmancer/internal/services"
	utils "github.com/KazanKK/seedmancer/internal/utils"
)

// silence unused-import warning in environments that haven't wired the
// new packages everywhere yet — they are real dependencies of RunCheck
// and the scenario-aware paths below.
var (
	_ = scenario.Normalize
	_ = schemadiff.Diff
)

// This file exposes the same logic the CLI `Action` bodies run, but as
// context-aware functions with typed input/output that never write to
// stdout/stderr. They are the single authoritative surface the MCP server
// (internal/mcp/) builds on, so agents get structured results instead of
// having to shell out and parse ANSI-decorated human output.
//
// Living in the `cmd` package means the runners can reuse private helpers
// (`listLocalEntries`, `fetchRemoteSchemas`, `resolveSingleDB`, …) without
// leaking them into the public API. When a runner overlaps a CLI helper,
// the CLI Action body stays the source of UI-aware behavior; the runner
// re-implements just the logic+result-shaping path.

// ─── list ─────────────────────────────────────────────────────────────────────

// ListInput is currently empty — the new scenario layout makes the
// local/remote split meaningless because cloud sync only carries the
// latest revision of each scenario as a single dataset id. We keep the
// type so the MCP tool surface stays stable.
type ListInput struct{}

// ListOutput mirrors `seedmancer list --json`: scenarios known on disk,
// each with its latest/stable pointers and schema fingerprint.
type ListOutput struct {
	Scenarios []listEntry `json:"scenarios"`
}

// RunList returns the scenarios known on disk. Cloud-side discovery is
// intentionally not performed here — cloud entries don't carry pointers
// or revisions, so mixing them into the same shape would just confuse
// callers.
func RunList(_ context.Context, _ ListInput) (ListOutput, error) {
	entries, err := listLocalEntries()
	if err != nil {
		return ListOutput{Scenarios: []listEntry{}}, err
	}
	if entries == nil {
		entries = []listEntry{}
	}
	return ListOutput{Scenarios: entries}, nil
}

// ─── describe_dataset ─────────────────────────────────────────────────────────

// DescribeDatasetInput is the user-supplied reference plus optional
// schema scope. `SchemaPrefix` disambiguates when the same dataset id
// exists under two schemas locally (rare but possible).
type DescribeDatasetInput struct {
	DatasetID    string `json:"datasetId" jsonschema:"Dataset id (the name given at export/generate time)"`
	SchemaPrefix string `json:"schemaPrefix,omitempty" jsonschema:"Optional fingerprint prefix to disambiguate same-named datasets"`
	// Number of CSV preview rows per table (default 5).
	PreviewRows int `json:"previewRows,omitempty" jsonschema:"Number of CSV preview rows per file (default 5, max 50)"`
}

// DescribeDatasetOutput summarises a dataset folder — the files it
// contains, their row counts, and a small preview — so agents don't
// need shell access to understand what's in it.
type DescribeDatasetOutput struct {
	Dataset           string               `json:"dataset"`
	Path              string               `json:"path"`
	SchemaFingerprint string               `json:"schemaFingerprint"`
	SchemaShort       string               `json:"schemaShort"`
	SchemaDisplayName string               `json:"schemaDisplayName,omitempty"`
	SourceEnv         string               `json:"sourceEnv,omitempty"`
	UpdatedAt         string               `json:"updatedAt"`
	Files             []DatasetFilePreview `json:"files"`
	// HasGeneratorScript is true when a generator script has been saved for
	// this dataset. Retrieve the script with the get_dataset_script tool.
	HasGeneratorScript bool `json:"hasGeneratorScript,omitempty"`
}

// DatasetFilePreview is one row in DescribeDatasetOutput.Files. Rows is
// *approximate* for very large files — we cap the scan at 10k lines to
// avoid reading multi-gig CSVs end-to-end when an agent is browsing.
type DatasetFilePreview struct {
	Name       string   `json:"name"`
	SizeBytes  int64    `json:"sizeBytes"`
	ApproxRows int      `json:"approxRows,omitempty"`
	Preview    []string `json:"preview,omitempty"`
	Truncated  bool     `json:"truncated,omitempty"`
}

// RunDescribeDataset resolves a dataset and previews its contents.
func RunDescribeDataset(_ context.Context, in DescribeDatasetInput) (DescribeDatasetOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return DescribeDatasetOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return DescribeDatasetOutput{}, err
	}

	schema, datasetDir, err := utils.FindLocalDataset(projectRoot, cfg.StoragePath, in.SchemaPrefix, in.DatasetID)
	if err != nil {
		return DescribeDatasetOutput{}, err
	}

	files, err := utils.DatasetFiles(datasetDir)
	if err != nil {
		return DescribeDatasetOutput{}, err
	}
	sort.Strings(files)

	previewRows := in.PreviewRows
	if previewRows <= 0 {
		previewRows = 5
	}
	if previewRows > 50 {
		previewRows = 50
	}

	out := DescribeDatasetOutput{
		Dataset:           in.DatasetID,
		Path:              datasetDir,
		SchemaFingerprint: schema.Fingerprint,
		SchemaShort:       schema.FingerprintShort,
		SchemaDisplayName: schema.DisplayName,
		SourceEnv:         utils.ReadDatasetMeta(datasetDir).SourceEnv,
		Files:             make([]DatasetFilePreview, 0, len(files)),
	}
	if info, err := os.Stat(datasetDir); err == nil {
		out.UpdatedAt = info.ModTime().UTC().Format(time.RFC3339)
	}

	for _, p := range files {
		fp, err := previewDatasetFile(p, previewRows)
		if err != nil {
			continue
		}
		out.Files = append(out.Files, fp)
	}
	// Signal whether a generator script was saved for this dataset so agents
	// know to call get_dataset_script before writing a new script from scratch.
	if script, err := utils.LoadGeneratorScript(projectRoot, in.DatasetID); err == nil && script != "" {
		out.HasGeneratorScript = true
	}
	return out, nil
}

// ─── get_dataset_script ───────────────────────────────────────────────────────

type GetDatasetScriptInput struct {
	DatasetID string `json:"datasetId" jsonschema:"Dataset id whose generator script to retrieve"`
}

type GetDatasetScriptOutput struct {
	DatasetID string `json:"datasetId"`
	Script    string `json:"script"`
}

// RunGetDatasetScript returns the generator script that was saved when the
// dataset was created with generate_dataset_local / generate-local. Returns
// an error when no script has been saved for the given dataset id.
func RunGetDatasetScript(_ context.Context, in GetDatasetScriptInput) (GetDatasetScriptOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return GetDatasetScriptOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)

	script, err := utils.LoadGeneratorScript(projectRoot, in.DatasetID)
	if err != nil {
		return GetDatasetScriptOutput{}, fmt.Errorf("loading generator script: %w", err)
	}
	if script == "" {
		return GetDatasetScriptOutput{}, fmt.Errorf(
			"no generator script found for dataset %q — it may have been created without generate_dataset_local",
			in.DatasetID,
		)
	}
	return GetDatasetScriptOutput{DatasetID: in.DatasetID, Script: script}, nil
}

// previewDatasetFile reads the first `n+1` lines of a CSV/JSON file
// (header + n data rows) and returns them along with an approximate row
// count derived from a bounded line scan. `approxRows` reports -1 when
// the file is larger than scanRowsCap so callers don't confuse the cap
// with the actual count.
func previewDatasetFile(path string, n int) (DatasetFilePreview, error) {
	info, err := os.Stat(path)
	if err != nil {
		return DatasetFilePreview{}, err
	}
	fp := DatasetFilePreview{
		Name:      filepath.Base(path),
		SizeBytes: info.Size(),
	}

	f, err := os.Open(path)
	if err != nil {
		return fp, err
	}
	defer f.Close()

	const scanRowsCap = 10_000
	const maxLineBytes = 256 * 1024

	buf := make([]byte, 0, maxLineBytes)
	scratch := make([]byte, maxLineBytes)

	preview := make([]string, 0, n+1)
	rows := 0
	truncated := false
	tmp := [1]byte{}
	lineLen := 0

	for rows < scanRowsCap {
		_, err := f.Read(tmp[:])
		if err != nil {
			if lineLen > 0 {
				if len(preview) <= n {
					preview = append(preview, string(buf))
				}
				rows++
			}
			break
		}
		b := tmp[0]
		if b == '\n' {
			if len(preview) <= n {
				preview = append(preview, string(buf))
			}
			rows++
			buf = buf[:0]
			lineLen = 0
			continue
		}
		if lineLen < maxLineBytes {
			buf = append(buf, b)
			lineLen++
		}
	}
	if rows == scanRowsCap {
		truncated = true
	}
	_ = scratch
	fp.Preview = preview
	fp.ApproxRows = rows
	fp.Truncated = truncated
	return fp, nil
}

// ─── list_schemas ─────────────────────────────────────────────────────────────

// ListSchemasInput mirrors the CLI `schemas list` flag set.
type ListSchemasInput struct {
	Token  string `json:"token,omitempty" jsonschema:"API token override"`
	Local  bool   `json:"local,omitempty" jsonschema:"Only list local schemas"`
	Remote bool   `json:"remote,omitempty" jsonschema:"Only list remote schemas"`
}

type ListSchemasOutput struct {
	Local  []localSchemaJSON `json:"local"`
	Remote []schemaSummary   `json:"remote"`
}

func RunListSchemas(_ context.Context, in ListSchemasInput) (ListSchemasOutput, error) {
	localWanted, remoteWanted := in.Local, in.Remote
	if !localWanted && !remoteWanted {
		localWanted, remoteWanted = true, true
	}

	out := ListSchemasOutput{Local: []localSchemaJSON{}, Remote: []schemaSummary{}}

	if localWanted {
		locals, err := listLocalSchemasForCmd()
		if err == nil {
			for _, s := range locals {
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
		} else if !remoteWanted {
			return out, err
		}
	}

	if remoteWanted {
		token, tokenErr := utils.ResolveAPIToken(in.Token)
		if tokenErr != nil {
			if !localWanted {
				return out, tokenErr
			}
		} else {
			remote, err := fetchRemoteSchemas(token)
			if err != nil {
				if !localWanted {
					return out, err
				}
			} else {
				out.Remote = remote
			}
		}
	}
	return out, nil
}

// ─── describe_schema ──────────────────────────────────────────────────────────

type DescribeSchemaInput struct {
	Ref string `json:"ref" jsonschema:"Fingerprint prefix (≥4 chars) or full SHA-256 fingerprint"`
}

type SchemaTable struct {
	Name    string         `json:"name"`
	Columns []SchemaColumn `json:"columns"`
}

type SchemaColumn struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Nullable   bool   `json:"nullable"`
	IsPrimary  bool   `json:"isPrimary"`
	IsUnique   bool   `json:"isUnique"`
	Default    string `json:"default,omitempty"`
	Enum       string `json:"enum,omitempty"`
	ForeignKey string `json:"foreignKey,omitempty"`
}

type DescribeSchemaOutput struct {
	Fingerprint      string        `json:"fingerprint"`
	FingerprintShort string        `json:"fingerprintShort"`
	DisplayName      string        `json:"displayName,omitempty"`
	Path             string        `json:"path"`
	Tables           []SchemaTable `json:"tables"`
}

// RunDescribeSchema loads schema.json from the local on-disk folder for
// the given ref and returns a structured view of its tables/columns.
func RunDescribeSchema(_ context.Context, in DescribeSchemaInput) (DescribeSchemaOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return DescribeSchemaOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return DescribeSchemaOutput{}, err
	}
	schema, err := utils.ResolveLocalSchema(projectRoot, cfg.StoragePath, in.Ref)
	if err != nil {
		return DescribeSchemaOutput{}, err
	}

	schemaJSONPath := filepath.Join(schema.Path, "schema.json")
	data, err := os.ReadFile(schemaJSONPath)
	if err != nil {
		return DescribeSchemaOutput{}, fmt.Errorf("reading %s: %v", schemaJSONPath, err)
	}

	// schema.json has the same shape as the generate-payload `generateSchema`,
	// so we reuse those types for decoding without adding yet another DTO.
	var raw struct {
		Tables []generateTable `json:"tables"`
		Enums  []generateEnum  `json:"enums"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return DescribeSchemaOutput{}, fmt.Errorf("parsing %s: %v", schemaJSONPath, err)
	}

	out := DescribeSchemaOutput{
		Fingerprint:      schema.Fingerprint,
		FingerprintShort: schema.FingerprintShort,
		DisplayName:      schema.DisplayName,
		Path:             schema.Path,
		Tables:           make([]SchemaTable, 0, len(raw.Tables)),
	}
	for _, t := range raw.Tables {
		st := SchemaTable{Name: t.Name, Columns: make([]SchemaColumn, 0, len(t.Columns))}
		for _, c := range t.Columns {
			col := SchemaColumn{
				Name:      c.Name,
				Type:      c.Type,
				Nullable:  c.Nullable,
				IsPrimary: c.IsPrimary,
				IsUnique:  c.IsUnique,
				Default:   c.Default,
				Enum:      c.Enum,
			}
			if c.ForeignKey != nil {
				col.ForeignKey = c.ForeignKey.Table + "." + c.ForeignKey.Column
			}
			st.Columns = append(st.Columns, col)
		}
		out.Tables = append(out.Tables, st)
	}
	return out, nil
}

// ─── status ───────────────────────────────────────────────────────────────────

type StatusInput struct {
	Offline   bool `json:"offline,omitempty" jsonschema:"Skip the API reachability probe"`
	ShowDBURL bool `json:"showDbUrl,omitempty" jsonschema:"Return database URLs with credentials (default masks the password)"`
}

// StatusOutput is the same shape the CLI emits for `status --json`. We
// alias the internal type so MCP clients that already parse the CLI's
// JSON get a consistent schema.
type StatusOutput = statusReport

func RunStatus(_ context.Context, in StatusInput) (StatusOutput, error) {
	report := buildStatusReport(in.ShowDBURL)
	if !in.Offline && report.Auth.SignedIn {
		ok, errMsg := probeAPIReachable(report.API.URL, resolveActiveTokenForProbe())
		report.Auth.Reachable = &ok
		if !ok {
			report.Auth.ReachableError = errMsg
		}
	}
	return report, nil
}

// ─── env list / env add / env remove ──────────────────────────────────────────

type EnvEntry struct {
	Name        string `json:"name"`
	DatabaseURL string `json:"databaseUrl,omitempty"`
	IsDefault   bool   `json:"isDefault"`
}

type ListEnvsInput struct {
	ShowSecret bool `json:"showSecret,omitempty" jsonschema:"Return URLs with credentials (default masks the password)"`
}

type ListEnvsOutput struct {
	DefaultEnv   string     `json:"defaultEnv"`
	Environments []EnvEntry `json:"environments"`
}

func RunListEnvs(_ context.Context, in ListEnvsInput) (ListEnvsOutput, error) {
	path, err := utils.FindConfigFile()
	if err != nil {
		return ListEnvsOutput{}, fmt.Errorf("%v — run `seedmancer init` first", err)
	}
	cfg, err := utils.LoadConfig(path)
	if err != nil {
		return ListEnvsOutput{}, err
	}
	envs := cfg.EffectiveEnvs()
	active := cfg.ActiveEnvName()
	out := ListEnvsOutput{DefaultEnv: active, Environments: []EnvEntry{}}
	for _, name := range cfg.SortedEnvNames() {
		u := envs[name].DatabaseURL
		if !in.ShowSecret {
			u = maskDatabaseURL(u)
		}
		out.Environments = append(out.Environments, EnvEntry{
			Name:        name,
			DatabaseURL: u,
			IsDefault:   name == active,
		})
	}
	return out, nil
}

type AddEnvInput struct {
	Name        string `json:"name" jsonschema:"Environment name (letters, digits, '-' or '_')"`
	DatabaseURL string `json:"databaseUrl" jsonschema:"Database connection URL"`
	SetDefault  bool   `json:"setDefault,omitempty" jsonschema:"Also make this the default environment"`
	Force       bool   `json:"force,omitempty" jsonschema:"Overwrite if the env already exists"`
}

type AddEnvOutput struct {
	Name       string `json:"name"`
	IsDefault  bool   `json:"isDefault"`
	ConfigPath string `json:"configPath"`
}

func RunAddEnv(_ context.Context, in AddEnvInput) (AddEnvOutput, error) {
	name := strings.TrimSpace(in.Name)
	if err := validateEnvName(name); err != nil {
		return AddEnvOutput{}, err
	}
	url := strings.TrimSpace(in.DatabaseURL)
	if url == "" {
		return AddEnvOutput{}, fmt.Errorf("database URL cannot be empty")
	}
	path, err := utils.FindConfigFile()
	if err != nil {
		return AddEnvOutput{}, fmt.Errorf("%v — run `seedmancer init` first", err)
	}
	cfg, err := utils.LoadConfig(path)
	if err != nil {
		return AddEnvOutput{}, err
	}
	if _, exists := cfg.EffectiveEnvs()[name]; exists && !in.Force {
		return AddEnvOutput{}, fmt.Errorf("environment %q already exists — set force:true to overwrite", name)
	}
	cfg.SetEnv(name, utils.EnvConfig{DatabaseURL: url})
	if in.SetDefault || cfg.DefaultEnv == "" {
		cfg.DefaultEnv = name
	}
	if err := utils.SaveConfig(path, cfg); err != nil {
		return AddEnvOutput{}, err
	}
	return AddEnvOutput{Name: name, IsDefault: cfg.DefaultEnv == name, ConfigPath: path}, nil
}

type RemoveEnvInput struct {
	Name  string `json:"name" jsonschema:"Environment name to remove"`
	Force bool   `json:"force,omitempty" jsonschema:"Allow removing the active default environment"`
}

type RemoveEnvOutput struct {
	Name       string `json:"name"`
	ConfigPath string `json:"configPath"`
	DefaultEnv string `json:"defaultEnv"`
}

func RunRemoveEnv(_ context.Context, in RemoveEnvInput) (RemoveEnvOutput, error) {
	name := strings.TrimSpace(in.Name)
	path, err := utils.FindConfigFile()
	if err != nil {
		return RemoveEnvOutput{}, fmt.Errorf("%v — run `seedmancer init` first", err)
	}
	cfg, err := utils.LoadConfig(path)
	if err != nil {
		return RemoveEnvOutput{}, err
	}
	if _, ok := cfg.EffectiveEnvs()[name]; !ok {
		return RemoveEnvOutput{}, fmt.Errorf(
			"unknown environment %q (available: %s)", name,
			strings.Join(cfg.SortedEnvNames(), ", "),
		)
	}
	if name == cfg.ActiveEnvName() && !in.Force {
		return RemoveEnvOutput{}, fmt.Errorf(
			"%q is the active default environment — change default first or set force:true", name,
		)
	}
	if !cfg.RemoveEnv(name) {
		return RemoveEnvOutput{}, fmt.Errorf("environment %q not found", name)
	}
	if cfg.DefaultEnv == name {
		cfg.DefaultEnv = ""
	}
	if err := utils.SaveConfig(path, cfg); err != nil {
		return RemoveEnvOutput{}, err
	}
	return RemoveEnvOutput{Name: name, ConfigPath: path, DefaultEnv: cfg.DefaultEnv}, nil
}

type UseEnvInput struct {
	Name string `json:"name" jsonschema:"Environment name to set as default"`
}

type UseEnvOutput struct {
	Name       string `json:"name"`
	ConfigPath string `json:"configPath"`
}

func RunUseEnv(_ context.Context, in UseEnvInput) (UseEnvOutput, error) {
	name := strings.TrimSpace(in.Name)
	path, err := utils.FindConfigFile()
	if err != nil {
		return UseEnvOutput{}, fmt.Errorf("%v — run `seedmancer init` first", err)
	}
	cfg, err := utils.LoadConfig(path)
	if err != nil {
		return UseEnvOutput{}, err
	}
	if _, err := cfg.ResolveEnv(name); err != nil {
		return UseEnvOutput{}, err
	}
	cfg.DefaultEnv = name
	if err := utils.SaveConfig(path, cfg); err != nil {
		return UseEnvOutput{}, err
	}
	return UseEnvOutput{Name: name, ConfigPath: path}, nil
}

// ─── init ─────────────────────────────────────────────────────────────────────

type InitInput struct {
	StoragePath string `json:"storagePath,omitempty" jsonschema:"Directory for local schema folders (default: .seedmancer)"`
	EnvName     string `json:"envName,omitempty" jsonschema:"Name for the initial environment (default: local)"`
	DatabaseURL string `json:"databaseUrl,omitempty" jsonschema:"Database connection URL for the initial environment"`
}

type InitOutput struct {
	ConfigPath     string `json:"configPath"`
	StoragePath    string `json:"storagePath"`
	EnvName        string `json:"envName"`
	CursorRulePath string `json:"cursorRulePath,omitempty"`
	ClaudeMdPath   string `json:"claudeMdPath,omitempty"`
}

func RunInit(_ context.Context, in InitInput) (InitOutput, error) {
	storagePath := strings.TrimSpace(in.StoragePath)
	if storagePath == "" {
		storagePath = ".seedmancer"
	}
	envName := strings.TrimSpace(in.EnvName)
	if envName == "" {
		envName = "local"
	}
	if err := validateEnvName(envName); err != nil {
		return InitOutput{}, err
	}
	cfg := utils.Config{
		StoragePath: storagePath,
		DefaultEnv:  envName,
		Environments: map[string]utils.EnvConfig{
			envName: {DatabaseURL: strings.TrimSpace(in.DatabaseURL)},
		},
	}
	if err := utils.SaveConfig("seedmancer.yaml", cfg); err != nil {
		return InitOutput{}, err
	}
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		return InitOutput{}, fmt.Errorf("creating storage directory: %v", err)
	}
	abs, _ := filepath.Abs("seedmancer.yaml")

	// Best-effort: install agent rules so AI tools (Cursor, Claude Code) know
	// to use Seedmancer MCP tools for test data tasks. Failure is non-fatal —
	// the user can call install_agent_rules separately at any time.
	rulesOut, _ := RunInstallAgentRules(context.Background(), InstallAgentRulesInput{})

	return InitOutput{
		ConfigPath:     abs,
		StoragePath:    storagePath,
		EnvName:        envName,
		CursorRulePath: rulesOut.CursorRulePath,
		ClaudeMdPath:   rulesOut.ClaudeMdPath,
	}, nil
}

// ─── install agent rules ──────────────────────────────────────────────────────

// cursorRuleContent is written to .cursor/rules/seedmancer.mdc in the user's
// project. Cursor reads every .mdc file in that directory automatically so the
// AI agent will follow these rules in every conversation without the user having
// to remember to mention Seedmancer.
const cursorRuleContent = `---
description: Use Seedmancer MCP tools for all test/seed data tasks in this project.
alwaysApply: true
---

# Seedmancer test data rules

This project uses **Seedmancer** for test data management. Test data lives in
**scenarios** (slash-separated paths like ` + "`basic`" + ` or ` + "`billing/pro`" + `). Every
` + "`export_database`" + ` creates a new immutable **revision** under the scenario
(e.g. ` + "`r001`" + `, ` + "`r002`" + `). The ` + "`latest`" + ` revision is used by default; pin a
revision as ` + "`stable`" + ` for CI.

**Never** write CSV files to ` + "`.seedmancer/`" + ` by hand.
**Never** create seed.sql or similar workarounds.
**Never** save generator scripts to project directories (` + "`scripts/`" + `, ` + "`src/`" + `, etc.) — use MCP or stdin pipe.
**Never** show script content or generation internals to the user. Say "Generating test data…" and report the result.
**Always** use the Seedmancer MCP tools.

## Standard workflow for generating test data:

1. ` + "`get_status`" + ` — confirm project config and default env are set.
2. ` + "`list_datasets`" + ` — check existing scenarios and their pointers.
   - **If no scenarios exist**: call ` + "`export_database`" + ` with a scenario name
     (e.g. ` + "`basic`" + ` or ` + "`billing/pro`" + `). The DB is already running
     (configured in seedmancer.yaml), so this always works. The export
     creates ` + "`.seedmancer/scenarios/<scenario>/revisions/r001/`" + ` plus the
     content-addressed ` + "`.seedmancer/schemas/<fp>/schema.json`" + ` that other
     tools rely on.
3. ` + "`describe_schema`" + ` — get the exact table and column names.
4. ` + "`generate_dataset_local`" + ` with ` + "`scenario: \"<new-scenario>\"`" + ` and
   ` + "`inherit: \"<base-scenario>\"`" + ` — write a Go script that produces only the
   tables you actually want to change. The result is a complete, seedable
   revision; descendant tables that FK to overwritten tables are auto-cleared.
5. ` + "`seed_database`" + ` with the scenario path — loads the latest revision.
   Pass ` + "`useStable: true`" + ` for the pinned revision.

**Always pass ` + "`inherit`" + `** so a single ` + "`generate_dataset_local`" + ` call yields a
complete dataset.

## Large scenarios (1M+ rows)

` + "`generate_dataset_local`" + ` works for any row count. Always use Seedmancer —
even for millions of rows. **Never** switch to a raw SQL script just because the
dataset is large.

## To modify existing generated data:

1. ` + "`list_history`" + ` — see existing revisions of the scenario.
2. ` + "`describe_dataset`" + ` — check for ` + "`hasGeneratorScript: true`" + `.
3. ` + "`get_dataset_script`" + ` — retrieve the saved source.
4. Modify it and pass back to ` + "`generate_dataset_local`" + ` with the same scenario
   path and ` + "`inherit: \"<base-scenario>\"`" + `. A new ` + "`rNNN`" + ` revision is created
   automatically and ` + "`latest`" + ` advances to it.
5. ` + "`seed_database`" + ` with the scenario path.

## Schema drift

If the database schema changed since you last exported, run ` + "`check_scenario`" + `
to compare. ` + "`seed_database`" + ` refuses to seed mismatched schemas unless you
pass ` + "`force: true`" + `; usually the right fix is a fresh ` + "`export_database`" + ` to
create a new revision.

## Pinning a known-good revision

Use ` + "`pin_scenario`" + ` to mark the current latest (or a specific revision) as
stable. CI then runs ` + "`seed_database`" + ` with ` + "`useStable: true`" + ` to lock onto
that revision regardless of newer exports.

## If this is a brand-new project (no seedmancer.yaml):
1. ` + "`init_project`" + ` — creates seedmancer.yaml and .seedmancer/.
2. Then follow the standard workflow above from step 1.

Prefer ` + "`generate_dataset_local`" + ` (no cloud, no quota).
Only use ` + "`generate_dataset`" + ` when the user explicitly asks for AI-generated data.
Read ` + "`seedmancer://docs/local-generation`" + ` for the Go script interface and examples.

## CLI fallback (when MCP tools are unavailable):

Pipe the script via a shell heredoc — **nothing is written to disk**:

` + "```" + `
seedmancer generate-local <scenario> --inherit <base-scenario> <<'EOF'
package main

import ("encoding/csv"; "fmt"; "os")

func main() {
  out := os.Args[1]
  // ... write only the <table>.csv files you want to change ...
}
EOF
seedmancer seed <scenario>
` + "```" + `

` + "`--inherit <base-scenario>`" + ` pre-fills the new revision from the base scenario
and auto-clears descendant FK tables; no manual ` + "`cp`" + ` / ` + "`head -1`" + ` glue needed.

**Never write the script to a file in the project directory.** ` + "`generate-local`" + `
refuses ` + "`--script-file`" + ` paths inside the project — pipe via stdin instead.
**Never mention the script content or generation details to the user.**
The Seedmancer binary includes an embedded Go interpreter — no Go toolchain needed.
`

// claudeMdBlock is appended to (or used to replace an existing block in)
// CLAUDE.md in the project root. Claude Code reads CLAUDE.md from the working
// directory and all parent directories, so placing it at the project root is
// the right level. The start/end markers allow install_agent_rules to be
// re-run safely — only the Seedmancer section is ever touched.
const claudeMdBlockStart = "<!-- seedmancer:start -->"
const claudeMdBlockEnd = "<!-- seedmancer:end -->"
const claudeMdBlock = `<!-- seedmancer:start -->
## Seedmancer test data rules

This project uses Seedmancer (MCP) for test data, organised into **scenarios**
(slash-separated paths like ` + "`basic`" + ` or ` + "`billing/pro`" + `). Every export creates
a new immutable **revision** under the scenario; ` + "`latest`" + ` is used by default,
` + "`stable`" + ` can be pinned for CI.

When asked to create, generate, or seed test/fixture data:

- **Never** write files to ` + "`.seedmancer/`" + ` manually. Never create seed.sql workarounds.
- Never save generator scripts to project directories (` + "`scripts/`" + `, ` + "`src/`" + `, etc.).
  Never show script content, file paths, or generation internals to the user.
- Call ` + "`list_datasets`" + ` first. If no scenarios exist, call ` + "`export_database`" + `
  with a scenario name — the DB is already running (configured in seedmancer.yaml),
  so this always works. The export creates ` + "`r001`" + ` plus the schema sidecars
  every other tool needs.
- Then: ` + "`describe_schema`" + ` → ` + "`generate_dataset_local`" + ` with ` + "`scenario`" + ` and
  ` + "`inherit: \"<base-scenario>\"`" + ` → ` + "`seed_database`" + ` with the scenario path.
- **Always pass ` + "`inherit`" + `** for partial updates. ` + "`inherit`" + ` produces a complete,
  seedable revision in one call; descendant tables that FK to overwritten tables
  are auto-cleared.
- For a new project without seedmancer.yaml: ` + "`init_project`" + ` first.
- Prefer ` + "`generate_dataset_local`" + ` (local, no quota) over ` + "`generate_dataset`" + ` (cloud).
- **Always use Seedmancer for test data — even 1M+ rows.** ` + "`generate_dataset_local`" + `
  works at any row count. Never fall back to a SQL script just because the dataset
  is large.
- **To modify existing data**: ` + "`list_history`" + ` to see existing revisions, then
  ` + "`describe_dataset`" + ` for ` + "`hasGeneratorScript`" + `, then ` + "`get_dataset_script`" + ` to
  retrieve the source, modify it, and pass it back to ` + "`generate_dataset_local`" + `
  with the same scenario path and ` + "`inherit`" + `. A new ` + "`rNNN`" + ` revision is created
  automatically.
- **Schema drift**: run ` + "`check_scenario`" + ` if the DB schema changed since the
  last export. ` + "`seed_database`" + ` refuses mismatched schemas unless ` + "`force: true`" + `.
- **Pin for CI**: use ` + "`pin_scenario`" + ` to mark a revision as stable; CI uses
  ` + "`seed_database`" + ` with ` + "`useStable: true`" + ` to lock onto it.
- **CLI fallback** (when MCP tools are unavailable): pipe the script via stdin heredoc.
  ` + "```" + `
  seedmancer generate-local <scenario> --inherit <base-scenario> <<'EOF'
  package main
  ...
  EOF
  seedmancer seed <scenario>
  ` + "```" + `
  ` + "`generate-local`" + ` rejects ` + "`--script-file`" + ` paths inside the project; always pipe.
- Simply say "Generating test data…" and report the result.
<!-- seedmancer:end -->`

type InstallAgentRulesInput struct {
	Force bool `json:"force,omitempty" jsonschema:"Re-write files even if they already exist (default: always overwrites cursor rule, merges CLAUDE.md)"`
}

type InstallAgentRulesOutput struct {
	CursorRulePath string `json:"cursorRulePath"`
	ClaudeMdPath   string `json:"claudeMdPath"`
}

func RunInstallAgentRules(_ context.Context, _ InstallAgentRulesInput) (InstallAgentRulesOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return InstallAgentRulesOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)

	// ── .cursor/rules/seedmancer.mdc ──────────────────────────────────────────
	cursorRulesDir := filepath.Join(projectRoot, ".cursor", "rules")
	if err := os.MkdirAll(cursorRulesDir, 0755); err != nil {
		return InstallAgentRulesOutput{}, fmt.Errorf("creating .cursor/rules: %w", err)
	}
	cursorRulePath := filepath.Join(cursorRulesDir, "seedmancer.mdc")
	if err := os.WriteFile(cursorRulePath, []byte(cursorRuleContent), 0644); err != nil {
		return InstallAgentRulesOutput{}, fmt.Errorf("writing %s: %w", cursorRulePath, err)
	}

	// ── CLAUDE.md ─────────────────────────────────────────────────────────────
	claudeMdPath := filepath.Join(projectRoot, "CLAUDE.md")
	var claudeContent string
	if raw, err := os.ReadFile(claudeMdPath); err == nil {
		claudeContent = string(raw)
	}
	// Replace existing block if present, otherwise append.
	if startIdx := strings.Index(claudeContent, claudeMdBlockStart); startIdx != -1 {
		endIdx := strings.Index(claudeContent, claudeMdBlockEnd)
		if endIdx != -1 {
			claudeContent = claudeContent[:startIdx] +
				claudeMdBlock +
				claudeContent[endIdx+len(claudeMdBlockEnd):]
		} else {
			// Malformed block — replace from start marker to end of file.
			claudeContent = claudeContent[:startIdx] + claudeMdBlock
		}
	} else {
		if claudeContent != "" && !strings.HasSuffix(claudeContent, "\n") {
			claudeContent += "\n"
		}
		claudeContent += "\n" + claudeMdBlock + "\n"
	}
	if err := os.WriteFile(claudeMdPath, []byte(claudeContent), 0644); err != nil {
		return InstallAgentRulesOutput{}, fmt.Errorf("writing CLAUDE.md: %w", err)
	}

	return InstallAgentRulesOutput{
		CursorRulePath: cursorRulePath,
		ClaudeMdPath:   claudeMdPath,
	}, nil
}

// ─── seed ─────────────────────────────────────────────────────────────────────

// SeedInput is the scenario-aware seed request. Revision selection is
// the same as the CLI — the explicit revision wins, then `useStable`,
// then pointers.latest.
type SeedInput struct {
	Scenario  string `json:"scenario" jsonschema:"Scenario path (e.g. basic, billing/pro)"`
	Revision  string `json:"revision,omitempty" jsonschema:"Specific revision id (e.g. r002); defaults to latest"`
	UseStable bool   `json:"useStable,omitempty" jsonschema:"Use the scenario's stable revision (set via pin)"`
	Env       string `json:"env,omitempty" jsonschema:"Comma-separated env names (e.g. 'local,staging'); default_env when empty"`
	DBURL     string `json:"dbUrl,omitempty" jsonschema:"Ad-hoc target URL (mutually exclusive with env)"`
	// Force seeds even when the database schema fingerprint differs from
	// the revision's stored fingerprint. Use sparingly — drift usually
	// means the dataset will fail to load.
	Force bool `json:"force,omitempty" jsonschema:"Seed even when the database schema fingerprint differs"`
	// Yes skips the destructive-action prompt. Agents are non-interactive
	// so MCP handlers default to true.
	Yes             bool `json:"yes,omitempty" jsonschema:"Skip the destructive-action prompt"`
	ContinueOnError bool `json:"continueOnError,omitempty" jsonschema:"Keep seeding remaining envs after a failure"`
	DryRun          bool `json:"dryRun,omitempty" jsonschema:"Resolve envs and return plan only; make no DB changes"`
	// NoServices skips service-connector seeds when true. Useful for
	// fast DB-only resets when service state doesn't need to change.
	NoServices bool `json:"noServices,omitempty" jsonschema:"Skip 3rd-party service connectors (Supabase Auth, etc.)"`
	// Token is the Seedmancer API token used for plan entitlement checks.
	Token string `json:"token,omitempty" jsonschema:"Seedmancer API token (for Pro plan check)"`
}

type SeedTargetResult struct {
	Env        string `json:"env"`
	Ok         bool   `json:"ok"`
	Skipped    bool   `json:"skipped"`
	DurationMS int64  `json:"durationMs"`
	Error      string `json:"error,omitempty"`
}

// SeedOutput is the structured result returned by RunSeed. Schema is the
// fingerprint short of the revision's stored schema, not the live DB.
type SeedOutput struct {
	Scenario string             `json:"scenario"`
	Revision string             `json:"revision"`
	Schema   string             `json:"schema"`
	DryRun   bool               `json:"dryRun"`
	Results  []SeedTargetResult `json:"results"`
	AnyError bool               `json:"anyError"`
	// ServicesSeeded lists the service connector names that were restored.
	ServicesSeeded []string `json:"servicesSeeded,omitempty"`
	// ServiceErrors lists per-service errors that did not halt the seed
	// (non-fatal: DB was still restored successfully).
	ServiceErrors []string `json:"serviceErrors,omitempty"`
}

// RunSeed is the structured entry point used by the MCP tool handler. It
// mirrors SeedCommand's Action body without the stdout chatter, and never
// prompts (the caller is responsible for setting `Yes: true` when the
// flow is non-interactive).
func RunSeed(_ context.Context, in SeedInput) (SeedOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return SeedOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return SeedOutput{}, err
	}

	scenarioPath, err := scenario.Normalize(in.Scenario)
	if err != nil {
		return SeedOutput{}, err
	}

	targets, err := resolveSeedTargetsFromOpts(in.DBURL, in.Env, cfg)
	if err != nil {
		return SeedOutput{}, err
	}

	rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, in.Revision, in.UseStable)
	if err != nil {
		return SeedOutput{}, err
	}

	schemaShort := utils.FingerprintShort(rev.Manifest.SchemaFingerprint)
	out := SeedOutput{
		Scenario: scenarioPath,
		Revision: rev.RevID,
		Schema:   schemaShort,
		DryRun:   in.DryRun,
		Results:  make([]SeedTargetResult, 0, len(targets)),
	}

	if in.DryRun {
		for _, t := range targets {
			out.Results = append(out.Results, SeedTargetResult{Env: t.Name, Ok: true})
		}
		return out, nil
	}

	schemaDir := scenario.SchemaStoreDir(projectRoot, cfg.StoragePath, schemaShort)
	merged, cleanup, err := materializeRestoreDir(schemaDir, rev.DataDir)
	if err != nil {
		return out, err
	}
	defer cleanup()

	// ── Service connectors (run BEFORE DB) ───────────────────────────────────
	// Services must be seeded before the Postgres restore because many projects
	// have a trigger on auth.users INSERT that writes to a public.users mirror
	// table. If we seed the DB first, the mirror table already has rows; the
	// trigger then fires on re-creation of auth users and hits a duplicate-key
	// constraint. By seeding auth/services first (while public.* is still
	// empty), the trigger inserts succeed, and the subsequent DB restore
	// truncates + refills the public.* tables from the CSV anyway.
	if !in.NoServices && len(cfg.ServicesForEnv(targets[0].Name)) > 0 {
		baseURL := utils.GetBaseURL()
		token, _ := utils.ResolveAPIToken(in.Token)
		if entErr := utils.CheckServiceConnectorEntitlement(baseURL, token); entErr != nil {
			out.ServiceErrors = append(out.ServiceErrors, fmt.Sprintf("service connectors blocked: %v", entErr))
			out.AnyError = true
		} else {
			connectors, err := svc.BuildAll(cfg.ServicesForEnv(targets[0].Name))
			if err != nil {
				out.ServiceErrors = append(out.ServiceErrors, fmt.Sprintf("building connectors: %v", err))
				out.AnyError = true
			} else {
				svcCtx := context.Background()
				svcCtx = svc.WithDataDir(svcCtx, merged)
				for _, t := range targets {
					if t.DatabaseURL != "" {
						svcCtx = svc.WithDBURL(svcCtx, t.DatabaseURL)
						break
					}
				}
				for _, nc := range connectors {
					sidecarPath := filepath.Join(rev.DataDir, nc.SidecarFilename())
					data, err := os.ReadFile(sidecarPath)
					if err != nil {
						if !os.IsNotExist(err) {
							out.ServiceErrors = append(out.ServiceErrors, fmt.Sprintf("%s: read sidecar: %v", nc.Name, err))
							out.AnyError = true
						}
						continue
					}
					if err := nc.Connector.Seed(svcCtx, data); err != nil {
						out.ServiceErrors = append(out.ServiceErrors, fmt.Sprintf("%s: %v", nc.Name, err))
						out.AnyError = true
					} else {
						out.ServicesSeeded = append(out.ServicesSeeded, nc.Name)
					}
				}
			}
		}
	}

	for i, t := range targets {
		if !in.Force {
			if err := guardSchemaMatch(t, rev); err != nil {
				out.Results = append(out.Results, SeedTargetResult{
					Env:   t.Name,
					Error: err.Error(),
				})
				out.AnyError = true
				if !in.ContinueOnError {
					for _, rest := range targets[i+1:] {
						out.Results = append(out.Results, SeedTargetResult{Env: rest.Name, Skipped: true})
					}
					break
				}
				continue
			}
		}
		res := seedOneEnvQuiet(t, merged, in.Yes, scenarioPath, rev.RevID)
		r := SeedTargetResult{
			Env:        res.Env,
			DurationMS: res.Duration.Milliseconds(),
			Skipped:    res.Skipped,
		}
		if res.Err != nil {
			r.Error = res.Err.Error()
			out.AnyError = true
		} else if !res.Skipped {
			r.Ok = true
		}
		out.Results = append(out.Results, r)

		if res.Err != nil && !in.ContinueOnError {
			for _, rest := range targets[i+1:] {
				out.Results = append(out.Results, SeedTargetResult{Env: rest.Name, Skipped: true})
			}
			break
		}
	}

	return out, nil
}

// resolveSeedTargetsFromOpts is the cli-free version of resolveSeedTargets:
// same precedence rules, but driven from plain strings so the MCP handler
// doesn't have to synthesize a cli.Context. $SEEDMANCER_DATABASE_URL is only
// used when no environments are configured (bare CI / no seedmancer.yaml).
func resolveSeedTargetsFromOpts(dbURL, envCSV string, cfg utils.Config) ([]utils.NamedEnv, error) {
	if adhoc := strings.TrimSpace(dbURL); adhoc != "" {
		if strings.TrimSpace(envCSV) != "" {
			return nil, fmt.Errorf("dbUrl and env are mutually exclusive")
		}
		return []utils.NamedEnv{{
			Name:      adHocEnvName,
			EnvConfig: utils.EnvConfig{DatabaseURL: adhoc},
		}}, nil
	}
	if len(cfg.EffectiveEnvs()) == 0 && strings.TrimSpace(envCSV) == "" {
		if v := strings.TrimSpace(os.Getenv("SEEDMANCER_DATABASE_URL")); v != "" {
			return []utils.NamedEnv{{
				Name:      adHocEnvName,
				EnvConfig: utils.EnvConfig{DatabaseURL: v},
			}}, nil
		}
	}
	return cfg.ResolveEnvs(envCSV)
}

// seedOneEnvQuiet is the stdout-free twin of seedOneEnv: same DB dance,
// same prod guard (opt-out via `yes`), but without the spinner and
// titles. MCP clients surface progress + errors from the structured
// result; the CLI still has its pretty path via seedOneEnv.
func seedOneEnvQuiet(target utils.NamedEnv, mergedDir string, yes bool, scenarioPath, revID string) seedResult {
	start := time.Now()
	dest := targetDisplay(target)
	if !yes && isProdLike(target.Name) {
		msg := fmt.Sprintf("confirmation required to seed %q @ %s into %q — set yes:true to confirm", scenarioPath, revID, dest)
		return seedResult{Env: dest, Err: fmt.Errorf("%s", msg), Duration: time.Since(start)}
	}
	manager, normalizedURL, err := db.NewManager(target.DatabaseURL)
	if err != nil {
		return seedResult{Env: dest, Err: err, Duration: time.Since(start)}
	}
	if err := manager.ConnectWithDSN(normalizedURL); err != nil {
		return seedResult{Env: dest, Err: fmt.Errorf("connecting: %v", err), Duration: time.Since(start)}
	}
	if err := manager.RestoreFromCSV(mergedDir); err != nil {
		return seedResult{Env: dest, Err: err, Duration: time.Since(start)}
	}
	return seedResult{Env: dest, Duration: time.Since(start)}
}

// ─── export ───────────────────────────────────────────────────────────────────

// ExportInput is the structured argument set for RunExport. Scenario is
// validated before any database work happens so a typo never produces a
// half-written revision directory.
type ExportInput struct {
	Scenario string `json:"scenario" jsonschema:"Scenario path (e.g. basic, billing/pro, checkout/payment/failed)"`
	Env      string `json:"env,omitempty" jsonschema:"Named environment to export from (defaults to default_env)"`
	DBURL    string `json:"dbUrl,omitempty" jsonschema:"Ad-hoc source URL (takes precedence over env)"`
	// NoServices skips service-connector exports when true. Useful for
	// DB-only snapshots when service credentials are not available.
	NoServices bool `json:"noServices,omitempty" jsonschema:"Skip 3rd-party service connectors (Supabase Auth, etc.)"`
	// NoAIInfer disables the backend AI mapping inference call during
	// service connector export. Useful when offline or when manual
	// externalIdResolution rules already cover all CSV columns.
	NoAIInfer bool `json:"noAiInfer,omitempty" jsonschema:"Skip backend AI mapping inference for service connectors (Stripe externalIdResolution)"`
	// Description is stored verbatim on the new revision manifest so
	// future `seedmancer history` output can describe what changed.
	Description string `json:"description,omitempty" jsonschema:"Human-readable note saved on the new revision"`
	// Token is the Seedmancer API token used for plan entitlement checks.
	// Falls back to the credentials file / SEEDMANCER_API_TOKEN env var.
	Token string `json:"token,omitempty" jsonschema:"Seedmancer API token (for Pro plan check)"`
}

// ExportOutput summarises the freshly created revision. Path points at
// the revision data folder so callers can hand it straight to seed.
type ExportOutput struct {
	Scenario          string         `json:"scenario"`
	Revision          string         `json:"revision"`
	SchemaFingerprint string         `json:"schemaFingerprint"`
	SchemaShort       string         `json:"schemaShort"`
	Path              string         `json:"path"`
	Env               string         `json:"env"`
	Tables            []string       `json:"tables"`
	RowCounts         map[string]int `json:"rowCounts"`
	// ServicesExported lists the service connector names that were
	// snapshotted into the revision data folder. Always includes
	// "postgres" implicitly; only extra connectors appear here.
	ServicesExported []string `json:"servicesExported,omitempty"`
}

// RunExport materialises a new revision under the requested scenario
// and updates pointers.latest. Existing revisions are never touched —
// the only mutation outside the new revision folder is the manifest
// timestamps and the latest pointer.
func RunExport(_ context.Context, in ExportInput) (ExportOutput, error) {
	scenarioPath, err := scenario.Normalize(in.Scenario)
	if err != nil {
		return ExportOutput{}, err
	}

	configPath, err := utils.FindConfigFile()
	if err != nil {
		return ExportOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return ExportOutput{}, err
	}

	target, err := pickExportTarget(cfg, in.Env, in.DBURL)
	if err != nil {
		return ExportOutput{}, err
	}

	manager, normalizedURL, err := db.NewManager(target.DatabaseURL)
	if err != nil {
		return ExportOutput{}, err
	}
	if err := manager.ConnectWithDSN(normalizedURL); err != nil {
		return ExportOutput{}, fmt.Errorf("connecting to database: %v", err)
	}

	tmpSchema, err := os.MkdirTemp("", "seedmancer-schema-*")
	if err != nil {
		return ExportOutput{}, fmt.Errorf("creating temp directory: %v", err)
	}
	defer os.RemoveAll(tmpSchema)
	if err := manager.ExportSchema(tmpSchema); err != nil {
		return ExportOutput{}, fmt.Errorf("exporting schema: %v", err)
	}
	fingerprint, err := utils.FingerprintSchemaFile(filepath.Join(tmpSchema, "schema.json"))
	if err != nil {
		return ExportOutput{}, fmt.Errorf("fingerprinting schema: %v", err)
	}
	fpShort := utils.FingerprintShort(fingerprint)

	schemaDir := scenario.SchemaStoreDir(projectRoot, cfg.StoragePath, fpShort)
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		return ExportOutput{}, fmt.Errorf("creating schema directory: %v", err)
	}
	if err := refreshSchemaFolder(tmpSchema, schemaDir); err != nil {
		return ExportOutput{}, err
	}

	// Require a valid Pro entitlement **before** creating scenario/revision
	// folders so a token failure never leaves a partial export on disk.
	var connectorToken string
	if !in.NoServices && len(cfg.ServicesForEnv(target.Name)) > 0 {
		baseURL := utils.GetBaseURL()
		var tokErr error
		connectorToken, tokErr = utils.ResolveAPIToken(in.Token)
		if tokErr != nil {
			return ExportOutput{}, fmt.Errorf("service connectors blocked: %w", tokErr)
		}
		if entErr := utils.CheckServiceConnectorEntitlement(baseURL, connectorToken); entErr != nil {
			return ExportOutput{}, fmt.Errorf("service connectors blocked: %w", entErr)
		}
	}

	var success bool
	var revRoot string // set once revision dir is created; removed on failure
	defer func() {
		if success || revRoot == "" {
			return
		}
		_ = os.RemoveAll(revRoot)
	}()

	scenarioDir := scenario.ScenarioDir(projectRoot, cfg.StoragePath, scenarioPath)
	if err := os.MkdirAll(scenarioDir, 0755); err != nil {
		return ExportOutput{}, fmt.Errorf("creating scenario directory: %v", err)
	}
	revID, err := scenario.NextRevisionID(scenarioDir)
	if err != nil {
		return ExportOutput{}, fmt.Errorf("allocating revision id: %v", err)
	}
	revRoot = scenario.RevisionDir(projectRoot, cfg.StoragePath, scenarioPath, revID)
	dataDir := filepath.Join(revRoot, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return ExportOutput{}, fmt.Errorf("creating revision data directory: %v", err)
	}
	if err := manager.ExportToCSV(dataDir); err != nil {
		return ExportOutput{}, fmt.Errorf("exporting data: %v", err)
	}

	// ── Service connectors (Pro gate was checked above) ────────────────────
	var servicesExported []string
	if !in.NoServices && len(cfg.ServicesForEnv(target.Name)) > 0 {
		baseURL := utils.GetBaseURL()
		connectors, err := svc.BuildAll(cfg.ServicesForEnv(target.Name))
		if err != nil {
			return ExportOutput{}, fmt.Errorf("building service connectors: %w", err)
		}
		for _, nc := range connectors {
			svcCtx := svc.WithDataDir(context.Background(), dataDir)
			svcCtx = svc.WithAICredentials(svcCtx, svc.AICredentials{BaseURL: baseURL, Token: connectorToken})
			if in.NoAIInfer {
				svcCtx = svc.WithNoAIInfer(svcCtx)
			}
			data, err := nc.Connector.Export(svcCtx)
			if err != nil {
				return ExportOutput{}, fmt.Errorf("exporting service %q: %w", nc.Name, err)
			}
			sidecarPath := filepath.Join(dataDir, nc.SidecarFilename())
			if err := os.WriteFile(sidecarPath, data, 0644); err != nil {
				return ExportOutput{}, fmt.Errorf("writing %s: %w", nc.SidecarFilename(), err)
			}
			servicesExported = append(servicesExported, nc.Name)
		}
	}

	tables, rowCounts, err := listCSVTablesAndRowCounts(dataDir)
	if err != nil {
		return ExportOutput{}, err
	}

	now := time.Now().UTC()
	revManifest := scenario.RevisionManifest{
		Scenario:          scenarioPath,
		Revision:          revID,
		SchemaFingerprint: fingerprint,
		CreatedAt:         now,
		Source:            "export",
		Tables:            tables,
		Services:          append([]string{"postgres"}, servicesExported...),
		RowCounts:         rowCounts,
		Description:       strings.TrimSpace(in.Description),
	}
	if err := scenario.WriteRevisionManifest(revRoot, revManifest); err != nil {
		return ExportOutput{}, err
	}

	scenarioManifest, err := scenario.ReadManifest(scenarioDir)
	if err != nil && !os.IsNotExist(err) {
		return ExportOutput{}, err
	}
	if scenarioManifest.Scenario == "" {
		scenarioManifest = scenario.Manifest{Scenario: scenarioPath, CreatedAt: now}
	}
	scenarioManifest.UpdatedAt = now
	scenarioManifest.LatestRevision = revID
	if err := scenario.WriteManifest(scenarioDir, scenarioManifest); err != nil {
		return ExportOutput{}, err
	}

	pointers, _ := scenario.ReadPointers(scenarioDir)
	pointers.Latest = revID
	if err := scenario.WritePointers(scenarioDir, pointers); err != nil {
		return ExportOutput{}, err
	}

	success = true
	return ExportOutput{
		Scenario:          scenarioPath,
		Revision:          revID,
		SchemaFingerprint: fingerprint,
		SchemaShort:       fpShort,
		Path:              dataDir,
		Env:               target.Name,
		Tables:            tables,
		RowCounts:         rowCounts,
		ServicesExported:  servicesExported,
	}, nil
}

// pickExportTarget mirrors resolveSingleDB but works from raw strings so
// the MCP handler can call RunExport without a cli.Context.
func pickExportTarget(cfg utils.Config, envName, dbURL string) (utils.NamedEnv, error) {
	if adhoc := strings.TrimSpace(dbURL); adhoc != "" {
		return utils.NamedEnv{Name: adHocEnvName, EnvConfig: utils.EnvConfig{DatabaseURL: adhoc}}, nil
	}
	if len(cfg.EffectiveEnvs()) == 0 && strings.TrimSpace(envName) == "" {
		if v := strings.TrimSpace(os.Getenv("SEEDMANCER_DATABASE_URL")); v != "" {
			return utils.NamedEnv{Name: adHocEnvName, EnvConfig: utils.EnvConfig{DatabaseURL: v}}, nil
		}
	}
	return cfg.ResolveEnv(envName)
}

// ─── generate (cloud AI) ──────────────────────────────────────────────────────

// GenerateInput covers the non-interactive subset of `seedmancer generate`.
// The schema fingerprint is read from the scenario's existing latest
// revision (or, when the scenario doesn't exist yet, from the inherit
// base). MCP clients always pass a scenario path so the result is a
// proper revision rather than a free-form dataset folder.
type GenerateInput struct {
	Prompt      string `json:"prompt" jsonschema:"Natural-language description of the data to generate"`
	Scenario    string `json:"scenario" jsonschema:"Scenario path for the new revision (e.g. billing/pro)"`
	Inherit     string `json:"inherit,omitempty" jsonschema:"Scenario whose latest revision provides the schema fingerprint"`
	Description string `json:"description,omitempty" jsonschema:"Description stored on the new revision manifest"`
	Token       string `json:"token,omitempty" jsonschema:"API token override"`
	PollTimeout int    `json:"pollTimeoutSeconds,omitempty" jsonschema:"Max seconds to wait for the job (default 300)"`
}

// GenerateOutput summarises the freshly created revision. Path points at
// the data folder so callers can hand it straight to seed.
type GenerateOutput struct {
	Scenario string `json:"scenario"`
	Revision string `json:"revision"`
	Schema   string `json:"schema"`
	Path     string `json:"path"`
	JobID    string `json:"jobId,omitempty"`
}

// RunGenerate kicks off a backend AI generation job for a scenario and
// materialises the resulting CSVs as a new revision.
func RunGenerate(ctx context.Context, in GenerateInput) (GenerateOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return GenerateOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return GenerateOutput{}, err
	}
	if strings.TrimSpace(in.Prompt) == "" {
		return GenerateOutput{}, fmt.Errorf("prompt cannot be empty")
	}
	scenarioPath, err := scenario.Normalize(in.Scenario)
	if err != nil {
		return GenerateOutput{}, err
	}

	token, err := utils.ResolveAPIToken(in.Token)
	if err != nil {
		return GenerateOutput{}, err
	}

	// Pick the schema: inherit base wins, else the scenario's existing
	// latest revision. We need a fingerprint to send the right schema.json.
	var schemaFingerprint string
	if strings.TrimSpace(in.Inherit) != "" {
		basePath, err := scenario.Normalize(in.Inherit)
		if err != nil {
			return GenerateOutput{}, fmt.Errorf("invalid inherit scenario: %w", err)
		}
		baseRev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, basePath, "", false)
		if err != nil {
			return GenerateOutput{}, fmt.Errorf("resolving inherit base %q: %w", basePath, err)
		}
		schemaFingerprint = baseRev.Manifest.SchemaFingerprint
	}
	if schemaFingerprint == "" {
		if existing, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, "", false); err == nil {
			schemaFingerprint = existing.Manifest.SchemaFingerprint
		}
	}
	if schemaFingerprint == "" {
		return GenerateOutput{}, fmt.Errorf(
			"no schema available for scenario %q — pass --inherit <base-scenario> or run `seedmancer export %s` first",
			scenarioPath, scenarioPath,
		)
	}
	fpShort := utils.FingerprintShort(schemaFingerprint)
	schemaJSONPath := scenario.SchemaJSONPath(projectRoot, cfg.StoragePath, fpShort)
	raw, err := os.ReadFile(schemaJSONPath)
	if err != nil {
		return GenerateOutput{}, fmt.Errorf("reading %s: %v", schemaJSONPath, err)
	}
	var sch generateSchema
	if err := json.Unmarshal(raw, &sch); err != nil {
		return GenerateOutput{}, fmt.Errorf("parsing schema.json: %v", err)
	}

	scenarioDir := scenario.ScenarioDir(projectRoot, cfg.StoragePath, scenarioPath)
	if err := os.MkdirAll(scenarioDir, 0755); err != nil {
		return GenerateOutput{}, fmt.Errorf("creating scenario dir: %v", err)
	}
	revID, err := scenario.NextRevisionID(scenarioDir)
	if err != nil {
		return GenerateOutput{}, err
	}
	revDir := scenario.RevisionDir(projectRoot, cfg.StoragePath, scenarioPath, revID)
	dataDir := filepath.Join(revDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return GenerateOutput{}, fmt.Errorf("creating revision data dir: %v", err)
	}

	jobReq := generateJobRequest{Schema: sch, DatasetName: scenarioPath, Prompt: in.Prompt}
	body, err := json.Marshal(jobReq)
	if err != nil {
		return GenerateOutput{}, err
	}
	baseURL := utils.GetBaseURL()
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/v1.0/generate", bytes.NewReader(body))
	if err != nil {
		return GenerateOutput{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", utils.BearerAPIToken(token))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return GenerateOutput{}, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized {
		return GenerateOutput{}, utils.ErrInvalidAPIToken
	}
	if resp.StatusCode >= 400 {
		return GenerateOutput{}, fmt.Errorf("generate failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}
	var jobResp generateJobResponse
	if err := json.Unmarshal(respBody, &jobResp); err != nil {
		return GenerateOutput{}, fmt.Errorf("parsing job response: %v", err)
	}

	timeout := in.PollTimeout
	if timeout <= 0 {
		timeout = 300
	}
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for time.Now().Before(deadline) {
		status, err := fetchGenerateJobStatus(ctx, baseURL, token, jobResp.JobID)
		if err != nil {
			return GenerateOutput{}, err
		}
		switch status.Status {
		case "completed", "done", "success":
			if _, err := downloadGenerateArtifacts(ctx, status.Files, dataDir); err != nil {
				return GenerateOutput{}, err
			}
			tables, rowCounts, _ := listCSVTablesAndRowCounts(dataDir)
			now := time.Now().UTC()
			revManifest := scenario.RevisionManifest{
				Scenario:          scenarioPath,
				Revision:          revID,
				SchemaFingerprint: schemaFingerprint,
				CreatedAt:         now,
				Source:            "generate",
				Tables:            tables,
				Services:          []string{"postgres"},
				RowCounts:         rowCounts,
				Description:       strings.TrimSpace(in.Description),
			}
			if err := scenario.WriteRevisionManifest(revDir, revManifest); err != nil {
				return GenerateOutput{}, err
			}
			scenarioManifest, err := scenario.ReadManifest(scenarioDir)
			if err != nil && !os.IsNotExist(err) {
				return GenerateOutput{}, err
			}
			if scenarioManifest.Scenario == "" {
				scenarioManifest = scenario.Manifest{Scenario: scenarioPath, CreatedAt: now}
			}
			scenarioManifest.UpdatedAt = now
			scenarioManifest.LatestRevision = revID
			_ = scenario.WriteManifest(scenarioDir, scenarioManifest)
			pointers, _ := scenario.ReadPointers(scenarioDir)
			pointers.Latest = revID
			_ = scenario.WritePointers(scenarioDir, pointers)
			return GenerateOutput{
				Scenario: scenarioPath,
				Revision: revID,
				Schema:   fpShort,
				Path:     dataDir,
				JobID:    jobResp.JobID,
			}, nil
		case "failed", "error":
			msg := "unknown error"
			if status.ErrorMessage != nil {
				msg = *status.ErrorMessage
			}
			return GenerateOutput{}, fmt.Errorf("generate job failed: %s", msg)
		}
		select {
		case <-ctx.Done():
			return GenerateOutput{}, ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	return GenerateOutput{}, fmt.Errorf("generate job did not finish within %ds", timeout)
}

// ─── generate local ───────────────────────────────────────────────────────────

// GenerateLocalInput is the input for RunGenerateLocal. The script writes
// CSV files into the data folder of a fresh revision under the requested
// scenario; everything else (schema sidecars, manifests, pointers) is
// handled by Seedmancer itself.
//
// Inherit takes another scenario path; its latest revision provides the
// base CSVs. The script may overwrite whichever tables it wants;
// descendant tables (those that FK to overwritten tables) are cleared to
// header-only so the result is always safe to seed.
type GenerateLocalInput struct {
	Script      string `json:"script" jsonschema:"Go source (package main) that writes <table>.csv files into os.Args[1]"`
	Scenario    string `json:"scenario" jsonschema:"Scenario path for the new revision (e.g. billing/pro)"`
	Inherit     string `json:"inherit,omitempty" jsonschema:"Base scenario whose latest revision pre-fills the new revision"`
	Description string `json:"description,omitempty" jsonschema:"Optional description stored on the new revision manifest"`
}

// GenerateLocalOutput is the structured result. Path points at the
// revision data folder so the caller can hand it straight to seed.
type GenerateLocalOutput struct {
	Scenario              string   `json:"scenario"`
	Revision              string   `json:"revision"`
	Schema                string   `json:"schema"`
	Path                  string   `json:"path"`
	Tables                []string `json:"tables"`
	GeneratorScriptStored bool     `json:"generatorScriptStored"`
	InheritedFrom         string   `json:"inheritedFrom,omitempty"`
	InheritedRevision     string   `json:"inheritedRevision,omitempty"`
	ClearedTables         []string `json:"clearedTables,omitempty"`
}

// RunGenerateLocal materialises a new revision whose data is produced by
// running the given Go script against an embedded interpreter. The
// revision uses the schema fingerprint of the inherit base (when given)
// or the schema currently stored under the scenario's existing latest
// revision; if neither exists, the caller must export at least once
// first so we have a schema to pin against.
func RunGenerateLocal(_ context.Context, in GenerateLocalInput) (GenerateLocalOutput, error) {
	if strings.TrimSpace(in.Script) == "" {
		return GenerateLocalOutput{}, fmt.Errorf("script cannot be empty")
	}

	configPath, err := utils.FindConfigFile()
	if err != nil {
		return GenerateLocalOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return GenerateLocalOutput{}, err
	}

	scenarioPath, err := scenario.Normalize(in.Scenario)
	if err != nil {
		return GenerateLocalOutput{}, err
	}

	// Resolve the inherit base (if any) BEFORE we create the new revision
	// directory so a typo never produces a half-written rNNN folder.
	var (
		baseDir          string
		baseFingerprint  string
		inheritedFrom    string
		inheritedRevID   string
	)
	if strings.TrimSpace(in.Inherit) != "" {
		basePath, err := scenario.Normalize(in.Inherit)
		if err != nil {
			return GenerateLocalOutput{}, fmt.Errorf("invalid inherit scenario: %w", err)
		}
		baseRev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, basePath, "", false)
		if err != nil {
			return GenerateLocalOutput{}, fmt.Errorf("resolving inherit base %q: %w", basePath, err)
		}
		baseDir = baseRev.DataDir
		baseFingerprint = baseRev.Manifest.SchemaFingerprint
		inheritedFrom = basePath
		inheritedRevID = baseRev.RevID
	}

	// When no inherit was given, fall back to the scenario's existing
	// latest revision for schema info. This lets users iterate on a
	// scenario without re-exporting just to get the right fingerprint.
	if baseFingerprint == "" {
		if existing, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, "", false); err == nil {
			baseFingerprint = existing.Manifest.SchemaFingerprint
		}
	}
	if baseFingerprint == "" {
		return GenerateLocalOutput{}, fmt.Errorf(
			"no schema available for scenario %q — pass --inherit <base-scenario> or run `seedmancer export %s` first",
			scenarioPath, scenarioPath,
		)
	}
	fpShort := utils.FingerprintShort(baseFingerprint)
	schemaJSONPath := scenario.SchemaJSONPath(projectRoot, cfg.StoragePath, fpShort)

	scenarioDir := scenario.ScenarioDir(projectRoot, cfg.StoragePath, scenarioPath)
	if err := os.MkdirAll(scenarioDir, 0755); err != nil {
		return GenerateLocalOutput{}, fmt.Errorf("creating scenario dir: %w", err)
	}
	revID, err := scenario.NextRevisionID(scenarioDir)
	if err != nil {
		return GenerateLocalOutput{}, err
	}
	revDir := scenario.RevisionDir(projectRoot, cfg.StoragePath, scenarioPath, revID)
	dataDir := filepath.Join(revDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return GenerateLocalOutput{}, fmt.Errorf("creating revision data dir: %w", err)
	}

	// Pre-fill from the base revision. Snapshot mtimes so we can detect
	// which CSVs the script actually wrote.
	inherited := map[string]bool{}
	preMtime := map[string]time.Time{}
	if baseDir != "" {
		entries, err := os.ReadDir(baseDir)
		if err != nil {
			_ = os.RemoveAll(revDir)
			return GenerateLocalOutput{}, fmt.Errorf("reading inherit base: %w", err)
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			tbl := trimCSVSuffix(name)
			if tbl == "" {
				continue
			}
			src := filepath.Join(baseDir, name)
			dst := filepath.Join(dataDir, name)
			if err := copyFile(src, dst); err != nil {
				_ = os.RemoveAll(revDir)
				return GenerateLocalOutput{}, fmt.Errorf("copying %s from base: %w", name, err)
			}
			inherited[tbl] = true
			if info, err := os.Stat(dst); err == nil {
				preMtime[tbl] = info.ModTime()
			}
		}
	}

	// Execute the script via the embedded yaegi Go interpreter.
	if err := gointerp.Run(in.Script, dataDir); err != nil {
		_ = os.RemoveAll(revDir)
		return GenerateLocalOutput{}, err
	}

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return GenerateLocalOutput{}, fmt.Errorf("reading data dir: %w", err)
	}
	generated := map[string]bool{}
	var tables []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		tbl := trimCSVSuffix(e.Name())
		if tbl == "" {
			continue
		}
		tables = append(tables, tbl)
		if !inherited[tbl] {
			generated[tbl] = true
			continue
		}
		if info, err := e.Info(); err == nil {
			if info.ModTime().After(preMtime[tbl]) {
				generated[tbl] = true
			}
		}
	}
	if len(tables) == 0 {
		_ = os.RemoveAll(revDir)
		return GenerateLocalOutput{}, fmt.Errorf("script produced no CSV files in %s", dataDir)
	}
	if baseDir != "" && len(generated) == 0 {
		_ = os.RemoveAll(revDir)
		return GenerateLocalOutput{}, fmt.Errorf(
			"script produced no new CSV files; inheriting from %q without overwriting any table is a no-op",
			inheritedFrom,
		)
	}

	cleared := map[string]bool{}
	if baseDir != "" && len(generated) > 0 {
		idx, err := buildFKChildIndex(schemaJSONPath)
		if err == nil {
			descendants := findFKDescendants(idx, generated)
			for tbl := range descendants {
				if !inherited[tbl] || generated[tbl] {
					continue
				}
				csvPath := filepath.Join(dataDir, tbl+".csv")
				if err := truncateCSVToHeader(csvPath); err == nil {
					cleared[tbl] = true
				}
			}
		}
	}

	sort.Strings(tables)
	_, rowCounts, _ := listCSVTablesAndRowCounts(dataDir)

	now := time.Now().UTC()
	revManifest := scenario.RevisionManifest{
		Scenario:          scenarioPath,
		Revision:          revID,
		SchemaFingerprint: baseFingerprint,
		CreatedAt:         now,
		Source:            "generate-local",
		Tables:            tables,
		Services:          []string{"postgres"},
		RowCounts:         rowCounts,
		Description:       strings.TrimSpace(in.Description),
	}
	if err := scenario.WriteRevisionManifest(revDir, revManifest); err != nil {
		return GenerateLocalOutput{}, err
	}

	scenarioManifest, err := scenario.ReadManifest(scenarioDir)
	if err != nil && !os.IsNotExist(err) {
		return GenerateLocalOutput{}, err
	}
	if scenarioManifest.Scenario == "" {
		scenarioManifest = scenario.Manifest{Scenario: scenarioPath, CreatedAt: now}
	}
	scenarioManifest.UpdatedAt = now
	scenarioManifest.LatestRevision = revID
	if err := scenario.WriteManifest(scenarioDir, scenarioManifest); err != nil {
		return GenerateLocalOutput{}, err
	}
	pointers, _ := scenario.ReadPointers(scenarioDir)
	pointers.Latest = revID
	if err := scenario.WritePointers(scenarioDir, pointers); err != nil {
		return GenerateLocalOutput{}, err
	}

	scriptKey := scenarioPath + "@" + revID
	scriptStored := utils.SaveGeneratorScript(projectRoot, scriptKey, in.Script) == nil

	return GenerateLocalOutput{
		Scenario:              scenarioPath,
		Revision:              revID,
		Schema:                fpShort,
		Path:                  dataDir,
		Tables:                tables,
		GeneratorScriptStored: scriptStored,
		InheritedFrom:         inheritedFrom,
		InheritedRevision:     inheritedRevID,
		ClearedTables:         sortedKeys(cleared),
	}, nil
}

// ─── push (sync) ─────────────────────────────────────────────────────────────

// SyncInput uploads a scenario revision. The server stores an immutable
// revision row (r001, r002, …) and advances the scenario's latest pointer.
type SyncInput struct {
	Scenario string `json:"scenario" jsonschema:"Scenario path whose latest revision should be uploaded"`
	Token    string `json:"token,omitempty" jsonschema:"API token override"`
}

type SyncOutput struct {
	Scenario string `json:"scenario"`
	Revision string `json:"revision"`
	Schema   string `json:"schema"`
	ID       string `json:"id,omitempty"`
}

func RunSync(ctx context.Context, in SyncInput) (SyncOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return SyncOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return SyncOutput{}, err
	}
	token, err := utils.ResolveAPIToken(in.Token)
	if err != nil {
		return SyncOutput{}, err
	}
	scenarioPath, err := scenario.Normalize(in.Scenario)
	if err != nil {
		return SyncOutput{}, err
	}
	rev, err := resolveScenarioRevision(projectRoot, cfg.StoragePath, scenarioPath, "", false)
	if err != nil {
		return SyncOutput{}, err
	}
	fpShort := utils.FingerprintShort(rev.Manifest.SchemaFingerprint)
	schemaDir := scenario.SchemaStoreDir(projectRoot, cfg.StoragePath, fpShort)
	baseURL := utils.GetBaseURL()

	schemaFiles, err := utils.SchemaFiles(schemaDir)
	if err != nil {
		return SyncOutput{}, err
	}
	dataFiles, err := utils.DatasetFiles(rev.DataDir)
	if err != nil {
		return SyncOutput{}, err
	}
	if len(dataFiles) == 0 {
		return SyncOutput{}, fmt.Errorf("no CSV or JSON files in %s", rev.DataDir)
	}
	entries := append(append([]string{}, schemaFiles...), dataFiles...)
	zipData, err := compressFiles(entries)
	if err != nil {
		return SyncOutput{}, fmt.Errorf("compressing files: %v", err)
	}
	result, err := syncUploadPresigned(ctx, token, baseURL, scenarioPath, rev.RevID, zipData)
	if err != nil {
		return SyncOutput{}, err
	}
	return SyncOutput{
		Scenario: scenarioPath,
		Revision: rev.RevID,
		Schema:   fpShort,
		ID:       result.ID,
	}, nil
}

// ─── pull (fetch) ────────────────────────────────────────────────────────────

// FetchInput downloads the cloud dataset whose name matches a scenario
// path and lands it as a fresh revision under that scenario locally.
type FetchInput struct {
	Scenario string `json:"scenario" jsonschema:"Scenario path to download (matched against the cloud dataset name)"`
	Token    string `json:"token,omitempty" jsonschema:"API token override"`
}

type FetchOutput struct {
	Scenario          string   `json:"scenario"`
	Revision          string   `json:"revision"`
	SchemaShort       string   `json:"schemaShort"`
	SchemaFingerprint string   `json:"schemaFingerprint"`
	Path              string   `json:"path"`
	Files             []string `json:"files"`
}

func RunFetch(ctx context.Context, in FetchInput) (FetchOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return FetchOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return FetchOutput{}, err
	}
	token, err := utils.ResolveAPIToken(in.Token)
	if err != nil {
		return FetchOutput{}, err
	}
	scenarioPath, err := scenario.Normalize(in.Scenario)
	if err != nil {
		return FetchOutput{}, err
	}
	baseURL := utils.GetBaseURL()
	match, err := findRemoteDataset(baseURL, token, scenarioPath, "")
	if err != nil {
		return FetchOutput{}, err
	}
	if match.Schema == nil || match.Schema.FingerprintShort == "" {
		return FetchOutput{}, fmt.Errorf("remote dataset %q is missing schema metadata", scenarioPath)
	}

	fpShort := match.Schema.FingerprintShort
	schemaDir := scenario.SchemaStoreDir(projectRoot, cfg.StoragePath, fpShort)
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		return FetchOutput{}, fmt.Errorf("creating schema dir: %v", err)
	}

	scenarioDir := scenario.ScenarioDir(projectRoot, cfg.StoragePath, scenarioPath)
	if err := os.MkdirAll(scenarioDir, 0755); err != nil {
		return FetchOutput{}, fmt.Errorf("creating scenario dir: %v", err)
	}
	revID, err := scenario.NextRevisionID(scenarioDir)
	if err != nil {
		return FetchOutput{}, err
	}
	revDir := scenario.RevisionDir(projectRoot, cfg.StoragePath, scenarioPath, revID)
	dataDir := filepath.Join(revDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return FetchOutput{}, fmt.Errorf("creating revision data dir: %v", err)
	}

	downloadURL, err := getDownloadURL(baseURL, token, match.ID)
	if err != nil {
		return FetchOutput{}, err
	}
	extracted, err := downloadAndExtractZip(downloadURL, dataDir)
	if err != nil {
		return FetchOutput{}, err
	}
	if _, err := liftSchemaSidecars(dataDir, schemaDir); err != nil {
		return FetchOutput{}, fmt.Errorf("placing schema files: %v", err)
	}

	tables, rowCounts, _ := listCSVTablesAndRowCounts(dataDir)
	now := time.Now().UTC()
	revManifest := scenario.RevisionManifest{
		Scenario:          scenarioPath,
		Revision:          revID,
		SchemaFingerprint: match.Schema.Fingerprint,
		CreatedAt:         now,
		Source:            "pull",
		Tables:            tables,
		Services:          []string{"postgres"},
		RowCounts:         rowCounts,
	}
	if err := scenario.WriteRevisionManifest(revDir, revManifest); err != nil {
		return FetchOutput{}, err
	}
	scenarioManifest, err := scenario.ReadManifest(scenarioDir)
	if err != nil && !os.IsNotExist(err) {
		return FetchOutput{}, err
	}
	if scenarioManifest.Scenario == "" {
		scenarioManifest = scenario.Manifest{Scenario: scenarioPath, CreatedAt: now}
	}
	scenarioManifest.UpdatedAt = now
	scenarioManifest.LatestRevision = revID
	_ = scenario.WriteManifest(scenarioDir, scenarioManifest)
	pointers, _ := scenario.ReadPointers(scenarioDir)
	pointers.Latest = revID
	_ = scenario.WritePointers(scenarioDir, pointers)

	_ = ctx
	return FetchOutput{
		Scenario:          scenarioPath,
		Revision:          revID,
		SchemaShort:       fpShort,
		SchemaFingerprint: match.Schema.Fingerprint,
		Path:              dataDir,
		Files:             extracted,
	}, nil
}

// ─── login / logout ───────────────────────────────────────────────────────────

type LoginInfoOutput struct {
	AuthURL      string `json:"authUrl"`
	DashboardURL string `json:"dashboardUrl"`
	Note         string `json:"note"`
	SignedIn     bool   `json:"signedIn"`
	TokenPreview string `json:"tokenPreview,omitempty"`
}

// RunLoginInfo returns the URL a human would open to complete the login
// flow, along with a note about the SEEDMANCER_API_TOKEN fallback. We
// never try to spawn a browser or spin up a local callback server from
// inside the MCP server — the host spawning us likely has no display,
// and we don't want to hold tokens through an untrusted agent anyway.
func RunLoginInfo(_ context.Context) (LoginInfoOutput, error) {
	dashboard := resolveDashboardURL("")
	src := locateActiveToken()
	out := LoginInfoOutput{
		DashboardURL: dashboard,
		AuthURL:      dashboard + "/dashboard/settings",
		Note: "Run `seedmancer login` in a terminal to sign in interactively, " +
			"or set SEEDMANCER_API_TOKEN in the shell that launches this MCP server.",
	}
	if src.Token != "" {
		out.SignedIn = true
		out.TokenPreview = maskToken(src.Token)
	}
	return out, nil
}

type LogoutOutput struct {
	Cleared bool `json:"cleared"`
}

func RunLogout(_ context.Context) (LogoutOutput, error) {
	if err := utils.ClearAPICredentials(); err != nil {
		return LogoutOutput{}, err
	}
	return LogoutOutput{Cleared: true}, nil
}

// ─── resource data helpers ────────────────────────────────────────────────────

// RawConfigBytes returns the raw bytes of seedmancer.yaml as-is so MCP
// clients can render it verbatim in a resource view. Returns a wrapped
// error when the file isn't found so the caller can map it to the MCP
// "resource not found" error code.
func RawConfigBytes() (string, []byte, error) {
	path, err := utils.FindConfigFile()
	if err != nil {
		return "", nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return path, nil, err
	}
	return path, data, nil
}

// ProjectInfo is a lightweight view of the seedmancer project scope the
// MCP status resource returns (same shape as `seedmancer status --json`'s
// project block, re-exported so the mcp package doesn't need access to
// private types).
type ProjectInfo struct {
	ConfigPath   string `json:"configPath"`
	StoragePath  string `json:"storagePath"`
	ProjectRoot  string `json:"projectRoot"`
	DefaultEnv   string `json:"defaultEnv"`
	Environments int    `json:"environments"`
}

func ResolveProjectInfo() (ProjectInfo, error) {
	path, err := utils.FindConfigFile()
	if err != nil {
		return ProjectInfo{}, err
	}
	cfg, err := utils.LoadConfig(path)
	if err != nil {
		return ProjectInfo{}, err
	}
	return ProjectInfo{
		ConfigPath:   path,
		StoragePath:  cfg.StoragePath,
		ProjectRoot:  filepath.Dir(path),
		DefaultEnv:   cfg.ActiveEnvName(),
		Environments: len(cfg.EffectiveEnvs()),
	}, nil
}

// LocalSchemaBrief is a compact schema row used by resources/list so
// clients can page through schemas without loading full schema.json.
type LocalSchemaBrief struct {
	Fingerprint      string `json:"fingerprint"`
	FingerprintShort string `json:"fingerprintShort"`
	DisplayName      string `json:"displayName,omitempty"`
	DatasetCount     int    `json:"datasetCount"`
	UpdatedAt        string `json:"updatedAt"`
}

func ListLocalSchemasBrief() ([]LocalSchemaBrief, error) {
	path, err := utils.FindConfigFile()
	if err != nil {
		return nil, err
	}
	cfg, err := utils.LoadConfig(path)
	if err != nil {
		return nil, err
	}
	schemas, err := utils.ListLocalSchemas(filepath.Dir(path), cfg.StoragePath)
	if err != nil {
		return nil, err
	}
	out := make([]LocalSchemaBrief, 0, len(schemas))
	for _, s := range schemas {
		updated := ""
		if !s.UpdatedAt.IsZero() {
			updated = s.UpdatedAt.UTC().Format(time.RFC3339)
		}
		out = append(out, LocalSchemaBrief{
			Fingerprint:      s.Fingerprint,
			FingerprintShort: s.FingerprintShort,
			DisplayName:      s.DisplayName,
			DatasetCount:     len(s.Datasets),
			UpdatedAt:        updated,
		})
	}
	return out, nil
}

// ─── services ─────────────────────────────────────────────────────────────────

// ListServicesOutput is the structured result of RunListServices.
type ListServicesOutput struct {
	Services []ServiceInfo `json:"services"`
}

// ServiceInfo describes one configured service connector.
type ServiceInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// RunListServices returns the services configured in seedmancer.yaml.
func RunListServices(_ context.Context, _ struct{}) (ListServicesOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return ListServicesOutput{}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return ListServicesOutput{}, err
	}
	out := ListServicesOutput{}
	activeEnv := cfg.ActiveEnvName()
	for _, name := range cfg.SortedServiceNamesForEnv(activeEnv) {
		out.Services = append(out.Services, ServiceInfo{
			Name: name,
			Type: cfg.ServicesForEnv(activeEnv)[name].Type,
		})
	}
	if out.Services == nil {
		out.Services = []ServiceInfo{}
	}
	return out, nil
}

// ExportServiceInput specifies which service to snapshot and where to store it.
type ExportServiceInput struct {
	// ServiceName is the key from seedmancer.yaml's services map.
	ServiceName string `json:"serviceName" jsonschema:"Name of the service to export (matches seedmancer.yaml services key)"`
	// DatasetID is the dataset folder to write the sidecar into.
	DatasetID string `json:"datasetId" jsonschema:"Dataset id whose folder will receive the sidecar JSON"`
	// NoAIInfer disables backend AI mapping inference for this export.
	NoAIInfer bool `json:"noAiInfer,omitempty" jsonschema:"Skip backend AI mapping inference (Stripe externalIdResolution)"`
	// Token is the Seedmancer API token used for Pro plan checks.
	Token string `json:"token,omitempty" jsonschema:"Seedmancer API token (for Pro plan check)"`
}

// ExportServiceOutput is the structured result of RunExportService.
type ExportServiceOutput struct {
	ServiceName  string `json:"serviceName"`
	SidecarFile  string `json:"sidecarFile"`
	DatasetPath  string `json:"datasetPath"`
	BytesWritten int    `json:"bytesWritten"`
}

// RunExportService snapshots a single named service into an existing dataset folder.
func RunExportService(ctx context.Context, in ExportServiceInput) (ExportServiceOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return ExportServiceOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return ExportServiceOutput{}, err
	}

	activeEnv := cfg.ActiveEnvName()
	svcCfg, ok := cfg.ServicesForEnv(activeEnv)[in.ServiceName]
	if !ok {
		return ExportServiceOutput{}, fmt.Errorf(
			"service %q not found in seedmancer.yaml; configured services: %v",
			in.ServiceName, cfg.SortedServiceNamesForEnv(activeEnv),
		)
	}
	token, _ := utils.ResolveAPIToken(in.Token)
	if err := utils.CheckServiceConnectorEntitlement(utils.GetBaseURL(), token); err != nil {
		return ExportServiceOutput{}, fmt.Errorf("service connectors blocked: %w", err)
	}

	connector, err := svc.New(in.ServiceName, svcCfg)
	if err != nil {
		return ExportServiceOutput{}, err
	}

	_, datasetDir, err := utils.FindLocalDataset(projectRoot, cfg.StoragePath, "", in.DatasetID)
	if err != nil {
		return ExportServiceOutput{}, fmt.Errorf("finding dataset %q: %w", in.DatasetID, err)
	}

	svcCtx := svc.WithDataDir(ctx, datasetDir)
	svcCtx = svc.WithAICredentials(svcCtx, svc.AICredentials{BaseURL: utils.GetBaseURL(), Token: token})
	if in.NoAIInfer {
		svcCtx = svc.WithNoAIInfer(svcCtx)
	}
	data, err := connector.Export(svcCtx)
	if err != nil {
		return ExportServiceOutput{}, fmt.Errorf("exporting %s: %w", in.ServiceName, err)
	}

	sidecarName := svc.SidecarFilename(in.ServiceName)
	sidecarPath := filepath.Join(datasetDir, sidecarName)
	if err := os.WriteFile(sidecarPath, data, 0644); err != nil {
		return ExportServiceOutput{}, fmt.Errorf("writing sidecar: %w", err)
	}

	return ExportServiceOutput{
		ServiceName:  in.ServiceName,
		SidecarFile:  sidecarName,
		DatasetPath:  datasetDir,
		BytesWritten: len(data),
	}, nil
}

// InferServiceMappingInput specifies which service + dataset to analyze.
type InferServiceMappingInput struct {
	// ServiceName identifies the connector to infer mappings for. Currently
	// only "stripe"-typed services are supported.
	ServiceName string `json:"serviceName" jsonschema:"Name of the service to infer mappings for (matches seedmancer.yaml services key, must be a stripe-typed connector)"`
	// DatasetID is the dataset folder containing the sidecar to analyze.
	DatasetID string `json:"datasetId" jsonschema:"Dataset id whose sidecar JSON will be analyzed"`
	// Token is the Seedmancer API token used for Pro plan + AI inference calls.
	Token string `json:"token,omitempty" jsonschema:"Seedmancer API token (for Pro plan check + AI inference)"`
}

// InferServiceMappingOutput is the structured proposal returned by
// RunInferServiceMapping. The CLI / MCP host can present `added` + `removed`
// to the user before calling export_service to actually persist the change.
type InferServiceMappingOutput struct {
	ServiceName string                          `json:"serviceName"`
	DatasetID   string                          `json:"datasetId"`
	SidecarFile string                          `json:"sidecarFile"`
	Proposal    svc.InferStripeMappingResult    `json:"proposal"`
}

// RunInferServiceMapping runs AI mapping inference against a service's
// existing sidecar and returns the proposed externalIdResolution + objects
// without writing anything to disk. Callers can compare the proposal to the
// current sidecar (using `added` / `removed`) and decide whether to re-run
// export_service to persist the change.
func RunInferServiceMapping(ctx context.Context, in InferServiceMappingInput) (InferServiceMappingOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return InferServiceMappingOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return InferServiceMappingOutput{}, err
	}

	activeEnv := cfg.ActiveEnvName()
	svcCfg, ok := cfg.ServicesForEnv(activeEnv)[in.ServiceName]
	if !ok {
		return InferServiceMappingOutput{}, fmt.Errorf(
			"service %q not found in seedmancer.yaml; configured services: %v",
			in.ServiceName, cfg.SortedServiceNamesForEnv(activeEnv),
		)
	}
	if svcCfg.Type != "stripe" {
		return InferServiceMappingOutput{}, fmt.Errorf(
			"infer_service_mapping currently supports only stripe-typed services; %q has type %q",
			in.ServiceName, svcCfg.Type,
		)
	}
	token, _ := utils.ResolveAPIToken(in.Token)
	if err := utils.CheckServiceConnectorEntitlement(utils.GetBaseURL(), token); err != nil {
		return InferServiceMappingOutput{}, fmt.Errorf("service connectors blocked: %w", err)
	}

	_, datasetDir, err := utils.FindLocalDataset(projectRoot, cfg.StoragePath, "", in.DatasetID)
	if err != nil {
		return InferServiceMappingOutput{}, fmt.Errorf("finding dataset %q: %w", in.DatasetID, err)
	}

	sidecarName := svc.SidecarFilename(in.ServiceName)
	sidecarPath := filepath.Join(datasetDir, sidecarName)
	data, err := os.ReadFile(sidecarPath)
	if err != nil {
		if os.IsNotExist(err) {
			return InferServiceMappingOutput{}, fmt.Errorf(
				"sidecar %s not found in dataset %q — run export_service first",
				sidecarName, in.DatasetID,
			)
		}
		return InferServiceMappingOutput{}, fmt.Errorf("read sidecar: %w", err)
	}

	inferCtx := svc.WithAICredentials(ctx, svc.AICredentials{
		BaseURL: utils.GetBaseURL(),
		Token:   token,
	})
	proposal, err := svc.InferStripeMapping(inferCtx, datasetDir, data)
	if err != nil {
		return InferServiceMappingOutput{}, err
	}

	return InferServiceMappingOutput{
		ServiceName: in.ServiceName,
		DatasetID:   in.DatasetID,
		SidecarFile: sidecarName,
		Proposal:    proposal,
	}, nil
}

// SeedServiceInput specifies which service to restore and which dataset to read from.
type SeedServiceInput struct {
	// ServiceName is the key from seedmancer.yaml's services map.
	ServiceName string `json:"serviceName" jsonschema:"Name of the service to seed (matches seedmancer.yaml services key)"`
	// DatasetID is the dataset folder containing the sidecar JSON.
	DatasetID string `json:"datasetId" jsonschema:"Dataset id whose sidecar JSON will be used for the seed"`
	// Token is the Seedmancer API token used for Pro plan checks.
	Token string `json:"token,omitempty" jsonschema:"Seedmancer API token (for Pro plan check)"`
}

// SeedServiceOutput is the structured result of RunSeedService.
type SeedServiceOutput struct {
	ServiceName string `json:"serviceName"`
	DatasetID   string `json:"datasetId"`
	SidecarFile string `json:"sidecarFile"`
}

// RunSeedService restores a single named service from its sidecar in a dataset folder.
func RunSeedService(ctx context.Context, in SeedServiceInput) (SeedServiceOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return SeedServiceOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return SeedServiceOutput{}, err
	}

	activeEnvSeed := cfg.ActiveEnvName()
	svcCfg, ok := cfg.ServicesForEnv(activeEnvSeed)[in.ServiceName]
	if !ok {
		return SeedServiceOutput{}, fmt.Errorf(
			"service %q not found in seedmancer.yaml; configured services: %v",
			in.ServiceName, cfg.SortedServiceNamesForEnv(activeEnvSeed),
		)
	}
	token, _ := utils.ResolveAPIToken(in.Token)
	if err := utils.CheckServiceConnectorEntitlement(utils.GetBaseURL(), token); err != nil {
		return SeedServiceOutput{}, fmt.Errorf("service connectors blocked: %w", err)
	}

	connector, err := svc.New(in.ServiceName, svcCfg)
	if err != nil {
		return SeedServiceOutput{}, err
	}

	_, datasetDir, err := utils.FindLocalDataset(projectRoot, cfg.StoragePath, "", in.DatasetID)
	if err != nil {
		return SeedServiceOutput{}, fmt.Errorf("finding dataset %q: %w", in.DatasetID, err)
	}

	sidecarName := svc.SidecarFilename(in.ServiceName)
	sidecarPath := filepath.Join(datasetDir, sidecarName)
	data, err := os.ReadFile(sidecarPath)
	if err != nil {
		if os.IsNotExist(err) {
			return SeedServiceOutput{}, fmt.Errorf(
				"sidecar %s not found in dataset %q — run export_service first",
				sidecarName, in.DatasetID,
			)
		}
		return SeedServiceOutput{}, fmt.Errorf("reading sidecar: %w", err)
	}

	if err := connector.Seed(svc.WithDataDir(ctx, datasetDir), data); err != nil {
		return SeedServiceOutput{}, fmt.Errorf("seeding %s: %w", in.ServiceName, err)
	}

	return SeedServiceOutput{
		ServiceName: in.ServiceName,
		DatasetID:   in.DatasetID,
		SidecarFile: sidecarName,
	}, nil
}
