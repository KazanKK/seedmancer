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
	utils "github.com/KazanKK/seedmancer/internal/utils"
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

// ListInput controls which side(s) `RunList` walks. Zero-value (both
// flags false) is interpreted as "both sides", matching the CLI default.
type ListInput struct {
	Token  string `json:"token,omitempty" jsonschema:"API token override (falls back to credentials / SEEDMANCER_API_TOKEN)"`
	Local  bool   `json:"local,omitempty" jsonschema:"only list local datasets (skips the API call)"`
	Remote bool   `json:"remote,omitempty" jsonschema:"only list remote datasets (skips the local walk)"`
}

// ListOutput mirrors the JSON shape emitted by `seedmancer list --json`, so
// MCP clients get the same schema whether they use the tool or pipe the
// CLI into jq.
type ListOutput struct {
	Local  []listEntry `json:"local"`
	Remote []listEntry `json:"remote"`
}

// RunList returns the list of datasets on either or both sides. Errors
// from one side (e.g. missing API token) do not fail the whole call when
// both sides are requested — they surface as an empty slice on that side.
func RunList(_ context.Context, in ListInput) (ListOutput, error) {
	localWanted, remoteWanted := in.Local, in.Remote
	if !localWanted && !remoteWanted {
		localWanted, remoteWanted = true, true
	}

	out := ListOutput{Local: []listEntry{}, Remote: []listEntry{}}

	if localWanted {
		entries, err := listLocalEntries()
		if err == nil {
			if entries != nil {
				out.Local = entries
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
			entries, err := listRemoteEntries(token)
			if err != nil {
				if !localWanted {
					return out, err
				}
			} else if entries != nil {
				out.Remote = entries
			}
		}
	}

	return out, nil
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
	Dataset           string             `json:"dataset"`
	Path              string             `json:"path"`
	SchemaFingerprint string             `json:"schemaFingerprint"`
	SchemaShort       string             `json:"schemaShort"`
	SchemaDisplayName string             `json:"schemaDisplayName,omitempty"`
	UpdatedAt         string             `json:"updatedAt"`
	Files             []DatasetFilePreview `json:"files"`
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
	return out, nil
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
	Name    string        `json:"name"`
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
	ConfigPath  string `json:"configPath"`
	StoragePath string `json:"storagePath"`
	EnvName     string `json:"envName"`
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
	return InitOutput{ConfigPath: abs, StoragePath: storagePath, EnvName: envName}, nil
}

// ─── seed ─────────────────────────────────────────────────────────────────────

type SeedInput struct {
	DatasetID string `json:"datasetId" jsonschema:"Dataset id to restore (the name given at export/generate time)"`
	Env       string `json:"env,omitempty" jsonschema:"Comma-separated env names (e.g. 'local,staging'); default_env when empty"`
	DBURL     string `json:"dbUrl,omitempty" jsonschema:"Ad-hoc target URL (mutually exclusive with env)"`
	// Agents always run non-interactive; we default to skipping the prompt
	// when Yes is omitted by setting `Yes: true` in the MCP handler.
	Yes             bool `json:"yes,omitempty" jsonschema:"Skip the prod confirmation prompt"`
	ContinueOnError bool `json:"continueOnError,omitempty" jsonschema:"Keep seeding remaining envs after a failure"`
	DryRun          bool `json:"dryRun,omitempty" jsonschema:"Resolve envs and return plan only; make no DB changes"`
}

type SeedTargetResult struct {
	Env        string `json:"env"`
	Ok         bool   `json:"ok"`
	Skipped    bool   `json:"skipped"`
	DurationMS int64  `json:"durationMs"`
	Error      string `json:"error,omitempty"`
}

type SeedOutput struct {
	Dataset  string             `json:"dataset"`
	Schema   string             `json:"schema"`
	DryRun   bool               `json:"dryRun"`
	Results  []SeedTargetResult `json:"results"`
	AnyError bool               `json:"anyError"`
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

	targets, err := resolveSeedTargetsFromOpts(in.DBURL, in.Env, cfg)
	if err != nil {
		return SeedOutput{}, err
	}

	datasetName := strings.TrimSpace(in.DatasetID)
	schema, datasetDir, err := utils.FindLocalDataset(projectRoot, cfg.StoragePath, "", datasetName)
	if err != nil {
		return SeedOutput{}, err
	}

	out := SeedOutput{
		Dataset: datasetName,
		Schema:  schema.FingerprintShort,
		DryRun:  in.DryRun,
		Results: make([]SeedTargetResult, 0, len(targets)),
	}

	if in.DryRun {
		for _, t := range targets {
			out.Results = append(out.Results, SeedTargetResult{Env: t.Name, Ok: true})
		}
		return out, nil
	}

	merged, cleanup, err := materializeRestoreDir(schema.Path, datasetDir)
	if err != nil {
		return out, err
	}
	defer cleanup()

	for i, t := range targets {
		res := seedOneEnvQuiet(t, merged, in.Yes)
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
// same precedence rules (ad-hoc URL > named env > default), but driven
// from plain strings so the MCP handler doesn't have to synthesize a
// cli.Context.
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
	if v := strings.TrimSpace(os.Getenv("SEEDMANCER_DATABASE_URL")); v != "" && strings.TrimSpace(envCSV) == "" {
		return []utils.NamedEnv{{
			Name:      adHocEnvName,
			EnvConfig: utils.EnvConfig{DatabaseURL: v},
		}}, nil
	}
	return cfg.ResolveEnvs(envCSV)
}

// seedOneEnvQuiet is the stdout-free twin of seedOneEnv: same DB dance,
// same prod guard (opt-out via `yes`), but without the spinner and
// titles. MCP clients surface progress + errors from the structured
// result; the CLI still has its pretty path via seedOneEnv.
func seedOneEnvQuiet(target utils.NamedEnv, mergedDir string, yes bool) seedResult {
	start := time.Now()
	if isProdLike(target.Name) && !yes {
		// Agents can't prompt — refuse rather than silently seeding prod.
		return seedResult{
			Env:      target.Name,
			Err:      fmt.Errorf("refusing to seed prod-like env %q without yes:true", target.Name),
			Duration: time.Since(start),
		}
	}
	dbURL, scheme, err := normalizePostgresDSN(target.DatabaseURL)
	if err != nil {
		return seedResult{Env: target.Name, Err: err, Duration: time.Since(start)}
	}
	if scheme != "postgres" {
		return seedResult{
			Env:      target.Name,
			Err:      fmt.Errorf("unsupported database type: %s (only postgres is supported)", scheme),
			Duration: time.Since(start),
		}
	}
	pg := &db.PostgresManager{}
	if err := pg.ConnectWithDSN(dbURL); err != nil {
		return seedResult{Env: target.Name, Err: fmt.Errorf("connecting: %v", err), Duration: time.Since(start)}
	}
	if err := pg.RestoreFromCSV(mergedDir); err != nil {
		return seedResult{Env: target.Name, Err: err, Duration: time.Since(start)}
	}
	return seedResult{Env: target.Name, Duration: time.Since(start)}
}

// ─── export ───────────────────────────────────────────────────────────────────

type ExportInput struct {
	ID    string `json:"id,omitempty" jsonschema:"Dataset id for the new dump (defaults to a YYYYMMDDHHMMSS timestamp)"`
	Env   string `json:"env,omitempty" jsonschema:"Named environment to export from (defaults to default_env)"`
	DBURL string `json:"dbUrl,omitempty" jsonschema:"Ad-hoc source URL (takes precedence over env)"`
	Force bool   `json:"force,omitempty" jsonschema:"Overwrite an existing dataset without prompting"`
}

type ExportOutput struct {
	Dataset           string `json:"dataset"`
	SchemaFingerprint string `json:"schemaFingerprint"`
	SchemaShort       string `json:"schemaShort"`
	Path              string `json:"path"`
	Env               string `json:"env"`
}

func RunExport(_ context.Context, in ExportInput) (ExportOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return ExportOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return ExportOutput{}, err
	}

	var target utils.NamedEnv
	if adhoc := strings.TrimSpace(in.DBURL); adhoc != "" {
		target = utils.NamedEnv{Name: adHocEnvName, EnvConfig: utils.EnvConfig{DatabaseURL: adhoc}}
	} else if v := strings.TrimSpace(os.Getenv("SEEDMANCER_DATABASE_URL")); v != "" && strings.TrimSpace(in.Env) == "" {
		target = utils.NamedEnv{Name: adHocEnvName, EnvConfig: utils.EnvConfig{DatabaseURL: v}}
	} else {
		t, err := cfg.ResolveEnv(in.Env)
		if err != nil {
			return ExportOutput{}, err
		}
		target = t
	}

	datasetName := strings.TrimSpace(in.ID)
	if datasetName == "" {
		datasetName = time.Now().UTC().Format("20060102150405")
	}
	datasetName = utils.SanitizeDatasetSegment(datasetName)

	dbURL, scheme, err := normalizePostgresDSN(target.DatabaseURL)
	if err != nil {
		return ExportOutput{}, err
	}
	if scheme != "postgres" {
		return ExportOutput{}, fmt.Errorf("unsupported database type: %s", scheme)
	}
	pg := &db.PostgresManager{}
	if err := pg.ConnectWithDSN(dbURL); err != nil {
		return ExportOutput{}, fmt.Errorf("connecting to database: %v", err)
	}

	tmpSchema, err := os.MkdirTemp("", "seedmancer-schema-*")
	if err != nil {
		return ExportOutput{}, fmt.Errorf("creating temp directory: %v", err)
	}
	defer os.RemoveAll(tmpSchema)
	if err := pg.ExportSchema(tmpSchema); err != nil {
		return ExportOutput{}, fmt.Errorf("exporting schema: %v", err)
	}
	fingerprint, err := utils.FingerprintSchemaFile(filepath.Join(tmpSchema, "schema.json"))
	if err != nil {
		return ExportOutput{}, fmt.Errorf("fingerprinting schema: %v", err)
	}
	fpShort := utils.FingerprintShort(fingerprint)

	schemaDir := utils.SchemaDir(projectRoot, cfg.StoragePath, fpShort)
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		return ExportOutput{}, fmt.Errorf("creating schema directory: %v", err)
	}
	if err := refreshSchemaFolder(tmpSchema, schemaDir); err != nil {
		return ExportOutput{}, err
	}

	datasetDir := utils.DatasetPath(projectRoot, cfg.StoragePath, fpShort, datasetName)
	if info, statErr := os.Stat(datasetDir); statErr == nil && info.IsDir() {
		if !in.Force {
			return ExportOutput{}, fmt.Errorf("dataset %q already exists at %s — set force:true to overwrite", datasetName, datasetDir)
		}
		if err := os.RemoveAll(datasetDir); err != nil {
			return ExportOutput{}, fmt.Errorf("removing existing dataset directory: %v", err)
		}
	}
	if err := os.MkdirAll(datasetDir, 0755); err != nil {
		return ExportOutput{}, fmt.Errorf("creating dataset directory: %v", err)
	}
	if err := pg.ExportToCSV(datasetDir); err != nil {
		return ExportOutput{}, fmt.Errorf("exporting data: %v", err)
	}
	return ExportOutput{
		Dataset:           datasetName,
		SchemaFingerprint: fingerprint,
		SchemaShort:       fpShort,
		Path:              datasetDir,
		Env:               target.Name,
	}, nil
}

// ─── generate ─────────────────────────────────────────────────────────────────

// GenerateInput covers the non-interactive subset of `seedmancer generate`.
// MCP clients that want to reuse an already-exported schema point at its
// fingerprint; otherwise the tool errors and asks them to run export first
// (we do not open DB connections from the MCP surface opportunistically).
type GenerateInput struct {
	Prompt      string `json:"prompt" jsonschema:"Natural-language description of the data to generate"`
	SchemaRef   string `json:"schemaRef" jsonschema:"Fingerprint prefix (≥4 chars) of the target schema folder"`
	DatasetID   string `json:"datasetId,omitempty" jsonschema:"Dataset id for the result (timestamp when empty)"`
	Token       string `json:"token,omitempty" jsonschema:"API token override"`
	Force       bool   `json:"force,omitempty" jsonschema:"Overwrite an existing dataset folder"`
	PollTimeout int    `json:"pollTimeoutSeconds,omitempty" jsonschema:"Max seconds to wait for the job (default 300)"`
}

type GenerateOutput struct {
	Dataset string `json:"dataset"`
	Schema  string `json:"schema"`
	Path    string `json:"path"`
	JobID   string `json:"jobId,omitempty"`
}

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
	if strings.TrimSpace(in.SchemaRef) == "" {
		return GenerateOutput{}, fmt.Errorf("schemaRef cannot be empty — run export_database first")
	}

	token, err := utils.ResolveAPIToken(in.Token)
	if err != nil {
		return GenerateOutput{}, err
	}
	schema, err := utils.ResolveLocalSchema(projectRoot, cfg.StoragePath, in.SchemaRef)
	if err != nil {
		return GenerateOutput{}, err
	}

	schemaJSONPath := filepath.Join(schema.Path, "schema.json")
	raw, err := os.ReadFile(schemaJSONPath)
	if err != nil {
		return GenerateOutput{}, fmt.Errorf("reading %s: %v", schemaJSONPath, err)
	}
	var sch generateSchema
	if err := json.Unmarshal(raw, &sch); err != nil {
		return GenerateOutput{}, fmt.Errorf("parsing schema.json: %v", err)
	}

	datasetName := strings.TrimSpace(in.DatasetID)
	if datasetName == "" {
		datasetName = time.Now().UTC().Format("20060102150405")
	}
	datasetName = utils.SanitizeDatasetSegment(datasetName)

	datasetDir := utils.DatasetPath(projectRoot, cfg.StoragePath, schema.FingerprintShort, datasetName)
	if info, statErr := os.Stat(datasetDir); statErr == nil && info.IsDir() {
		if !in.Force {
			return GenerateOutput{}, fmt.Errorf("dataset %q already exists at %s — set force:true to overwrite", datasetName, datasetDir)
		}
		if err := os.RemoveAll(datasetDir); err != nil {
			return GenerateOutput{}, err
		}
	}
	if err := os.MkdirAll(datasetDir, 0755); err != nil {
		return GenerateOutput{}, err
	}

	jobReq := generateJobRequest{Schema: sch, DatasetName: datasetName, Prompt: in.Prompt}
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
			if _, err := downloadGenerateArtifacts(ctx, status.Files, datasetDir); err != nil {
				return GenerateOutput{}, err
			}
			return GenerateOutput{
				Dataset: datasetName,
				Schema:  schema.FingerprintShort,
				Path:    datasetDir,
				JobID:   jobResp.JobID,
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

// ─── sync ─────────────────────────────────────────────────────────────────────

type SyncInput struct {
	DatasetID string `json:"datasetId" jsonschema:"Dataset id to upload"`
	Token     string `json:"token,omitempty" jsonschema:"API token override"`
}

type SyncOutput struct {
	Dataset string `json:"dataset"`
	Schema  string `json:"schema"`
	ID      string `json:"id,omitempty"`
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
	datasetName := strings.TrimSpace(in.DatasetID)
	if datasetName == "" {
		return SyncOutput{}, fmt.Errorf("datasetId is required")
	}
	schema, datasetDir, err := utils.FindLocalDataset(projectRoot, cfg.StoragePath, "", datasetName)
	if err != nil {
		return SyncOutput{}, err
	}
	baseURL := utils.GetBaseURL()
	result, err := syncDatasetUpload(ctx, token, schema, datasetDir, datasetName, baseURL)
	if err != nil {
		return SyncOutput{}, err
	}
	return SyncOutput{Dataset: datasetName, Schema: schema.FingerprintShort, ID: result.ID}, nil
}

// ─── fetch ────────────────────────────────────────────────────────────────────

type FetchInput struct {
	DatasetID string `json:"datasetId" jsonschema:"Dataset id to download"`
	Token     string `json:"token,omitempty" jsonschema:"API token override"`
	Force     bool   `json:"force,omitempty" jsonschema:"Overwrite existing local dataset"`
}

type FetchOutput struct {
	Dataset          string   `json:"dataset"`
	SchemaShort      string   `json:"schemaShort"`
	SchemaFingerprint string  `json:"schemaFingerprint"`
	Path             string   `json:"path"`
	Files            []string `json:"files"`
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
	datasetName := strings.TrimSpace(in.DatasetID)
	if datasetName == "" {
		return FetchOutput{}, fmt.Errorf("datasetId is required")
	}
	baseURL := utils.GetBaseURL()
	res, err := fetchDatasetDownload(ctx, baseURL, token, projectRoot, cfg.StoragePath, datasetName)
	if err != nil {
		return FetchOutput{}, err
	}
	out := FetchOutput{
		Dataset: datasetName,
		Path:    res.OutputDir,
		Files:   res.Files,
	}
	if res.Match.Schema != nil {
		out.SchemaShort = res.Match.Schema.FingerprintShort
		out.SchemaFingerprint = res.Match.Schema.Fingerprint
	}
	_ = in.Force
	return out, nil
}

// ─── login / logout ───────────────────────────────────────────────────────────

type LoginInfoOutput struct {
	AuthURL       string `json:"authUrl"`
	DashboardURL  string `json:"dashboardUrl"`
	Note          string `json:"note"`
	SignedIn      bool   `json:"signedIn"`
	TokenPreview  string `json:"tokenPreview,omitempty"`
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
