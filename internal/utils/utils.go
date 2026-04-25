package utils

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ErrMissingAPIToken is returned when no API token could be resolved from the
// flag, env var, project config, or global config. Commands and the top-level
// error handler use errors.Is to detect it so they can render the interactive
// login guide (see ui.PrintLoginHint) instead of a plain error line.
var ErrMissingAPIToken = errors.New("API token required")

// ErrInvalidAPIToken is returned when the API rejects the configured token
// (HTTP 401). It shares the login guide with ErrMissingAPIToken because the
// remediation is identical from the user's perspective: sign in again.
var ErrInvalidAPIToken = errors.New("API token invalid or expired")

// FindConfigFile locates seedmancer.yaml in the current or parent directories,
// falling back to ~/.seedmancer/config.yaml for global defaults.
func FindConfigFile() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %v", err)
	}
	for {
		configPath := filepath.Join(dir, "seedmancer.yaml")
		if _, err := os.Stat(configPath); err == nil {
			return configPath, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("getting home directory: %v", err)
	}

	globalConfig := filepath.Join(homeDir, ".seedmancer", "config.yaml")
	if _, err := os.Stat(globalConfig); err == nil {
		return globalConfig, nil
	}

	return "", fmt.Errorf("no seedmancer.yaml found in project or ~/.seedmancer/config.yaml — run 'seedmancer init' first")
}

// ─── On-disk layout helpers ──────────────────────────────────────────────────
//
// <projectRoot>/<storagePath>/schemas/<fp-short>/
//   ├── schema.json              # source-of-truth for fingerprint
//   └── datasets/
//       └── <dataset-name>/
//           └── <table>.csv ...
//
// The 12-char fingerprint prefix is the folder name so re-dumping the same
// database shape lands in the same folder (no manual bookkeeping).

// SchemasDir returns <projectRoot>/<storagePath>/schemas.
func SchemasDir(projectRoot, storagePath string) string {
	return filepath.Join(projectRoot, storagePath, "schemas")
}

// SchemaDir returns the folder for a given schema (by fingerprint-prefix name).
func SchemaDir(projectRoot, storagePath, fpShort string) string {
	return filepath.Join(SchemasDir(projectRoot, storagePath), fpShort)
}

// SchemaJSONPath returns the canonical location of a schema's schema.json.
func SchemaJSONPath(projectRoot, storagePath, fpShort string) string {
	return filepath.Join(SchemaDir(projectRoot, storagePath, fpShort), "schema.json")
}

// DatasetsDir returns the datasets root for a schema.
func DatasetsDir(projectRoot, storagePath, fpShort string) string {
	return filepath.Join(SchemaDir(projectRoot, storagePath, fpShort), "datasets")
}

// DatasetPath returns the on-disk path for a single dataset.
func DatasetPath(projectRoot, storagePath, fpShort, datasetName string) string {
	return filepath.Join(DatasetsDir(projectRoot, storagePath, fpShort), datasetName)
}

// DatasetMetaName is the filename of the per-dataset metadata sidecar.
const DatasetMetaName = "_meta.yaml"

// DatasetMeta holds lightweight, user-visible metadata for a single dataset
// folder. It is written by `export` and read by `list` / `describe_dataset`
// so callers always know which environment a dataset was captured from.
type DatasetMeta struct {
	SourceEnv string
}

// DatasetMetaPath returns the full path to the _meta.yaml sidecar for a
// dataset folder.
func DatasetMetaPath(datasetDir string) string {
	return filepath.Join(datasetDir, DatasetMetaName)
}

// WriteDatasetMeta writes m into <datasetDir>/_meta.yaml. Errors are
// non-fatal from the caller's perspective (the export still succeeded),
// but are returned so callers can log a warning.
func WriteDatasetMeta(datasetDir string, m DatasetMeta) error {
	lines := ""
	if m.SourceEnv != "" {
		lines += "source_env: " + m.SourceEnv + "\n"
	}
	if lines == "" {
		return nil
	}
	return os.WriteFile(DatasetMetaPath(datasetDir), []byte(lines), 0644)
}

// ReadDatasetMeta loads <datasetDir>/_meta.yaml. Missing file is not an
// error — older datasets written before this feature return a zero value.
func ReadDatasetMeta(datasetDir string) DatasetMeta {
	data, err := os.ReadFile(DatasetMetaPath(datasetDir))
	if err != nil {
		return DatasetMeta{}
	}
	var m DatasetMeta
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "source_env:") {
			m.SourceEnv = strings.TrimSpace(strings.TrimPrefix(line, "source_env:"))
		}
	}
	return m
}

// SchemaMetaPath returns the on-disk path for a schema's local metadata file.
// The file stores editable, user-facing metadata (e.g. display name) so the
// fingerprint-derived folder name can remain stable while users attach a
// human-friendly label. Missing file == no custom metadata.
func SchemaMetaPath(schemaDir string) string {
	return filepath.Join(schemaDir, "meta.yaml")
}

// ─── Schema discovery ───────────────────────────────────────────────────────

// LocalDataset is one dataset folder under a schema, with its latest mtime so
// callers can sort by recency. A dataset can be written by `export`,
// `generate`, or `fetch` — all three land at `<schemaDir>/datasets/<name>/`.
type LocalDataset struct {
	Name      string
	UpdatedAt time.Time
}

// LocalSchema describes an on-disk schema folder.
type LocalSchema struct {
	// Full SHA-256 hex fingerprint computed from the folder's schema.json.
	Fingerprint string
	// 12-char prefix — always the folder name on disk.
	FingerprintShort string
	// Optional user-facing name from meta.yaml (empty when unset).
	DisplayName string
	// Path to the schema folder (…/schemas/<fp-short>).
	Path string
	// Path to schema.json inside the folder.
	SchemaJSONPath string
	// Datasets inside `<schemaDir>/datasets/`, sorted by UpdatedAt DESC
	// (newest first).
	Datasets []LocalDataset
	// max(schema.json mtime, newest dataset mtime). Drives sort order in
	// `seedmancer list` so the most-recently-touched schema bubbles to the
	// top, regardless of whether it was touched by export / generate / fetch.
	UpdatedAt time.Time
}

// ListLocalSchemas walks <storagePath>/schemas and returns every folder that
// contains a schema.json, sorted by UpdatedAt DESC (newest first). The
// fingerprint is recomputed from the file — if the recomputed prefix doesn't
// match the folder name, the folder is still returned but the
// FingerprintShort reflects the real fingerprint (folder rename follow-up is
// done by the caller / `seedmancer export`).
func ListLocalSchemas(projectRoot, storagePath string) ([]LocalSchema, error) {
	root := SchemasDir(projectRoot, storagePath)
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading %s: %w", root, err)
	}

	var schemas []LocalSchema
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		schemaDir := filepath.Join(root, e.Name())
		schemaJSON := filepath.Join(schemaDir, "schema.json")
		schemaInfo, err := os.Stat(schemaJSON)
		if err != nil {
			// Skip folders without a schema.json — they're not valid schema dirs.
			continue
		}
		fp, err := FingerprintSchemaFile(schemaJSON)
		if err != nil {
			return nil, fmt.Errorf("fingerprinting %s: %w", schemaJSON, err)
		}
		datasets, err := listDatasetDirs(filepath.Join(schemaDir, "datasets"))
		if err != nil {
			return nil, err
		}
		// Schema is as "recent" as its schema.json or its newest dataset.
		updatedAt := schemaInfo.ModTime()
		if len(datasets) > 0 && datasets[0].UpdatedAt.After(updatedAt) {
			updatedAt = datasets[0].UpdatedAt
		}
		meta, _ := LoadLocalSchemaMeta(schemaDir)
		schemas = append(schemas, LocalSchema{
			Fingerprint:      fp,
			FingerprintShort: FingerprintShort(fp),
			DisplayName:      strings.TrimSpace(meta.DisplayName),
			Path:             schemaDir,
			SchemaJSONPath:   schemaJSON,
			Datasets:         datasets,
			UpdatedAt:        updatedAt,
		})
	}
	sort.SliceStable(schemas, func(i, j int) bool {
		if schemas[i].UpdatedAt.Equal(schemas[j].UpdatedAt) {
			return schemas[i].FingerprintShort < schemas[j].FingerprintShort
		}
		return schemas[i].UpdatedAt.After(schemas[j].UpdatedAt)
	})
	return schemas, nil
}

// listDatasetDirs returns the dataset folders inside `dir`, sorted by mtime
// DESC (newest first). Missing `dir` is not an error — schemas without any
// datasets return an empty slice.
func listDatasetDirs(dir string) ([]LocalDataset, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading %s: %w", dir, err)
	}
	var datasets []LocalDataset
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", filepath.Join(dir, e.Name()), err)
		}
		datasets = append(datasets, LocalDataset{
			Name:      e.Name(),
			UpdatedAt: info.ModTime(),
		})
	}
	sort.SliceStable(datasets, func(i, j int) bool {
		if datasets[i].UpdatedAt.Equal(datasets[j].UpdatedAt) {
			return datasets[i].Name < datasets[j].Name
		}
		return datasets[i].UpdatedAt.After(datasets[j].UpdatedAt)
	})
	return datasets, nil
}

// HumanizeAgo renders a time as a human-friendly relative string:
//   - zero / future / very recent → "just now"
//   - < 1 min                     → "just now"
//   - < 1 hour                    → "N minutes ago"
//   - < 24 hours                  → "N hours ago"
//   - < 48 hours                  → "yesterday"
//   - < 30 days                   → "N days ago"
//   - otherwise                   → "YYYY-MM-DD"
//
// Dependency-free on purpose — we don't want to pull in go-humanize for this.
func HumanizeAgo(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	d := time.Since(t)
	if d < time.Minute {
		return "just now"
	}
	if d < time.Hour {
		m := int(d / time.Minute)
		if m == 1 {
			return "1 minute ago"
		}
		return fmt.Sprintf("%d minutes ago", m)
	}
	if d < 24*time.Hour {
		h := int(d / time.Hour)
		if h == 1 {
			return "1 hour ago"
		}
		return fmt.Sprintf("%d hours ago", h)
	}
	if d < 48*time.Hour {
		return "yesterday"
	}
	if d < 30*24*time.Hour {
		return fmt.Sprintf("%d days ago", int(d/(24*time.Hour)))
	}
	return t.Format("2006-01-02")
}

// ResolveLocalSchema picks exactly one on-disk schema for a command to
// operate on. Pass ref == "" when the caller wants auto-detection; the
// call errors if there is more than one schema folder. When ref is
// non-empty it's matched (case-insensitively) as (a) a prefix of the
// fingerprint / fingerprint-short, or (b) an exact match of the optional
// meta.yaml display name. Ambiguity → error listing candidates.
func ResolveLocalSchema(projectRoot, storagePath, ref string) (LocalSchema, error) {
	schemas, err := ListLocalSchemas(projectRoot, storagePath)
	if err != nil {
		return LocalSchema{}, err
	}
	if len(schemas) == 0 {
		return LocalSchema{}, fmt.Errorf(
			"no local schemas found under %s — run `seedmancer export` first",
			SchemasDir(projectRoot, storagePath),
		)
	}

	ref = strings.ToLower(strings.TrimSpace(ref))
	if ref == "" {
		if len(schemas) == 1 {
			return schemas[0], nil
		}
		return LocalSchema{}, ambiguousSchemaError(schemas, "")
	}

	var hits []LocalSchema
	for _, s := range schemas {
		if strings.HasPrefix(strings.ToLower(s.FingerprintShort), ref) ||
			strings.HasPrefix(strings.ToLower(s.Fingerprint), ref) ||
			(s.DisplayName != "" && strings.EqualFold(s.DisplayName, ref)) {
			hits = append(hits, s)
		}
	}
	switch len(hits) {
	case 0:
		return LocalSchema{}, fmt.Errorf("no schema matching %q (available: %s)", ref, formatSchemaList(schemas))
	case 1:
		return hits[0], nil
	default:
		return LocalSchema{}, ambiguousSchemaError(hits, ref)
	}
}

// FindLocalDataset locates a specific dataset folder across all local schemas.
//
// When schemaPrefix is empty we walk every local schema and collect folders
// that hold a `datasets/<datasetName>/` directory. Exactly one hit is required;
// ambiguous hits surface the fingerprint prefixes the caller can pick from.
// When schemaPrefix is non-empty the usual ResolveLocalSchema picks the
// schema first, then the dataset folder is required to exist inside it.
func FindLocalDataset(projectRoot, storagePath, schemaPrefix, datasetName string) (LocalSchema, string, error) {
	datasetName = strings.TrimSpace(datasetName)
	if datasetName == "" {
		return LocalSchema{}, "", fmt.Errorf("dataset name is required")
	}

	if strings.TrimSpace(schemaPrefix) != "" {
		schema, err := ResolveLocalSchema(projectRoot, storagePath, schemaPrefix)
		if err != nil {
			return LocalSchema{}, "", err
		}
		dir := filepath.Join(schema.Path, "datasets", datasetName)
		if st, err := os.Stat(dir); err != nil || !st.IsDir() {
			return LocalSchema{}, "", fmt.Errorf(
				"no dataset %q under schema %s (expected %s)",
				datasetName, schema.FingerprintShort, dir,
			)
		}
		return schema, dir, nil
	}

	schemas, err := ListLocalSchemas(projectRoot, storagePath)
	if err != nil {
		return LocalSchema{}, "", err
	}

	var hits []LocalSchema
	for _, s := range schemas {
		for _, d := range s.Datasets {
			if d.Name == datasetName {
				hits = append(hits, s)
				break
			}
		}
	}
	switch len(hits) {
	case 0:
		if len(schemas) == 0 {
			return LocalSchema{}, "", fmt.Errorf(
				"no local schemas under %s — run `seedmancer export` first",
				SchemasDir(projectRoot, storagePath),
			)
		}
		return LocalSchema{}, "", fmt.Errorf(
			"no local dataset named %q (available schemas: %s)",
			datasetName, formatSchemaList(schemas),
		)
	case 1:
		return hits[0], filepath.Join(hits[0].Path, "datasets", datasetName), nil
	default:
		return LocalSchema{}, "", fmt.Errorf(
			"dataset name %q exists under multiple schemas (%s) — rename one so the dataset ids are unique",
			datasetName, formatSchemaList(hits),
		)
	}
}

// SchemaFiles returns the schema-level files inside a schema folder that
// belong next to the data CSVs at seed time — schema.json plus any `*_func.sql`
// or `*_trigger.sql` sidecar files produced by `seedmancer export`.
//
// Returned paths are absolute. Missing files are silently skipped so the helper
// works on partially-populated folders (e.g. a user-hand-crafted schema.json
// without function sidecars).
func SchemaFiles(schemaDir string) ([]string, error) {
	schemaJSON := filepath.Join(schemaDir, "schema.json")
	if _, err := os.Stat(schemaJSON); err != nil {
		return nil, fmt.Errorf("missing %s — this does not look like a schema folder", schemaJSON)
	}
	files := []string{schemaJSON}

	entries, err := os.ReadDir(schemaDir)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", schemaDir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, "_func.sql") || strings.HasSuffix(name, "_trigger.sql") {
			files = append(files, filepath.Join(schemaDir, name))
		}
	}
	return files, nil
}

// IsSchemaSidecarName reports whether the given filename is a schema-level
// file (the per-schema JSON or a function / trigger SQL sidecar) rather than
// a dataset payload. The same rule is shared by export, fetch, and sync so
// files end up in the right folder regardless of entry point.
func IsSchemaSidecarName(name string) bool {
	if name == "schema.json" {
		return true
	}
	return strings.HasSuffix(name, "_func.sql") || strings.HasSuffix(name, "_trigger.sql")
}

// DatasetFiles returns the CSV/JSON payload files inside a dataset folder.
// Subdirectories are skipped; the caller doesn't care about them.
func DatasetFiles(datasetDir string) ([]string, error) {
	entries, err := os.ReadDir(datasetDir)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", datasetDir, err)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		lower := strings.ToLower(name)
		if strings.HasSuffix(lower, ".csv") || strings.HasSuffix(lower, ".json") {
			files = append(files, filepath.Join(datasetDir, name))
		}
	}
	return files, nil
}

func ambiguousSchemaError(schemas []LocalSchema, ref string) error {
	if ref == "" {
		return fmt.Errorf(
			"multiple schemas found — pass --schema-id <fp-short-or-name> to pick one (available: %s)",
			formatSchemaList(schemas),
		)
	}
	return fmt.Errorf(
		"%q matches multiple schemas — use a longer fingerprint prefix (matches: %s)",
		ref, formatSchemaList(schemas),
	)
}

func formatSchemaList(schemas []LocalSchema) string {
	names := make([]string, len(schemas))
	for i, s := range schemas {
		names[i] = s.FingerprintShort
	}
	return strings.Join(names, ", ")
}

// ─── API config helpers ─────────────────────────────────────────────────────

// GetBaseURL resolves the Seedmancer HTTP API origin (no trailing slash).
// Priority: SEEDMANCER_API_URL → api_url in seedmancer.yaml → https://api.seedmancer.dev
func GetBaseURL() string {
	if v := os.Getenv("SEEDMANCER_API_URL"); v != "" {
		return strings.TrimRight(v, "/")
	}
	if cfgPath, err := FindConfigFile(); err == nil {
		if cfg, err := LoadConfig(cfgPath); err == nil && cfg.APIURL != "" {
			return strings.TrimRight(cfg.APIURL, "/")
		}
	}
	return "https://api.seedmancer.dev"
}

// BearerAPIToken returns the Authorization header value for a dashboard API token.
func BearerAPIToken(token string) string {
	return "Bearer " + token
}

// ResolveAPIToken returns a token from the first available source.
//
// Resolution order (highest priority first):
//  1. explicit --token CLI flag          (always wins)
//  2. ~/.seedmancer/credentials          (written by `seedmancer login`)
//  3. SEEDMANCER_API_TOKEN env var       (for CI and ad-hoc use)
//  4. legacy api_token: in seedmancer.yaml / ~/.seedmancer/config.yaml
//     (read-only fallback so pre-credentials-file installs keep working)
//
// The credentials file intentionally ranks above the env var: otherwise a
// stale `export SEEDMANCER_API_TOKEN=...` silently shadows every
// `seedmancer login`, which is confusing and was the source of a real
// support report. CI pipelines are unaffected because they don't have
// a credentials file to begin with.
//
// Note: callers must NOT wire SEEDMANCER_API_TOKEN through urfave/cli's
// flag EnvVars — that would let the env var sneak in as a "flag value"
// ahead of the credentials file and defeat the whole ordering.
func ResolveAPIToken(flagValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}

	if tok, err := LoadAPICredentials(); err == nil && tok != "" {
		return tok, nil
	}

	if tok := strings.TrimSpace(os.Getenv("SEEDMANCER_API_TOKEN")); tok != "" {
		return tok, nil
	}

	if configPath, err := FindConfigFile(); err == nil {
		if cfg, err := LoadConfig(configPath); err == nil && cfg.APIToken != "" {
			return cfg.APIToken, nil
		}
	}

	return "", ErrMissingAPIToken
}
