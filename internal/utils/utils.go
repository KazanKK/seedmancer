package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

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

// ─── Schema discovery ───────────────────────────────────────────────────────

// LocalSchema describes an on-disk schema folder.
type LocalSchema struct {
	// Full SHA-256 hex fingerprint computed from the folder's schema.json.
	Fingerprint string
	// 12-char prefix — always the folder name on disk.
	FingerprintShort string
	// Path to the schema folder (…/schemas/<fp-short>).
	Path string
	// Path to schema.json inside the folder.
	SchemaJSONPath string
	// Dataset folder names sorted ascending.
	Datasets []string
}

// ListLocalSchemas walks <storagePath>/schemas and returns every folder that
// contains a schema.json. The fingerprint is recomputed from the file — if
// the recomputed prefix doesn't match the folder name, the folder is still
// returned but the FingerprintShort reflects the real fingerprint (folder
// rename follow-up is done by the caller / `seedmancer export`).
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
		if _, err := os.Stat(schemaJSON); err != nil {
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
		schemas = append(schemas, LocalSchema{
			Fingerprint:      fp,
			FingerprintShort: FingerprintShort(fp),
			Path:             schemaDir,
			SchemaJSONPath:   schemaJSON,
			Datasets:         datasets,
		})
	}
	sort.Slice(schemas, func(i, j int) bool {
		return schemas[i].FingerprintShort < schemas[j].FingerprintShort
	})
	return schemas, nil
}

func listDatasetDirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading %s: %w", dir, err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// ResolveLocalSchema picks exactly one on-disk schema for a command to
// operate on. Pass prefix == "" when the caller wants auto-detection; the
// call errors if there is more than one schema folder. When prefix is
// non-empty it's matched (case-insensitively) as a prefix of the folder
// name (== fingerprint prefix). Ambiguity → error listing candidates.
func ResolveLocalSchema(projectRoot, storagePath, prefix string) (LocalSchema, error) {
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

	prefix = strings.ToLower(strings.TrimSpace(prefix))
	if prefix == "" {
		if len(schemas) == 1 {
			return schemas[0], nil
		}
		return LocalSchema{}, ambiguousSchemaError(schemas, "")
	}

	var hits []LocalSchema
	for _, s := range schemas {
		if strings.HasPrefix(strings.ToLower(s.FingerprintShort), prefix) ||
			strings.HasPrefix(strings.ToLower(s.Fingerprint), prefix) {
			hits = append(hits, s)
		}
	}
	switch len(hits) {
	case 0:
		return LocalSchema{}, fmt.Errorf("no schema matching prefix %q (available: %s)", prefix, formatSchemaList(schemas))
	case 1:
		return hits[0], nil
	default:
		return LocalSchema{}, ambiguousSchemaError(hits, prefix)
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
			if d == datasetName {
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
			"dataset name %q exists under multiple schemas (%s) — pass --schema <fp-prefix> to pick one",
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

func ambiguousSchemaError(schemas []LocalSchema, prefix string) error {
	if prefix == "" {
		return fmt.Errorf(
			"multiple schemas found — pass --schema <fp-prefix> to pick one (available: %s)",
			formatSchemaList(schemas),
		)
	}
	return fmt.Errorf(
		"prefix %q matches multiple schemas — use a longer prefix (matches: %s)",
		prefix, formatSchemaList(schemas),
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
func ResolveAPIToken(flagValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}

	if configPath, err := FindConfigFile(); err == nil {
		if cfg, err := LoadConfig(configPath); err == nil && cfg.APIToken != "" {
			return cfg.APIToken, nil
		}
	}

	homeDir, err := os.UserHomeDir()
	if err == nil {
		globalCfg, cfgErr := LoadConfig(filepath.Join(homeDir, ".seedmancer", "config.yaml"))
		if cfgErr == nil && globalCfg.APIToken != "" {
			return globalCfg.APIToken, nil
		}
	}

	return "", fmt.Errorf(
		"API token required.\n" +
			"  Use --token flag or set SEEDMANCER_API_TOKEN environment variable.\n" +
			"  Get your token at: https://seedmancer.dev/dashboard/settings",
	)
}
