package cmd

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	db "github.com/KazanKK/seedmancer/database"
	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/utils"
)

// resolvedRevision describes the revision a command picked, after the
// --revision / latest precedence rules. It carries the
// loaded manifest so callers don't need a second read.
type resolvedRevision struct {
	Scenario string
	RevID    string
	RevDir   string
	DataDir  string
	Manifest scenario.RevisionManifest
}

// resolveScenarioRevision picks one revision for a scenario. Precedence:
//  1. explicit revID (--revision)
//  2. manifest.latest
//
// Errors are user-friendly so they can be surfaced verbatim by the CLI.
func resolveScenarioRevision(projectRoot, storagePath, scenarioPath, revID string) (resolvedRevision, error) {
	scenarioDir := scenario.ScenarioDir(projectRoot, storagePath, scenarioPath)
	if _, err := os.Stat(scenarioDir); os.IsNotExist(err) {
		return resolvedRevision{}, fmt.Errorf("scenario %q does not exist — run `seedmancer export %s` first", scenarioPath, scenarioPath)
	}

	manifest, _ := scenario.ReadManifest(scenarioDir)

	target := strings.TrimSpace(revID)
	if target == "" {
		if manifest.Latest == "" {
			return resolvedRevision{}, fmt.Errorf(
				"scenario %q has no revisions yet — run `seedmancer export %s` first",
				scenarioPath, scenarioPath,
			)
		}
		target = manifest.Latest
	}

	revDir := scenario.RevisionDir(projectRoot, storagePath, scenarioPath, target)
	if st, err := os.Stat(revDir); err != nil || !st.IsDir() {
		return resolvedRevision{}, fmt.Errorf(
			"scenario %q has no revision %q (looked in %s)",
			scenarioPath, target, revDir,
		)
	}

	revManifest, err := scenario.ReadRevisionManifest(revDir)
	if err != nil {
		return resolvedRevision{}, fmt.Errorf("reading revision manifest: %w", err)
	}

	return resolvedRevision{
		Scenario: scenarioPath,
		RevID:    target,
		RevDir:   revDir,
		DataDir:  filepath.Join(revDir, "data"),
		Manifest: revManifest,
	}, nil
}

// fingerprintCurrentDB connects to target, dumps the schema to a temp
// dir, and returns its fingerprint along with the raw schema.json bytes.
// The temp dir is cleaned up before return so callers don't have to.
func fingerprintCurrentDB(target utils.NamedEnv) (fingerprint string, schemaJSON []byte, err error) {
	manager, normalizedURL, err := db.NewManager(target.DatabaseURL)
	if err != nil {
		return "", nil, err
	}
	if err := manager.ConnectWithDSN(normalizedURL); err != nil {
		return "", nil, fmt.Errorf("connecting to database: %v", err)
	}
	tmp, err := os.MkdirTemp("", "seedmancer-schema-*")
	if err != nil {
		return "", nil, fmt.Errorf("creating temp directory: %v", err)
	}
	defer os.RemoveAll(tmp)

	if err := manager.ExportSchema(tmp); err != nil {
		return "", nil, fmt.Errorf("exporting schema: %v", err)
	}
	schemaPath := filepath.Join(tmp, "schema.json")
	fp, err := utils.FingerprintSchemaFile(schemaPath)
	if err != nil {
		return "", nil, fmt.Errorf("fingerprinting schema: %w", err)
	}
	raw, err := os.ReadFile(schemaPath)
	if err != nil {
		return "", nil, fmt.Errorf("reading temp schema.json: %v", err)
	}
	return fp, raw, nil
}

// listCSVTablesAndRowCounts walks dataDir, returns the sorted list of
// tables (.csv basename) and their data-row counts (header excluded).
// Files larger than the row scan threshold report rowCount=-1 so callers
// can decide how to display.
func listCSVTablesAndRowCounts(dataDir string) (tables []string, rowCounts map[string]int, err error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, nil, fmt.Errorf("reading %s: %w", dataDir, err)
	}
	rowCounts = map[string]int{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".csv") {
			continue
		}
		table := name[:len(name)-len(".csv")]
		count, err := countCSVDataRows(filepath.Join(dataDir, name))
		if err != nil {
			return nil, nil, fmt.Errorf("counting rows in %s: %w", name, err)
		}
		tables = append(tables, table)
		rowCounts[table] = count
	}
	sort.Strings(tables)
	return tables, rowCounts, nil
}

// countCSVDataRows returns the number of data rows (excluding the
// header) in a CSV file. Uses csv.Reader so quoted multi-line cells
// don't get miscounted.
func countCSVDataRows(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReader(f))
	r.FieldsPerRecord = -1 // tolerate ragged rows from hand-edited CSVs
	r.LazyQuotes = true
	count := -1
	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	if count < 0 {
		count = 0 // empty file → 0 data rows
	}
	return count, nil
}

// servicesForRevision returns the sorted list of services recorded on a revision manifest.
func servicesForRevision(_ utils.Config, _ string) []string {
	return []string{"postgres"}
}

// formatExportTime renders a manifest timestamp as the human-readable
// "YYYY-MM-DD HH:MM" form used by `list` and `history`.
func formatExportTime(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return t.UTC().Format("2006-01-02 15:04")
}

// pointerLabel returns "latest" when revID matches the latest pointer, or "".
func pointerLabel(revID, latest string) string {
	if revID == latest {
		return "latest"
	}
	return ""
}

// fileExists reports whether path resolves to an existing, regular file.
// Returns false for directories, broken symlinks, and any stat error.
func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// liftDatasetSQL moves a dataset.sql file (if present) out of dataDir
// and into revDir, where get_dataset_sql expects it. Push bundles the
// SQL into the zip flat alongside the CSVs; pull lifts it one level up
// so the on-disk layout matches what generate_dataset_local produces.
// No-op when the file isn't in the zip.
func liftDatasetSQL(dataDir, revDir string) error {
	src := filepath.Join(dataDir, utils.DatasetSQLFileName)
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	dst := filepath.Join(revDir, utils.DatasetSQLFileName)
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	// Cross-device rename can fail on some filesystems; copy then delete.
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Remove(src)
}
