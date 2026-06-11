package scenario

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// revisionPattern matches the canonical revision id format. Three
// digits is enough for the foreseeable future and keeps the lexical
// sort working — when we eventually overflow we can just widen the
// padding without breaking anything.
var revisionPattern = regexp.MustCompile(`^r(\d+)$`)

// FormatRevisionID renders a numeric revision number as the canonical
// "rNNN" string.
func FormatRevisionID(n int) string {
	return fmt.Sprintf("r%03d", n)
}

// ParseRevisionID extracts the numeric portion of a "rNNN" id. Returns
// (-1, false) when the id doesn't match the canonical format.
func ParseRevisionID(id string) (int, bool) {
	m := revisionPattern.FindStringSubmatch(id)
	if m == nil {
		return -1, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return -1, false
	}
	return n, true
}

// RevisionInfo is one entry in ListRevisions: the directory name plus
// its mtime so callers can render "created at" without re-reading the
// manifest. Manifests carry the authoritative timestamp; ModTime is
// here only as a fallback for corrupted manifests.
type RevisionInfo struct {
	ID      string
	Number  int
	ModTime time.Time
}

// ListRevisions scans <scenarioDir>/revisions for "rNNN" folders and
// returns them sorted by revision number, ascending. Missing
// directories are not an error (returns an empty slice).
func ListRevisions(scenarioDir string) ([]RevisionInfo, error) {
	root := filepath.Join(scenarioDir, "revisions")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading %s: %w", root, err)
	}
	var revs []RevisionInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		n, ok := ParseRevisionID(e.Name())
		if !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", filepath.Join(root, e.Name()), err)
		}
		revs = append(revs, RevisionInfo{
			ID:      e.Name(),
			Number:  n,
			ModTime: info.ModTime(),
		})
	}
	sort.Slice(revs, func(i, j int) bool { return revs[i].Number < revs[j].Number })
	return revs, nil
}

// NextRevisionID picks the next "rNNN" id by scanning existing
// revisions and incrementing the highest. Empty / missing directory →
// "r001".
func NextRevisionID(scenarioDir string) (string, error) {
	revs, err := ListRevisions(scenarioDir)
	if err != nil {
		return "", err
	}
	max := 0
	for _, r := range revs {
		if r.Number > max {
			max = r.Number
		}
	}
	return FormatRevisionID(max + 1), nil
}

// WalkScenarios returns every scenario rooted under <storagePath>/
// scenarios that has a manifest.json. Scenario paths use forward
// slashes regardless of the host OS so they round-trip cleanly through
// the rest of the codebase.
//
// Errors reading individual subtrees are logged into the returned
// errors map (keyed by scenario path) instead of aborting the whole
// walk — a corrupt manifest in one scenario should not hide the rest.
func WalkScenarios(projectRoot, storagePath string) (paths []string, badManifests map[string]error, err error) {
	root := ScenariosRoot(projectRoot, storagePath)
	if _, statErr := os.Stat(root); os.IsNotExist(statErr) {
		return nil, nil, nil
	}
	badManifests = map[string]error{}
	walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() {
			return nil
		}
		// Skip the revisions/ subtrees — manifests inside them belong to
		// individual revisions, not scenarios.
		if filepath.Base(path) == "revisions" {
			return filepath.SkipDir
		}
		manifestPath := filepath.Join(path, manifestName)
		if _, statErr := os.Stat(manifestPath); statErr != nil {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			badManifests[path] = relErr
			return nil
		}
		// Use forward slashes for the user-facing path even on Windows.
		scenarioPath := filepath.ToSlash(rel)
		if scenarioPath == "." || scenarioPath == "" {
			return nil
		}
		// Validate that the parsed manifest actually loads — corrupt
		// manifests get reported but don't kill the walk.
		if _, mErr := ReadManifest(path); mErr != nil {
			badManifests[scenarioPath] = mErr
			return nil
		}
		paths = append(paths, scenarioPath)
		return nil
	})
	if walkErr != nil {
		return nil, badManifests, walkErr
	}
	sort.Strings(paths)
	return paths, badManifests, nil
}

// SchemaStoreDir returns the content-addressed schema folder for a
// fingerprint. This is the same folder shape the legacy CLI used for
// storing schema.json + sql sidecars — we just no longer hang
// `datasets/` off it.
func SchemaStoreDir(projectRoot, storagePath, fpShort string) string {
	return filepath.Join(projectRoot, storagePath, "schemas", fpShort)
}

// SchemaJSONPath is the canonical location of a schema's schema.json.
func SchemaJSONPath(projectRoot, storagePath, fpShort string) string {
	return filepath.Join(SchemaStoreDir(projectRoot, storagePath, fpShort), "schema.json")
}