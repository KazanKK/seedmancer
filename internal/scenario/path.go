// Package scenario implements the user-facing "scenario" model:
// a slash-separated path that names a reusable test-data state, plus
// the immutable rNNN revisions and pointers that live underneath it on
// disk.
//
// The on-disk layout is:
//
//	<storagePath>/scenarios/<scenario>/
//	  manifest.json
//	  pointers.json
//	  revisions/<revID>/
//	    manifest.json
//	    data/<table>.csv ...
//
// Schema sidecars (schema.json + *_func.sql / *_trigger.sql) live in a
// separate, content-addressed folder so multiple revisions and scenarios
// that share a schema fingerprint reuse the same files:
//
//	<storagePath>/schemas/<fp-short>/schema.json
package scenario

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// segmentPattern is the validation regex applied to each path segment.
// Letters, digits, dot, underscore, and hyphen are allowed — the same
// alphabet a typical test name uses, plus dots so things like
// `auth.success` work for users who prefer dot notation in a single
// segment. Slashes are not allowed inside a segment because that's how
// the scenario path is composed.
var segmentPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// Normalize validates a user-supplied scenario path and returns its
// canonical form. The canonical form has no leading or trailing slashes
// and no repeated separators.
//
// Returns an error for:
//   - empty input
//   - leading "/" (absolute path)
//   - "." or ".." segments (path traversal)
//   - segments that contain unsupported characters
func Normalize(input string) (string, error) {
	s := strings.TrimSpace(input)
	if s == "" {
		return "", fmt.Errorf("scenario path is required")
	}
	if strings.HasPrefix(s, "/") {
		return "", fmt.Errorf("scenario path %q must not start with /", input)
	}
	// Reject explicit "./" prefix; a bare "." segment is caught below.
	if strings.HasPrefix(s, "./") {
		return "", fmt.Errorf("scenario path %q must not start with ./", input)
	}
	// Reject trailing slashes — `auth/` is a typo we shouldn't accept.
	// Internal repeated slashes still collapse silently below.
	if strings.HasSuffix(s, "/") {
		return "", fmt.Errorf("scenario path %q must not end with /", input)
	}

	parts := strings.Split(s, "/")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			// Repeated slashes collapse silently — `billing//pro` → `billing/pro`.
			continue
		}
		if p == "." || p == ".." {
			return "", fmt.Errorf("scenario path %q must not contain %q segments", input, p)
		}
		if !segmentPattern.MatchString(p) {
			return "", fmt.Errorf(
				"scenario path %q has invalid segment %q (allowed: letters, digits, '.', '_', '-')",
				input, p,
			)
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return "", fmt.Errorf("scenario path %q has no segments after normalization", input)
	}
	return strings.Join(out, "/"), nil
}

// Segments returns the slash-separated parts of a scenario path. The
// input is normalized first so callers can pass raw user input.
func Segments(scenario string) []string {
	norm, err := Normalize(scenario)
	if err != nil {
		return nil
	}
	return strings.Split(norm, "/")
}

// ScenariosRoot returns the top-level scenarios directory inside a
// project's storagePath.
func ScenariosRoot(projectRoot, storagePath string) string {
	return filepath.Join(projectRoot, storagePath, "scenarios")
}

// ScenarioDir returns the on-disk directory for a single scenario. It
// joins the normalized segments with the OS-native separator so the
// layout works on Windows too.
func ScenarioDir(projectRoot, storagePath, scenario string) string {
	segs := Segments(scenario)
	parts := append([]string{ScenariosRoot(projectRoot, storagePath)}, segs...)
	return filepath.Join(parts...)
}

// RevisionsDir returns the directory that holds every revision of a
// scenario.
func RevisionsDir(projectRoot, storagePath, scenario string) string {
	return filepath.Join(ScenarioDir(projectRoot, storagePath, scenario), "revisions")
}

// RevisionDir returns the directory for a single revision. revID is the
// canonical "rNNN" id; it's returned by NextRevisionID.
func RevisionDir(projectRoot, storagePath, scenario, revID string) string {
	return filepath.Join(RevisionsDir(projectRoot, storagePath, scenario), revID)
}

// RevisionDataDir returns the data/ folder inside a revision where CSVs
// (and service sidecars) live.
func RevisionDataDir(projectRoot, storagePath, scenario, revID string) string {
	return filepath.Join(RevisionDir(projectRoot, storagePath, scenario, revID), "data")
}
