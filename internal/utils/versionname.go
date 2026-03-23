package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Timestamped version directories use: YYYYMMDDHHMMSS_(database_name)
var timestampedVersionPattern = regexp.MustCompile(`^\d{14}_\(`)

// DefaultVersionName is used when export omits --version-name (UTC timestamp + database label).
func DefaultVersionName(databaseName string) string {
	ts := time.Now().UTC().Format("20060102150405")
	safe := SanitizeVersionSegment(databaseName)
	return fmt.Sprintf("%s_(%s)", ts, safe)
}

// SanitizeVersionSegment makes databaseName safe inside a directory name (no path separators or parens).
func SanitizeVersionSegment(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\':
			return '_'
		case ':', '*', '?', '"', '<', '>', '|', '(', ')':
			return '_'
		case 0:
			return -1
		default:
			return r
		}
	}, s)
	if s == "" {
		return "database"
	}
	return s
}

// ResolveSeedVersion returns the version directory when seed omits --version-name, or validates an explicit name.
func ResolveSeedVersion(projectRoot, storagePath, databaseName, versionName string) (resolved string, dir string, err error) {
	base := filepath.Join(projectRoot, storagePath, "databases", databaseName)
	v := strings.TrimSpace(versionName)
	if v != "" {
		dir = filepath.Join(base, v)
		st, err := os.Stat(dir)
		if err != nil {
			if os.IsNotExist(err) {
				return "", "", fmt.Errorf("local test data not found for version %q", v)
			}
			return "", "", fmt.Errorf("checking version directory: %v", err)
		}
		if !st.IsDir() {
			return "", "", fmt.Errorf("version path exists but is not a directory: %s", v)
		}
		return v, dir, nil
	}

	entries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return "", "", fmt.Errorf("no local test data for database %q", databaseName)
		}
		return "", "", fmt.Errorf("reading database directory: %v", err)
	}

	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}
	if len(dirs) == 0 {
		return "", "", fmt.Errorf("no local test data for database %q", databaseName)
	}

	var tsDirs []string
	for _, name := range dirs {
		if timestampedVersionPattern.MatchString(name) {
			tsDirs = append(tsDirs, name)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(tsDirs)))
	if len(tsDirs) > 0 {
		resolved = tsDirs[0]
		return resolved, filepath.Join(base, resolved), nil
	}

	for _, name := range dirs {
		if name == "unversioned" {
			return "unversioned", filepath.Join(base, name), nil
		}
	}

	if len(dirs) == 1 {
		resolved = dirs[0]
		return resolved, filepath.Join(base, resolved), nil
	}

	return "", "", fmt.Errorf("multiple test data versions exist for database %q; specify --version-name (choices: %s)",
		databaseName, strings.Join(dirs, ", "))
}
