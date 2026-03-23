package utils

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestSanitizeVersionSegment(t *testing.T) {
	if got := SanitizeVersionSegment("my/db"); got != "my_db" {
		t.Fatalf("got %q", got)
	}
	if got := SanitizeVersionSegment("a(b)"); got != "a(b)" {
		t.Fatalf("got %q", got)
	}
	if got := SanitizeVersionSegment("   "); got != "database" {
		t.Fatalf("got %q", got)
	}
}

func TestDefaultVersionName_shape(t *testing.T) {
	n := DefaultVersionName("myapp")
	re := regexp.MustCompile(`^\d{14}_myapp$`)
	if !re.MatchString(n) {
		t.Fatalf("expected YYYYMMDDHHMMSS_myapp, got %q", n)
	}
}

func TestResolveSeedVersion_explicit(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "data", "databases", "db1", "v1")
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatal(err)
	}
	got, p, err := ResolveSeedVersion(root, "data", "db1", "v1")
	if err != nil || got != "v1" || p != base {
		t.Fatalf("got %q %q %v", got, p, err)
	}
}

func TestResolveSeedVersion_latestTimestamped(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "s", "databases", "db1")
	_ = os.MkdirAll(filepath.Join(data, "20250101120000_db1"), 0755)
	_ = os.MkdirAll(filepath.Join(data, "20250201120000_db1"), 0755)
	got, _, err := ResolveSeedVersion(root, "s", "db1", "")
	if err != nil || got != "20250201120000_db1" {
		t.Fatalf("got %q %v", got, err)
	}
}

func TestResolveSeedVersion_unversionedFallback(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "s", "databases", "db1")
	_ = os.MkdirAll(filepath.Join(data, "unversioned"), 0755)
	got, _, err := ResolveSeedVersion(root, "s", "db1", "")
	if err != nil || got != "unversioned" {
		t.Fatalf("got %q %v", got, err)
	}
}
