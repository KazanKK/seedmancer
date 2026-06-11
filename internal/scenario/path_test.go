package scenario

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalize_valid(t *testing.T) {
	cases := map[string]string{
		"basic":                   "basic",
		"auth/success":            "auth/success",
		"billing/pro":             "billing/pro",
		"checkout/payment/failed": "checkout/payment/failed",
		"  basic  ":               "basic",
		"billing//pro":            "billing/pro",
		"billing///pro":           "billing/pro",
		"a.b":                     "a.b",
		"a-b":                     "a-b",
		"a_b/c-1":                 "a_b/c-1",
	}
	for in, want := range cases {
		got, err := Normalize(in)
		if err != nil {
			t.Errorf("Normalize(%q) error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalize_invalid(t *testing.T) {
	cases := []struct {
		in      string
		wantSub string
	}{
		{"", "required"},
		{"   ", "required"},
		{"/auth/success", "must not start with /"},
		{"./basic", "must not start with ./"},
		{"../secret", `".."`},
		{"auth/../secret", `".."`},
		{"auth/.", `"."`},
		{"basic/", "must not end with /"},
		{"foo bar", "invalid segment"},
		{"foo|bar", "invalid segment"},
		{"foo:bar", "invalid segment"},
		{"foo*bar", "invalid segment"},
		{`a\b`, "invalid segment"},
	}
	for _, tc := range cases {
		got, err := Normalize(tc.in)
		if err == nil {
			t.Errorf("Normalize(%q) = %q, want error containing %q", tc.in, got, tc.wantSub)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantSub) {
			t.Errorf("Normalize(%q) error %q missing %q", tc.in, err.Error(), tc.wantSub)
		}
	}
}

func TestNormalize_trailingSlashOnly(t *testing.T) {
	// Trailing slashes are typos we should surface, not silently strip.
	if _, err := Normalize("basic/"); err == nil {
		t.Fatal("expected error for trailing slash only")
	}
}

func TestSegments(t *testing.T) {
	got := Segments("billing//pro")
	want := []string{"billing", "pro"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Segments = %v, want %v", got, want)
	}
	if Segments("") != nil {
		t.Fatal("Segments of invalid path should be nil")
	}
}

func TestScenarioDir(t *testing.T) {
	got := ScenarioDir("/proj", ".seedmancer", "billing/pro")
	want := filepath.Join("/proj", ".seedmancer", "scenarios", "billing", "pro")
	if got != want {
		t.Fatalf("ScenarioDir = %q, want %q", got, want)
	}
}

func TestRevisionDataDir(t *testing.T) {
	got := RevisionDataDir("/proj", ".seedmancer", "auth/success", "r002")
	want := filepath.Join(
		"/proj", ".seedmancer", "scenarios", "auth", "success",
		"revisions", "r002", "data",
	)
	if got != want {
		t.Fatalf("RevisionDataDir = %q, want %q", got, want)
	}
}
