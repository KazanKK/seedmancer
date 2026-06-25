package envmarker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── IsMarker ──────────────────────────────────────────────────────────────────

func TestIsMarker(t *testing.T) {
	valid := []string{
		"@env:FIXED_USER_ID",
		"@env:ADMIN_USER_ID",
		"@env:ORG_ID_1",
		"@env:A",
		"@env:KEY123",
	}
	for _, s := range valid {
		if !IsMarker(s) {
			t.Errorf("IsMarker(%q) = false, want true", s)
		}
	}

	invalid := []string{
		"",
		"normal-value",
		"user-@env:ID",         // partial — not supported
		"@env:lowercase_key",   // lowercase not allowed
		"@env:",                // empty key
		"@ENV:KEY",             // wrong prefix case
		"@env:KEY with space",
	}
	for _, s := range invalid {
		if IsMarker(s) {
			t.Errorf("IsMarker(%q) = true, want false", s)
		}
	}
}

// ── ResolveValue ─────────────────────────────────────────────────────────────

func TestResolveValue_HitsYamlValues(t *testing.T) {
	values := EnvironmentValues{"FIXED_USER_ID": "11111111-1111-1111-1111-111111111111"}
	got, replaced, err := ResolveValue("@env:FIXED_USER_ID", values, "local", "users.csv", 1, "id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !replaced {
		t.Error("replaced = false, want true")
	}
	if got != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("got %q, want UUIDv4 value", got)
	}
}

func TestResolveValue_FallsBackToOsEnv(t *testing.T) {
	t.Setenv("FALLBACK_KEY", "env-var-value")
	values := EnvironmentValues{} // key absent from yaml

	got, replaced, err := ResolveValue("@env:FALLBACK_KEY", values, "local", "users.csv", 1, "id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !replaced {
		t.Error("replaced = false, want true")
	}
	if got != "env-var-value" {
		t.Errorf("got %q, want env-var-value", got)
	}
}

func TestResolveValue_YamlWinsOverOsEnv(t *testing.T) {
	t.Setenv("PRIORITY_KEY", "from-os-env")
	values := EnvironmentValues{"PRIORITY_KEY": "from-yaml"}

	got, replaced, err := ResolveValue("@env:PRIORITY_KEY", values, "local", "f.csv", 1, "col")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !replaced {
		t.Error("replaced = false, want true")
	}
	if got != "from-yaml" {
		t.Errorf("got %q, want from-yaml", got)
	}
}

func TestResolveValue_NoMarker(t *testing.T) {
	values := EnvironmentValues{"KEY": "val"}
	got, replaced, err := ResolveValue("plain-value", values, "local", "f.csv", 1, "col")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if replaced {
		t.Error("replaced = true, want false")
	}
	if got != "plain-value" {
		t.Errorf("got %q, want plain-value", got)
	}
}

func TestResolveValue_PartialMarkerIgnored(t *testing.T) {
	values := EnvironmentValues{"ID": "some-uuid"}
	// Partial markers are not supported — treated as plain strings.
	got, replaced, err := ResolveValue("user-@env:ID", values, "local", "f.csv", 1, "col")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if replaced {
		t.Error("replaced = true, want false for partial marker")
	}
	if got != "user-@env:ID" {
		t.Errorf("got %q, want original string", got)
	}
}

func TestResolveValue_MissingKeyErrors(t *testing.T) {
	values := EnvironmentValues{}
	os.Unsetenv("MISSING_KEY")

	_, _, err := ResolveValue("@env:MISSING_KEY", values, "staging", "users.csv", 2, "id")
	if err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
	msg := err.Error()
	for _, want := range []string{"MISSING_KEY", "staging", "users.csv", "id"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q:\n%s", want, msg)
		}
	}
	// Should hint both resolution paths
	if !strings.Contains(msg, "values:") {
		t.Error("error message should hint seedmancer.yaml values section")
	}
	if !strings.Contains(msg, "export") {
		t.Error("error message should hint export env var")
	}
}

// ── HasAnyMarker ─────────────────────────────────────────────────────────────

func TestHasAnyMarker(t *testing.T) {
	withMarker := [][]string{
		{"id", "email"},
		{"@env:FIXED_USER_ID", "test@example.com"},
	}
	if !HasAnyMarker(withMarker) {
		t.Error("HasAnyMarker = false, want true")
	}

	noMarker := [][]string{
		{"id", "email"},
		{"plain-id", "test@example.com"},
	}
	if HasAnyMarker(noMarker) {
		t.Error("HasAnyMarker = true, want false")
	}
}

// ── ResolveRecords ────────────────────────────────────────────────────────────

func TestResolveRecords_MultipleColumnsAndRows(t *testing.T) {
	records := [][]string{
		{"id", "org_id", "email"},
		{"@env:USER_ID", "@env:ORG_ID", "test@example.com"},
		{"@env:USER_ID", "@env:ORG_ID", "other@example.com"},
	}
	values := EnvironmentValues{
		"USER_ID": "uid-1",
		"ORG_ID":  "org-1",
	}

	out, markers, err := ResolveRecords(records, values, "local", "users.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(markers) != 4 {
		t.Errorf("got %d resolved markers, want 4", len(markers))
	}
	// Header unchanged
	if out[0][0] != "id" {
		t.Error("header row was modified")
	}
	// Data rows resolved
	if out[1][0] != "uid-1" {
		t.Errorf("row 1 col id = %q, want uid-1", out[1][0])
	}
	if out[2][1] != "org-1" {
		t.Errorf("row 2 col org_id = %q, want org-1", out[2][1])
	}
	// Non-marker cell unchanged
	if out[1][2] != "test@example.com" {
		t.Errorf("non-marker cell was modified: %q", out[1][2])
	}
}

func TestResolveRecords_SameMarkerReusedMultipleTimes(t *testing.T) {
	records := [][]string{
		{"a", "b", "c"},
		{"@env:KEY", "@env:KEY", "@env:KEY"},
	}
	values := EnvironmentValues{"KEY": "resolved"}

	out, markers, err := ResolveRecords(records, values, "local", "f.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(markers) != 3 {
		t.Errorf("got %d resolved markers, want 3", len(markers))
	}
	for j, cell := range out[1] {
		if cell != "resolved" {
			t.Errorf("col %d = %q, want resolved", j, cell)
		}
	}
}

func TestResolveRecords_OriginalSliceUnchanged(t *testing.T) {
	records := [][]string{
		{"id"},
		{"@env:MY_KEY"},
	}
	original := "@env:MY_KEY"
	values := EnvironmentValues{"MY_KEY": "new-value"}

	_, _, err := ResolveRecords(records, values, "local", "f.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if records[1][0] != original {
		t.Errorf("original record was mutated: got %q", records[1][0])
	}
}

func TestResolveRecords_MissingKeyPropagatesError(t *testing.T) {
	records := [][]string{
		{"id"},
		{"@env:NO_SUCH_KEY"},
	}
	os.Unsetenv("NO_SUCH_KEY")

	_, _, err := ResolveRecords(records, EnvironmentValues{}, "staging", "users.csv")
	if err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
}

// ── ResolveCSVFile + WriteCSV ─────────────────────────────────────────────────

func TestResolveCSVFile_OriginalFileUnchanged(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "users.csv")

	original := "id,email\n@env:USER_ID,test@example.com\n"
	if err := os.WriteFile(csvPath, []byte(original), 0644); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}

	values := EnvironmentValues{"USER_ID": "resolved-uuid"}
	_, _, err := ResolveCSVFile(csvPath, values, "local")
	if err != nil {
		t.Fatalf("ResolveCSVFile error: %v", err)
	}

	got, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("reading file: %v", err)
	}
	if string(got) != original {
		t.Errorf("original file was modified:\ngot:  %q\nwant: %q", string(got), original)
	}
}

func TestResolveCSVFile_TwoEnvsDifferentValues(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "users.csv")
	if err := os.WriteFile(csvPath, []byte("id\n@env:UID\n"), 0644); err != nil {
		t.Fatal(err)
	}

	localValues := EnvironmentValues{"UID": "local-uid"}
	stagingValues := EnvironmentValues{"UID": "staging-uid"}

	localRecords, _, err := ResolveCSVFile(csvPath, localValues, "local")
	if err != nil {
		t.Fatalf("local resolve error: %v", err)
	}
	stagingRecords, _, err := ResolveCSVFile(csvPath, stagingValues, "staging")
	if err != nil {
		t.Fatalf("staging resolve error: %v", err)
	}

	if localRecords[1][0] != "local-uid" {
		t.Errorf("local = %q, want local-uid", localRecords[1][0])
	}
	if stagingRecords[1][0] != "staging-uid" {
		t.Errorf("staging = %q, want staging-uid", stagingRecords[1][0])
	}
}
