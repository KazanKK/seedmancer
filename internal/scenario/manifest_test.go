package scenario

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC)
	in := Manifest{
		Scenario:  "billing/pro",
		CreatedAt: now,
		UpdatedAt: now,
		Latest:    "r003",
	}
	if err := WriteManifest(dir, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if out.Scenario != "billing/pro" || out.Latest != "r003" {
		t.Fatalf("unexpected manifest: %+v", out)
	}
	if !out.CreatedAt.Equal(now) || !out.UpdatedAt.Equal(now) {
		t.Fatalf("timestamps not preserved: %+v", out)
	}
}

func TestRevisionManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	in := RevisionManifest{
		Scenario:          "billing/pro",
		Revision:          "r003",
		SchemaFingerprint: "fp_ab12cd34ef56",
		CreatedAt:         time.Date(2026, 4, 29, 0, 0, 0, 0, time.UTC),
		Source:            "export",
		Tables:            []string{"User", "Plan"},
		Services:          []string{"postgres"},
		RowCounts:         map[string]int{"User": 20, "Plan": 3},
		Description:       "billing pro test",
	}
	if err := WriteRevisionManifest(dir, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadRevisionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if out.Scenario != in.Scenario || out.Revision != in.Revision ||
		out.SchemaFingerprint != in.SchemaFingerprint || out.Source != in.Source {
		t.Fatalf("unexpected revision manifest: %+v", out)
	}
	if len(out.Tables) != 2 || out.Tables[0] != "User" {
		t.Fatalf("tables not preserved: %+v", out.Tables)
	}
	if out.RowCounts["User"] != 20 || out.RowCounts["Plan"] != 3 {
		t.Fatalf("rowCounts not preserved: %+v", out.RowCounts)
	}
}

func TestReadManifest_missingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadManifest(dir)
	if err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected os.IsNotExist, got %v", err)
	}
}

func TestWriteManifest_atomic(t *testing.T) {
	dir := t.TempDir()
	if err := WriteManifest(dir, Manifest{Scenario: "basic"}); err != nil {
		t.Fatal(err)
	}
	// No leftover .tmp file.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("found leftover temp file: %s", e.Name())
		}
	}
}
