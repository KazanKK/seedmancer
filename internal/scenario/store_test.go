package scenario

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNextRevisionID_empty(t *testing.T) {
	dir := t.TempDir()
	got, err := NextRevisionID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != "r001" {
		t.Fatalf("got %q, want r001", got)
	}
}

func TestNextRevisionID_existing(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"r001", "r002"} {
		if err := os.MkdirAll(filepath.Join(dir, "revisions", name), 0755); err != nil {
			t.Fatal(err)
		}
	}
	got, err := NextRevisionID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != "r003" {
		t.Fatalf("got %q, want r003", got)
	}
}

func TestNextRevisionID_skipsNonRevDirs(t *testing.T) {
	dir := t.TempDir()
	// Mix of valid and invalid revision dirs — only `r*` should count.
	for _, name := range []string{"r001", "r010", "draft", "rNotANumber"} {
		if err := os.MkdirAll(filepath.Join(dir, "revisions", name), 0755); err != nil {
			t.Fatal(err)
		}
	}
	got, err := NextRevisionID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != "r011" {
		t.Fatalf("got %q, want r011", got)
	}
}

func TestParseRevisionID(t *testing.T) {
	cases := map[string]struct {
		n  int
		ok bool
	}{
		"r001":  {1, true},
		"r042":  {42, true},
		"r1000": {1000, true},
		"R001":  {-1, false},
		"r":     {-1, false},
		"rfoo":  {-1, false},
		"":      {-1, false},
	}
	for in, want := range cases {
		gotN, gotOK := ParseRevisionID(in)
		if gotOK != want.ok || gotN != want.n {
			t.Errorf("ParseRevisionID(%q) = (%d, %v), want (%d, %v)", in, gotN, gotOK, want.n, want.ok)
		}
	}
}

func TestListRevisions_orderAndMtime(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"r002", "r001", "r010"} {
		if err := os.MkdirAll(filepath.Join(dir, "revisions", name), 0755); err != nil {
			t.Fatal(err)
		}
	}
	revs, err := ListRevisions(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(revs) != 3 {
		t.Fatalf("expected 3 revisions, got %d", len(revs))
	}
	want := []string{"r001", "r002", "r010"}
	for i, r := range revs {
		if r.ID != want[i] {
			t.Errorf("revs[%d] = %q, want %q", i, r.ID, want[i])
		}
		if r.ModTime.IsZero() {
			t.Errorf("revs[%d] has zero ModTime", i)
		}
		if time.Since(r.ModTime) > time.Minute {
			t.Errorf("revs[%d] ModTime is suspicious: %v", i, r.ModTime)
		}
	}
}

func TestWalkScenarios_nested(t *testing.T) {
	root := t.TempDir()
	storage := ".seedmancer"
	scenariosRoot := ScenariosRoot(root, storage)
	for _, p := range []string{
		"basic",
		"auth/success",
		"billing/pro",
		"checkout/payment/failed",
	} {
		dir := filepath.Join(append([]string{scenariosRoot}, filepath.SplitList(filepath.FromSlash(p))...)...)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := WriteManifest(dir, Manifest{Scenario: p}); err != nil {
			t.Fatal(err)
		}
	}
	paths, bad, err := WalkScenarios(root, storage)
	if err != nil {
		t.Fatal(err)
	}
	if len(bad) != 0 {
		t.Fatalf("unexpected bad manifests: %v", bad)
	}
	want := []string{
		"auth/success",
		"basic",
		"billing/pro",
		"checkout/payment/failed",
	}
	if len(paths) != len(want) {
		t.Fatalf("got %v, want %v", paths, want)
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Fatalf("paths[%d] = %q, want %q", i, paths[i], want[i])
		}
	}
}

func TestWalkScenarios_corruptManifestReported(t *testing.T) {
	root := t.TempDir()
	storage := ".seedmancer"
	dir := filepath.Join(ScenariosRoot(root, storage), "broken")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte("{not json"), 0644); err != nil {
		t.Fatal(err)
	}
	paths, bad, err := WalkScenarios(root, storage)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Fatalf("expected no good scenarios, got %v", paths)
	}
	if _, ok := bad["broken"]; !ok {
		t.Fatalf("expected broken manifest to be reported, got %v", bad)
	}
}

func TestWalkScenarios_missingRoot(t *testing.T) {
	root := t.TempDir()
	paths, bad, err := WalkScenarios(root, ".seedmancer")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 || len(bad) != 0 {
		t.Fatalf("expected no scenarios from missing root, got paths=%v bad=%v", paths, bad)
	}
}

func TestScenarioFromDir(t *testing.T) {
	root := "/proj"
	storage := ".seedmancer"
	got := ScenarioFromDir(root, storage, ScenarioDir(root, storage, "billing/pro"))
	if got != "billing/pro" {
		t.Fatalf("got %q, want billing/pro", got)
	}
	if got := ScenarioFromDir(root, storage, "/elsewhere"); got != "" {
		t.Fatalf("expected empty for outside dir, got %q", got)
	}
}
