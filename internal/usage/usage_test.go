package usage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeEvent(t *testing.T, dir string, name string, ev Event) {
	t.Helper()
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
		t.Fatalf("write event: %v", err)
	}
}

func TestLoadMissingEventsDirIsEmpty(t *testing.T) {
	root := t.TempDir()
	agg, err := Load(root, ".seedmancer")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(agg.States) != 0 {
		t.Fatalf("expected no states, got %d", len(agg.States))
	}
}

func TestLoadAggregatesAndDedupes(t *testing.T) {
	root := t.TempDir()
	dir := EventsDir(root, ".seedmancer")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	t0 := time.Date(2026, 6, 29, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	// Same file+title+project for login-success seen twice — newest wins.
	writeEvent(t, dir, "a.json", Event{
		State: "auth/login-success", File: "tests/login.spec.ts",
		Title: "user can login", Project: "chromium", ResetMode: "beforeEach", LastSeenAt: t0,
	})
	writeEvent(t, dir, "b.json", Event{
		State: "auth/login-success", File: "tests/login.spec.ts",
		Title: "user can login", Project: "chromium", ResetMode: "beforeEach", LastSeenAt: t1,
	})
	// A different test for the same state.
	writeEvent(t, dir, "c.json", Event{
		State: "auth/login-success", File: "tests/session.spec.ts",
		Title: "session persists", Project: "chromium", LastSeenAt: t0,
	})
	// A different state.
	writeEvent(t, dir, "d.json", Event{
		State: "billing/pro-user", File: "tests/billing.spec.ts",
		Title: "pro can checkout", Project: "chromium", LastSeenAt: t0,
	})
	// Malformed file is ignored.
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}

	agg, err := Load(root, ".seedmancer")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	login := agg.States["auth/login-success"]
	if login == nil {
		t.Fatal("expected auth/login-success state")
	}
	if len(login.UsedBy) != 2 {
		t.Fatalf("expected 2 distinct tests, got %d", len(login.UsedBy))
	}
	if !login.LastUsedAt.Equal(t1) {
		t.Fatalf("expected lastUsedAt %v, got %v", t1, login.LastUsedAt)
	}
	if len(agg.States) != 2 {
		t.Fatalf("expected 2 states, got %d", len(agg.States))
	}
}

func TestPersistRoundTrips(t *testing.T) {
	root := t.TempDir()
	agg := Aggregate{States: map[string]*StateUsage{
		"auth/login-success": {
			State:      "auth/login-success",
			UsedBy:     []Ref{{File: "tests/login.spec.ts", Title: "user can login"}},
			LastUsedAt: time.Date(2026, 6, 29, 10, 0, 0, 0, time.UTC),
		},
	}}
	if err := Persist(root, ".seedmancer", agg); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	raw, err := os.ReadFile(StateUsagePath(root, ".seedmancer"))
	if err != nil {
		t.Fatalf("read persisted: %v", err)
	}
	var got Aggregate
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.States["auth/login-success"] == nil {
		t.Fatal("expected persisted state")
	}
}
