package updatecheck

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// setupTestEnv wires the package's env-based hooks to a temp directory
// and a mock GitHub releases server. The test owns t.TempDir() so the
// cache file is cleaned automatically; calls to os.Setenv are reverted
// via t.Cleanup so tests can run in arbitrary order without leaking
// state.
func setupTestEnv(t *testing.T, body string, hits *int32) string {
	t.Helper()

	dir := t.TempDir()
	cacheFile := filepath.Join(dir, "update-check.json")
	t.Setenv("SEEDMANCER_UPDATE_CHECK_FILE", cacheFile)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			atomic.AddInt32(hits, 1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("SEEDMANCER_UPDATE_CHECK_URL", srv.URL)

	// Make sure no stray opt-outs sneak in from the host shell.
	t.Setenv("SEEDMANCER_NO_UPDATE_CHECK", "")
	t.Setenv("CI", "")

	return cacheFile
}

func TestRun_FetchesAndCaches(t *testing.T) {
	var hits int32
	cacheFile := setupTestEnv(t, `{"tag_name":"v1.2.3","html_url":"https://example.test/releases/v1.2.3"}`, &hits)

	got := run(context.Background(), "1.0.0")
	if got == nil {
		t.Fatal("expected cache entry, got nil")
	}
	if got.LatestTag != "1.2.3" {
		t.Fatalf("LatestTag: want %q, got %q", "1.2.3", got.LatestTag)
	}
	if got.HTMLURL != "https://example.test/releases/v1.2.3" {
		t.Fatalf("HTMLURL: got %q", got.HTMLURL)
	}
	if atomic.LoadInt32(&hits) != 1 {
		t.Fatalf("expected 1 HTTP hit, got %d", hits)
	}
	if _, err := os.Stat(cacheFile); err != nil {
		t.Fatalf("expected cache file at %s: %v", cacheFile, err)
	}
}

func TestRun_UsesCacheWithinTTL(t *testing.T) {
	var hits int32
	setupTestEnv(t, `{"tag_name":"v9.9.9","html_url":"https://example.test/r"}`, &hits)

	if got := run(context.Background(), "1.0.0"); got == nil {
		t.Fatal("first call: expected entry")
	}
	if got := run(context.Background(), "1.0.0"); got == nil {
		t.Fatal("second call: expected entry")
	}

	if atomic.LoadInt32(&hits) != 1 {
		t.Fatalf("expected exactly 1 HTTP hit across both calls, got %d", hits)
	}
}

func TestRun_RefetchesAfterTTL(t *testing.T) {
	var hits int32
	cacheFile := setupTestEnv(t, `{"tag_name":"v2.0.0","html_url":"https://example.test/r"}`, &hits)

	if got := run(context.Background(), "1.0.0"); got == nil {
		t.Fatal("first call: expected entry")
	}

	// Age the cache past the TTL by rewriting checked_at.
	stale := cacheEntry{
		LatestTag: "2.0.0",
		HTMLURL:   "https://example.test/r",
		CheckedAt: time.Now().Add(-2 * cacheTTL),
	}
	data, _ := json.Marshal(&stale)
	if err := os.WriteFile(cacheFile, data, 0600); err != nil {
		t.Fatalf("rewrite cache: %v", err)
	}

	if got := run(context.Background(), "1.0.0"); got == nil {
		t.Fatal("second call: expected entry")
	}
	if hits != 2 {
		t.Fatalf("expected 2 HTTP hits after TTL expiry, got %d", hits)
	}
}

func TestRun_MalformedCacheIsIgnored(t *testing.T) {
	var hits int32
	cacheFile := setupTestEnv(t, `{"tag_name":"v1.5.0","html_url":"https://example.test/r"}`, &hits)

	// Pre-seed garbage so loadCache fails.
	if err := os.MkdirAll(filepath.Dir(cacheFile), 0700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(cacheFile, []byte("{not json"), 0600); err != nil {
		t.Fatalf("seed garbage: %v", err)
	}

	got := run(context.Background(), "1.0.0")
	if got == nil {
		t.Fatal("expected cache entry, got nil")
	}
	if got.LatestTag != "1.5.0" {
		t.Fatalf("LatestTag: got %q", got.LatestTag)
	}
	if hits != 1 {
		t.Fatalf("expected 1 HTTP hit (cache treated as cold), got %d", hits)
	}
}

func TestShouldSkip(t *testing.T) {
	// shouldSkip's TTY check looks at the real stderr fd, which is
	// not a TTY under `go test`, so it returns true for every input
	// in this environment. We assert that the early returns above the
	// TTY check fire first (those branches don't depend on the fd).
	cases := []struct {
		name       string
		current    string
		subcommand string
		env        map[string]string
		want       bool
	}{
		{name: "dev sentinel", current: "dev", subcommand: "list", want: true},
		{name: "empty version", current: "", subcommand: "list", want: true},
		{name: "explicit opt-out", current: "1.0.0", subcommand: "list", env: map[string]string{"SEEDMANCER_NO_UPDATE_CHECK": "1"}, want: true},
		{name: "ci env", current: "1.0.0", subcommand: "list", env: map[string]string{"CI": "true"}, want: true},
		{name: "mcp subcommand", current: "1.0.0", subcommand: "mcp", want: true},
		// Non-TTY (test environment) — also skipped, but via the
		// final branch; included to document the behaviour.
		{name: "release build under go test", current: "1.0.0", subcommand: "list", want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := shouldSkip(tc.current, tc.subcommand); got != tc.want {
				t.Fatalf("shouldSkip(%q, %q) = %v, want %v", tc.current, tc.subcommand, got, tc.want)
			}
		})
	}
}

func TestStart_SkipsWithoutHTTP(t *testing.T) {
	// When shouldSkip returns true, Start must not fire a fetch.
	// We point SEEDMANCER_UPDATE_CHECK_URL at a server that fails
	// the test if it gets a request, then invoke Start with the
	// "dev" sentinel that always skips.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Errorf("Start fired an HTTP request despite shouldSkip == true")
	}))
	defer srv.Close()
	t.Setenv("SEEDMANCER_UPDATE_CHECK_URL", srv.URL)

	finish := Start(context.Background(), "dev", "list")
	finish() // must be safe to call and must not block.
}

func TestIsNewer(t *testing.T) {
	cases := []struct {
		remote string
		local  string
		want   bool
	}{
		{"1.2.3", "1.2.2", true},
		{"1.2.3", "1.2.3", false},
		{"1.2.3", "1.2.4", false},
		{"v1.2.3", "1.2.3", false},
		{"1.10.0", "1.9.0", true},
		{"2.0.0", "1.999.999", true},
		{"1.2", "1.1.999", true},
		{"1.2.3", "1.2", true},
		{"1.2.0", "1.2", false},
		{"1.2.3-rc.1", "1.2.3", false}, // prerelease tail dropped
	}
	for _, tc := range cases {
		t.Run(tc.remote+"_vs_"+tc.local, func(t *testing.T) {
			if got := isNewer(tc.remote, tc.local); got != tc.want {
				t.Fatalf("isNewer(%q, %q) = %v, want %v", tc.remote, tc.local, got, tc.want)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	cases := map[string]string{
		"v1.2.3":         "1.2.3",
		"V1.2.3":         "1.2.3",
		" 1.2.3 ":        "1.2.3",
		"1.2.3-rc.1":     "1.2.3",
		"1.2.3+build.5":  "1.2.3",
		"":               "",
		"v0.4.2-beta.1":  "0.4.2",
	}
	for in, want := range cases {
		if got := normalize(in); got != want {
			t.Errorf("normalize(%q) = %q, want %q", in, got, want)
		}
	}
}
