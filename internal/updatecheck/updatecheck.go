// Package updatecheck nudges users to upgrade when a newer Seedmancer
// release is available on GitHub. It is intentionally best-effort: any
// error (network, parse, cache I/O) is swallowed silently so we never
// turn a usable CLI session into a failure because update-checking went
// wrong.
//
// Design notes:
//
//   - The check runs in a background goroutine kicked off from main()
//     so the user-visible command never waits on GitHub.
//   - Results are cached in ~/.seedmancer/update-check.json for 24h so
//     we hit api.github.com at most once per day per user (well under
//     the 60 req/hr unauthenticated cap).
//   - Output is printed to stderr only on TTYs, and only after the
//     command finishes, so it never interferes with stdout pipelines
//     or the MCP JSON-RPC transport.
package updatecheck

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/KazanKK/seedmancer/internal/ui"
	"golang.org/x/term"
)

const (
	// releasesURL points at GitHub's "latest release" endpoint. It
	// follows redirects to the most recent non-prerelease tag and
	// returns JSON with `tag_name` + `html_url`.
	releasesURL = "https://api.github.com/repos/KazanKK/seedmancer/releases/latest"

	// cacheTTL caps how often we hit GitHub. 24h is the de-facto
	// standard across npm, brew, supabase, gh, etc.
	cacheTTL = 24 * time.Hour

	// httpTimeout keeps the background goroutine from hanging the
	// CLI for an unbounded amount of time on flaky networks. The
	// goroutine will finish (or be abandoned) within this window.
	httpTimeout = 3 * time.Second
)

// cacheEntry is the on-disk shape of the update-check cache. Stored as
// JSON so it's trivial to inspect / debug, and so adding fields later
// stays backwards-compatible.
type cacheEntry struct {
	LatestTag string    `json:"latest_tag"`
	HTMLURL   string    `json:"html_url"`
	CheckedAt time.Time `json:"checked_at"`
}

type githubRelease struct {
	TagName string `json:"tag_name"`
	HTMLURL string `json:"html_url"`
}

// Start kicks off a background update-check (if conditions allow) and
// returns a finish function. The caller is expected to invoke the
// returned function once, right before the program exits, to print the
// notice if a newer version was discovered.
//
// `current` is the running binary's version string (e.g. "0.4.2"); pass
// the `Version` variable from main. A "dev" sentinel (or empty string)
// turns the whole mechanism into a no-op so local builds and `go run`
// invocations stay quiet.
//
// `subcommand` is the first non-flag argument from os.Args (or empty
// when the user invoked `seedmancer` bare). We use it to suppress the
// notice for the `mcp` subcommand, whose stdio is owned by the JSON-RPC
// transport.
func Start(ctx context.Context, current, subcommand string) func() {
	noop := func() {}

	if shouldSkip(current, subcommand) {
		return noop
	}

	resultCh := make(chan *cacheEntry, 1)
	go func() {
		resultCh <- run(ctx, current)
	}()

	return func() {
		// Give the background fetch a brief grace period to land
		// before we exit. If it hasn't returned by then, drop the
		// notice for this run — better than blocking the user on
		// a slow network.
		select {
		case entry := <-resultCh:
			printIfNewer(current, entry)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// run performs the cache lookup or network fetch and returns the entry
// to compare against. All errors are swallowed: failing to check for
// updates must never bubble up as a user-facing error.
func run(ctx context.Context, current string) *cacheEntry {
	if entry, ok := loadCache(); ok && time.Since(entry.CheckedAt) < cacheTTL {
		return entry
	}

	entry, err := fetchLatest(ctx)
	if err != nil || entry == nil {
		return nil
	}

	_ = saveCache(entry)
	return entry
}

func shouldSkip(current, subcommand string) bool {
	current = strings.TrimSpace(current)
	if current == "" || current == "dev" {
		return true
	}
	if os.Getenv("SEEDMANCER_NO_UPDATE_CHECK") == "1" {
		return true
	}
	if os.Getenv("CI") == "true" {
		return true
	}
	if subcommand == "mcp" {
		return true
	}
	// Stderr-only output: don't spam scripts that pipe stderr
	// somewhere structured (CI logs, log aggregators, etc.).
	if !term.IsTerminal(int(os.Stderr.Fd())) {
		return true
	}
	return false
}

func fetchLatest(ctx context.Context) (*cacheEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, httpTimeout)
	defer cancel()

	url := releasesURL
	if override := strings.TrimSpace(os.Getenv("SEEDMANCER_UPDATE_CHECK_URL")); override != "" {
		// Test hook: lets the package's own tests point at an
		// httptest.Server without monkey-patching package state.
		url = override
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "seedmancer-cli")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("github releases returned status %d", resp.StatusCode)
	}

	var rel githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		return nil, err
	}
	tag := normalize(rel.TagName)
	if tag == "" {
		return nil, fmt.Errorf("github releases returned empty tag_name")
	}
	htmlURL := strings.TrimSpace(rel.HTMLURL)
	if htmlURL == "" {
		htmlURL = "https://github.com/KazanKK/seedmancer/releases/latest"
	}
	return &cacheEntry{
		LatestTag: tag,
		HTMLURL:   htmlURL,
		CheckedAt: time.Now().UTC(),
	}, nil
}

func printIfNewer(current string, entry *cacheEntry) {
	if entry == nil {
		return
	}
	if !isNewer(entry.LatestTag, current) {
		return
	}
	ui.PrintUpdateNotice(normalize(current), entry.LatestTag, entry.HTMLURL)
}

// ─── cache ─────────────────────────────────────────────────────────────────

func cachePath() (string, error) {
	// Test hook: tests point this at a t.TempDir() via env so they
	// can't accidentally touch the real ~/.seedmancer.
	if override := strings.TrimSpace(os.Getenv("SEEDMANCER_UPDATE_CHECK_FILE")); override != "" {
		return override, nil
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(homeDir, ".seedmancer", "update-check.json"), nil
}

func loadCache() (*cacheEntry, bool) {
	path, err := cachePath()
	if err != nil {
		return nil, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var entry cacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, false
	}
	if entry.LatestTag == "" {
		return nil, false
	}
	return &entry, true
}

func saveCache(entry *cacheEntry) error {
	path, err := cachePath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

// ─── semver ────────────────────────────────────────────────────────────────

// normalize strips a leading "v" and trims whitespace so "v1.2.3" and
// "1.2.3" compare equal. Anything past a "-" (e.g. "-rc.1") is dropped
// to keep the comparator simple — prereleases never trigger the notice
// since we follow GitHub's "latest" endpoint which already excludes them.
func normalize(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "v")
	v = strings.TrimPrefix(v, "V")
	if i := strings.Index(v, "-"); i >= 0 {
		v = v[:i]
	}
	if i := strings.Index(v, "+"); i >= 0 {
		v = v[:i]
	}
	return v
}

// isNewer reports whether `remote` is strictly greater than `local` in
// the usual major.minor.patch ordering. Missing segments are treated as
// 0 ("1.2" == "1.2.0"); non-numeric segments compare as -1 so garbage
// never triggers a misleading notice.
func isNewer(remote, local string) bool {
	r := splitVersion(normalize(remote))
	l := splitVersion(normalize(local))
	maxLen := len(r)
	if len(l) > maxLen {
		maxLen = len(l)
	}
	for i := 0; i < maxLen; i++ {
		rp := atIndex(r, i)
		lp := atIndex(l, i)
		if rp > lp {
			return true
		}
		if rp < lp {
			return false
		}
	}
	return false
}

func splitVersion(v string) []int {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ".")
	out := make([]int, len(parts))
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			out[i] = -1
			continue
		}
		out[i] = n
	}
	return out
}

func atIndex(parts []int, i int) int {
	if i >= len(parts) {
		return 0
	}
	return parts[i]
}
