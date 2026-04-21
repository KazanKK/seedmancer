package utils

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "seedmancer.yaml")
	writeFile(t, cfgPath, "storage_path: .seed\napi_token: tok\napi_url: https://example.com\ndatabase_url: postgres://x\n")

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if cfg.StoragePath != ".seed" || cfg.APIToken != "tok" ||
		cfg.APIURL != "https://example.com" ||
		cfg.DatabaseURL != "postgres://x" {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}

func TestLoadConfig_missingFile(t *testing.T) {
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestAPICredentials_roundtrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	if err := SaveAPICredentials("my-secret"); err != nil {
		t.Fatalf("save: %v", err)
	}
	path := filepath.Join(dir, ".seedmancer", "credentials")

	// Token file must be 0600 — tokens are secrets.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("perm = %v, want 0600", info.Mode().Perm())
	}

	got, err := LoadAPICredentials()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got != "my-secret" {
		t.Fatalf("got %q", got)
	}

	// Overwriting must replace the token, not append.
	if err := SaveAPICredentials("new-secret"); err != nil {
		t.Fatalf("save2: %v", err)
	}
	got, _ = LoadAPICredentials()
	if got != "new-secret" {
		t.Fatalf("got %q", got)
	}

	if err := ClearAPICredentials(); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if got, _ := LoadAPICredentials(); got != "" {
		t.Fatalf("after clear got %q", got)
	}
}

func TestSaveAPICredentials_rejectsEmpty(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := SaveAPICredentials("   "); err == nil {
		t.Fatal("expected error for empty token")
	}
}

// TestResolveAPIToken_prefersCredentialsFile pins the source order: an
// explicit credentials file wins over a legacy `api_token:` left in an
// older ~/.seedmancer/config.yaml.
func TestResolveAPIToken_prefersCredentialsFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	// Unset any inherited env var so the test isolates the
	// credentials-file-vs-legacy-config precedence. The env-vs-file
	// precedence is covered by its own test.
	t.Setenv("SEEDMANCER_API_TOKEN", "")
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	// Legacy user state: api_token in config.yaml.
	writeFile(t, filepath.Join(dir, ".seedmancer", "config.yaml"), "api_token: legacy\n")

	got, err := ResolveAPIToken("")
	if err != nil || got != "legacy" {
		t.Fatalf("legacy read: got=%q err=%v", got, err)
	}

	// After login, credentials file must take precedence.
	if err := SaveAPICredentials("fresh"); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err = ResolveAPIToken("")
	if err != nil || got != "fresh" {
		t.Fatalf("credentials first: got=%q err=%v", got, err)
	}
}

// TestResolveAPIToken_credentialsFileBeatsEnvVar regression-tests the
// support report where a stale `export SEEDMANCER_API_TOKEN=...` in the
// user's shell silently shadowed every `seedmancer login`. The
// credentials file must now win, so login "just sticks."
func TestResolveAPIToken_credentialsFileBeatsEnvVar(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	t.Setenv("SEEDMANCER_API_TOKEN", "stale-env")
	if err := SaveAPICredentials("fresh-login"); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := ResolveAPIToken("")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "fresh-login" {
		t.Fatalf("credentials file did not win over env var: got %q", got)
	}
}

// TestResolveAPIToken_envVarUsedWhenNoCredentials keeps the CI path
// working: if there's no credentials file (a fresh `seedmancer` install
// inside a container that only has the env var), the env var still
// resolves the token.
func TestResolveAPIToken_envVarUsedWhenNoCredentials(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	t.Setenv("SEEDMANCER_API_TOKEN", "ci-env-token")

	got, err := ResolveAPIToken("")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "ci-env-token" {
		t.Fatalf("env var not honored: got %q", got)
	}
}

// TestResolveAPIToken_flagBeatsEverything makes sure an explicit
// --token argument still overrides both the credentials file and the
// env var. This is the escape hatch for running one-off commands as a
// different identity without touching `seedmancer logout`.
func TestResolveAPIToken_flagBeatsEverything(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	t.Setenv("SEEDMANCER_API_TOKEN", "env-tok")
	if err := SaveAPICredentials("cred-tok"); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := ResolveAPIToken("flag-tok")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "flag-tok" {
		t.Fatalf("flag did not win: got %q", got)
	}
}
