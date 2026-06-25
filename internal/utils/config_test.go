package utils

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "seedmancer.yaml")
	writeFile(t, cfgPath, "storage_path: .seed\napi_token: tok\ndatabase_url: postgres://x\n")

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if cfg.StoragePath != ".seed" || cfg.APIToken != "tok" ||
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
	t.Setenv("SEEDMANCER_API_URL", "") // use prod path so filename is always "credentials"

	if err := SaveAPICredentials("my-secret"); err != nil {
		t.Fatalf("save: %v", err)
	}
	path, err := CredentialsPath()
	if err != nil {
		t.Fatalf("CredentialsPath: %v", err)
	}

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

// TestResolveAPIToken_credentialsFileUsedWithoutEnvVar verifies that the
// credentials file written by `seedmancer login` is used when
// SEEDMANCER_API_TOKEN is not set.
func TestResolveAPIToken_credentialsFileUsedWithoutEnvVar(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("SEEDMANCER_API_TOKEN", "")
	t.Setenv("SEEDMANCER_API_URL", "")
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	if err := SaveAPICredentials("login-token"); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := ResolveAPIToken("")
	if err != nil || got != "login-token" {
		t.Fatalf("credentials file: got=%q err=%v", got, err)
	}
}

// TestResolveAPIToken_envVarWinsOnLogin ensures the env var takes
// precedence over a freshly-written credentials file.
func TestResolveAPIToken_envVarWinsOnLogin(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("SEEDMANCER_API_URL", "")
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	t.Setenv("SEEDMANCER_API_TOKEN", "env-token")
	if err := SaveAPICredentials("login-token"); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := ResolveAPIToken("")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "env-token" {
		t.Fatalf("env var must beat credentials file: got %q", got)
	}
}

// TestResolveAPIToken_envVarUsedWhenNoCredentials keeps the CI path
// working: if there's no credentials file (a fresh `seedmancer` install
// inside a container that only has the env var), the env var still
// resolves the token.
func TestResolveAPIToken_envVarUsedWhenNoCredentials(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("SEEDMANCER_API_URL", "")
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
	t.Setenv("SEEDMANCER_API_URL", "")
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
