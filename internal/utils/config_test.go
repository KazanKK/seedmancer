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

func TestSaveAPIToken_roundtrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	if err := SaveAPIToken("my-secret"); err != nil {
		t.Fatalf("save: %v", err)
	}
	cfgPath := filepath.Join(dir, ".seedmancer", "config.yaml")
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.APIToken != "my-secret" {
		t.Fatalf("got %q", cfg.APIToken)
	}

	// Saving again must preserve the previous key plus update the token.
	if err := SaveAPIToken("new-secret"); err != nil {
		t.Fatalf("save2: %v", err)
	}
	cfg2, _ := LoadConfig(cfgPath)
	if cfg2.APIToken != "new-secret" {
		t.Fatalf("got %q", cfg2.APIToken)
	}

	// File permissions must be 0600 — tokens are secrets.
	info, err := os.Stat(cfgPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("perm = %v, want 0600", info.Mode().Perm())
	}
}
