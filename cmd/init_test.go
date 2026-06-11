package cmd

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	utils "github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
)

// runInitInTempDir runs the init Action inside a fresh temp directory with the
// supplied flag values. It returns the absolute path of the temp dir so the
// caller can assert on the produced files.
func runInitInTempDir(t *testing.T, flags map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	prev, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.String("storage-path", "", "")
	fs.String("env", "", "")
	fs.String("database-url", "", "")

	var args []string
	for k, v := range flags {
		args = append(args, "--"+k, v)
	}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("flag parse: %v", err)
	}

	app := cli.NewApp()
	ctx := cli.NewContext(app, fs, nil)

	cmd := InitCommand()
	if err := cmd.Action(ctx); err != nil {
		t.Fatalf("init action: %v", err)
	}
	return dir
}

func TestInit_writesConfigFile(t *testing.T) {
	dir := runInitInTempDir(t, map[string]string{
		"storage-path": ".seedmancer",
		"env":          "local",
		"database-url": "postgres://u:p@localhost:5432/db",
	})

	cfgPath := filepath.Join(dir, "seedmancer.yaml")
	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("seedmancer.yaml not created: %v", err)
	}

	cfg, err := utils.LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.StoragePath != ".seedmancer" {
		t.Errorf("StoragePath = %q, want %q", cfg.StoragePath, ".seedmancer")
	}
	if cfg.DefaultEnv != "local" {
		t.Errorf("DefaultEnv = %q, want local", cfg.DefaultEnv)
	}
	if cfg.Environments["local"].DatabaseURL != "postgres://u:p@localhost:5432/db" {
		t.Errorf("environments.local.database_url = %q", cfg.Environments["local"].DatabaseURL)
	}
	if cfg.DatabaseURL != "" {
		t.Errorf("legacy DatabaseURL should be empty, got %q", cfg.DatabaseURL)
	}

	storageDir := filepath.Join(dir, ".seedmancer")
	info, err := os.Stat(storageDir)
	if err != nil {
		t.Fatalf("storage dir not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("storage path is not a directory")
	}
}

func TestInit_rejectsEmptyStoragePath(t *testing.T) {
	dir := t.TempDir()
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.String("storage-path", "", "")
	fs.String("env", "", "")
	fs.String("database-url", "", "")
	// Explicitly pass an empty storage-path so the interactive prompt is skipped
	// via c.IsSet but the value is empty.
	if err := fs.Parse([]string{"--storage-path", "   "}); err != nil {
		t.Fatalf("flag parse: %v", err)
	}

	app := cli.NewApp()
	ctx := cli.NewContext(app, fs, nil)

	err := InitCommand().Action(ctx)
	if err == nil {
		t.Fatal("expected error for empty storage path")
	}
}
