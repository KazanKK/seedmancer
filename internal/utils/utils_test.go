package utils

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestHumanizeAgo(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		t    time.Time
		want string
	}{
		{"zero", time.Time{}, "—"},
		{"30s ago", now.Add(-30 * time.Second), "just now"},
		{"1 minute ago", now.Add(-61 * time.Second), "1 minute ago"},
		{"5 minutes ago", now.Add(-5 * time.Minute), "5 minutes ago"},
		{"1 hour ago", now.Add(-61 * time.Minute), "1 hour ago"},
		{"3 hours ago", now.Add(-3 * time.Hour), "3 hours ago"},
		{"yesterday", now.Add(-30 * time.Hour), "yesterday"},
		{"5 days ago", now.Add(-5 * 24 * time.Hour), "5 days ago"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := HumanizeAgo(c.t); got != c.want {
				t.Errorf("HumanizeAgo = %q, want %q", got, c.want)
			}
		})
	}
}

func TestHumanizeAgo_longAgoFormatted(t *testing.T) {
	past := time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC)
	got := HumanizeAgo(past)
	if got != "2020-01-15" {
		t.Errorf("got %q, want 2020-01-15", got)
	}
}

func TestBearerAPIToken(t *testing.T) {
	if got := BearerAPIToken("abc"); got != "Bearer abc" {
		t.Fatalf("got %q", got)
	}
}

func TestGetBaseURL_defaults(t *testing.T) {
	dir := t.TempDir()
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Setenv("HOME", dir)
	t.Setenv("SEEDMANCER_CLOUD_API_URL", "")
	if got := GetBaseURL(); got != "https://api.seedmancer.dev" {
		t.Errorf("default base URL = %q", got)
	}
}

func TestGetBaseURL_cloudEnvOverride(t *testing.T) {
	dir := t.TempDir()
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Setenv("HOME", dir)
	writeFile(t, filepath.Join(dir, "seedmancer.yaml"), "storage_path: .seedmancer\n")

	t.Setenv("SEEDMANCER_CLOUD_API_URL", "https://from-env.example.com/")
	if got := GetBaseURL(); got != "https://from-env.example.com" {
		t.Fatalf("SEEDMANCER_CLOUD_API_URL should win: got %q", got)
	}

	t.Setenv("SEEDMANCER_CLOUD_API_URL", "")
	if got := GetBaseURL(); got != "https://api.seedmancer.dev" {
		t.Fatalf("default production URL when env unset: got %q", got)
	}
}

func TestGetBaseURL_ignoresLegacyApiURLInYaml(t *testing.T) {
	dir := t.TempDir()
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Setenv("HOME", dir)
	t.Setenv("SEEDMANCER_CLOUD_API_URL", "")
	writeFile(t, filepath.Join(dir, "seedmancer.yaml"), "storage_path: .seedmancer\napi_url: https://legacy-in-yaml.example\n")

	if got := GetBaseURL(); got != "https://api.seedmancer.dev" {
		t.Fatalf("api_url in yaml must be ignored: got %q", got)
	}
}

func TestResolveAPIToken_flagBeatsConfig(t *testing.T) {
	got, err := ResolveAPIToken("flag-token")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "flag-token" {
		t.Fatalf("flag token not honored: %q", got)
	}
}

func TestResolveAPIToken_fromProjectConfig(t *testing.T) {
	dir := t.TempDir()
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Setenv("HOME", dir)
	// Unset any inherited SEEDMANCER_API_TOKEN so the project config is
	// actually the lowest-priority source that contains a value.
	t.Setenv("SEEDMANCER_API_TOKEN", "")
	writeFile(t, filepath.Join(dir, "seedmancer.yaml"), "storage_path: .seedmancer\napi_token: cfg-tok\n")

	got, err := ResolveAPIToken("")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "cfg-tok" {
		t.Fatalf("got %q", got)
	}
}

func TestResolveAPIToken_errorWhenMissing(t *testing.T) {
	dir := t.TempDir()
	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Setenv("HOME", dir)
	t.Setenv("SEEDMANCER_API_TOKEN", "")

	_, err := ResolveAPIToken("")
	if err == nil || !errors.Is(err, ErrMissingAPIToken) {
		t.Fatalf("want ErrMissingAPIToken, got: %v", err)
	}
}

// ── On-disk layout helpers ──────────────────────────────────────────────────

func makeSchema(t *testing.T, projectRoot, fpShort string) {
	t.Helper()
	writeFile(t,
		SchemaJSONPath(projectRoot, ".seedmancer", fpShort),
		`{"tables":[{"name":"t","columns":[{"name":"id","type":"uuid"}]}]}`,
	)
}

func makeDataset(t *testing.T, projectRoot, fpShort, name string) {
	t.Helper()
	writeFile(t,
		filepath.Join(DatasetPath(projectRoot, ".seedmancer", fpShort, name), "t.csv"),
		"id\n1\n",
	)
}

func TestListLocalSchemas_returnsEmptyWhenAbsent(t *testing.T) {
	dir := t.TempDir()
	got, err := ListLocalSchemas(dir, ".seedmancer")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d, want 0", len(got))
	}
}

func TestListLocalSchemas_sortsByRecency(t *testing.T) {
	dir := t.TempDir()
	makeSchema(t, dir, "aaaa")
	makeSchema(t, dir, "bbbb")
	makeDataset(t, dir, "bbbb", "b1")

	// Bump bbbb's dataset mtime into the future so recency ordering is stable
	// regardless of filesystem timing.
	_ = os.Chtimes(DatasetPath(dir, ".seedmancer", "bbbb", "b1"),
		time.Now().Add(time.Hour), time.Now().Add(time.Hour))

	got, err := ListLocalSchemas(dir, ".seedmancer")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d", len(got))
	}
	// bbbb has a fresher dataset than aaaa → bbbb first.
	if filepath.Base(got[0].Path) != "bbbb" {
		t.Fatalf("want bbbb first, got %s", filepath.Base(got[0].Path))
	}
}

func TestResolveLocalSchema_errorsWithoutSchemas(t *testing.T) {
	dir := t.TempDir()
	if _, err := ResolveLocalSchema(dir, ".seedmancer", ""); err == nil {
		t.Fatal("expected error when no schemas")
	}
}

func TestResolveLocalSchema_ambiguousErrorsOnMultiWithoutPrefix(t *testing.T) {
	dir := t.TempDir()
	makeSchema(t, dir, "a")
	makeSchema(t, dir, "b")
	if _, err := ResolveLocalSchema(dir, ".seedmancer", ""); err == nil {
		t.Fatal("expected ambiguous error")
	}
}

func TestResolveLocalSchema_prefixMatchesFolderByFingerprint(t *testing.T) {
	dir := t.TempDir()
	makeSchema(t, dir, "aaaa")
	schema, err := ResolveLocalSchema(dir, ".seedmancer", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// Use the schema's real fingerprint prefix (first 4 chars) to re-resolve.
	got, err := ResolveLocalSchema(dir, ".seedmancer", schema.FingerprintShort[:4])
	if err != nil {
		t.Fatalf("prefix resolve: %v", err)
	}
	if got.FingerprintShort != schema.FingerprintShort {
		t.Fatalf("got %q want %q", got.FingerprintShort, schema.FingerprintShort)
	}
}

func TestResolveLocalSchema_matchesByDisplayName(t *testing.T) {
	dir := t.TempDir()
	makeSchema(t, dir, "aaaa")
	schema, err := ResolveLocalSchema(dir, ".seedmancer", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := SaveLocalSchemaMeta(schema.Path, LocalSchemaMeta{DisplayName: "MyNice"}); err != nil {
		t.Fatalf("SaveLocalSchemaMeta: %v", err)
	}

	got, err := ResolveLocalSchema(dir, ".seedmancer", "mynice")
	if err != nil {
		t.Fatalf("resolve by display name: %v", err)
	}
	if got.FingerprintShort != schema.FingerprintShort {
		t.Fatalf("got %q want %q", got.FingerprintShort, schema.FingerprintShort)
	}
}

func TestLocalSchemaMeta_saveReadDelete(t *testing.T) {
	dir := t.TempDir()
	makeSchema(t, dir, "aaaa")
	schemas, err := ListLocalSchemas(dir, ".seedmancer")
	if err != nil || len(schemas) != 1 {
		t.Fatalf("ListLocalSchemas: len=%d err=%v", len(schemas), err)
	}
	schemaDir := schemas[0].Path

	if err := SaveLocalSchemaMeta(schemaDir, LocalSchemaMeta{DisplayName: "stage"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	meta, err := LoadLocalSchemaMeta(schemaDir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if meta.DisplayName != "stage" {
		t.Fatalf("display_name = %q, want %q", meta.DisplayName, "stage")
	}

	// Saving the zero value should delete the sidecar so the folder stays
	// tidy for users that clear a label.
	if err := SaveLocalSchemaMeta(schemaDir, LocalSchemaMeta{}); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if _, err := os.Stat(SchemaMetaPath(schemaDir)); !os.IsNotExist(err) {
		t.Fatalf("meta.yaml should have been removed, stat err=%v", err)
	}
	meta, err = LoadLocalSchemaMeta(schemaDir)
	if err != nil || meta.DisplayName != "" {
		t.Fatalf("after clear: meta=%+v err=%v", meta, err)
	}
}

func TestFindLocalDataset_requiresName(t *testing.T) {
	dir := t.TempDir()
	if _, _, err := FindLocalDataset(dir, ".seedmancer", "", ""); err == nil {
		t.Fatal("expected error when dataset name empty")
	}
}

func TestFindLocalDataset_uniqueAcrossSchemas(t *testing.T) {
	dir := t.TempDir()
	makeSchema(t, dir, "aaaa")
	makeDataset(t, dir, "aaaa", "basic")
	makeSchema(t, dir, "bbbb")

	s, ds, err := FindLocalDataset(dir, ".seedmancer", "", "basic")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !strings.HasSuffix(ds, filepath.Join("datasets", "basic")) {
		t.Fatalf("ds path unexpected: %q", ds)
	}
	if s.FingerprintShort == "" {
		t.Fatal("expected schema info")
	}
}

func TestFindLocalDataset_ambiguousAcrossSchemas(t *testing.T) {
	dir := t.TempDir()
	makeSchema(t, dir, "aaaa")
	makeDataset(t, dir, "aaaa", "basic")
	makeSchema(t, dir, "bbbb")
	makeDataset(t, dir, "bbbb", "basic")

	_, _, err := FindLocalDataset(dir, ".seedmancer", "", "basic")
	if err == nil {
		t.Fatal("expected ambiguous error")
	}
}

func TestSchemaFiles_listsSidecars(t *testing.T) {
	dir := t.TempDir()
	schemaDir := filepath.Join(dir, "s")
	writeFile(t, filepath.Join(schemaDir, "schema.json"), "{}")
	writeFile(t, filepath.Join(schemaDir, "do_stuff_func.sql"), "--")
	writeFile(t, filepath.Join(schemaDir, "t_x_trigger.sql"), "--")
	writeFile(t, filepath.Join(schemaDir, "README.md"), "--")
	// Subdir must be skipped.
	writeFile(t, filepath.Join(schemaDir, "sub", "extra_func.sql"), "--")

	got, err := SchemaFiles(schemaDir)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d files: %v", len(got), got)
	}
}

func TestDatasetFiles_filtersCSVAndJSON(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a.csv"), "")
	writeFile(t, filepath.Join(dir, "b.JSON"), "")
	writeFile(t, filepath.Join(dir, "ignore.txt"), "")

	got, err := DatasetFiles(dir)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d: %v", len(got), got)
	}
}

func TestFindConfigFile_walksUpward(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "a", "b", "c")
	if err := os.MkdirAll(sub, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeFile(t, filepath.Join(dir, "seedmancer.yaml"), "storage_path: .seedmancer\n")

	prev, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(prev) })
	if err := os.Chdir(sub); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	// Home must not contain a config so we know the project walk is what found it.
	t.Setenv("HOME", t.TempDir())

	got, err := FindConfigFile()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != filepath.Join(dir, "seedmancer.yaml") {
		t.Fatalf("got %q", got)
	}
}
