package mcp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// newTestServer wires up the exact same tools/resources/prompts the
// real binary does. Keeping the factory local means tests can't
// accidentally mask a wiring regression — if registerTools is broken,
// every test here fails too.
func newTestServer(t *testing.T) *sdk.Server {
	t.Helper()
	s := sdk.NewServer(&sdk.Implementation{Name: "seedmancer-test", Version: Version}, nil)
	registerTools(s)
	registerResources(s)
	registerPrompts(s)
	return s
}

// connect builds an in-memory client session against a fresh server.
// Keeping the returned session simplifies callers: defer session.Close()
// is enough to tear down both sides.
func connect(t *testing.T, srv *sdk.Server) (context.Context, *sdk.ClientSession) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	ct, st := sdk.NewInMemoryTransports()
	if _, err := srv.Connect(ctx, st, nil); err != nil {
		t.Fatalf("server.Connect: %v", err)
	}
	client := sdk.NewClient(&sdk.Implementation{Name: "test-client", Version: "0"}, nil)
	session, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return ctx, session
}

// TestToolsListIncludesCoreSurface asserts that every tool name the
// rest of the package wires up is actually reachable from a client —
// i.e. renames / missing registrations surface immediately.
func TestToolsListIncludesCoreSurface(t *testing.T) {
	srv := newTestServer(t)
	ctx, session := connect(t, srv)

	got, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	names := map[string]struct{}{}
	for _, tool := range got.Tools {
		names[tool.Name] = struct{}{}
	}
	want := []string{
		"list_datasets", "describe_dataset", "list_schemas", "describe_schema",
		"get_status", "list_envs", "add_env", "remove_env", "use_env",
		"init_project", "seed_database", "export_database", "generate_dataset",
		"sync_dataset", "fetch_dataset", "login_info", "logout",
	}
	for _, n := range want {
		if _, ok := names[n]; !ok {
			t.Errorf("tool %q missing from ListTools; got %v", n, names)
		}
	}
}

// TestResourcesAndPromptsListed covers the static resource + prompt
// registrations. The dynamic `seedmancer://dataset/{id}` lives behind
// AddResourceTemplate, which doesn't show up in ListResources — that's
// SDK-intended, so we don't assert it here.
func TestResourcesAndPromptsListed(t *testing.T) {
	srv := newTestServer(t)
	ctx, session := connect(t, srv)

	rres, err := session.ListResources(ctx, nil)
	if err != nil {
		t.Fatalf("ListResources: %v", err)
	}
	wantURIs := []string{
		"seedmancer://config", "seedmancer://status", "seedmancer://datasets",
		"seedmancer://schemas", "seedmancer://docs/quickstart",
		"seedmancer://docs/playwright-recipe",
	}
	have := map[string]struct{}{}
	for _, r := range rres.Resources {
		have[r.URI] = struct{}{}
	}
	for _, u := range wantURIs {
		if _, ok := have[u]; !ok {
			t.Errorf("resource %q missing; got %v", u, have)
		}
	}

	pres, err := session.ListPrompts(ctx, nil)
	if err != nil {
		t.Fatalf("ListPrompts: %v", err)
	}
	wantPrompts := map[string]bool{"reset_db_for_tests": true, "generate_test_data": true}
	for _, p := range pres.Prompts {
		delete(wantPrompts, p.Name)
	}
	if len(wantPrompts) > 0 {
		t.Errorf("missing prompts: %v", wantPrompts)
	}
}

// TestQuickstartDocResource confirms that the bundled docs reach the
// client intact. This guards against the all-too-easy footgun of a
// misspelled URI registration breaking the onboarding flow silently.
func TestQuickstartDocResource(t *testing.T) {
	srv := newTestServer(t)
	ctx, session := connect(t, srv)

	res, err := session.ReadResource(ctx, &sdk.ReadResourceParams{URI: "seedmancer://docs/quickstart"})
	if err != nil {
		t.Fatalf("ReadResource: %v", err)
	}
	if len(res.Contents) == 0 {
		t.Fatalf("empty contents")
	}
	if !strings.Contains(res.Contents[0].Text, "Seedmancer quickstart") {
		t.Errorf("quickstart doc missing expected header; got %q", res.Contents[0].Text[:80])
	}
	if res.Contents[0].MIMEType != "text/markdown" {
		t.Errorf("quickstart mime type = %q, want text/markdown", res.Contents[0].MIMEType)
	}
}

// TestDescribeDatasetReportsMissingConfig exercises an error path the
// host will hit first when pointed at an uninitialized directory. The
// SDK packs handler errors into IsError+Content rather than surfacing
// them as protocol errors, so we assert against that shape.
func TestDescribeDatasetReportsMissingConfig(t *testing.T) {
	srv := newTestServer(t)
	ctx, session := connect(t, srv)

	tmp := t.TempDir()
	prev, _ := os.Getwd()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(prev) })

	res, err := session.CallTool(ctx, &sdk.CallToolParams{
		Name:      "describe_dataset",
		Arguments: map[string]any{"datasetId": "does-not-exist"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected IsError=true for missing config; got %+v", res)
	}
	var combined string
	for _, c := range res.Content {
		if tc, ok := c.(*sdk.TextContent); ok {
			combined += tc.Text
		}
	}
	if !strings.Contains(strings.ToLower(combined), "seedmancer.yaml") &&
		!strings.Contains(strings.ToLower(combined), "config") {
		t.Errorf("error text %q should mention the missing config", combined)
	}
}

// TestListDatasetsHappyPath seeds a minimal schema-first layout on
// disk, then asks the server to list it. This is the "I work on an
// initialized repo" path every agent will hit first.
func TestListDatasetsHappyPath(t *testing.T) {
	srv := newTestServer(t)
	ctx, session := connect(t, srv)

	tmp := setupFakeProject(t)
	prev, _ := os.Getwd()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(prev) })

	res, err := session.CallTool(ctx, &sdk.CallToolParams{
		Name:      "list_datasets",
		Arguments: map[string]any{"local": true},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("unexpected error: %+v", res.Content)
	}
	// StructuredContent is set by the SDK when the tool has an output
	// schema (which our Run* types produce via jsonschema tags).
	if res.StructuredContent == nil {
		t.Fatalf("expected structured content for list_datasets")
	}
	raw, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(raw), `"local"`) {
		t.Errorf("structured content missing \"local\" key: %s", raw)
	}
}

// setupFakeProject builds a minimum-viable seedmancer project under
// t.TempDir(): a seedmancer.yaml, the storage dir, and one empty schema
// folder. Enough for list_* tools to return a sensible (usually empty)
// result without hitting network.
func setupFakeProject(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	yaml := "storage_path: .seedmancer\ndefault_env: local\nenvironments:\n  local:\n    database_url: \"\"\n"
	if err := os.WriteFile(filepath.Join(dir, "seedmancer.yaml"), []byte(yaml), 0644); err != nil {
		t.Fatalf("write yaml: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, ".seedmancer"), 0755); err != nil {
		t.Fatalf("mkdir storage: %v", err)
	}
	return dir
}
