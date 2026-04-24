package mcp

import (
	"context"

	"github.com/KazanKK/seedmancer/cmd"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// registerTools wires every Seedmancer CLI capability into the MCP
// server as a typed tool. The handlers are deliberately thin — all real
// work lives in cmd/runners.go, which is also what the CLI's own tests
// exercise. This keeps the MCP surface in lockstep with the CLI so
// agents never see a stale or divergent set of operations.
//
// Annotations follow the MCP spec:
//   - ReadOnlyHint:    tool does not mutate state.
//   - DestructiveHint: tool can destroy or replace data (`*bool` because
//                      the default for non-read-only tools is `true`; we
//                      pin it explicitly to avoid ambiguity).
//   - IdempotentHint:  same input → same end state (seed + fetch both
//                      overwrite, so they qualify).
//
// We lean on generic AddTool + `jsonschema` struct tags on the Run*Input
// types, so JSON schemas ship automatically and stay in sync with the Go
// source of truth.
func registerTools(s *mcp.Server) {
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}
	falsePtr := func() *bool { b := false; return &b }
	truePtr := func() *bool { b := true; return &b }

	// ── Introspection (read-only) ─────────────────────────────────────
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_datasets",
		Title:       "List datasets",
		Description: "List datasets available locally and/or in the connected cloud account.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.ListInput) (*mcp.CallToolResult, cmd.ListOutput, error) {
		out, err := cmd.RunList(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "describe_dataset",
		Title:       "Describe dataset",
		Description: "Show files, row counts, and a small CSV preview for a local dataset.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.DescribeDatasetInput) (*mcp.CallToolResult, cmd.DescribeDatasetOutput, error) {
		out, err := cmd.RunDescribeDataset(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_schemas",
		Title:       "List schemas",
		Description: "List schemas known locally and/or remotely.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.ListSchemasInput) (*mcp.CallToolResult, cmd.ListSchemasOutput, error) {
		out, err := cmd.RunListSchemas(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "describe_schema",
		Title:       "Describe schema",
		Description: "Return tables + columns for a local schema (fingerprint prefix or full fingerprint).",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.DescribeSchemaInput) (*mcp.CallToolResult, cmd.DescribeSchemaOutput, error) {
		out, err := cmd.RunDescribeSchema(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_status",
		Title:       "Get Seedmancer status",
		Description: "Project layout, configured environments, auth state, and optional API reachability probe.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.StatusInput) (*mcp.CallToolResult, cmd.StatusOutput, error) {
		out, err := cmd.RunStatus(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_envs",
		Title:       "List environments",
		Description: "List configured Seedmancer environments (names + masked DB URLs).",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.ListEnvsInput) (*mcp.CallToolResult, cmd.ListEnvsOutput, error) {
		out, err := cmd.RunListEnvs(ctx, in)
		return nil, out, err
	})

	// ── Config mutation (non-destructive to data) ─────────────────────
	mcp.AddTool(s, &mcp.Tool{
		Name:        "add_env",
		Title:       "Add environment",
		Description: "Add or overwrite a named environment in seedmancer.yaml.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: falsePtr(), IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.AddEnvInput) (*mcp.CallToolResult, cmd.AddEnvOutput, error) {
		out, err := cmd.RunAddEnv(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "remove_env",
		Title:       "Remove environment",
		Description: "Remove a named environment from seedmancer.yaml.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: truePtr(), IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.RemoveEnvInput) (*mcp.CallToolResult, cmd.RemoveEnvOutput, error) {
		out, err := cmd.RunRemoveEnv(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "use_env",
		Title:       "Set default environment",
		Description: "Mark an existing environment as the new default_env.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: falsePtr(), IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.UseEnvInput) (*mcp.CallToolResult, cmd.UseEnvOutput, error) {
		out, err := cmd.RunUseEnv(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "init_project",
		Title:       "Initialize Seedmancer",
		Description: "Create seedmancer.yaml and the local storage directory in the current working directory.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: falsePtr(), IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.InitInput) (*mcp.CallToolResult, cmd.InitOutput, error) {
		out, err := cmd.RunInit(ctx, in)
		return nil, out, err
	})

	// ── Data plane (destructive by nature) ────────────────────────────
	mcp.AddTool(s, &mcp.Tool{
		Name: "seed_database",
		Title: "Seed database",
		Description: "Truncate the target env(s) and reload the named dataset. " +
			"This overwrites existing data — intended for test/dev resets.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: truePtr(), IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.SeedInput) (*mcp.CallToolResult, cmd.SeedOutput, error) {
		// Agents can't answer interactive prompts, so we force the "yes"
		// confirmation path. The prod-guard inside seedOneEnvQuiet still
		// refuses to seed prod-like envs without an explicit Yes:true.
		if !in.Yes {
			in.Yes = true
		}
		out, err := cmd.RunSeed(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "export_database",
		Title:       "Export database",
		Description: "Dump the current schema + data from an env into a new content-addressed dataset folder.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: falsePtr(), IdempotentHint: false},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.ExportInput) (*mcp.CallToolResult, cmd.ExportOutput, error) {
		out, err := cmd.RunExport(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name: "generate_dataset",
		Title: "Generate dataset",
		Description: "Ask the Seedmancer cloud to synthesize a dataset that matches an existing schema, " +
			"using a natural-language prompt.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: falsePtr(), IdempotentHint: false},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.GenerateInput) (*mcp.CallToolResult, cmd.GenerateOutput, error) {
		out, err := cmd.RunGenerate(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:  "generate_dataset_local",
		Title: "Generate dataset locally",
		Description: "Run an agent-written Go script locally to synthesise a dataset. " +
			"No cloud API, no quota, and no internet connection are needed. " +
			"Read seedmancer://docs/local-generation first to learn the Go script contract, " +
			"then call describe_schema to get the exact column names before writing the script.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: falsePtr(), IdempotentHint: false},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.GenerateLocalInput) (*mcp.CallToolResult, cmd.GenerateLocalOutput, error) {
		out, err := cmd.RunGenerateLocal(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "sync_dataset",
		Title:       "Sync dataset to cloud",
		Description: "Upload a local dataset + its schema sidecars to the connected Seedmancer account.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: falsePtr(), IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.SyncInput) (*mcp.CallToolResult, cmd.SyncOutput, error) {
		out, err := cmd.RunSync(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "fetch_dataset",
		Title:       "Fetch dataset from cloud",
		Description: "Download a remote dataset into the local schema-first layout; overwrites any existing folder.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: truePtr(), IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.FetchInput) (*mcp.CallToolResult, cmd.FetchOutput, error) {
		out, err := cmd.RunFetch(ctx, in)
		return nil, out, err
	})

	// ── Auth surface ─────────────────────────────────────────────────
	mcp.AddTool(s, &mcp.Tool{
		Name:        "login_info",
		Title:       "Get login info",
		Description: "Return the URL a human would open to sign in, and whether an API token is already configured.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, cmd.LoginInfoOutput, error) {
		out, err := cmd.RunLoginInfo(ctx)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "logout",
		Title:       "Clear stored credentials",
		Description: "Remove the cached API token from ~/.seedmancer/credentials.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: truePtr(), IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, cmd.LogoutOutput, error) {
		out, err := cmd.RunLogout(ctx)
		return nil, out, err
	})
}
