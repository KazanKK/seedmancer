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
//     the default for non-read-only tools is `true`; we
//     pin it explicitly to avoid ambiguity).
//   - IdempotentHint:  same input → same end state (seed + pull both
//     overwrite, so they qualify).
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
		Name:  "list_datasets",
		Title: "List scenarios",
		Description: "List every scenario known on disk along with its latest revision pointer, " +
			"schema fingerprint, and the services snapshotted with the latest revision.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.ListInput) (*mcp.CallToolResult, cmd.ListOutput, error) {
		out, err := cmd.RunList(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "describe_dataset",
		Title:       "Describe dataset",
		Description: "Show files, row counts, a small CSV preview, and the saved purpose for a local dataset.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.DescribeDatasetInput) (*mcp.CallToolResult, cmd.DescribeDatasetOutput, error) {
		out, err := cmd.RunDescribeDataset(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:  "get_dataset_sql",
		Title: "Get dataset SQL",
		Description: "Return the SQL block that was used to create a scenario revision via " +
			"generate_dataset_local. Use it as a REFERENCE for what the previous revision " +
			"produced. When generating a new revision, REWRITE the SQL as a fresh, full, " +
			"self-contained script — do NOT append delta statements to the returned SQL. " +
			"The next generate_dataset_local call will reject any SQL whose populated tables " +
			"are missing a leading TRUNCATE or unconditional DELETE FROM. Defaults to the " +
			"scenario's latest revision; pass `revision: \"rNNN\"` for a specific one. " +
			"The output includes the scenario's saved purpose — keep any rewrite true to it. " +
			"Returns an error when the revision " +
			"has no dataset.sql sidecar (e.g. it was produced by export_database or pull_dataset).",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.GetDatasetSQLInput) (*mcp.CallToolResult, cmd.GetDatasetSQLOutput, error) {
		out, err := cmd.RunGetDatasetSQL(ctx, in)
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

	mcp.AddTool(s, &mcp.Tool{
		Name:  "install_agent_rules",
		Title: "Install agent rules",
		Description: "Write .cursor/rules/seedmancer.mdc and a CLAUDE.md block into the project " +
			"so Cursor and Claude Code automatically use Seedmancer MCP tools whenever the user " +
			"asks to create or manage test data. Idempotent — safe to call multiple times. " +
			"Already called by init_project; call this manually when adopting Seedmancer in an existing project.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: falsePtr(), IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.InstallAgentRulesInput) (*mcp.CallToolResult, cmd.InstallAgentRulesOutput, error) {
		out, err := cmd.RunInstallAgentRules(ctx, in)
		return nil, out, err
	})

	// ── Data plane (destructive by nature) ────────────────────────────
	mcp.AddTool(s, &mcp.Tool{
		Name:  "seed_database",
		Title: "Seed database",
		Description: "Truncate the target env(s) and reload a scenario revision into them. " +
			"Defaults to the scenario's latest revision; pass `revision: \"rNNN\"` for a specific one. " +
			"Refuses to seed when the database schema fingerprint differs from the revision's, " +
			"unless `force: true` is set. This overwrites existing data — intended for test/dev resets.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: truePtr(), IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.SeedInput) (*mcp.CallToolResult, cmd.SeedOutput, error) {
		if !in.Yes {
			in.Yes = true
		}
		out, err := cmd.RunSeed(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:  "export_database",
		Title: "Export database",
		Description: "Dump the current schema + data into a new revision under the given scenario. " +
			"Every export creates a new immutable rNNN revision and advances pointers.latest. " +
			"Schema sidecars live in a content-addressed folder shared across scenarios.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: falsePtr(), IdempotentHint: false},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.ExportInput) (*mcp.CallToolResult, cmd.ExportOutput, error) {
		out, err := cmd.RunExport(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:  "generate_dataset_local",
		Title: "Generate scenario revision locally",
		Description: "Synthesise a new revision under the given scenario by running agent-written " +
			"SQL. The SQL MUST be a FULL, self-contained, idempotent script: every populated " +
			"table starts with `TRUNCATE TABLE <t> RESTART IDENTITY CASCADE` (or an unconditional " +
			"`DELETE FROM <t>`) before its INSERTs, and running the SQL alone against an empty " +
			"migrated schema reproduces the dataset. Partial / delta scripts are REJECTED after " +
			"export — the error lists every offending table. Pipeline: (1) seed the inherit base " +
			"into the configured local env (a runtime safety net, not a data source the SQL relies " +
			"on), (2) apply your SQL inside a transaction, (3) export the resulting tables back " +
			"to CSV as a new revision, (4) validate the SQL against the populated tables, " +
			"(5) save the SQL as dataset.sql so it can be retrieved later with get_dataset_sql. " +
			"Read seedmancer://docs/local-generation first for the SQL contract and examples, " +
			"then call describe_schema to get the exact column names before writing the SQL. " +
			"`inherit` is REQUIRED. Pass `prompt` with the user's natural-language purpose for " +
			"this test data — it is saved on the scenario and reused later (by refresh and by " +
			"future regenerations) to keep the data's intent. " +
			"Pointers.latest advances to the new revision automatically. " +
			"Note: this overwrites data in the configured local env (the SQL runs against it). " +
			"No cloud API, no quota, and no internet connection are needed.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: truePtr(), IdempotentHint: false},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.GenerateLocalInput) (*mcp.CallToolResult, cmd.GenerateLocalOutput, error) {
		out, err := cmd.RunGenerateLocal(ctx, in)
		return nil, out, err
	})


	mcp.AddTool(s, &mcp.Tool{
		Name:        "push_dataset",
		Title:       "Push scenario to cloud",
		Description: "Upload a scenario's latest revision (CSVs + schema sidecars) to the connected Seedmancer account. The scenario path is used as the cloud dataset name.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: falsePtr(), IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.SyncInput) (*mcp.CallToolResult, cmd.SyncOutput, error) {
		out, err := cmd.RunSync(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "pull_dataset",
		Title:       "Pull scenario from cloud",
		Description: "Download a cloud-stored scenario as a new local revision. Pointers.latest advances to the new revision so seed_database picks it up immediately.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: falsePtr(), IdempotentHint: false},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.FetchInput) (*mcp.CallToolResult, cmd.FetchOutput, error) {
		out, err := cmd.RunFetch(ctx, in)
		return nil, out, err
	})

	// ── Scenario revisions ───────────────────────────────────────────
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_history",
		Title:       "List scenario revisions",
		Description: "Show every revision of a scenario newest-first, marking which is latest.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.HistoryInput) (*mcp.CallToolResult, cmd.HistoryOutput, error) {
		out, err := cmd.RunHistory(ctx, in)
		return nil, out, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "check_scenario",
		Title:       "Check scenario schema vs. live DB",
		Description: "Compare a scenario revision's stored schema with the live database schema and return a structured diff.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.CheckInput) (*mcp.CallToolResult, cmd.CheckOutput, error) {
		out, err := cmd.RunCheck(ctx, in)
		return nil, out, err
	})

	// ── Schema drift and refresh ──────────────────────────────────────

	mcp.AddTool(s, &mcp.Tool{
		Name:  "check_state_schema",
		Title: "Check scenario schema drift (structured)",
		Description: "Compare a scenario revision's stored schema with the live database schema. " +
			"Returns a structured drift report with changes classified as auto/likely/decision/breaking. " +
			"Use this to inspect schema drift, then fix it locally: get_dataset_sql (reference) → " +
			"rewrite the full SQL script to account for schema changes → generate_dataset_local.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in cmd.CheckStateSchemaInput) (*mcp.CallToolResult, cmd.CheckStateSchemaOutput, error) {
		out, err := cmd.RunCheckStateSchema(ctx, in)
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
