package mcp

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// registerPrompts attaches parameterised prompt templates. They give
// agents a shortcut for the two flows Seedmancer exists for: resetting a
// DB before tests, and generating new data against an existing schema.
//
// Prompts are returned as user-role PromptMessages — the host stitches
// them into the conversation before the model sees them.
func registerPrompts(s *mcp.Server) {
	s.AddPrompt(&mcp.Prompt{
		Name:        "reset_db_for_tests",
		Title:       "Reset DB for tests",
		Description: "Generate a plan that resets the database to a scenario revision and runs the given test command.",
		Arguments: []*mcp.PromptArgument{
			{Name: "scenario", Description: "Scenario path to seed (e.g. 'api-test')", Required: true},
			{Name: "env", Description: "Target env name (defaults to default_env)"},
			{Name: "useStable", Description: "Set to 'true' to load the pinned stable revision"},
			{Name: "testCommand", Description: "Command to run after the reset (e.g. 'pnpm playwright test')"},
		},
	}, func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		args := req.Params.Arguments
		scenarioArg := args["scenario"]
		if scenarioArg == "" {
			return nil, fmt.Errorf("scenario argument is required")
		}
		env := args["env"]
		testCmd := args["testCommand"]
		useStable := args["useStable"] == "true"

		envLine := ""
		if env != "" {
			envLine = fmt.Sprintf(" (env: %s)", env)
		}
		stableLine := ""
		if useStable {
			stableLine = " and useStable=true"
		}
		testLine := ""
		if testCmd != "" {
			testLine = fmt.Sprintf("\n3. Run the test command: `%s`", testCmd)
		}

		text := fmt.Sprintf(`Goal: reset the database to a known-good scenario revision before running tests.

Steps:
1. Call the MCP tool 'seed_database' with scenario=%q%s%s and yes=true (agents can't answer interactive prompts).
2. If it fails on a schema mismatch, call 'check_scenario' to see the diff. Re-export the scenario or pass force=true if the diff is intentional.%s

Success criteria:
- 'seed_database' result has anyError=false.
- The listed scenario revision matches what the app under test expects.`, scenarioArg, envLine, stableLine, testLine)

		return &mcp.GetPromptResult{
			Description: "Reset-the-DB-before-tests playbook",
			Messages: []*mcp.PromptMessage{
				{Role: "user", Content: &mcp.TextContent{Text: text}},
			},
		}, nil
	})

	s.AddPrompt(&mcp.Prompt{
		Name:        "generate_test_data",
		Title:       "Generate test data",
		Description: "Generate a plan for creating a new revision under a scenario using a SQL block on top of an inherit base.",
		Arguments: []*mcp.PromptArgument{
			{Name: "scenario", Description: "Scenario path for the new revision (e.g. 'billing/pro')", Required: true},
			{Name: "inherit", Description: "REQUIRED base scenario; its latest revision is seeded into the local env before the SQL runs", Required: true},
		},
	}, func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		args := req.Params.Arguments
		if args["scenario"] == "" {
			return nil, fmt.Errorf("scenario argument is required")
		}
		if args["inherit"] == "" {
			return nil, fmt.Errorf("inherit argument is required")
		}

		text := fmt.Sprintf(`Goal: synthesize a new revision under scenario %q locally.

Steps:
1. Read seedmancer://docs/local-generation for the SQL contract and examples.
2. Call 'describe_schema' to get exact table and column names.
3. Before writing fresh SQL, check 'list_history' for prior revisions with hasSql=true
   and 'get_dataset_sql' to retrieve and edit existing SQL instead of starting over.
4. Call 'generate_dataset_local' with scenario=%q, inherit=%q, and a SQL block of
   INSERT/UPDATE/DELETE statements. Seedmancer seeds the inherit base into the
   configured local env, runs your SQL in a transaction, then exports the result.
5. Call 'seed_database' with the scenario path to load the revision into other envs.
6. Optionally call 'pin_scenario' to mark the revision as stable, or 'push_dataset' to publish.

Success criteria:
- 'generate_dataset_local' returns with a non-empty Path, SQLPath, and a new revision id.
- The revision's dataset.sql round-trips through 'get_dataset_sql' unchanged.`, args["scenario"], args["scenario"], args["inherit"])

		return &mcp.GetPromptResult{
			Description: "Generate-test-data playbook (local)",
			Messages: []*mcp.PromptMessage{
				{Role: "user", Content: &mcp.TextContent{Text: text}},
			},
		}, nil
	})
}
