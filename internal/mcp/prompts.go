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

		envLine := ""
		if env != "" {
			envLine = fmt.Sprintf(" (env: %s)", env)
		}
		testLine := ""
		if testCmd != "" {
			testLine = fmt.Sprintf("\n3. Run the test command: `%s`", testCmd)
		}

		text := fmt.Sprintf(`Goal: reset the database to a known-good scenario revision before running tests.

Steps:
1. Call the MCP tool 'seed_database' with scenario=%q%s and yes=true (agents can't answer interactive prompts).
2. If it fails on a schema mismatch, call 'check_scenario' to see the diff. Re-export the scenario or pass force=true if the diff is intentional.%s

Success criteria:
- 'seed_database' result has anyError=false.
- The listed scenario revision matches what the app under test expects.`, scenarioArg, envLine, testLine)

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
		Description: "Generate a plan for creating a new scenario revision locally. You write the data; Seedmancer manages it.",
		Arguments: []*mcp.PromptArgument{
			{Name: "scenario", Description: "Scenario path for the new revision (e.g. 'billing/pro')", Required: true},
			{Name: "inherit", Description: "Base scenario to seed into the local DB before your data runs (required)", Required: true},
			{Name: "prompt", Description: "Natural-language description of the data to generate (guidance for you as you write the SQL)"},
		},
	}, func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		args := req.Params.Arguments
		if args["scenario"] == "" {
			return nil, fmt.Errorf("scenario argument is required")
		}
		if args["inherit"] == "" {
			return nil, fmt.Errorf("inherit argument is required")
		}

		guidance := ""
		if v := args["prompt"]; v != "" {
			guidance = fmt.Sprintf("\n\nData requirements from the user:\n%s", v)
		}

		text := fmt.Sprintf(`Goal: create a new revision under scenario %q using local generation.
You are the AI. You write the data; Seedmancer manages the revision.%s

Steps:
1. Call 'describe_schema' to get exact table and column names for every table you will populate.
2. Optionally call 'list_history' then 'get_dataset_sql' to see what a prior revision contained.
   Use it as a REFERENCE only — do NOT patch it with deltas. Rewrite the whole script.
3. Write a FULL, self-contained, idempotent SQL script:
   - Start with one TRUNCATE covering every table you populate:
       TRUNCATE TABLE a, b, c RESTART IDENTITY CASCADE;
   - Follow with INSERT statements for every populated table.
   - Running the script twice must produce the same state.
4. Call 'generate_dataset_local' with scenario=%q, inherit=%q, and your SQL.
   Seedmancer seeds the inherit base first as a safety net, runs your SQL, exports
   the result as CSVs, and REJECTS the revision if any populated table is missing a wipe.
5. Call 'seed_database' with the scenario path to load the revision into the target env.

Success criteria:
- 'generate_dataset_local' returns with a non-empty Path and a new revision id.
- Every populated table has a TRUNCATE before its INSERTs in the SQL.`, args["scenario"], guidance, args["scenario"], args["inherit"])

		return &mcp.GetPromptResult{
			Description: "Generate-test-data playbook (local)",
			Messages: []*mcp.PromptMessage{
				{Role: "user", Content: &mcp.TextContent{Text: text}},
			},
		}, nil
	})
}
