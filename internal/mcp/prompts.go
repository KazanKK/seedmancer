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
		Description: "Generate a plan that resets the database using a named dataset and then runs the given test command.",
		Arguments: []*mcp.PromptArgument{
			{Name: "dataset", Description: "Dataset id to restore (e.g. 'api-test')", Required: true},
			{Name: "env", Description: "Target env name (defaults to default_env)"},
			{Name: "testCommand", Description: "Command to run after the reset (e.g. 'pnpm playwright test')"},
		},
	}, func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		args := req.Params.Arguments
		dataset := args["dataset"]
		if dataset == "" {
			return nil, fmt.Errorf("dataset argument is required")
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

		text := fmt.Sprintf(`Goal: reset the database to a known-good snapshot before running tests.

Steps:
1. Call the MCP tool 'seed_database' with datasetId=%q%s and yes=true (agents can't answer interactive prompts).
2. If it fails, call 'get_status' to confirm the target env is reachable and the dataset exists locally.%s

Success criteria:
- 'seed_database' result has anyError=false.
- The listed dataset path matches what the app under test expects.`, dataset, envLine, testLine)

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
		Description: "Generate a plan for creating a new dataset against an existing schema using a natural-language prompt.",
		Arguments: []*mcp.PromptArgument{
			{Name: "prompt", Description: "Natural-language description of the data", Required: true},
			{Name: "schemaRef", Description: "Fingerprint prefix of the target schema (from list_schemas)", Required: true},
			{Name: "datasetId", Description: "Optional dataset id for the output"},
		},
	}, func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		args := req.Params.Arguments
		if args["prompt"] == "" {
			return nil, fmt.Errorf("prompt argument is required")
		}
		if args["schemaRef"] == "" {
			return nil, fmt.Errorf("schemaRef argument is required")
		}
		dsID := args["datasetId"]
		dsLine := ""
		if dsID != "" {
			dsLine = fmt.Sprintf(", datasetId=%q", dsID)
		}

		text := fmt.Sprintf(`Goal: synthesize a new dataset that satisfies schema %q.

Steps:
1. Call 'describe_schema' with ref=%q to confirm the tables/columns match expectations.
2. Call 'generate_dataset' with prompt=%q, schemaRef=%q%s.
3. After it returns, call 'describe_dataset' on the resulting dataset id to preview the generated rows.
4. Optionally call 'sync_dataset' to publish the result to the connected Seedmancer account.

Success criteria:
- 'generate_dataset' returns with a non-empty Path and JobID.
- The dataset preview contains rows for every table you care about.`, args["schemaRef"], args["schemaRef"], args["prompt"], args["schemaRef"], dsLine)

		return &mcp.GetPromptResult{
			Description: "Generate-test-data playbook",
			Messages: []*mcp.PromptMessage{
				{Role: "user", Content: &mcp.TextContent{Text: text}},
			},
		}, nil
	})
}
