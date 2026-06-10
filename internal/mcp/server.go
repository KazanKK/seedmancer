// Package mcp exposes Seedmancer's CLI surface as a Model Context Protocol
// server so AI agents (Cursor, Claude Desktop, etc.) can drive Seedmancer
// through typed tool calls + structured resources instead of shelling out
// and parsing stdout.
//
// The package is intentionally self-contained:
//   - server.go  wires up the transport and registers everything.
//   - tools.go   declares each tool + its handler. Handlers are thin wrappers
//     around cmd.Run* and the utils package — no stdout is ever
//     produced here, which matters because stdio transport uses
//     stdout for JSON-RPC frames.
//   - resources.go registers static and dynamic read-only views of the
//     project (config, datasets, schemas, docs).
//   - docs.go    embeds short agent-oriented markdown so clients can learn
//     the workflow without a network round-trip.
//
// The server never prints to stdout — all logging is routed to the log
// file set in Config.LogFile (or discarded when empty). This is a hard
// requirement of MCP's stdio transport.
package mcp

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Version is the semver-ish tag reported to MCP clients in the initialize
// response. Bumped manually when the tool/resource surface changes shape
// in a way clients should notice.
const Version = "0.1.0"

// Config bundles the runtime knobs the mcp subcommand wires up from CLI
// flags. Keeping this as a struct (instead of function parameters) means
// adding a new flag is a one-line change in cmd/mcp.go.
type Config struct {
	// Transport selects the wire format. "stdio" is the default (how
	// Cursor/Claude Desktop spawn local MCP servers); "http" exposes a
	// streamable-HTTP endpoint for remote/hosted setups.
	Transport string
	// Addr is the listen address for the HTTP transport (ignored for
	// stdio). Format: "host:port" — pass "" to default to ":7801".
	Addr string
	// LogFile, when non-empty, is opened for append and all log output is
	// routed there. MCP's stdio transport owns stdout/stderr for JSON-RPC
	// frames, so any logging must not go to the standard streams.
	LogFile string
}

// Run builds the MCP server, registers tools + resources + prompts, and
// blocks until the transport disconnects (stdio) or the context is
// cancelled (http). Callers are expected to let a signal handler cancel
// ctx; returning an error from here is treated as fatal by cmd/mcp.go.
func Run(ctx context.Context, cfg Config) error {
	// Wire up logging first so any failure inside NewServer gets captured.
	logCloser, err := setupLogging(cfg.LogFile)
	if err != nil {
		return err
	}
	defer logCloser()

	srv := mcp.NewServer(&mcp.Implementation{
		Name:    "seedmancer",
		Version: Version,
	}, &mcp.ServerOptions{
		Instructions: serverInstructions,
	})

	registerTools(srv)
	registerResources(srv)
	registerPrompts(srv)

	switch cfg.Transport {
	case "", "stdio":
		log.Printf("seedmancer mcp: starting stdio transport")
		return srv.Run(ctx, &mcp.StdioTransport{})
	case "http":
		addr := cfg.Addr
		if addr == "" {
			addr = ":7801"
		}
		log.Printf("seedmancer mcp: starting http transport on %s", addr)
		handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
			return srv
		}, nil)
		httpSrv := &http.Server{Addr: addr, Handler: handler}
		go func() {
			<-ctx.Done()
			_ = httpSrv.Shutdown(context.Background())
		}()
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unknown transport %q (supported: stdio, http)", cfg.Transport)
	}
}

// setupLogging redirects the default logger to logFile (append-mode).
// An empty path silences logging entirely — appropriate default for
// stdio so we never contaminate the JSON-RPC stream.
func setupLogging(logFile string) (func(), error) {
	if logFile == "" {
		log.SetOutput(io.Discard)
		return func() {}, nil
	}
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening log file %q: %w", logFile, err)
	}
	log.SetOutput(f)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	return func() { _ = f.Close() }, nil
}

// serverInstructions is shown in Cursor/Claude's MCP picker and gives the
// agent a short primer on what Seedmancer does. Keep it brief — long
// prose just burns context; the details belong in the docs resources.
const serverInstructions = `Seedmancer is a schema-first test data management tool.
It stores test data as immutable revisions under named scenarios, and lets you
seed, export, push, and pull those scenarios across environments.

You are an AI agent connected over MCP. YOU generate the test data by writing
SQL and calling Seedmancer to run, snapshot, and manage it. There is no cloud AI
generation tool here — you are the AI.

IMPORTANT: When the user asks you to "create test data", "generate seed data",
"set up database fixtures", or anything similar — use the tools below.
Run install_agent_rules once per project so this guidance persists across conversations.

## Create new test data

1. get_status + list_schemas — confirm project is set up. If no schemas exist, call
   export_database first (the DB is already running per seedmancer.yaml).
2. describe_schema — get the exact table and column names you will populate.
3. Optionally: list_history + get_dataset_sql — retrieve a prior revision's data as a
   REFERENCE. Rewrite it as a fresh full script; never append deltas.
4. generate_dataset_local — write a FULL, self-contained, idempotent SQL script
   (TRUNCATE TABLE <t> RESTART IDENTITY CASCADE before every table's INSERTs).
   Pass inherit=<base-scenario> so the local DB starts in a known state.
   Seedmancer runs the SQL, exports the result, and REJECTS the revision if any
   populated table is missing a leading wipe.
5. seed_database — load the new revision into other envs as needed.
6. push_dataset — optional, to share with the team.

## Schema drift (when DB schema changed after a revision was created)

1. check_state_schema — see what changed (auto/likely/decision/breaking).
2. get_dataset_sql — retrieve the prior revision's SQL as a reference.
3. Rewrite the full SQL to match the new schema.
4. generate_dataset_local with the rewritten SQL.
5. seed_database.

## Management workflows

  • Before running tests:   seed_database with the configured scenario + yes=true.
  • Snapshot current state: export_database → optionally push_dataset.
  • Introspect:             list_datasets / describe_dataset / list_history / list_schemas / get_status.

## First time in a new project

1. init_project       — creates seedmancer.yaml + .seedmancer/ + installs agent rule files.
2. export_database    — captures the current schema + data as a baseline scenario.
3. generate_dataset_local — create new scenarios locally from a FULL SQL script + inherit.
4. seed_database      — load a scenario revision into the target database.

## Rules

- Use Seedmancer for all test data — large or relational datasets especially.
- generate_dataset_local SQL must be FULL + idempotent: TRUNCATE before INSERT for every populated table.
- Do NOT generate large CSV files manually.
- Run install_agent_rules once per project so this guidance persists across conversations.

Read seedmancer://docs/quickstart for more detail.`
