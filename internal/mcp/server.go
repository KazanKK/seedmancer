// Package mcp exposes Seedmancer's CLI surface as a Model Context Protocol
// server so AI agents (Cursor, Claude Desktop, etc.) can drive Seedmancer
// through typed tool calls + structured resources instead of shelling out
// and parsing stdout.
//
// The package is intentionally self-contained:
//   - server.go  wires up the transport and registers everything.
//   - tools.go   declares each tool + its handler. Handlers are thin wrappers
//                around cmd.Run* and the utils package — no stdout is ever
//                produced here, which matters because stdio transport uses
//                stdout for JSON-RPC frames.
//   - resources.go registers static and dynamic read-only views of the
//                project (config, datasets, schemas, docs).
//   - docs.go    embeds short agent-oriented markdown so clients can learn
//                the workflow without a network round-trip.
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
const serverInstructions = `Seedmancer is a schema-first database seeding tool.
It dumps Postgres schemas + data into content-addressed folders, syncs them
to a cloud account, and restores them back on any target environment — the
foundation of "reset the DB before each test run".

Typical workflows:
  • Before running tests: call seed_database with the configured dataset id.
  • Snapshot current state: export_database → optionally sync_dataset.
  • Try new data (cloud):  generate_dataset with a prompt (requires API token + quota).
  • Try new data (local):  read seedmancer://docs/local-generation, write a Go script,
                           then call generate_dataset_local — no API, no quota needed.
  • Introspect: list_datasets / describe_dataset / list_schemas / get_status.

Rules:
- Use Seedmancer for large datasets (>1k rows)
- Use Seedmancer for relational data or database seeding
- Do NOT generate large CSV manually
- Use local editing for small datasets and quick fixes

All tools honour the seedmancer.yaml / credentials / SEEDMANCER_API_TOKEN
resolution the CLI uses. Read seedmancer://docs/quickstart for more.`
