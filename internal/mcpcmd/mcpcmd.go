// Package mcpcmd hosts the `seedmancer mcp` subcommand. It lives outside
// the cmd/ package to avoid an import cycle: internal/mcp imports cmd
// (to call the Run* helpers), and this subcommand imports internal/mcp.
package mcpcmd

import (
	"context"
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"github.com/KazanKK/seedmancer/internal/mcp"
	"github.com/urfave/cli/v2"
)

// Command returns the `seedmancer mcp` subcommand definition.
//
// The subcommand is intentionally minimal: it just translates flags into
// an mcp.Config and blocks in mcp.Run until the client disconnects or a
// signal arrives. All the real work is in internal/mcp/.
//
// Notes on UX:
//   - stdio is the default because that's how Cursor/Claude Desktop
//     spawn local MCP servers. The server MUST NOT write to stdout in
//     this mode — stdout is reserved for JSON-RPC frames. Pass
//     --log-file if you need runtime visibility.
//   - --transport http exposes a streamable-HTTP endpoint for hosted
//     or multi-tenant setups. Address defaults to :7801.
func Command() *cli.Command {
	return &cli.Command{
		Name:  "mcp",
		Usage: "Run Seedmancer as a Model Context Protocol server.",
		Description: "Start an MCP server that exposes Seedmancer's tools (seed, export, " +
			"generate, sync, …) to AI agents.\n\n" +
			"Default transport is stdio, matching how Cursor and Claude Desktop spawn\n" +
			"local MCP servers. Use --transport http for hosted setups.\n\n" +
			"Important: stdio mode owns stdout for JSON-RPC; pass --log-file to see logs.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "transport",
				Usage:   "Transport: stdio (default) or http",
				Value:   "stdio",
				EnvVars: []string{"SEEDMANCER_MCP_TRANSPORT"},
			},
			&cli.StringFlag{
				Name:    "addr",
				Usage:   "Listen address for --transport http (default :7801)",
				EnvVars: []string{"SEEDMANCER_MCP_ADDR"},
			},
			&cli.StringFlag{
				Name:    "log-file",
				Usage:   "Path to append logs to (recommended for stdio; defaults to silent)",
				EnvVars: []string{"SEEDMANCER_MCP_LOG_FILE"},
			},
		},
		Action: func(c *cli.Context) error {
			transport := strings.ToLower(strings.TrimSpace(c.String("transport")))
			if transport == "" {
				transport = "stdio"
			}
			if transport != "stdio" && transport != "http" {
				return fmt.Errorf("unsupported transport %q (expected stdio or http)", transport)
			}

			// SIGINT/SIGTERM triggers a graceful shutdown; stdio clients
			// close the pipe instead, so ctx.Done() also fires there.
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			cfg := mcp.Config{
				Transport: transport,
				Addr:      strings.TrimSpace(c.String("addr")),
				LogFile:   strings.TrimSpace(c.String("log-file")),
			}
			return mcp.Run(ctx, cfg)
		},
	}
}
