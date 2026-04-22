# AGENTS.md

Guidance for AI agents (Cursor, Claude Code, Continue, Codex, …) working in
this repository.

## What Seedmancer is

Schema-first PostgreSQL seeding CLI. Given a database, it dumps the schema
+ data into content-addressed folders, lets users sync datasets to a cloud
account, and restores them back on any target environment. The unit of
reuse is a **dataset** (schema fingerprint + CSV files).

## Prefer the MCP server over shelling out

The binary ships an MCP server (`seedmancer mcp`) that exposes the full
CLI surface as typed tools + structured resources. When your host
supports MCP, **use the tools instead of shelling out**:

- `seed_database` instead of `seedmancer seed --id X --yes`
- `list_datasets` / `describe_dataset` / `describe_schema` instead of
  parsing the CLI's human-readable output
- `export_database`, `generate_dataset`, `sync_dataset`, `fetch_dataset`
  for the snapshot/generate/push/pull flow

Call `login_info` first when you're unsure whether the host has an API
token configured.

## Safety rails worth knowing

- `seed_database` refuses to seed prod-like env names (`prod`,
  `production`, `live`, `main`, `master`) unless you pass `yes: true`.
  Agents can't answer interactive prompts, so this is the only gate.
- Destructive tools carry the MCP `destructiveHint` annotation — hosts
  may surface confirmation UI. Expect that.
- All state-mutating tools are **idempotent where possible**: re-running
  `seed_database` with the same dataset leaves the DB in the same state.

## Repository layout

| Path | Purpose |
|------|---------|
| `main.go` | CLI entry point; wires subcommands. |
| `cmd/` | One file per subcommand. The `Run*` functions in `cmd/runners.go` are the stdout-free logic shared with the MCP server. |
| `internal/mcp/` | MCP server: tools, resources, prompts, docs. |
| `internal/mcpcmd/` | `seedmancer mcp` subcommand (outside `cmd/` to avoid an import cycle with `internal/mcp`). |
| `internal/utils/` | Config, token resolution, schema fingerprint helpers. |
| `internal/ui/` | Human-facing spinners, colors, logging. Never imported from MCP paths — stdio owns stdout. |
| `database/` | Postgres-specific export/restore glue. |

## Conventions

- Same logic for every supported database, per `.cursorrules`. Don't
  branch on driver type inside `cmd/` — do it once in `database/`.
- Keep UI out of `Run*` functions. They must not write to stdout/stderr
  so the MCP stdio transport stays clean.
- Reach for existing helpers in `internal/utils` before duplicating
  path/config/token logic.
- Tests sit alongside the code they exercise; run `go test ./...` (plus
  `GOTOOLCHAIN=auto` if your system Go is older than the one in
  `go.mod`).

## Quick verification loop

```sh
GOTOOLCHAIN=auto go build ./...
GOTOOLCHAIN=auto go test ./...
```

Both should be green before you hand changes back.
