# Seedmancer CLI

Schema-first database seeding — export, AI-generate, sync, and restore PostgreSQL snapshots from the command line.

Seedmancer dumps your database schema + data into content-addressed folders, lets AI fabricate realistic rows against that schema, and syncs datasets to the cloud so teammates can pull them back.

## Features

- **Schema fingerprinting** — every export is hashed; two dumps of the same schema share a folder, so repeated syncs stay idempotent.
- **Snapshot & restore** — dump the live database to CSV, restore it back at any time. Functions and triggers are preserved via SQL sidecars.
- **AI data generation** — describe the data you want in plain English; Seedmancer's cloud worker generates matching CSVs against your actual schema.
- **Cloud sync** — push datasets to your Seedmancer account and `fetch` them on any machine (CI/CD friendly).
- **PostgreSQL today, MySQL on the roadmap.**

## Installation

Download the latest binary from the [GitHub releases page](https://github.com/KazanKK/Seedmancer/releases).

### Linux (arm64)

```sh
curl -L https://github.com/KazanKK/seedmancer/releases/latest/download/seedmancer_Linux_arm64 -o seedmancer
chmod +x seedmancer
sudo mv seedmancer /usr/local/bin/seedmancer
```

### Linux (x86_64)

```sh
curl -L https://github.com/KazanKK/seedmancer/releases/latest/download/seedmancer_Linux_x86_64 -o seedmancer
chmod +x seedmancer
sudo mv seedmancer /usr/local/bin/seedmancer
```

### macOS (arm64)

```sh
curl -L https://github.com/KazanKK/seedmancer/releases/latest/download/seedmancer_Darwin_arm64 -o seedmancer
chmod +x seedmancer
sudo mv seedmancer /usr/local/bin/seedmancer
```

## Quick Start

```sh
# 1. Initialize — writes seedmancer.yaml in the current directory.
seedmancer init

# 2. Sign in (opens your browser; skip this for local-only workflows).
seedmancer login

# 3. Export the current database state as a dataset.
seedmancer export --id baseline

# 4. Restore the database to that dataset at any time.
seedmancer seed --id baseline

# 5. (Optional) Push the dataset to the cloud so teammates can pull it.
seedmancer sync --id baseline
```

You can hand-edit the exported CSV files to curate test data. Never edit `schema.json` — it's the fingerprint source of truth.

## On-disk layout

`seedmancer export` stores data under `<storagePath>/schemas/<fp-short>/` where `<fp-short>` is the first 12 characters of the schema fingerprint. Multiple datasets share one schema folder:

```
.seedmancer/
  schemas/
    a1b2c3d4e5f6/               # ← SHA-256 prefix of schema.json
      schema.json               # source of truth for the fingerprint
      meta.yaml                 # optional user-editable display name
      *_func.sql                # function sidecars (one per function)
      *_trigger.sql             # trigger sidecars
      datasets/
        baseline/
          users.csv
          orders.csv
        20260420153022/         # auto-generated dataset id
          users.csv
          ...
```

## Command Reference

Run `seedmancer --help` or `seedmancer <command> --help` to see flags inline.

### Global flags

| Flag | Description | Env |
|------|-------------|-----|
| `--debug` | Show detailed debug output | `SEEDMANCER_DEBUG` |

### `seedmancer init`

Create `seedmancer.yaml` and the local storage folder. Interactive when run in a TTY; pass flags for CI.

| Flag | Description | Default |
|------|-------------|---------|
| `--storage-path` | Directory for local schema folders | `.seedmancer` |
| `--database-url` | Default PostgreSQL connection URL | *(prompted)* |

### `seedmancer login`

Sign in via a browser-based flow and save the API token to `~/.seedmancer/credentials` (mode 0600). Credentials are kept out of `seedmancer.yaml` by design so project config can be committed.

| Flag | Description | Env |
|------|-------------|-----|
| `--token` | Existing API token (skips the browser flow) | |
| `--dashboard-url` | Dashboard origin serving `/auth/cli` | `SEEDMANCER_DASHBOARD_URL` |
| `--no-browser` | Print the URL instead of opening a browser | |
| `--timeout` | How long to wait for the callback (default 5m) | |

### `seedmancer logout`

Delete the saved API token. Warns when `SEEDMANCER_API_TOKEN` is still set in the shell (the env var becomes authoritative once the credentials file is gone).

### `seedmancer status`

Show the effective configuration the CLI will use right now — which `seedmancer.yaml` was picked up, the API URL and its source, whether you're signed in and from where, plus a reachability probe. Great first stop when debugging "why isn't command X working?".

| Flag | Description |
|------|-------------|
| `--offline` | Skip the API reachability check |
| `--show-db-url` | Show `database_url` with credentials (default masks the password) |
| `--json` | Emit machine-readable snapshot for CI |

### `seedmancer export`

Dump the current database schema and data into a fingerprint-keyed schema folder, with the CSVs living under `datasets/<id>/`.

| Flag | Description | Required | Env |
|------|-------------|----------|-----|
| `--id` | Dataset id for the new dump (auto-generates a timestamp when omitted) | No | |
| `--db-url` | Source database URL | Yes (or `database_url:` in config) | `SEEDMANCER_DATABASE_URL` |
| `--force`, `-y` | Overwrite an existing dataset without confirmation | No | |

### `seedmancer seed`

Restore a previously-exported dataset into the target database. Tables are truncated and reloaded; functions and triggers are replayed from the SQL sidecars.

| Flag | Description | Required | Env |
|------|-------------|----------|-----|
| `--dataset-id`, `-d`, `--id` | Dataset id to restore | Yes | |
| `--db-url` | Target database URL | Yes (or `database_url:` in config) | `SEEDMANCER_DATABASE_URL` |

### `seedmancer generate`

Send your local schema plus a natural-language prompt to Seedmancer's AI generation worker, then stream the resulting CSVs into a new dataset folder.

| Flag | Description | Required | Env |
|------|-------------|----------|-----|
| `--prompt` | Natural-language description of data to generate | Yes | |
| `--schema-id` | Schema fingerprint short id to generate for (only needed when multiple local schemas exist) | No | |
| `--token` | API token | No (see auth precedence) | `SEEDMANCER_API_TOKEN` |

Example:

```sh
seedmancer generate --prompt "50 users with realistic names and emails, 200 orders"

# Then seed the generated dataset (the CLI prints its auto-generated id):
seedmancer seed --dataset-id 20260420153022
```

### `seedmancer list`

List datasets grouped by schema fingerprint, newest first. Shows both local and remote by default.

| Flag | Description | Env |
|------|-------------|-----|
| `--local` | Show only local datasets | |
| `--remote` | Show only remote datasets | |
| `--token` | API token (required for `--remote`) | `SEEDMANCER_API_TOKEN` |
| `--json` | Emit machine-readable output | |

### `seedmancer sync`

Upload one local dataset to your Seedmancer cloud account. The target schema is derived from `schema.json`'s fingerprint — no schema id needed.

| Flag | Description | Required | Env |
|------|-------------|----------|-----|
| `--dataset-id`, `-d`, `--id` | Dataset id to upload | Yes | |
| `--token` | API token | No (see auth precedence) | `SEEDMANCER_API_TOKEN` |

### `seedmancer fetch`

Download a cloud dataset and unpack it under `<storagePath>/schemas/<fp-short>/datasets/<name>/` so `seedmancer seed` can load it immediately.

| Flag | Description | Required | Env |
|------|-------------|----------|-----|
| `--dataset-id`, `-d`, `--id` | Dataset id to download | Yes | |
| `--output`, `-o` | Custom output directory (bypasses the schema-first layout) | No | |
| `--token` | API token | No (see auth precedence) | `SEEDMANCER_API_TOKEN` |
| `--json` | Emit machine-readable output | |

### `seedmancer schemas`

Inspect and manage schemas on both sides.

| Subcommand | What it does |
|------------|--------------|
| `schemas list` | Show every schema known locally and remotely, newest first. Supports `--local` / `--remote` / `--json`. |
| `schemas rename <fp-or-id> <name>` | Attach a human-friendly display name. `--clear` (or passing `""`) removes it. |
| `schemas rm <fp-or-id>` | Delete a schema locally and/or from the cloud. `--force` skips the prompt. |

## Configuration

### Project config — `seedmancer.yaml`

Created by `seedmancer init` in your project root. Safe to commit (no secrets).

| Key | Description | Default |
|-----|-------------|---------|
| `storage_path` | Directory for local schema folders | `.seedmancer` |
| `database_url` | Default PostgreSQL connection URL | *(unset)* |
| `api_url` | Seedmancer API base URL | `https://api.seedmancer.dev` |

### Credentials — `~/.seedmancer/credentials`

Written by `seedmancer login` (mode 0600, 0700 on the parent directory). Contains the API token only. Cleared by `seedmancer logout`.

## Environment Variables

| Variable | Flag equivalent | Used by |
|----------|-----------------|---------|
| `SEEDMANCER_DATABASE_URL` | `--db-url` | export, seed |
| `SEEDMANCER_API_TOKEN` | `--token` | generate, sync, fetch, list, schemas |
| `SEEDMANCER_API_URL` | — | all commands (API origin override) |
| `SEEDMANCER_DASHBOARD_URL` | `--dashboard-url` | login |
| `SEEDMANCER_DEBUG` | `--debug` | all commands |

### Token resolution order

1. Explicit `--token` flag
2. `~/.seedmancer/credentials` (written by `seedmancer login`)
3. `SEEDMANCER_API_TOKEN` environment variable
4. Legacy `api_token:` in `seedmancer.yaml` (read-only back-compat)

The credentials file intentionally wins over the env var so `seedmancer login` always "sticks" even when a stale `SEEDMANCER_API_TOKEN` is exported in your shell.

## Development

```sh
# Build the binary
go build -o seedmancer .

# Run the full test suite (unit tests only — integration tests need a Postgres)
go test ./...
```

Contributions welcome. See the issue tracker for open work.

## License

MIT — [https://seedmancer.dev](https://seedmancer.dev)
