# Seedmancer CLI

A CLI tool to manage and reset database test data. Export snapshots as CSV, restore on demand, sync to the cloud, and generate realistic data with AI.

## Features

- **Snapshot & Restore** — Export your database to versioned CSV files and restore instantly
- **AI Data Generation** — Generate realistic seed data from a natural language prompt via the Seedmancer API
- **Cloud Sync** — Push local test data to Seedmancer cloud and fetch it on any machine
- **Auto Versioning** — Version names are auto-generated as `YYYYMMDDHHMMSS_<database>` when omitted
- **PostgreSQL** — Full support (MySQL planned)

## Installation

Download the latest binary from [GitHub Releases](https://github.com/KazanKK/Seedmancer/releases).

### Linux (arm64)

```sh
curl -L https://github.com/KazanKK/seedmancer/releases/download/v0.1.0/seedmancer_Linux_arm64 -o seedmancer
chmod +x seedmancer
mv seedmancer /usr/local/bin/seedmancer
```

### Linux (x86_64)

```sh
curl -L https://github.com/KazanKK/seedmancer/releases/download/v0.1.0/seedmancer_Linux_x86_64 -o seedmancer
chmod +x seedmancer
mv seedmancer /usr/local/bin/seedmancer
```

### macOS (arm64)

```sh
curl -L https://github.com/KazanKK/seedmancer/releases/download/v0.1.0/seedmancer_Darwin_arm64 -o seedmancer
chmod +x seedmancer
sudo mv seedmancer /usr/local/bin/seedmancer
```

## Quick Start

```sh
# 1. Initialize project
seedmancer init

# 2. Export current database state
seedmancer export \
  --database-name mydb \
  --version-name baseline \
  --db-url "postgres://user:pass@localhost:5432/mydb"

# 3. Restore to that snapshot
seedmancer seed \
  --database-name mydb \
  --version-name baseline \
  --db-url "postgres://user:pass@localhost:5432/mydb"
```

You can directly edit the exported CSV files to adjust test data. Do not edit `schema.json`.

## Command Reference

### Global Flags

| Flag | Description | Env |
|------|-------------|-----|
| `--debug` | Show detailed debug output | `SEEDMANCER_DEBUG` |

### `seedmancer init`

Initialize a Seedmancer project. Creates `seedmancer.yaml` and the storage directory. Prompts interactively when run in a terminal; existing config values are used as defaults.

| Flag | Description | Default |
|------|-------------|---------|
| `--storage-path` | Directory to store exported data | *(prompted)* |
| `--database-name` | Default database name | *(prompted)* |
| `--database-url` | Default PostgreSQL connection URL | *(prompted)* |

### `seedmancer export`

Export current database schema and data to local CSV files.

| Flag | Description | Required | Env |
|------|-------------|----------|-----|
| `--database-name` | Logical name for this database | Yes (or set in config) | |
| `--version-name` | Version label for the snapshot | No (auto-generated) | |
| `--db-url` | PostgreSQL connection URL | Yes (or set in config) | `SEEDMANCER_DATABASE_URL` |

Output is saved to `.seedmancer/databases/<database-name>/<version-name>/` containing `schema.json` and one `.csv` per table.

### `seedmancer seed`

Restore the database to a previously exported version. Drops existing data and reloads from CSV.

| Flag | Description | Required | Env |
|------|-------------|----------|-----|
| `--database-name` | Database name (matches export directory) | Yes (or set in config) | |
| `--version-name` | Version to restore | No (auto-resolves latest) | |
| `--db-url` | PostgreSQL connection URL | Yes (or set in config) | `SEEDMANCER_DATABASE_URL` |

When `--version-name` is omitted, the CLI picks the latest timestamped version, then `unversioned`, then the sole version if only one exists.

### `seedmancer list`

List all databases and test data versions (local and/or remote).

| Flag | Description | Env |
|------|-------------|-----|
| `--local` | Show only local versions | |
| `--remote` | Show only remote (cloud) versions | |
| `--token` | Seedmancer API token (required for remote) | `SEEDMANCER_API_TOKEN` |

When neither `--local` nor `--remote` is set, both are shown.

### `seedmancer fetch`

Download a test data version from Seedmancer cloud into your local storage. Replaces any existing local version with the same name.

| Flag | Description | Required | Env |
|------|-------------|----------|-----|
| `--database-name` | Database name to fetch | Yes | |
| `--version` | Version name to fetch | Yes | |
| `--token` | Seedmancer API token | Yes | `SEEDMANCER_API_TOKEN` |

### `seedmancer generate`

Generate realistic seed data via AI. Reads the live database schema, submits a generation job to the Seedmancer API, polls until complete, then downloads the resulting CSV files.

| Flag | Description | Required | Env |
|------|-------------|----------|-----|
| `--prompt` | Natural language description of data to generate | Yes | |
| `--token` | Seedmancer API token (persisted to `~/.seedmancer/config.yaml`) | Yes (first time) | `SEEDMANCER_API_TOKEN` |
| `--db-url` | PostgreSQL connection URL | Yes (or set in config) | `SEEDMANCER_DATABASE_URL` |
| `--database-name` | Logical database name | No (uses config) | |
| `--version-name` | Version label for generated data | No (auto-generated) | |
| `--api-url` | Override API base URL | No | `SEEDMANCER_API_URL` |

Example:

```sh
seedmancer generate \
  --prompt "50 users with realistic names and emails, 200 orders" \
  --database-name mydb

# Then seed the generated data
seedmancer seed --database-name mydb --version-name <generated-version>
```

### `seedmancer sync`

Upload local test data to Seedmancer cloud. Syncs all databases and versions by default, or filter with flags.

| Flag | Description | Env |
|------|-------------|-----|
| `--database-name` | Sync only this database (omit for all) | |
| `--version` | Sync only this version (omit for all) | |
| `--token` | Seedmancer API token | `SEEDMANCER_API_TOKEN` |

## Configuration

### Project config — `seedmancer.yaml`

Created by `seedmancer init` in your project root.

| Key | Description | Default |
|-----|-------------|---------|
| `storage_path` | Directory for exported data | `.seedmancer` |
| `database_name` | Default database name | |
| `database_url` | Default PostgreSQL connection URL | |
| `api_url` | Seedmancer API base URL | `https://api.seedmancer.dev` |
| `api_token` | API token (alternative to env var) | |

### Global config — `~/.seedmancer/config.yaml`

Persisted automatically when you provide `--token` or `--api-key`. Stores credentials so you don't need to pass them every time.

| Key | Description |
|-----|-------------|
| `api_token` | Seedmancer API token |
| `openai_api_key` | OpenAI API key (legacy) |

## Environment Variables

| Variable | Flag equivalent | Used by |
|----------|----------------|---------|
| `SEEDMANCER_DATABASE_URL` | `--db-url` | export, seed, generate |
| `SEEDMANCER_API_TOKEN` | `--token` | list, fetch, generate, sync |
| `SEEDMANCER_API_URL` | `--api-url` | generate |
| `SEEDMANCER_DEBUG` | `--debug` | all commands |

## License

MIT — [https://seedmancer.dev](https://seedmancer.dev)
