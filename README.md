# Seedmancer CLI

Reset your PostgreSQL or MySQL database to a saved state in seconds — export snapshots, restore them on demand, push to the cloud, pull from teammates, and generate test data locally with Go scripts.

→ **Full documentation: [seedmancer.dev/docs](https://seedmancer.dev/docs)**

## Installation

### Linux (arm64)

```sh
curl -L https://github.com/KazanKK/seedmancer/releases/latest/download/seedmancer_Linux_arm64 -o seedmancer
chmod +x seedmancer
sudo mv seedmancer /usr/local/bin/seedmancer
```

### Linux (x86\_64)

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

Or install with Go:

```sh
go install github.com/KazanKK/seedmancer@latest
```

## Quick Start

```sh
# 1. Initialize — creates seedmancer.yaml in the current directory
seedmancer init

# 2. Sign in (optional — needed for cloud push/pull)
seedmancer login

# 3. Save a snapshot
seedmancer export baseline

# 4. Restore it at any time
seedmancer seed baseline

# 5. Push to the cloud so teammates can pull it
seedmancer push baseline
```

### Environment markers

If a value differs per environment (e.g. a Supabase Auth user ID), use an `@env:KEY` marker in your CSV data:

```csv
id,email
@env:FIXED_USER_ID,test@example.com
```

Configure the resolved value in `seedmancer.yaml`:

```yaml
environments:
  local:
    database_url: postgres://localhost:5432/mydb
    values:
      FIXED_USER_ID: "11111111-1111-1111-1111-111111111111"
  staging:
    database_url: postgres://staging-host/mydb
    values:
      FIXED_USER_ID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
```

Seedmancer replaces the marker at seed time. The original CSV is never modified.
If the key is absent from `values:`, Seedmancer falls back to `os.Getenv("FIXED_USER_ID")`.

For the full command reference, configuration guide, Playwright integration, and MCP server setup, see the **[docs](https://seedmancer.dev/docs)**.

## Development

```sh
go build -o seedmancer .
go test ./...
```

## License

MIT — [seedmancer.dev](https://seedmancer.dev)
