# Seedmancer CLI

Reset your PostgreSQL database to a saved state in seconds — export snapshots, restore them on demand, sync to the cloud, and generate test data with AI.

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

# 2. Sign in (optional — needed for cloud sync and AI generation)
seedmancer login

# 3. Save a snapshot
seedmancer export --id baseline

# 4. Restore it at any time
seedmancer seed --id baseline

# 5. Push to the cloud so teammates can pull it
seedmancer sync --id baseline
```

For the full command reference, configuration guide, Playwright integration, and MCP server setup, see the **[docs](https://seedmancer.dev/docs)**.

## Development

```sh
go build -o seedmancer .
go test ./...
```

## License

MIT — [seedmancer.dev](https://seedmancer.dev)
