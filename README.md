# Seedmancer

Seedmancer is an open-source CLI tool that enables developers and QA teams to efficiently manage and reset database states for testing. It supports local development, CI/CD workflows, and integrates seamlessly with Seedmancer Cloud for advanced features like versioning, cloud sync, and AI-generated test data.

## Features
- 🚀 **Fast Database Reset** – Restore your database to a specific state instantly.
- 🔄 **Test Data Versioning** – Save and manage multiple test data versions.
- 🔌 **Supports PostgreSQL & MySQL** – With planned support for more databases.
- 📦 **Seamless CI/CD Integration** – Works with GitHub Actions, Jenkins, and more.
- ☁️ **Cloud Sync (Pro)** – Sync test data across teams and environments.
- 🤖 **AI Test Data Generation (Pro)** – Automatically generate diverse test datasets.


## Installation
### Using Homebrew (macOS/Linux)
```sh
brew install KazanKK/Seedmancer
```

### Using Go Install
```sh
go install github.com/KazanKK/Seedmancer@latest
```

### Manual Download
1. Download the latest binary from [GitHub Releases](https://github.com/KazanKK/Seedmancer/releases)
2. Make it executable:
```sh
chmod +x seedmancer-linux-amd64
mv seedmancer-linux-amd64 /usr/local/bin/seedmancer
```

---

## Getting Started
### Initialize a New Project
```sh
seedmancer init
```
This sets up configuration files for Seedmancer in your project.

### Export Existing Database Schema & Data
```sh
seedmancer export --database-name mydb --db-url "postgres://user:pass@localhost:5432/mydb"
```
Exports the database schema and data to local CSV files.

### Restore Database to a Specific Test Data Version
```sh
seedmancer seed --database-name mydb --test-data-version-name baseline --db-url "postgres://user:pass@localhost:5432/mydb"
```
Resets the database to the specified test data version.

### Sync Test Data to Cloud (Pro Feature)
```sh
seedmancer sync --database-name mydb --version baseline --token $SEEDMANCER_API_TOKEN
```
Uploads your test dataset to Seedmancer Cloud for collaboration.


## Command Reference
### `seedmancer init`
Initializes a new Seedmancer project.

| Argument | Description | Required | Default |
|----------|------------|----------|---------|
| `--storage-path` | Path to store seedmancer files | No | `.seedmancer` |

### `seedmancer export`
Exports the database schema and data.

| Argument | Description | Required | Default |
|----------|------------|----------|---------|
| `--database-name` | Name of the database to export | ✅ Yes | - |
| `--db-url` | Database connection URL | ✅ Yes | - |
| `--version` | Version name for the export | No | `unversioned` |

### `seedmancer seed`
Restores the database to a specific test data version.

| Argument | Description | Required | Default |
|----------|------------|----------|---------|
| `--database-name` | Name of the database to reset | ✅ Yes | - |
| `--test-data-version-name` | Test data version to apply | ✅ Yes | - |
| `--db-url` | Database connection URL | ✅ Yes | - |
| `--token` | API token for remote data (if needed) | No | - |

### `seedmancer list` 
Lists available databases and test data versions.

| Argument | Description | Required | Default |
|----------|------------|----------|---------|
| `--token` | API token to include remote databases | No | - |

### `seedmancer sync` (Pro Feature)
Syncs test data to the cloud.

| Argument | Description | Required | Default |
|----------|------------|----------|---------|
| `--database-name` | Name of the database to sync | ✅ Yes | - |
| `--version` | Version name to sync | ✅ Yes | - |
| `--token` | API token for authentication | ✅ Yes | - |

### `seedmancer fetch` (Pro Feature)
Fetches test data from the cloud.

| Argument | Description | Required | Default |
|----------|------------|----------|---------|
| `--database-name` | Name of the database to fetch | ✅ Yes | - |
| `--version` | Version name to fetch | ✅ Yes | - |
| `--token` | API token for authentication | ✅ Yes | - |


## Configuration File
Seedmancer uses a YAML configuration file (`seedmancer.yaml`) to store settings:

| Key | Description | Default |
|-----|------------|---------|
| `storage_path` | Where to store test data files | `.seedmancer` |

---


## FAQ
### ❓ How do I reset my database?
```sh
seedmancer seed --database-name mydb --test-data-version-name baseline --db-url "postgres://user:pass@localhost:5432/mydb"
```

### ❓ Can I use Seedmancer without an internet connection?
✅ Yes, local CSV files work **without cloud sync**.

### ❓ How do I upgrade Seedmancer?
```sh
brew upgrade seedmancer
```

---

## Contributing
Seedmancer is open-source! Feel free to contribute by submitting issues, feature requests, or pull requests.
1. Fork the repo
2. Clone it: `git clone https://github.com/yourusername/seedmancer.git`
3. Build: `go build .`
4. Run tests: `go test ./...`

---

## License
Seedmancer is licensed under the **MIT License**.

## Support & Contact
- 🌐 [Website](https://seedmancer.com)
