# Seedmancer

Seedmancer is a simple CLI tool to manage and reset database test data for testing. Resetting to a specific test data version is done by restoring from CSV files.

## Features
- 🚀 **Fast Database Reset** – Restore your database to a specific state instantly.
- 🔄 **Test Data Versioning** – Save and manage multiple test data versions.
- 🔌 **Supports PostgreSQL & MySQL** – With planned support for more databases.
- 📦 **Seamless CI/CD Integration (Coming Soon)** – Works with GitHub Actions, Jenkins, and more.


## Installation

### Manual Download
download the latest binary from [GitHub Releases](https://github.com/KazanKK/Seedmancer/releases)
#### Linux(arm64)
```sh
curl -L https://github.com/KazanKK/seedmancer/releases/download/v0.1.0/seedmancer_Linux_arm64 -o seedmancer_Linux_arm64
chmod +x seedmancer_Linux_arm64
mv seedmancer_Linux_arm64 /usr/local/bin/seedmancer
```

#### Linux(x86_64)
```sh
curl -L https://github.com/KazanKK/seedmancer/releases/download/v0.1.0/seedmancer_Linux_x86_64 -o seedmancer_Linux_x86_64
chmod +x seedmancer_Linux_x86_64
mv seedmancer_Linux_x86_64 /usr/local/bin/seedmancer
```

#### MacOS(arm64)
```sh
curl -L https://github.com/KazanKK/seedmancer/releases/download/v0.1.0/seedmancer_Darwin_arm64 -o seedmancer_Darwin_arm64
chmod +x seedmancer_Darwin_arm64
sudo mv seedmancer_Darwin_arm64  /usr/local/bin/seedmancer
```

## Getting Started
### Initialize a New Project
```sh
seedmancer init
```
This sets up configuration files for Seedmancer in your project.

### Export Existing Database Schema & Data
```sh
seedmancer export --database-name mydb --version-name baseline --db-url "postgres://user:pass@localhost:5432/mydb"
```
Exports the database schema and data to local CSV files.

### Restore Database to a Specific Test Data Version
```sh
seedmancer seed --database-name mydb --version-name baseline --db-url "postgres://user:pass@localhost:5432/mydb"
```
Resets the database to the specified test data version.

### Direct Edit of CSV Files
You can directly edit the CSV files to change the test data.
Do not edit the `schema.json` file.


## Command Reference
### `seedmancer init`
Initializes a new Seedmancer project.

### `seedmancer export`
Exports the database schema and data.

| Argument | Description | Required | Default |
|----------|------------|----------|---------|
| `--database-name` | Name of the database to export. You can name it anything you want. | ✅ Yes | - |
| `--db-url` | Database connection URL | ✅ Yes | - |
| `--version-name` | Version name for the export | No | `unversioned` |

### `seedmancer seed`
Restores the database to a specific test data version.

| Argument | Description | Required | Default |
|----------|------------|----------|---------|
| `--database-name` | Name of the database you want to use | ✅ Yes | - |
| `--version-name` | Test data version to apply | ✅ Yes | - |
| `--db-url` | Database connection URL | ✅ Yes | - |

### `seedmancer list` 
Lists available databases and test data versions.


## Configuration File
Seedmancer uses a YAML configuration file (`seedmancer.yaml`) to store settings:

| Key | Description | Default |
|-----|------------|---------|
| `storage_path` | Where to store test data files | `.seedmancer` |

---

## License
Seedmancer is licensed under the **MIT License**.

## Support & Contact
- 🌐 [Website](https://seedmancer.com) (coming soon)
