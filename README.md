# ReSeeder: Database Seed Tool

A CLI tool for PostgreSQL database seeding, testing data management. (Currently under development)

## ✨ Features

- **Database Operations**
  - Generate fake data based on database schema
  - Create and restore database snapshots
  - CSV import/export functionality

## 🚀 Quick Start

### Installation Options

1. Using `go install`
```bash
go install github.com/KazanKK/reseeder@latest
```

2. Using Binary Releases
#### Linux (x64)
```bash
VERSION="v0.1.0"  # Change this to match your latest release
curl -L https://github.com/KazanKK/reseeder/releases/download/${VERSION}/reseeder_Linux_x86_64.tar.gz -o reseeder
chmod +x reseeder
sudo mv reseeder /usr/local/bin/
```

#### macOS (x64)
```bash
VERSION="v0.1.0"  # Change this to match your latest release
curl -L https://github.com/KazanKK/reseeder/releases/download/${VERSION}/reseeder_Darwin_x86_64.tar.gz -o reseeder.tar.gz
tar xzf reseeder.tar.gz
chmod +x reseeder
sudo mv reseeder /usr/local/bin/
```

#### Windows (x64)
```bash
VERSION="v0.1.0"  # Change this to match your latest release
curl -L https://github.com/KazanKK/reseeder/releases/download/${VERSION}/reseeder_Windows_x86_64.zip -o reseeder.zip
unzip reseeder.zip
mv reseeder.exe C:\Windows\System32\
```


## 📖 Command Reference

### Generate Fake Data
Generate fake data based on database schema or schema file:
```bash
reseeder generate-fake-data \
  --output-dir ./data \
  --rows 1000 \
  --db postgres \
  --dsn "postgres://user:pass@localhost:5432/dbname" \
  --debug
```

Options:
- `--output-dir`: Directory for generated CSV files (required)
- `--rows`: Number of rows to generate (default: 10)
- `--db`: Database type (postgres)
- `--dsn`: Database connection string
- `--schema-file`: Alternative to using database connection
- `--debug`: Enable debug logging

### Export Current Database to CSV
Export existing database data to CSV files:
```bash
reseeder export-to-csv \
  --output-dir ./data \
  --db postgres \
  --dsn "postgres://user:pass@localhost:5432/dbname" \
  --debug
```

### Restore Database from CSV
Restore database from previously exported CSV files:
```bash
reseeder restore-from-csv \
  --csv-dir ./data \
  --db postgres \
  --dsn "postgres://user:pass@localhost:5432/dbname" \
  --debug
```

### Export Database Schema
Export database schema to a JSON file:
```bash
reseeder export-schema \
  --output schema.json \
  --db postgres \
  --dsn "postgres://user:pass@localhost:5432/dbname" \
  --debug
```
