# ReSeeder: Database Seed Tool

A CLI tool for PostgreSQL database seeding, testing data management.

## ✨ Features

- **Database Operations**
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

### Export Current Database to CSV
Export existing database data to CSV files:
```bash
reseeder export-to-csv \
  --output-dir ./data \
  --db postgres \
  --dsn "postgres://user:pass@localhost:5432/dbname" \
```

### Restore Database from CSV
Restore database from previously exported CSV files:
```bash
reseeder restore-from-csv \
  --csv-dir ./data \
  --db postgres \
  --dsn "postgres://user:pass@localhost:5432/dbname" \
```