# ReSeeder: Database Seed Tool

A high-performance CLI tool for database seeding and testing data management.

## ✨ Features

- **Database Operations**
  - Create and restore database snapshots
  - Schema extraction and validation
  - Transaction-safe operations
  - Bulk data operations for performance
- **Data Management**
  - Smart type detection and handling
  - Automated fake data generation
  - CSV import/export with streaming
  - Referential integrity maintenance
- **Multiple Database Support**
  - PostgreSQL (primary)
  - MySQL (coming soon)

## 🚀 Quick Start

### Installation Options

1. Using `go install` (recommended)
```bash
go install github.com/reseeder/reseeder@latest
```

2. Using Docker
```bash
docker pull reseeder/reseeder:latest
docker run reseeder/reseeder:latest --help
```

3. Using Binary Releases
```bash
# Linux (x64)
curl -L https://github.com/reseeder/reseeder/releases/latest/download/reseeder-linux-amd64 -o reseeder
chmod +x reseeder
sudo mv reseeder /usr/local/bin/

# macOS (x64)
curl -L https://github.com/reseeder/reseeder/releases/latest/download/reseeder-darwin-amd64 -o reseeder
chmod +x reseeder
sudo mv reseeder /usr/local/bin/

# Windows (x64)
# Download from https://github.com/reseeder/reseeder/releases/latest
# Add to PATH
```

4. Using Homebrew (macOS and Linux)
```bash
brew tap reseeder/reseeder
brew install reseeder
```

### Basic Usage
reseeder seed --config ./seed-config.yaml
```

## 📖 Usage Examples

### Generate Test Data
```bash
reseeder generate \
  --output ./data \
  --rows 1000 \
  --db postgres \
  --dsn "postgres://user:pass@localhost:5432/dbname" \
  --concurrent 4
```

### Backup & Restore
```bash
# Export database to CSV
reseeder export \
  --output ./backup \
  --db postgres \
  --dsn "postgres://user:pass@localhost:5432/dbname"

# Restore from CSV
reseeder restore \
  --input ./backup \
  --db postgres \
  --dsn "postgres://user:pass@localhost:5432/dbname" \
  --batch-size 5000
```

## 🔧 Development

### Requirements
- Go 1.20+
- Docker
- Make

### Local Setup
```bash
make setup    # Install dependencies
make test     # Run tests
make lint     # Run linters
make security # Run security checks
```

### CI/CD
We use GitHub Actions for:
- Automated testing
- Code quality checks
- Security scanning
- Release automation
- Container builds

## 📚 Documentation

- [Full Documentation](https://docs.reseeder.dev)
- [API Reference](https://docs.reseeder.dev/api)
- [Configuration Guide](https://docs.reseeder.dev/config)
- [Contributing Guide](CONTRIBUTING.md)

## 🔐 Security

Security reports should be sent to security@reseeder.dev or via our [Security Policy](SECURITY.md).

## 📄 License

MIT License - see [LICENSE](LICENSE)

## 🆘 Support

- Documentation: [docs.reseeder.dev](https://docs.reseeder.dev)
- Issues: [GitHub Issues](https://github.com/reseeder/reseeder/issues)
- Community: [Discord](https://discord.gg/reseeder)

## ✨ Features in Detail

### Smart Type Detection
The tool automatically detects and handles various PostgreSQL data types:
- UUID fields
- Timestamps and dates
- JSON/JSONB
- Numeric types
- Text and VARCHAR
- Enums
- Foreign keys

### Data Generation
- Maintains referential integrity
- Generates unique values for unique constraints
- Handles NULL values appropriately
- Supports custom data patterns

For implementation details, see:

```132:192:database/faker.go
	defer writer.Flush()
```


### Database Operations
Supports various database operations including:
- Schema extraction
- Data import/export
- CSV handling
- Transaction management

For core database operations, see:

```36:233:database/postgres.go
func (p *PostgresManager) ConnectWithDSN(dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	p.DB = db
	return nil
}

func (p *PostgresManager) ExtractSchema() (*Schema, error) {
	if p.DB == nil {
		return nil, errors.New("no database connection")
	}
```

