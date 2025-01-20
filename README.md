# ReSeeder: Database Seed Tool

A powerful CLI tool for managing database seeding and testing data. Perfect for development and testing environments.

## ✨ Features

- **Database Snapshots**: Create and restore database snapshots
- **Schema Extraction**: Automatically extract database schema information
- **Fake Data Generation**: Generate realistic test data based on schema
- **CSV Import/Export**: Import and export data using CSV files
- **Multiple Database Support**: Currently supports PostgreSQL (MySQL coming soon)
- **Smart Type Detection**: Automatically detects and handles various data types
- **Referential Integrity**: Maintains foreign key relationships in generated data

## 🚀 Installation

### Download Binary

Download the latest release for your platform from the [releases page](https://github.com/reseeder/reseeder/releases).

### Build from Source

```bash
git clone https://github.com/reseeder/reseeder
cd reseeder
go build
```

## 📖 Usage

### Generate Fake Data

```bash
reseeder generate-fake-data \
  --output-dir ./data \
  --rows 100 \
  --db postgres \
  --dsn "postgres://user:pass@localhost:5432/dbname"
```

### Restore from CSV

```bash
reseeder restore-from-csv \
  --db postgres \
  --dsn "postgres://user:pass@localhost:5432/dbname" \
  --csv-dir ./data \
  --debug
```

### Export to CSV

```bash
reseeder export-to-csv \
  --output-dir ./backup \
  --db postgres \
  --dsn "postgres://user:pass@localhost:5432/dbname" \
  --debug
```

## 🔧 Development

### Prerequisites

- Go 1.16 or higher
- Docker (for running tests)
- PostgreSQL (for local development)

### Running Tests

```bash
# Run integration tests
go test ./tests -v

# Run unit tests
go test ./... -v
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## 📄 License

MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- Documentation: [docs.reseeder.dev](https://docs.reseeder.dev)
- Issues: [GitHub Issues](https://github.com/reseeder/reseeder/issues)
- Community: [Discord](https://discord.gg/reseeder)

## 🔐 Security

Found a security issue? Please report it privately via our [Security Policy](SECURITY.md).

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

