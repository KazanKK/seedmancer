package db

// DatabaseManager defines the interface for database operations
type DatabaseManager interface {
	ConnectWithDSN(dsn string) error
	ExportSchema(outputPath string) error
	ExportToCSV(outputDir string) error
	RestoreFromCSV(inputDir string) error
	// ExecSQL executes one or more SQL statements against the open
	// connection inside a transaction. On any error the transaction is
	// rolled back so the database is left in its pre-call state.
	ExecSQL(sql string) error
}
