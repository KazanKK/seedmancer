package db

// DatabaseManager defines the interface for database operations
type DatabaseManager interface {
	ConnectWithDSN(dsn string) error
	ExportSchema(outputPath string) error
	ExportToCSV(outputDir string) error
	RestoreFromCSV(inputDir string) error
}
