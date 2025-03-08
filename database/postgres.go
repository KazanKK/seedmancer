package db

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"archive/zip"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type PostgresManager struct {
	DB *sql.DB
}

func (p *PostgresManager) log(format string, args ...interface{}) {
	fmt.Printf("[postgres] "+format+"\n", args...)
}

func (p *PostgresManager) logSQL(operation, sql string) {
	fmt.Printf("[postgres] %s:\n%s\n", operation, sql)
}

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

	// First, get all enum types and their values
	enumQuery := `
		SELECT 
			t.typname as enum_name,
			array_agg(e.enumlabel ORDER BY e.enumsortorder) as enum_values
		FROM pg_type t
		JOIN pg_enum e ON t.oid = e.enumtypid
		JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = 'public'
		GROUP BY t.typname
	`
	enumRows, err := p.DB.Query(enumQuery)
	if err != nil {
		return nil, fmt.Errorf("querying enum types: %v", err)
	}
	defer enumRows.Close()

	// Store enum information directly as []EnumItem
	var enums []EnumItem
	for enumRows.Next() {
		var enumName string
		var enumValues []string
		if err := enumRows.Scan(&enumName, pq.Array(&enumValues)); err != nil {
			return nil, fmt.Errorf("scanning enum info: %v", err)
		}
		enums = append(enums, EnumItem{
			Name:   enumName,
			Values: enumValues,
		})
	}

	// Rest of the existing query for tables and columns
	rows, err := p.DB.Query(`
		WITH fk_info AS (
			SELECT
				tc.table_name,
				kcu.column_name,
				ccu.table_name AS foreign_table_name,
				ccu.column_name AS foreign_column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name
			JOIN information_schema.constraint_column_usage ccu
				ON ccu.constraint_name = tc.constraint_name
			WHERE tc.constraint_type = 'FOREIGN KEY'
		),
		pk_info AS (
			SELECT t.table_name, c.column_name
			FROM information_schema.table_constraints t
			JOIN information_schema.constraint_column_usage c
				ON c.constraint_name = t.constraint_name
			WHERE t.constraint_type = 'PRIMARY KEY'
		),
		unique_info AS (
			SELECT t.table_name, c.column_name
			FROM information_schema.table_constraints t
			JOIN information_schema.constraint_column_usage c
				ON c.constraint_name = t.constraint_name
			WHERE t.constraint_type = 'UNIQUE'
		)
		SELECT 
			t.table_name,
			c.column_name,
			c.udt_name,
			c.data_type,
			c.is_nullable,
			c.column_default,
			pk_info.column_name IS NOT NULL as is_primary,
			unique_info.column_name IS NOT NULL as is_unique,
			fk.foreign_table_name,
			fk.foreign_column_name
		FROM 
			information_schema.tables t
			JOIN information_schema.columns c ON t.table_name = c.table_name
			LEFT JOIN fk_info fk ON t.table_name = fk.table_name 
				AND c.column_name = fk.column_name
			LEFT JOIN pk_info ON t.table_name = pk_info.table_name 
				AND c.column_name = pk_info.column_name
			LEFT JOIN unique_info ON t.table_name = unique_info.table_name 
				AND c.column_name = unique_info.column_name
		WHERE 
			t.table_schema = 'public'
			AND t.table_type = 'BASE TABLE'
		ORDER BY 
			t.table_name, c.ordinal_position;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schema := &Schema{
		DatabaseType: "postgres",
		Enums:  enums,
		Tables: make([]Table, 0),
	}

	// Process tables and columns (existing logic)
	currentTable := ""
	var currentColumns []Column

	for rows.Next() {
		var tableName, columnName, udtName, dataType, isNullable string
		var columnDefault sql.NullString
		var isPrimary, isUnique bool
		var foreignTable, foreignColumn sql.NullString

		if err := rows.Scan(
			&tableName,
			&columnName,
			&udtName,
			&dataType,
			&isNullable,
			&columnDefault,
			&isPrimary,
			&isUnique,
			&foreignTable,
			&foreignColumn,
		); err != nil {
			return nil, err
		}

		column := Column{
			Name:       columnName,
			Type:       dataType,
			Nullable:   isNullable == "YES",
			IsPrimary:  isPrimary,
			IsUnique:   isUnique,
		}

		// Handle foreign keys
		if foreignTable.Valid && foreignColumn.Valid {
			column.ForeignKey = &ForeignKey{
				Table:  foreignTable.String,
				Column: foreignColumn.String,
			}
		}

		// Handle enums
		for _, enum := range enums {
			if enum.Name == udtName {
				column.Type = "enum"
				column.Enum = enum.Name
				break
			}
		}

		// Properly handle default value
		if columnDefault.Valid {
			column.Default = columnDefault.String
		}

		if currentTable != tableName {
			if currentTable != "" {
				schema.Tables = append(schema.Tables, Table{
					Name:    currentTable,
					Columns: currentColumns,
				})
			}
			currentTable = tableName
			currentColumns = []Column{}
		}

		currentColumns = append(currentColumns, column)
	}

	// Add the last table
	if currentTable != "" {
		schema.Tables = append(schema.Tables, Table{
			Name:    currentTable,
			Columns: currentColumns,
		})
	}

	return schema, nil
}

func (p *PostgresManager) RestoreFromCSV(directory string) error {
	if p.DB == nil {
		return errors.New("no database connection")
	}

	// Disable all triggers/constraints temporarily
	if _, err := p.DB.Exec("SET session_replication_role = 'replica';"); err != nil {
		return fmt.Errorf("disabling constraints: %v", err)
	}
	defer p.DB.Exec("SET session_replication_role = 'origin';")

	schema, err := p.ReadSchemaFromFile(filepath.Join(directory, "schema.json"))
	if err != nil {
		return fmt.Errorf("reading schema: %v", err)
	}

	// First create all enum types
	for _, enum := range schema.Enums {
		// Check if enum exists
		checkSQL := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM pg_type WHERE typname = '%s')", enum.Name)
		var exists bool
		if err := p.DB.QueryRow(checkSQL).Scan(&exists); err != nil {
			return fmt.Errorf("checking if enum %s exists: %v", enum.Name, err)
		}
		
		if !exists {
			// Create the enum type
			values := make([]string, len(enum.Values))
			for i, v := range enum.Values {
				values[i] = fmt.Sprintf("'%s'", v)
			}
			
			createEnumSQL := fmt.Sprintf("CREATE TYPE %s AS ENUM (%s);",
				pq.QuoteIdentifier(enum.Name),
				strings.Join(values, ", "))
				
			p.logSQL("Create Enum", createEnumSQL)
			
			if _, err := p.DB.Exec(createEnumSQL); err != nil {
				return fmt.Errorf("creating enum %s: %v", enum.Name, err)
			}
			
			p.log("Created enum: %s", enum.Name)
		}
	}

	// Then create tables if they don't exist (without foreign keys)
	for _, table := range schema.Tables {
		// Check if table exists
		checkSQL := fmt.Sprintf("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '%s')", table.Name)
		var exists bool
		if err := p.DB.QueryRow(checkSQL).Scan(&exists); err != nil {
			return fmt.Errorf("checking if table %s exists: %v", table.Name, err)
		}
		
		if !exists {
			p.log("Creating table: %s", table.Name)
			if err := p.createTable(table, false); err != nil {
				return fmt.Errorf("creating table %s: %v", table.Name, err)
			}
			p.log("Created table: %s", table.Name)
		} else {
			// Truncate existing tables
			truncateSQL := fmt.Sprintf("TRUNCATE TABLE %s CASCADE", pq.QuoteIdentifier(table.Name))
			p.logSQL(fmt.Sprintf("Truncate Table %s", table.Name), truncateSQL)
			
			if _, err := p.DB.Exec(truncateSQL); err != nil {
				return fmt.Errorf("truncating table %s: %v", table.Name, err)
			}
			p.log("Truncated table: %s", table.Name)
		}
	}
	
	// Now add foreign key constraints
	for _, table := range schema.Tables {
		if err := p.addForeignKeys(table); err != nil {
			return fmt.Errorf("adding foreign keys for table %s: %v", table.Name, err)
		}
	}

	// Then import data for each table
	for _, table := range schema.Tables {
		csvPath := filepath.Join(directory, table.Name+".csv")
		if _, err := os.Stat(csvPath); err == nil {
			p.log("Importing data for table: %s", table.Name)
			if err := p.importCSV(table.Name, csvPath, schema); err != nil {
				return fmt.Errorf("importing data for table %s: %v", table.Name, err)
			}
			p.log("Imported data for table: %s", table.Name)
		} else {
			p.log("No CSV file found for table: %s", table.Name)
		}
	}

	return nil
}

// createTable creates a new table based on schema definition
// If includeForeignKeys is false, foreign key constraints are skipped
func (p *PostgresManager) createTable(table Table, includeForeignKeys bool) error {
	var columnDefs []string
	var primaryKeys []string
	var uniqueConstraints []string

	for _, col := range table.Columns {
		// Build column definition
		colDef := fmt.Sprintf("%s ", pq.QuoteIdentifier(col.Name))
		
		// Handle data type
		if col.Type == "enum" && col.Enum != "" {
			// Use the exact enum name as stored in the schema
			colDef += pq.QuoteIdentifier(col.Enum)
		} else if strings.HasPrefix(col.Type, "ARRAY") {
			// Fix ARRAY type syntax - default to text[] if element type not specified
			colDef += "text[]"
		} else {
			colDef += col.Type
		}
		
		// Handle nullable
		if !col.Nullable {
			colDef += " NOT NULL"
		}
		
		// Handle default value - ensure it's properly formatted
		if col.Default != "" && col.Default != nil {
			// Check if Default is a string or another type
			switch v := col.Default.(type) {
			case string:
				colDef += " DEFAULT " + v
			default:
				colDef += " DEFAULT " + fmt.Sprintf("%v", v)
			}
		}
		
		columnDefs = append(columnDefs, colDef)
		
		// Track primary keys
		if col.IsPrimary {
			primaryKeys = append(primaryKeys, col.Name)
		}
		
		// Track unique constraints
		if col.IsUnique && !col.IsPrimary {
			uniqueConstraints = append(uniqueConstraints, col.Name)
		}
	}
	
	// Add primary key constraint if any
	if len(primaryKeys) > 0 {
		pkNames := make([]string, len(primaryKeys))
		for i, pk := range primaryKeys {
			pkNames[i] = pq.QuoteIdentifier(pk)
		}
		columnDefs = append(columnDefs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkNames, ", ")))
	}
	
	// Add unique constraints
	for _, uniqueCol := range uniqueConstraints {
		columnDefs = append(columnDefs, fmt.Sprintf("UNIQUE (%s)", pq.QuoteIdentifier(uniqueCol)))
	}
	
	// Build and execute CREATE TABLE statement
	createSQL := fmt.Sprintf("CREATE TABLE %s (\n  %s\n)",
		pq.QuoteIdentifier(table.Name),
		strings.Join(columnDefs, ",\n  "))
	
	p.logSQL("Create Table", createSQL)
	_, err := p.DB.Exec(createSQL)
	return err
}

// addForeignKeys adds foreign key constraints to an existing table
func (p *PostgresManager) addForeignKeys(table Table) error {
	for _, col := range table.Columns {
		if col.ForeignKey != nil {
			// Check if the referenced table exists
			checkTableSQL := fmt.Sprintf("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '%s')", col.ForeignKey.Table)
			var tableExists bool
			if err := p.DB.QueryRow(checkTableSQL).Scan(&tableExists); err != nil {
				return fmt.Errorf("checking if referenced table %s exists: %v", col.ForeignKey.Table, err)
			}
			
			if !tableExists {
				p.log("Warning: Cannot add foreign key from %s.%s to non-existent table %s", 
					table.Name, col.Name, col.ForeignKey.Table)
				continue
			}
			
			// Check if the referenced column exists
			checkColumnSQL := fmt.Sprintf("SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '%s' AND column_name = '%s')", 
				col.ForeignKey.Table, col.ForeignKey.Column)
			var columnExists bool
			if err := p.DB.QueryRow(checkColumnSQL).Scan(&columnExists); err != nil {
				return fmt.Errorf("checking if referenced column %s.%s exists: %v", 
					col.ForeignKey.Table, col.ForeignKey.Column, err)
			}
			
			if !columnExists {
				p.log("Warning: Cannot add foreign key from %s.%s to non-existent column %s.%s", 
					table.Name, col.Name, col.ForeignKey.Table, col.ForeignKey.Column)
				continue
			}
			
			// Add the foreign key constraint
			alterSQL := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)",
				pq.QuoteIdentifier(table.Name),
				pq.QuoteIdentifier(fmt.Sprintf("%s_%s_fkey", table.Name, col.Name)),
				pq.QuoteIdentifier(col.Name),
				pq.QuoteIdentifier(col.ForeignKey.Table),
				pq.QuoteIdentifier(col.ForeignKey.Column))
			
			p.logSQL("Add Foreign Key", alterSQL)
			
			if _, err := p.DB.Exec(alterSQL); err != nil {
				// If the constraint already exists, just log and continue
				if strings.Contains(err.Error(), "already exists") {
					p.log("Foreign key constraint already exists: %s.%s -> %s.%s", 
						table.Name, col.Name, col.ForeignKey.Table, col.ForeignKey.Column)
					continue
				}
				
				// Log warning and continue instead of failing
				p.log("Warning: Failed to add foreign key from %s.%s to %s.%s: %v", 
					table.Name, col.Name, col.ForeignKey.Table, col.ForeignKey.Column, err)
				continue
			}
			
			p.log("Added foreign key: %s.%s -> %s.%s", 
				table.Name, col.Name, col.ForeignKey.Table, col.ForeignKey.Column)
		}
	}
	
	return nil
}

func (p *PostgresManager) importCSV(tableName, csvPath string, schema *Schema) error {
	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("opening CSV file: %v", err)
	}
	defer file.Close()

	// Get columns from schema
	var columns []string
	for _, table := range schema.Tables {
		if table.Name == tableName {
			for _, col := range table.Columns {
				columns = append(columns, col.Name)
			}
			break
		}
	}

	if len(columns) == 0 {
		return fmt.Errorf("no columns found in schema for table %s", tableName)
	}

	tx, err := p.DB.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction: %v", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(pq.CopyIn(tableName, columns...))
	if err != nil {
		return fmt.Errorf("preparing COPY statement: %v", err)
	}
	defer stmt.Close()

	reader := csv.NewReader(file)
	// Skip header row
	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("reading CSV header: %v", err)
	}

	rowCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading CSV record: %v", err)
		}

		if len(record) != len(columns) {
			return fmt.Errorf("column count mismatch: expected %d, got %d in row %d", len(columns), len(record), rowCount+1)
		}

		values := make([]interface{}, len(record))
		for i, v := range record {
			switch v {
			case "", "NULL", "null":
				values[i] = nil
			default:
				if strings.Contains(v, "UTC") {
					if t, err := time.Parse("2006-01-02 15:04:05.999999 -0700 MST", v); err == nil {
						values[i] = t.Format("2006-01-02 15:04:05.999999-07")
						continue
					}
				}
				if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
					values[i] = t.Format("2006-01-02 15:04:05.999999-07")
					continue
				}
				values[i] = v
			}
		}

		if _, err := stmt.Exec(values...); err != nil {
			return fmt.Errorf("executing COPY for table %s row %d: %v\nValues: %v", tableName, rowCount+1, err, values)
		}
		rowCount++
	}

	if err := stmt.Close(); err != nil {
		return fmt.Errorf("closing COPY statement: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %v", err)
	}

	p.log("Imported %d rows into table %s", rowCount, tableName)
	return nil
}

// ReadSchemaFromFile reads a schema from a JSON file
func (p *PostgresManager) ReadSchemaFromFile(filename string) (*Schema, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	
	var schema Schema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("parsing schema file: %v", err)
	}

	// Validate database type if specified
	if schema.DatabaseType != "" && schema.DatabaseType != "postgres" {
		p.log("Warning: Schema was created for %s database, but using with PostgreSQL", schema.DatabaseType)
	}

	// Create enum types first
	for _, enum := range schema.Enums {
		createEnumSQL := fmt.Sprintf("CREATE TYPE %s AS ENUM (%s);",
			pq.QuoteIdentifier(enum.Name),
			joinQuotedStrings(enum.Values))
		
		p.logSQL("Create Enum", createEnumSQL)
		if _, err := p.DB.Exec(createEnumSQL); err != nil {
			// Ignore if enum already exists
			if !strings.Contains(err.Error(), "already exists") {
				return nil, fmt.Errorf("creating enum %s: %v", enum.Name, err)
			}
		}
	}

	return &schema, nil
}

// Helper function to properly quote and join enum values
func joinQuotedStrings(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	}
	return strings.Join(quoted, ", ")
}

func (p *PostgresManager) ExportToCSV(outputDir string) error {
	if p.DB == nil {
		return errors.New("no database connection")
	}
	// First export schema to the same directory
	schemaPath := filepath.Join(outputDir, "schema.json")
		if err := p.ExportSchema(schemaPath); err != nil {
			return fmt.Errorf("exporting schema: %v", err)
		}
		p.log("Exported schema to: %s", schemaPath)

	// Get list of tables
	rows, err := p.DB.Query(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_type = 'BASE TABLE'
	`)
	if err != nil {
		return fmt.Errorf("querying tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("scanning table name: %v", err)
		}
		tables = append(tables, tableName)
	}

	// Export each table
	for _, tableName := range tables {
		if err := p.exportTableToCSV(tableName, outputDir); err != nil {
			return fmt.Errorf("exporting table %s: %v", tableName, err)
		}
		p.log("Exported table: %s", tableName)
	}

	return nil
}

func (p *PostgresManager) exportTableToCSV(tableName, outputDir string) error {
	// Create CSV file
	csvPath := filepath.Join(outputDir, fmt.Sprintf("%s.csv", tableName))
	file, err := os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("creating CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Get column names
	rows, err := p.DB.Query(fmt.Sprintf(`
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = 'public' 
		AND table_name = '%s' 
		ORDER BY ordinal_position
	`, tableName))
	if err != nil {
		return fmt.Errorf("querying columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return fmt.Errorf("scanning column name: %v", err)
		}
		columns = append(columns, colName)
	}

	// Write header
	if err := writer.Write(columns); err != nil {
		return fmt.Errorf("writing CSV header: %v", err)
	}

	// Query all data with quoted column names
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = pq.QuoteIdentifier(col)
	}
	
	query := fmt.Sprintf("SELECT %s FROM %s", 
		strings.Join(quotedColumns, ", "), 
		pq.QuoteIdentifier(tableName))
	p.logSQL(fmt.Sprintf("Export Table %s", tableName), query)
	
	dataRows, err := p.DB.Query(query)
	if err != nil {
		return fmt.Errorf("querying data: %v", err)
	}
	defer dataRows.Close()

	// Write data rows
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for dataRows.Next() {
		if err := dataRows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("scanning row: %v", err)
		}

		row := make([]string, len(columns))
		for i, val := range values {
			if val == nil {
				row[i] = "NULL"
			} else {
				switch v := val.(type) {
				case []byte:
					row[i] = string(v)
				case time.Time:
					// Format timestamp with correct timezone format
					row[i] = v.Format("2006-01-02 15:04:05.999999 -0700 UTC")
				default:
					row[i] = fmt.Sprintf("%v", v)
				}
			}
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("writing CSV row: %v", err)
		}
	}

	return nil
}

func (p *PostgresManager) ExportSchema(outputFile string) error {
	if p.DB == nil {
		return errors.New("no database connection")
	}

	// Extract schema using existing method
	schema, err := p.ExtractSchema()
	if err != nil {
		return fmt.Errorf("extracting schema: %v", err)
	}

	// Convert to JSON with pretty printing
	jsonData, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("converting schema to JSON: %v", err)
	}

	// Write to file
	if err := os.WriteFile(outputFile, jsonData, 0644); err != nil {
		return fmt.Errorf("writing schema to file: %v", err)
	}

	p.log("Schema exported to: %s", outputFile)
	return nil
}

func (p *PostgresManager) Fetch(baseURL, databaseName, version, outputDir, token string) error {
	// Create request
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v1.0/databases/testdata/fetch?database_name=%s&version_name=%s", baseURL, databaseName, version), nil)
	if err != nil {
		return fmt.Errorf("creating request: %v", err)
	}

	// Add authorization header
	req.Header.Add("Authorization", "cli_"+token)

	// Send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("fetching data from API: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized: please check your API token")
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	// Create target directory
	// targetDir := filepath.Join("databases", databaseName, version)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("creating target directory: %v", err)
	}

	// Check Content-Type header
	contentType := resp.Header.Get("Content-Type")
	if contentType == "application/zip" {
		// Direct zip file download
		return p.extractZip(resp.Body, outputDir)
	}

	// Try to parse JSON response for S3 URL
	var response struct {
		URL string `json:"url"`
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}

	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("parsing API response: %v", err)
	}

	if response.URL == "" {
		return fmt.Errorf("invalid response format: missing URL")
	}

	// Download from S3 URL
	s3Resp, err := http.Get(response.URL)
	if err != nil {
		return fmt.Errorf("downloading from S3: %v", err)
	}
	defer s3Resp.Body.Close()

	if s3Resp.StatusCode != http.StatusOK {
		return fmt.Errorf("S3 download failed with status: %d", s3Resp.StatusCode)
	}

	return p.extractZip(s3Resp.Body, outputDir)
}

func (p *PostgresManager) extractZip(reader io.Reader, targetDir string) error {
	// Create temporary file for zip
	tmpFile, err := os.CreateTemp("", "database-*.zip")
	if err != nil {
		return fmt.Errorf("creating temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Copy download to temporary file
	if _, err := io.Copy(tmpFile, reader); err != nil {
		return fmt.Errorf("saving zip file: %v", err)
	}

	// Extract zip file
	zipReader, err := zip.OpenReader(tmpFile.Name())
	if err != nil {
		return fmt.Errorf("opening zip file: %v", err)
	}
	defer zipReader.Close()

	for _, file := range zipReader.File {
		rc, err := file.Open()
		if err != nil {
			return fmt.Errorf("opening file in zip: %v", err)
		}

		path := filepath.Join(targetDir, file.Name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			rc.Close()
			return fmt.Errorf("creating directories: %v", err)
		}

		outFile, err := os.Create(path)
		if err != nil {
			rc.Close()
			return fmt.Errorf("creating output file: %v", err)
		}

		if _, err := io.Copy(outFile, rc); err != nil {
			outFile.Close()
			rc.Close()
			return fmt.Errorf("extracting file: %v", err)
		}

		outFile.Close()
		rc.Close()
		p.log("Extracted: %s", path)
	}

	return nil
}

