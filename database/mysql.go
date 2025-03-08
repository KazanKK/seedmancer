package db

import (
	"archive/zip"
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

	_ "github.com/go-sql-driver/mysql"
)

// MySQLManager handles MySQL database operations
type MySQLManager struct {
	DB *sql.DB
}

func (m *MySQLManager) log(format string, args ...interface{}) {
	fmt.Printf("[mysql] "+format+"\n", args...)
}

func (m *MySQLManager) logSQL(operation, sql string) {
	fmt.Printf("[mysql] %s:\n%s\n", operation, sql)
}

// ConnectWithDSN connects to a MySQL database using a DSN string
func (m *MySQLManager) ConnectWithDSN(dsn string) error {
	// remove mysql:// from the dsn
	dsn = strings.Replace(dsn, "mysql://", "", 1)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	m.DB = db
	return nil
}

// ExportSchema exports the database schema to a JSON file
func (m *MySQLManager) ExportSchema(outputPath string) error {
	schema, err := m.GetSchema()
	if err != nil {
		return err
	}

	// Create the output directory if it doesn't exist
	outputDir := filepath.Dir(outputPath)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("creating output directory: %v", err)
	}

	// Marshal the schema to JSON with indentation
	jsonData, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling schema to JSON: %v", err)
	}

	// Write the JSON to the output file
	if err := os.WriteFile(outputPath, jsonData, 0644); err != nil {
		return fmt.Errorf("writing schema to file: %v", err)
	}

	return nil
}

// GetSchema retrieves the database schema
func (m *MySQLManager) GetSchema() (*Schema, error) {
	if m.DB == nil {
		return nil, errors.New("no database connection")
	}

	// Get the current database name
	var dbName string
	if err := m.DB.QueryRow("SELECT DATABASE()").Scan(&dbName); err != nil {
		return nil, fmt.Errorf("getting current database name: %v", err)
	}

	schema := &Schema{
		DatabaseType: MySQL,  // Set the database type to MySQL
		Tables: []Table{},
		Enums:  []EnumItem{},
	}

	// Get list of tables
	rows, err := m.DB.Query(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = ? 
		AND table_type = 'BASE TABLE'
	`, dbName)
	if err != nil {
		return nil, fmt.Errorf("querying tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scanning table name: %v", err)
		}
		tables = append(tables, tableName)
	}

	// For each table, get columns and constraints
	for _, tableName := range tables {
		// Get columns
		columnRows, err := m.DB.Query(`
			SELECT 
				column_name,
				data_type,
				is_nullable,
				column_default,
				column_key,
				extra,
				column_type
			FROM information_schema.columns
			WHERE table_schema = ?
			AND table_name = ?
			ORDER BY ordinal_position
		`, dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("querying columns for table %s: %v", tableName, err)
		}

		var columns []Column
		for columnRows.Next() {
			var colName, dataType, isNullable string
			var columnDefault, columnKey, extra, columnType sql.NullString
			
			if err := columnRows.Scan(&colName, &dataType, &isNullable, &columnDefault, &columnKey, &extra, &columnType); err != nil {
				columnRows.Close()
				return nil, fmt.Errorf("scanning column info: %v", err)
			}

			column := Column{
				Name:      colName,
				Type:      dataType,
				Nullable:  isNullable == "YES",
				IsPrimary: columnKey.Valid && columnKey.String == "PRI",
				IsUnique:  columnKey.Valid && (columnKey.String == "UNI" || columnKey.String == "PRI"),
			}

			// Store default value if present
			if columnDefault.Valid {
				column.Default = columnDefault.String
			}

			// Check if column is an enum
			if strings.HasPrefix(dataType, "enum") && columnType.Valid {
				// Extract enum values from the column_type - this contains the full definition
				enumStr := columnType.String
				if strings.HasPrefix(enumStr, "enum(") && strings.HasSuffix(enumStr, ")") {
					enumStr = enumStr[5 : len(enumStr)-1] // Remove 'enum(' and ')'
				}
				
				// Split by comma, but handle quoted values properly
				var enumValues []string
				inQuote := false
				currentValue := ""
				
				for _, char := range enumStr {
					if char == '\'' {
						inQuote = !inQuote
						if !inQuote && currentValue != "" {
							enumValues = append(enumValues, currentValue)
							currentValue = ""
						}
					} else if char == ',' && !inQuote {
						// Skip commas outside quotes
					} else if inQuote {
						currentValue += string(char)
					}
				}
				
				// Create a unique enum name based on table and column
				enumName := fmt.Sprintf("%s_%s_enum", tableName, colName)
				
				// Check if this enum already exists
				enumExists := false
				for _, e := range schema.Enums {
					if e.Name == enumName {
						enumExists = true
						break
					}
				}
				
				// Add the enum if it doesn't exist
				if !enumExists {
					schema.Enums = append(schema.Enums, EnumItem{
						Name:   enumName,
						Values: enumValues,
					})
				}
				
				// Set the column type to "enum" and store the enum name
				column.Type = "enum"
				column.Enum = enumName
			}

			columns = append(columns, column)
		}
		columnRows.Close()

		// Get foreign keys
		fkRows, err := m.DB.Query(`
			SELECT
				column_name,
				referenced_table_name,
				referenced_column_name
			FROM information_schema.key_column_usage
			WHERE table_schema = ?
			AND table_name = ?
			AND referenced_table_name IS NOT NULL
		`, dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("querying foreign keys for table %s: %v", tableName, err)
		}

		fkMap := make(map[string]*ForeignKey)
		for fkRows.Next() {
			var colName, refTable, refColumn string
			if err := fkRows.Scan(&colName, &refTable, &refColumn); err != nil {
				fkRows.Close()
				return nil, fmt.Errorf("scanning foreign key info: %v", err)
			}

			fkMap[colName] = &ForeignKey{
				Table:  refTable,
				Column: refColumn,
			}
		}
		fkRows.Close()

		// Add foreign keys to columns
		for i, col := range columns {
			if fk, exists := fkMap[col.Name]; exists {
				columns[i].ForeignKey = fk
			}
		}

		schema.Tables = append(schema.Tables, Table{
			Name:    tableName,
			Columns: columns,
		})
	}

	return schema, nil
}

// ExportToCSV exports all tables to CSV files
func (m *MySQLManager) ExportToCSV(outputDir string) error {
	if m.DB == nil {
		return errors.New("no database connection")
	}
	
	// First export schema to the same directory
	schemaPath := filepath.Join(outputDir, "schema.json")
	if err := m.ExportSchema(schemaPath); err != nil {
		return fmt.Errorf("exporting schema: %v", err)
	}
	m.log("Exported schema to: %s", schemaPath)

	// Get list of tables
	rows, err := m.DB.Query(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE() 
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
		if err := m.exportTableToCSV(tableName, outputDir); err != nil {
			return fmt.Errorf("exporting table %s: %v", tableName, err)
		}
		m.log("Exported table: %s", tableName)
	}

	return nil
}

// exportTableToCSV exports a single table to a CSV file
func (m *MySQLManager) exportTableToCSV(tableName, outputDir string) error {
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
	rows, err := m.DB.Query(fmt.Sprintf(`
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = DATABASE() 
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
		quotedColumns[i] = "`" + col + "`"
	}
	
	query := fmt.Sprintf("SELECT %s FROM `%s`", 
		strings.Join(quotedColumns, ", "), 
		tableName)
	m.logSQL(fmt.Sprintf("Export Table %s", tableName), query)
	
	dataRows, err := m.DB.Query(query)
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

// RestoreFromCSV imports data from CSV files into the database
func (m *MySQLManager) RestoreFromCSV(directory string) error {
	if m.DB == nil {
		return errors.New("no database connection")
	}

	// Start a transaction
	tx, err := m.DB.Begin()
	if err != nil {
		return fmt.Errorf("starting transaction: %v", err)
	}

	// Ensure transaction is rolled back if an error occurs
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Disable foreign key checks temporarily - make sure this is working
	if _, err := m.DB.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return fmt.Errorf("disabling foreign key checks: %v", err)
	}
	defer m.DB.Exec("SET FOREIGN_KEY_CHECKS = 1")

	schema, err := m.ReadSchemaFromFile(filepath.Join(directory, "schema.json"))
	if err != nil {
		return fmt.Errorf("reading schema: %v", err)
	}

	// Sort tables to handle dependencies - tables with foreign keys should be imported after their referenced tables
	sortedTables := sortTablesByDependencies(schema.Tables)

	// Create tables if they don't exist
	for _, table := range sortedTables {
		// Check if table exists
		var exists int
		checkSQL := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '%s'", table.Name)
		if err := m.DB.QueryRow(checkSQL).Scan(&exists); err != nil {
			return fmt.Errorf("checking if table %s exists: %v", table.Name, err)
		}
		
		if exists == 0 {
			m.log("Creating table: %s", table.Name)
			if err := m.createTable(table, schema); err != nil {
				return fmt.Errorf("creating table %s: %v", table.Name, err)
			}
			m.log("Created table: %s", table.Name)
		} else {
			// Truncate existing tables
			truncateSQL := fmt.Sprintf("TRUNCATE TABLE `%s`", table.Name)
			m.logSQL(fmt.Sprintf("Truncate Table %s", table.Name), truncateSQL)
			
			if _, err := m.DB.Exec(truncateSQL); err != nil {
				return fmt.Errorf("truncating table %s: %v", table.Name, err)
			}
			m.log("Truncated table: %s", table.Name)
		}
	}

	// Import data for each table in the sorted order
	for _, table := range sortedTables {
		csvPath := filepath.Join(directory, table.Name+".csv")
		if _, err := os.Stat(csvPath); err == nil {
			m.log("Importing data for table: %s", table.Name)
			if err := m.importCSV(table.Name, csvPath, schema); err != nil {
				return fmt.Errorf("importing data for table %s: %v", table.Name, err)
			}
			m.log("Imported data for table: %s", table.Name)
		} else {
			m.log("No CSV file found for table: %s", table.Name)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %v", err)
	}

	return nil
}

// sortTablesByDependencies sorts tables so that tables with foreign keys come after their referenced tables
func sortTablesByDependencies(tables []Table) []Table {
	// Create a map of table dependencies
	dependencies := make(map[string][]string)
	tableMap := make(map[string]Table)
	
	// Build dependency map
	for _, table := range tables {
		tableMap[table.Name] = table
		dependencies[table.Name] = []string{}
		
		// Find all foreign key dependencies
		for _, col := range table.Columns {
			if col.ForeignKey != nil {
				dependencies[table.Name] = append(dependencies[table.Name], col.ForeignKey.Table)
			}
		}
	}
	
	// Perform topological sort
	var sorted []Table
	var visit func(string, map[string]bool)
	
	visited := make(map[string]bool)
	tempMark := make(map[string]bool)
	
	visit = func(tableName string, path map[string]bool) {
		// Skip empty table names
		if tableName == "" {
			return
		}
		
		if tempMark[tableName] {
			// Circular dependency detected, but we'll continue
			return
		}
		if visited[tableName] {
			return
		}
		
		tempMark[tableName] = true
		path[tableName] = true
		
		for _, dep := range dependencies[tableName] {
			// Skip empty dependencies and check if the dependency exists in our tableMap
			if dep != "" && tableMap[dep].Name != "" {
				if !path[dep] { // Avoid circular dependencies
					visit(dep, path)
				}
			}
		}
		
		visited[tableName] = true
		tempMark[tableName] = false
		delete(path, tableName)
		
		// Only add tables that exist in our tableMap
		if table, exists := tableMap[tableName]; exists && table.Name != "" {
			sorted = append(sorted, table)
		}
	}
	
	// Visit each table
	for tableName := range dependencies {
		if !visited[tableName] && tableName != "" {
			visit(tableName, make(map[string]bool))
		}
	}
	
	// Reverse the result to get the correct order
	for i, j := 0, len(sorted)-1; i < j; i, j = i+1, j-1 {
		sorted[i], sorted[j] = sorted[j], sorted[i]
	}
	
	return sorted
}

func (m *MySQLManager) createTable(table Table, schema *Schema) error {
	var columnDefs []string
	var primaryKeys []string
	var uniqueConstraints []string
	var foreignKeys []string

	for _, col := range table.Columns {
		// Build column definition
		colDef := fmt.Sprintf("`%s` ", col.Name)
		
		// Use the column type directly without conversion
		dataType := col.Type
		
		// Handle enum type specifically
		if dataType == "enum" {
			// Find the enum definition in the schema
			for _, enum := range schema.Enums {
				if enum.Name == col.Enum {
					values := make([]string, len(enum.Values))
					for i, v := range enum.Values {
						values[i] = fmt.Sprintf("'%s'", v)
					}
					dataType = fmt.Sprintf("ENUM(%s)", strings.Join(values, ", "))
					break
				}
			}
		}
		
		colDef += dataType
		
		// Handle nullable
		if !col.Nullable {
			colDef += " NOT NULL"
		}
		
		// Handle default value
		if col.Default != nil && col.Default != "" {
			colDef += fmt.Sprintf(" DEFAULT %v", col.Default)
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
		
		// Track foreign keys
		if col.ForeignKey != nil {
			fkDef := fmt.Sprintf("FOREIGN KEY (`%s`) REFERENCES `%s`(`%s`)",
				col.Name,
				col.ForeignKey.Table,
				col.ForeignKey.Column)
			foreignKeys = append(foreignKeys, fkDef)
		}
	}
	
	// Add primary key constraint if any
	if len(primaryKeys) > 0 {
		pkNames := make([]string, len(primaryKeys))
		for i, pk := range primaryKeys {
			pkNames[i] = "`" + pk + "`"
		}
		columnDefs = append(columnDefs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkNames, ", ")))
	}
	
	// Add unique constraints
	for _, uniqueCol := range uniqueConstraints {
		columnDefs = append(columnDefs, fmt.Sprintf("UNIQUE KEY (`%s`)", uniqueCol))
	}
	
	// Add foreign key constraints
	columnDefs = append(columnDefs, foreignKeys...)
	
	// Build and execute CREATE TABLE statement
	createSQL := fmt.Sprintf("CREATE TABLE `%s` (\n  %s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
		table.Name,
		strings.Join(columnDefs, ",\n  "))
	
	m.logSQL("Create Table", createSQL)
	if _, err := m.DB.Exec(createSQL); err != nil {
		m.log("Error creating table %s: %v", table.Name, err)
		return fmt.Errorf("creating table %s: %v", table.Name, err)
	}
	return nil
}

func (m *MySQLManager) importCSV(tableName, csvPath string, schema *Schema) error {
	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("opening CSV file: %v", err)
	}
	defer file.Close()

	// Get columns from schema
	var columns []string
	var columnTypes []string
	
	for _, table := range schema.Tables {
		if table.Name == tableName {
			for _, col := range table.Columns {
				columns = append(columns, col.Name)
				columnTypes = append(columnTypes, strings.ToLower(col.Type))
			}
			break
		}
	}

	if len(columns) == 0 {
		return fmt.Errorf("no columns found in schema for table %s", tableName)
	}

	reader := csv.NewReader(file)
	// Skip header row
	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("reading CSV header: %v", err)
	}

	// Prepare placeholders for the INSERT statement
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	// Create INSERT statement
	insertSQL := fmt.Sprintf("INSERT INTO `%s` (`%s`) VALUES (%s)",
		tableName,
		strings.Join(columns, "`, `"),
		strings.Join(placeholders, ", "))

	m.logSQL("Insert", insertSQL)

	// Prepare statement
	stmt, err := m.DB.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("preparing insert statement: %v", err)
	}
	defer stmt.Close()

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
			// Handle NULL values
			if v == "" || v == "NULL" || v == "null" {
				values[i] = nil
				continue
			}

			// Handle boolean values
			if i < len(columnTypes) && (columnTypes[i] == "boolean" || columnTypes[i] == "bool") {
				switch strings.ToLower(v) {
				case "true", "t", "yes", "y", "1":
					values[i] = 1
				case "false", "f", "no", "n", "0":
					values[i] = 0
				default:
					values[i] = v
				}
				continue
			}

			// Handle JSON and array values
			if i < len(columnTypes) && (columnTypes[i] == "json" || columnTypes[i] == "jsonb" || strings.HasPrefix(columnTypes[i], "array")) {
				// Check if the value is already in JSON format
				if strings.HasPrefix(v, "{") && strings.HasSuffix(v, "}") && !strings.HasPrefix(v, "{\"") {
					// Convert PostgreSQL array format {a,b,c} to JSON array ["a","b","c"]
					elements := strings.Split(strings.Trim(v, "{}"), ",")
					jsonArray := make([]string, len(elements))
					for j, elem := range elements {
						jsonArray[j] = fmt.Sprintf("%q", strings.TrimSpace(elem))
					}
					v = "[" + strings.Join(jsonArray, ",") + "]"
				} else if strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]") {
					// Already in JSON array format, keep as is
				} else if !strings.HasPrefix(v, "{\"") && !strings.HasPrefix(v, "[") {
					// Convert single value to JSON array
					v = fmt.Sprintf("[\"%s\"]", v)
				}
				values[i] = v
				continue
			}

			// Handle timestamp values
			if strings.Contains(v, "UTC") {
				if t, err := time.Parse("2006-01-02 15:04:05.999999 -0700 MST", v); err == nil {
					values[i] = t.Format("2006-01-02 15:04:05")
					continue
				}
			}
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				values[i] = t.Format("2006-01-02 15:04:05")
				continue
			}

			// Default case
			values[i] = v
		}

		m.log("Executing insert for table %s row %d: %v", tableName, rowCount+1, values)
		
		if _, err := stmt.Exec(values...); err != nil {
			// If the error is related to enum values, log a warning and continue
			if strings.Contains(err.Error(), "Data truncated for column") {
				m.log("Warning: Data truncated for table %s row %d. This may be due to enum value constraints. Skipping row.", tableName, rowCount+1)
				rowCount++
				continue
			}
			return fmt.Errorf("executing insert for table %s row %d: %v\nValues: %v", tableName, rowCount+1, err, values)
		}
		rowCount++
	}

	m.log("Imported %d rows into table %s", rowCount, tableName)
	return nil
}

func (m *MySQLManager) ReadSchemaFromFile(filename string) (*Schema, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	
	var schema Schema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("parsing schema file: %v", err)
	}
	
	return &schema, nil
}

// Fetch implements the DatabaseManager interface
func (m *MySQLManager) Fetch(baseURL, databaseName, versionName, outputDir, token string) error {
	// Create the output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("creating output directory: %v", err)
	}

	// Construct the API URL
	apiURL := fmt.Sprintf("%s/api/v1.0/databases/testdata/download?database_name=%s&version_name=%s", 
		baseURL, databaseName, versionName)

	// Create HTTP request
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return fmt.Errorf("creating request: %v", err)
	}

	// Add authorization header
	req.Header.Set("Authorization", fmt.Sprintf("cli_%s", token))

	// Make the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized: please check your API token")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
	}

	// Create a temporary file to store the zip
	tempFile, err := os.CreateTemp("", "seedmancer-*.zip")
	if err != nil {
		return fmt.Errorf("creating temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Copy the response body to the temp file
	if _, err := io.Copy(tempFile, resp.Body); err != nil {
		return fmt.Errorf("downloading zip file: %v", err)
	}

	// Open the zip file
	zipReader, err := zip.OpenReader(tempFile.Name())
	if err != nil {
		return fmt.Errorf("opening zip file: %v", err)
	}
	defer zipReader.Close()

	// Extract the zip contents
	for _, file := range zipReader.File {
		// Open the file in the zip
		rc, err := file.Open()
		if err != nil {
			return fmt.Errorf("opening file in zip: %v", err)
		}

		// Create the output file
		outPath := filepath.Join(outputDir, file.Name)
		outFile, err := os.Create(outPath)
		if err != nil {
			rc.Close()
			return fmt.Errorf("creating output file: %v", err)
		}

		// Copy the contents
		if _, err := io.Copy(outFile, rc); err != nil {
			outFile.Close()
			rc.Close()
			return fmt.Errorf("extracting file: %v", err)
		}

		outFile.Close()
		rc.Close()
	}

	return nil
}
