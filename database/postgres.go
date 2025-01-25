package db

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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

	query := `
		WITH enum_info AS (
			SELECT 
				t.typname as enum_name,
				array_to_string(array_agg(e.enumlabel ORDER BY e.enumsortorder), ',') as enum_values
			FROM pg_type t
			JOIN pg_enum e ON t.oid = e.enumtypid
			JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
			WHERE n.nspname = 'public'
			GROUP BY t.typname
		),
		fk_info AS (
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
			SELECT 
				t.table_name, 
				c.column_name
			FROM information_schema.table_constraints t
			JOIN information_schema.constraint_column_usage c
				ON c.constraint_name = t.constraint_name
			WHERE t.constraint_type = 'PRIMARY KEY'
		),
		unique_info AS (
			SELECT 
				t.table_name, 
				c.column_name
			FROM information_schema.table_constraints t
			JOIN information_schema.constraint_column_usage c
				ON c.constraint_name = t.constraint_name
			WHERE t.constraint_type = 'UNIQUE'
		)
		SELECT 
			t.table_name,
			c.column_name,
			c.data_type,
			CASE 
				WHEN c.data_type LIKE '%char%' OR c.data_type = 'text' THEN 'string'
				WHEN c.data_type LIKE '%int%' OR c.data_type = 'numeric' THEN 'number'
				WHEN c.data_type LIKE '%timestamp%' OR c.data_type = 'date' THEN 'datetime'
				WHEN c.data_type = 'boolean' THEN 'boolean'
				WHEN c.data_type = 'jsonb' OR c.data_type = 'json' THEN 'json'
				WHEN EXISTS (
					SELECT 1 FROM pg_type pt
					JOIN pg_enum e ON pt.oid = e.enumtypid
					WHERE pt.typname = c.udt_name
				) THEN 'enum'
				ELSE 'string'
			END as system_type,
			c.is_nullable,
			CASE 
				WHEN c.column_default LIKE 'nextval%' THEN c.column_default
				WHEN c.column_default = 'CURRENT_TIMESTAMP' THEN 'CURRENT_TIMESTAMP'
				WHEN c.column_default LIKE '''%''::timestamp%' THEN 
					regexp_replace(c.column_default, '''(.+)''.*', '\1')
				WHEN c.column_default LIKE 'true' OR c.column_default LIKE 'false' THEN 
					c.column_default
				WHEN c.column_default ~ '^\d+$' THEN 
					c.column_default
				ELSE 
					regexp_replace(c.column_default, '''(.+)''.*', '\1')
			END as column_default,
			EXISTS (
				SELECT 1 FROM pk_info pk 
				WHERE pk.table_name = t.table_name 
				AND pk.column_name = c.column_name
			) as is_primary,
			EXISTS (
				SELECT 1 FROM unique_info u 
				WHERE u.table_name = t.table_name 
				AND u.column_name = c.column_name
			) as is_unique,
			fk.foreign_table_name,
			fk.foreign_column_name,
			COALESCE((SELECT enum_values FROM enum_info WHERE enum_name = c.udt_name), '') as enum_values
		FROM 
			information_schema.tables t
			JOIN information_schema.columns c ON t.table_name = c.table_name
			LEFT JOIN fk_info fk ON t.table_name = fk.table_name 
				AND c.column_name = fk.column_name
		WHERE 
			t.table_schema = 'public'
			AND t.table_type = 'BASE TABLE'
		ORDER BY 
			t.table_name, c.ordinal_position;
	`

	rows, err := p.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schema := &Schema{}
	currentTable := ""
	var currentColumns []Column

	for rows.Next() {
		var tableName, columnName, dataType, systemType, isNullable string
		var columnDefault sql.NullString
		var isPrimary, isUnique bool
		var foreignTable, foreignColumn sql.NullString
		var enumValuesStr string

		err := rows.Scan(
			&tableName,
			&columnName,
			&dataType,
			&systemType,
			&isNullable,
			&columnDefault,
			&isPrimary,
			&isUnique,
			&foreignTable,
			&foreignColumn,
			&enumValuesStr,
		)
		if err != nil {
			return nil, err
		}

		// Convert comma-separated string to slice
		var enumValues []string
		if enumValuesStr != "" {
			enumValues = strings.Split(enumValuesStr, ",")
		}

		column := Column{
			Name:       columnName,
			Type:       dataType,
			SystemType: systemType,
			Nullable:   isNullable == "YES",
			IsPrimary:  isPrimary,
			IsUnique:   isUnique,
			Values:     enumValues,
		}

		// Only set Default if it has a value
		if columnDefault.Valid {
			column.Default = columnDefault.String
		}

		if foreignTable.Valid && foreignColumn.Valid {
			column.ForeignKey = &ForeignKey{
				Table:  foreignTable.String,
				Column: foreignColumn.String,
			}
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

// Helper function to sort tables based on dependencies
func sortTablesByDependencies(tables []Table) []Table {
	// Create dependency graph
	deps := make(map[string]map[string]bool)
	for _, table := range tables {
		deps[table.Name] = make(map[string]bool)
		for _, col := range table.Columns {
			if col.ForeignKey != nil {
				deps[table.Name][col.ForeignKey.Table] = true
			}
		}
	}

	// Topological sort
	var sorted []Table
	visited := make(map[string]bool)
	visiting := make(map[string]bool)

	var visit func(table Table)
	visit = func(table Table) {
		if visiting[table.Name] {
			return // Handle circular dependencies
		}
		if visited[table.Name] {
			return
		}
		visiting[table.Name] = true

		for dep := range deps[table.Name] {
			for _, t := range tables {
				if t.Name == dep {
					visit(t)
				}
			}
		}

		visiting[table.Name] = false
		visited[table.Name] = true
		sorted = append(sorted, table)
	}

	for _, table := range tables {
		if !visited[table.Name] {
			visit(table)
		}
	}

	return sorted
}

func (p *PostgresManager) RestoreFromCSV(directory string) error {
	if p.DB == nil {
		return errors.New("no database connection")
	}

	// Read schema from schema.json
	schemaPath := filepath.Join(directory, "schema.json")
	schema, err := p.ReadSchemaFromFile(schemaPath)
	if err != nil {
		return fmt.Errorf("reading schema file: %v", err)
	}

	dropSQL := `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`
	p.logSQL("Reset Schema", dropSQL)
	if _, err := p.DB.Exec(dropSQL); err != nil {
		return fmt.Errorf("failed to reset schema: %v", err)
	}

	// Sort tables based on dependencies
	sortedTables := sortTablesByDependencies(schema.Tables)

	// First: Create sequences for serial/identity columns
	for _, table := range schema.Tables {
		for _, col := range table.Columns {
			if col.Default != nil {
				defaultStr, ok := col.Default.(string)
				if ok && strings.Contains(defaultStr, "nextval") {
					// Extract sequence name from nextval('sequence_name'::regclass)
					seqName := strings.Split(strings.Split(defaultStr, "'")[1], "::")[0]
					createSeqSQL := fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s", seqName)
					p.logSQL(fmt.Sprintf("Create Sequence %s", seqName), createSeqSQL)
					if _, err := p.DB.Exec(createSeqSQL); err != nil {
						return fmt.Errorf("creating sequence %s: %v", seqName, err)
					}
				}
			}
		}
	}

	// Second: Process and create all enum types
	enumTypes := make(map[string]string)
	for _, table := range sortedTables {
		for _, col := range table.Columns {
			if strings.Contains(strings.ToUpper(col.Type), "USER-DEFINED") {
				enumType := col.Name + "_type"
				enumTypes[col.Name] = enumType

				createTypeSQL := fmt.Sprintf(`DO $$ 
					BEGIN 
						IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '%s') THEN
							CREATE TYPE %s AS ENUM ('UNKNOWN');
						END IF;
					END $$;`, enumType, enumType)
				
				p.logSQL("Create Custom Type", createTypeSQL)
				if _, err := p.DB.Exec(createTypeSQL); err != nil {
					return fmt.Errorf("creating custom type: %v", err)
				}
			}
		}
	}

	// Third: Create tables with proper enum types in sorted order
	for _, table := range sortedTables {
		columns := make([]string, len(table.Columns))
		for i, col := range table.Columns {
			colType := col.Type
			if strings.Contains(strings.ToUpper(colType), "USER-DEFINED") {
				if enumType, ok := enumTypes[col.Name]; ok {
					colType = enumType
				}
			}

			colDef := fmt.Sprintf(`%s %s`, 
				pq.QuoteIdentifier(col.Name),
				colType)
			
			if !col.Nullable {
				colDef += " NOT NULL"
			}
			if col.Default != nil {
				defaultVal := ""
				switch v := col.Default.(type) {
				case string:
					if strings.HasPrefix(v, "nextval") || 
					   v == "CURRENT_TIMESTAMP" ||
					   strings.HasPrefix(v, "gen_random_uuid") {
						defaultVal = v
					} else {
						defaultVal = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
					}
				default:
					defaultVal = fmt.Sprintf("%v", v)
				}
				colDef += " DEFAULT " + defaultVal
			}
			if col.IsPrimary {
				colDef += " PRIMARY KEY"
			}
			if col.IsUnique {
				colDef += " UNIQUE"
			}
			columns[i] = colDef
		}

		createSQL := fmt.Sprintf(`CREATE TABLE %s (%s)`,
			pq.QuoteIdentifier(table.Name),
			strings.Join(columns, ", "))
		
		p.logSQL(fmt.Sprintf("Create Table %s", table.Name), createSQL)
		if _, err := p.DB.Exec(createSQL); err != nil {
			return fmt.Errorf("creating table %s: %v", table.Name, err)
		}

		// Add foreign key constraints immediately after creating each table
		for _, col := range table.Columns {
			if col.ForeignKey != nil {
				fkSQL := fmt.Sprintf(`ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s (%s)`,
					pq.QuoteIdentifier(table.Name),
					pq.QuoteIdentifier(col.Name),
					pq.QuoteIdentifier(col.ForeignKey.Table),
					pq.QuoteIdentifier(col.ForeignKey.Column))
				
				p.logSQL(fmt.Sprintf("Add Foreign Key %s.%s", table.Name, col.Name), fkSQL)
				if _, err := p.DB.Exec(fkSQL); err != nil {
					return fmt.Errorf("adding foreign key to %s.%s: %v", table.Name, col.Name, err)
				}
			}
		}
	}

	// Second pass: Import data from CSV files
	files, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("reading directory: %v", err)
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".csv") {
			continue
		}

		tableName := strings.TrimSuffix(file.Name(), ".csv")
		csvPath := filepath.Join(directory, file.Name())

		if err := p.importCSV(tableName, csvPath); err != nil {
			return fmt.Errorf("importing data for table %s: %v", tableName, err)
		}
		p.log("Imported data for table %s", tableName)
	}

	return nil
}

// inferColumnType attempts to determine the PostgreSQL type from a string value
func inferColumnType(value string) string {
	if value == "" {
		return "text"
	}

	// Try to parse as integer
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return "bigint"
	}

	// Try to parse as float
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return "double precision"
	}

	// Try to parse as boolean
	if value == "true" || value == "false" {
		return "boolean"
	}

	// Try to parse as timestamp
	if _, err := time.Parse(time.RFC3339, value); err == nil {
		return "timestamp with time zone"
	}

	// Try to parse as date
	if _, err := time.Parse("2006-01-02", value); err == nil {
		return "date"
	}

	// Default to text
	return "text"
}

func (p *PostgresManager) importCSV(tableName, csvPath string) error {
	file, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	defer file.Close()

	tx, err := p.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Read CSV header
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("reading CSV header: %v", err)
	}

	// Use header for column names in COPY statement
	quotedColumns := make([]string, len(header))
	for i, h := range header {
		quotedColumns[i] = pq.QuoteIdentifier(h)
	}

	stmt, err := tx.Prepare(pq.CopyIn(tableName, header...))
	if err != nil {
		return fmt.Errorf("preparing COPY statement: %v", err)
	}
	defer stmt.Close()

	// Read and process CSV data
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading CSV record: %v", err)
		}

		// Convert empty strings and "NULL" strings to nil
		values := make([]interface{}, len(record))
		for i, v := range record {
			switch v {
			case "", "NULL", "null":
				values[i] = nil
			default:
				// Try to parse timestamp formats
				if strings.Contains(v, "UTC") {
					// Parse UTC timestamp format
					if t, err := time.Parse("2006-01-02 15:04:05.999999 -0700 MST", v); err == nil {
						values[i] = t.Format("2006-01-02 15:04:05.999999-07")
						continue
					}
				}
				// Try to parse other timestamp formats
				if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
					values[i] = t.Format("2006-01-02 15:04:05.999999-07")
					continue
				}
				// Try to convert numeric strings to appropriate types
				if v, err := strconv.ParseInt(v, 10, 64); err == nil {
					values[i] = v
					continue
				}
				if v, err := strconv.ParseFloat(v, 64); err == nil {
					values[i] = v
					continue
				}
				// If not numeric or timestamp, keep as string
				values[i] = record[i]
			}
		}

		if _, err := stmt.Exec(values...); err != nil {
			return fmt.Errorf("executing COPY for table %s: %v", tableName, err)
		}
	}

	if err := stmt.Close(); err != nil {
		return fmt.Errorf("closing COPY statement: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %v", err)
	}

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
		return nil, err
	}
	
	return &schema, nil
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

func (p *PostgresManager) GenerateFakeDataFromSchema(schema *Schema, outputDir string, rowCount int) error {
	// Reset generated values for new run
	generatedValues = make(map[string][]string)
	idCounters = make(map[string]int)
	uniqueValueSets = make(map[string]map[string]bool)

	// First pass: Generate primary key data
	for _, table := range schema.Tables {
		for _, col := range table.Columns {
			if col.IsPrimary {
				key := fmt.Sprintf("%s.%s", table.Name, col.Name)
				values := make([]string, rowCount)
				for i := 0; i < rowCount; i++ {
					values[i] = generatePrimaryKeyValue(col, table.Name)
				}
				valueMutex.Lock()
				generatedValues[key] = values
				valueMutex.Unlock()
			}
		}
	}

	// Second pass: Generate data for each table
	for _, table := range schema.Tables {
		if err := generateTableData(table, outputDir, rowCount); err != nil {
			return fmt.Errorf("generating data for table %s: %v", table.Name, err)
		}
		p.log("Generated data for table: %s", table.Name)
	}

	return nil
}

