package db

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type PostgresManager struct {
	DB *sql.DB
	Debug bool
}

func (p *PostgresManager) SetDebug(debug bool) {
	p.Debug = debug
}

func (p *PostgresManager) log(format string, args ...interface{}) {
	if p.Debug {
		fmt.Printf(format+"\n", args...)
	}
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

func (p *PostgresManager) RestoreFromCSV(directory string) error {
	if p.DB == nil {
		return errors.New("no database connection")
	}

	// Drop and recreate schema
	if _, err := p.DB.Exec(`DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
		return fmt.Errorf("failed to reset schema: %v", err)
	}
	fmt.Println("Dropped and recreated public schema")

	// Get list of CSV files in directory
	files, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("reading directory: %v", err)
	}

	// First pass: Create tables based on CSV headers
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".csv") {
			continue
		}

		tableName := strings.TrimSuffix(file.Name(), ".csv")
		csvPath := filepath.Join(directory, file.Name())

		// Open and read CSV header
		csvFile, err := os.Open(csvPath)
		if err != nil {
			return fmt.Errorf("opening CSV file %s: %v", file.Name(), err)
		}

		reader := csv.NewReader(csvFile)
		header, err := reader.Read()
		if err != nil {
			csvFile.Close()
			return fmt.Errorf("reading CSV header from %s: %v", file.Name(), err)
		}

		// Read first data row to infer types
		firstRow, err := reader.Read()
		if err != nil && err != io.EOF {
			csvFile.Close()
			return fmt.Errorf("reading first data row from %s: %v", file.Name(), err)
		}
		csvFile.Close()

		// Generate CREATE TABLE statement
		columns := make([]string, len(header))
		for i, colName := range header {
			colType := "text" // default type
			if firstRow != nil && i < len(firstRow) {
				// Infer column type from data
				colType = inferColumnType(firstRow[i])
			}
			columns[i] = fmt.Sprintf(`"%s" %s`, colName, colType)
		}

		createSQL := fmt.Sprintf(`CREATE TABLE "%s" (%s)`,
			tableName,
			strings.Join(columns, ", "))

		if _, err := p.DB.Exec(createSQL); err != nil {
			return fmt.Errorf("creating table %s: %v", tableName, err)
		}
		p.log("Created table %s", tableName)
	}

	// Second pass: Import data
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
				// Try to convert numeric strings to appropriate types
				if v, err := strconv.ParseInt(v, 10, 64); err == nil {
					values[i] = v
					continue
				}
				if v, err := strconv.ParseFloat(v, 64); err == nil {
					values[i] = v
					continue
				}
				// If not numeric, keep as string
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

func (p *PostgresManager) readSchemaFromFile(filename string) (*Schema, error) {
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

// Helper functions
func getColumnNames(table Table) []string {
	names := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		names[i] = col.Name
	}
	return names
}

func isNullable(column Column) bool {
	return column.Nullable
}

func executeShellCommand(command string) error {
	cmd := exec.Command("sh", "-c", command)
	return cmd.Run()
}

func executeShellCommandWithOutput(command string) (string, error) {
	cmd := exec.Command("sh", "-c", command)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

