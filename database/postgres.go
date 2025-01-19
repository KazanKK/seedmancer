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
	"strings"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type PostgresManager struct {
	DB *sql.DB
	Debug bool
}

func (p *PostgresManager) ConnectWithDSN(dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	p.DB = db
	return nil
}

func (p *PostgresManager) CreateSnapshot(snapshotName string) error {
	if p.DB == nil {
		return errors.New("no database connection")
	}

	// Get schema
	schema, err := p.ExtractSchema()
	if err != nil {
		return err
	}

	// Save schema
	if err := p.SaveSchemaToFile(schema, "schema.json"); err != nil {
		return err
	}

	// Export each table to CSV
	for _, table := range schema.Tables {
		query := fmt.Sprintf(`COPY "%s" TO '%s.csv' WITH CSV HEADER`, table.Name, snapshotName)
		if _, err := p.DB.Exec(query); err != nil {
			return fmt.Errorf("exporting table %s: %v", table.Name, err)
		}
	}

	return nil
}

func (p *PostgresManager) RestoreSnapshot(snapshotName string) error {
	if p.DB == nil {
		return errors.New("no database connection")
	}

	// Drop and recreate schema
	if _, err := p.DB.Exec(`DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
		return fmt.Errorf("failed to reset schema: %v", err)
	}

	// Read schema.json
	schema, err := p.readSchemaFromFile("schema.json")
	if err != nil {
		return err
	}

	// Create tables
	for _, table := range schema.Tables {
		createSQL := generateCreateTableSQL(table)
		statements := strings.Split(createSQL, ";")
		for _, stmt := range statements {
			if stmt = strings.TrimSpace(stmt); stmt != "" {
				if _, err := p.DB.Exec(stmt + ";"); err != nil {
					return fmt.Errorf("creating table %s: %v", table.Name, err)
				}
			}
		}
	}

	// Import data
	for _, table := range schema.Tables {
		query := fmt.Sprintf(`COPY "%s" FROM '%s.csv' WITH CSV HEADER`, table.Name, snapshotName)
		if _, err := p.DB.Exec(query); err != nil {
			return fmt.Errorf("importing table %s: %v", table.Name, err)
		}
	}

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
			CASE 
				WHEN EXISTS (
					SELECT 1 FROM pg_type pt
					JOIN pg_enum e ON pt.oid = e.enumtypid
					WHERE pt.typname = c.udt_name
				) THEN 'USER-DEFINED'
				ELSE c.data_type
			END as data_type,
			c.is_nullable,
			c.column_default,
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
		var tableName, columnName, dataType, isNullable string
		var columnDefault sql.NullString
		var isPrimary, isUnique bool
		var foreignTable, foreignColumn sql.NullString
		var enumValuesStr string

		if err := rows.Scan(&tableName, &columnName, &dataType, &isNullable, &columnDefault, 
			&isPrimary, &isUnique, &foreignTable, &foreignColumn, &enumValuesStr); err != nil {
			return nil, err
		}

		// Convert comma-separated string to slice
		var enumValues []string
		if enumValuesStr != "" {
			enumValues = strings.Split(enumValuesStr, ",")
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

		var foreignKey *ForeignKey
		if foreignTable.Valid && foreignColumn.Valid {
			foreignKey = &ForeignKey{
				Table:  foreignTable.String,
				Column: foreignColumn.String,
			}
		}

		column := Column{
			Name:       columnName,
			Type:       dataType,
			Nullable:   isNullable == "YES",
			IsPrimary:  isPrimary,
			IsUnique:   isUnique,
			ForeignKey: foreignKey,
			Values:     enumValues,
		}

		// Only set Default if it has a value
		if columnDefault.Valid {
			column.Default = columnDefault.String
		}

		currentColumns = append(currentColumns, column)
	}

	if currentTable != "" {
		schema.Tables = append(schema.Tables, Table{
			Name:    currentTable,
			Columns: currentColumns,
		})
	}

	return schema, nil
}

func (p *PostgresManager) SaveSchemaToFile(schema *Schema, filename string) error {
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
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

	// Read schema.json
	schema, err := p.readSchemaFromFile("schema.json")
	if err != nil {
		return fmt.Errorf("reading schema.json: %v", err)
	}

	// Sort tables based on dependencies
	sortedTables := sortTablesByDependencies(schema.Tables)

	// Create sequences first
	for _, table := range sortedTables {
		for _, col := range table.Columns {
			if col.Default != nil && strings.Contains(fmt.Sprintf("%v", col.Default), "nextval") {
				seqName := strings.Trim(strings.Split(fmt.Sprintf("%v", col.Default), "'")[1], "()")
				if _, err := p.DB.Exec(fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s;", seqName)); err != nil {
					return fmt.Errorf("failed to create sequence %s: %v", seqName, err)
				}
			}
		}
	}

	// Create tables in sorted order
	for _, table := range sortedTables {
		createSQL := generateCreateTableSQL(table)
		p.log("Creating table %s with SQL:\n%s", table.Name, createSQL)
		
		statements := strings.Split(createSQL, ";")
		for _, stmt := range statements {
			if stmt = strings.TrimSpace(stmt); stmt != "" {
				if _, err := p.DB.Exec(stmt + ";"); err != nil {
					return fmt.Errorf("executing SQL for table %s: %v\nSQL: %s", table.Name, err, stmt)
				}
			}
		}

		if !p.Debug {
			fmt.Printf("Created table: %s\n", table.Name)
		}
	}

	// Import data in sorted order
	for _, table := range sortedTables {
		csvPath := filepath.Join(directory, fmt.Sprintf("%s.csv", strings.ToLower(table.Name)))
		if err := p.importCSV(table.Name, csvPath); err != nil {
			return fmt.Errorf("importing data for table %s: %v", table.Name, err)
		}
		if !p.Debug {
			fmt.Printf("Imported data for table: %s\n", table.Name)
		}
	}

	return nil
}

// sortTablesByDependencies sorts tables so that referenced tables come before tables that reference them
func sortTablesByDependencies(tables []Table) []Table {
	// Create dependency graph
	graph := make(map[string][]string)
	inDegree := make(map[string]int)
	tableMap := make(map[string]Table)

	// Initialize
	for _, table := range tables {
		graph[table.Name] = []string{}
		inDegree[table.Name] = 0
		tableMap[table.Name] = table
	}

	// Build dependency graph
	for _, table := range tables {
		for _, col := range table.Columns {
			if col.ForeignKey != nil {
				graph[table.Name] = append(graph[table.Name], col.ForeignKey.Table)
				inDegree[col.ForeignKey.Table]++
			}
		}
	}

	// Topological sort using Kahn's algorithm
	var sorted []Table
	var queue []string

	// Add all nodes with no dependencies to queue
	for tableName := range graph {
		if inDegree[tableName] == 0 {
			queue = append(queue, tableName)
		}
	}

	// Process queue
	for len(queue) > 0 {
		tableName := queue[0]
		queue = queue[1:]
		sorted = append(sorted, tableMap[tableName])

		// Update dependencies
		for _, dep := range graph[tableName] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}

	// Reverse the order since we want referenced tables first
	for i := 0; i < len(sorted)/2; i++ {
		j := len(sorted) - 1 - i
		sorted[i], sorted[j] = sorted[j], sorted[i]
	}

	return sorted
}


func generateCreateTableSQL(table Table) string {
	var columns []string
	var primaryKeys []string

	// Quote the table name to handle reserved keywords
	quotedTableName := fmt.Sprintf(`"%s"`, table.Name)

	for _, col := range table.Columns {
		colDef := fmt.Sprintf(`"%s"`, col.Name)  // Quote column names
		
		// Handle enum types
		if strings.Contains(strings.ToLower(col.Type), "user-defined") {
			colDef += " text" // Convert enum to text for simplicity
		} else {
			colDef += " " + col.Type
		}

		if !col.Nullable {
			colDef += " NOT NULL"
		}
		if col.Default != nil {
			defaultVal := fmt.Sprintf("%v", col.Default)
			if defaultVal == "CURRENT_TIMESTAMP" {
				colDef += " DEFAULT CURRENT_TIMESTAMP"
			} else if defaultVal == "" && col.Nullable {
				// Skip empty default for nullable columns
				continue
			} else if strings.Contains(defaultVal, "nextval") {
				colDef += fmt.Sprintf(" DEFAULT %s", defaultVal)
			} else if !strings.Contains(strings.ToLower(col.Type), "int") &&
					  !strings.Contains(strings.ToLower(col.Type), "bool") {
				colDef += fmt.Sprintf(" DEFAULT '%s'", defaultVal)
			} else {
				colDef += fmt.Sprintf(" DEFAULT %v", defaultVal)
			}
		}

		columns = append(columns, colDef)

		if col.IsPrimary {
			primaryKeys = append(primaryKeys, fmt.Sprintf(`"%s"`, col.Name))
		}
	}

	// Add PRIMARY KEY constraint as a separate ALTER TABLE statement
	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n);",
		quotedTableName,
		strings.Join(columns, ",\n  "))

	if len(primaryKeys) > 0 {
		createSQL += fmt.Sprintf("\nALTER TABLE %s ADD PRIMARY KEY (%s);",
			quotedTableName,
			strings.Join(primaryKeys, ", "))
	}

	return createSQL
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

func (p *PostgresManager) log(format string, args ...interface{}) {
	if p.Debug {
		fmt.Printf(format+"\n", args...)
	}
}

func (p *PostgresManager) SetDebug(debug bool) {
	p.Debug = debug
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
		return err
	}

	// Create the copy statement
	stmt, err := tx.Prepare(pq.CopyIn(tableName, header...))
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Copy data
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		values := make([]interface{}, len(record))
		for i, v := range record {
			if v == "NULL" {
				values[i] = nil
			} else {
				values[i] = v
			}
		}

		if _, err := stmt.Exec(values...); err != nil {
			return err
		}
	}

	if err := stmt.Close(); err != nil {
		return err
	}

	return tx.Commit()
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

