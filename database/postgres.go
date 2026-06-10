package db

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/KazanKK/seedmancer/internal/ui"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type PostgresManager struct {
	DB *sql.DB
}

func (p *PostgresManager) log(format string, args ...interface{}) {
	ui.Debug("[postgres] "+format, args...)
}

func (p *PostgresManager) logSQL(operation, sql string) {
	ui.Debug("[postgres] %s:\n%s", operation, sql)
}

func (p *PostgresManager) ConnectWithDSN(dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	p.DB = db
	return nil
}

// ExecSQL runs sqlText inside a single transaction. On any error the
// transaction is rolled back so the database stays at its pre-call state.
// lib/pq's simple query protocol accepts multi-statement strings as long
// as no parameter placeholders are used, which is the contract callers
// rely on for agent-written DML scripts (INSERT/UPDATE/DELETE chains).
func (p *PostgresManager) ExecSQL(sqlText string) error {
	if p.DB == nil {
		return errors.New("no database connection")
	}
	tx, err := p.DB.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction: %v", err)
	}
	if _, err := tx.Exec(sqlText); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("executing SQL: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing SQL transaction: %v", err)
	}
	return nil
}

func (p *PostgresManager) ExtractSchema() (*Schema, error) {
	if p.DB == nil {
		return nil, errors.New("no database connection")
	}

	// Diagnostic-only query — skip the round trip unless --debug is on.
	if ui.DebugEnabled() {
		p.debugVarcharLengths()
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

	// Extract user-defined functions (prokind 'f'=function, 'p'=procedure; requires PG11+)
	var functions []Function
	funcRows, err := p.DB.Query(`
		SELECT
			p.proname AS name,
			pg_get_functiondef(p.oid) AS definition
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public'
		AND p.prokind IN ('f', 'p')
		ORDER BY p.proname
	`)
	if err != nil {
		p.log("Warning: could not query functions (requires PostgreSQL 11+): %v", err)
	} else {
		defer funcRows.Close()
		for funcRows.Next() {
			var name, definition string
			if err := funcRows.Scan(&name, &definition); err != nil {
				return nil, fmt.Errorf("scanning function info: %v", err)
			}
			functions = append(functions, Function{
				Name:       name,
				Definition: definition,
			})
		}
	}

	// Extract user-defined triggers whose function lives in the public schema.
	// This captures triggers on any schema (e.g. auth.users) as long as they
	// call a user-defined public function, while excluding Supabase system
	// triggers (pgsodium, realtime, storage, vault, etc.).
	var triggers []Trigger
	triggerRows, err := p.DB.Query(`
		SELECT
			t.tgname AS name,
			c.relname AS table_name,
			tn.nspname AS table_schema,
			pg_get_triggerdef(t.oid) AS definition
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace tn ON tn.oid = c.relnamespace
		JOIN pg_proc p ON p.oid = t.tgfoid
		JOIN pg_namespace fn ON fn.oid = p.pronamespace
		WHERE NOT t.tgisinternal
		AND fn.nspname = 'public'
		ORDER BY tn.nspname, c.relname, t.tgname
	`)
	if err != nil {
		return nil, fmt.Errorf("querying triggers: %v", err)
	}
	defer triggerRows.Close()
	for triggerRows.Next() {
		var name, tableName, tableSchema, definition string
		if err := triggerRows.Scan(&name, &tableName, &tableSchema, &definition); err != nil {
			return nil, fmt.Errorf("scanning trigger info: %v", err)
		}
		triggers = append(triggers, Trigger{
			Name:        name,
			TableName:   tableName,
			TableSchema: tableSchema,
			Definition:  definition,
		})
	}

	// Updated query to include character_maximum_length for varchar columns
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
				AND tc.table_schema = kcu.table_schema
			JOIN information_schema.constraint_column_usage ccu
				ON ccu.constraint_name = tc.constraint_name
				AND ccu.table_schema = tc.table_schema
			WHERE tc.constraint_type = 'FOREIGN KEY'
				AND tc.table_schema = 'public'
		),
		pk_info AS (
			SELECT t.table_name, c.column_name
			FROM information_schema.table_constraints t
			JOIN information_schema.constraint_column_usage c
				ON c.constraint_name = t.constraint_name
				AND c.table_schema = t.table_schema
			WHERE t.constraint_type = 'PRIMARY KEY'
				AND t.table_schema = 'public'
		),
		unique_info AS (
			SELECT t.table_name, c.column_name
			FROM information_schema.table_constraints t
			JOIN information_schema.constraint_column_usage c
				ON c.constraint_name = t.constraint_name
				AND c.table_schema = t.table_schema
			WHERE t.constraint_type = 'UNIQUE'
				AND t.table_schema = 'public'
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
			fk.foreign_column_name,
			c.character_maximum_length,
			COALESCE(c.is_generated, 'NEVER') AS is_generated,
			c.identity_generation
		FROM 
			information_schema.tables t
			JOIN information_schema.columns c
				ON  t.table_name   = c.table_name
				AND c.table_schema = 'public'
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
		Enums:        enums,
		Tables:       make([]Table, 0),
		Functions:    functions,
		Triggers:     triggers,
	}

	// Process tables and columns
	currentTable := ""
	var currentColumns []Column

	for rows.Next() {
		var tableName, columnName, udtName, dataType, isNullable string
		var columnDefault sql.NullString
		var isPrimary, isUnique bool
		var foreignTable, foreignColumn sql.NullString
		var charMaxLength sql.NullInt64
		var isGenerated string
		var identityGeneration sql.NullString

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
			&charMaxLength,
			&isGenerated,
			&identityGeneration,
		); err != nil {
			return nil, err
		}

		// Log all varchar/character varying columns and their length values
		if dataType == "character varying" || dataType == "varchar" {
			if charMaxLength.Valid {
				p.log("VARCHAR column: %s.%s, MaxLength: %d", tableName, columnName, charMaxLength.Int64)
			} else {
				p.log("VARCHAR column: %s.%s, MaxLength: NULL", tableName, columnName)
			}
		}

		column := Column{
			Name:        columnName,
			Type:        dataType,
			Nullable:    isNullable == "YES",
			IsPrimary:   isPrimary,
			IsUnique:    isUnique,
			IsGenerated: isGenerated == "ALWAYS" || identityGeneration.Valid,
		}

		// Handle varchar length
		if charMaxLength.Valid && (dataType == "character varying" || dataType == "varchar") {
			lengthStr := strconv.FormatInt(charMaxLength.Int64, 10)
			column.Varchar = &lengthStr
			p.log("Setting varchar length for %s.%s: %s", tableName, columnName, lengthStr)
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

	// ── CHECK constraints → AllowedValues ────────────────────────────────────
	// Query IN / ANY(ARRAY[...]) check constraints and attach allowed values to
	// the matching column so the AI can respect them during generation.
	// Postgres stores simple enum-style CHECKs as:
	//   CHECK ((col = ANY (ARRAY['a'::text, 'b'::text])))
	// rather than the SQL-standard IN (...) syntax, so we match both forms.
	checkRows, err := p.DB.Query(`
		SELECT
			cls.relname   AS table_name,
			att.attname   AS column_name,
			pg_get_constraintdef(con.oid) AS check_clause
		FROM pg_constraint con
		JOIN pg_class     cls ON cls.oid = con.conrelid
		JOIN pg_namespace ns  ON ns.oid  = cls.relnamespace
		JOIN pg_attribute att ON att.attrelid = con.conrelid
		                     AND att.attnum   = ANY(con.conkey)
		WHERE con.contype = 'c'
		  AND ns.nspname  = 'public'
		  AND (
			pg_get_constraintdef(con.oid) LIKE '%IN (%'
			OR pg_get_constraintdef(con.oid) LIKE '%ANY (ARRAY[%'
		  )
	`)
	if err == nil {
		defer checkRows.Close()
		// Handles both:  IN ('a', 'b')  and  = ANY (ARRAY['a'::text, 'b'::text])
		valRegion := regexp.MustCompile(`(?i)(?:\bIN\s*\(([^)]+)\)|ANY\s*\(\s*ARRAY\[([^\]]+)\]\s*\))`)
		quotedVal := regexp.MustCompile(`'([^']*)'`)
		for checkRows.Next() {
			var tblName, colName, clause string
			if err := checkRows.Scan(&tblName, &colName, &clause); err != nil {
				continue
			}
			sub := valRegion.FindStringSubmatch(clause)
			if sub == nil {
				continue
			}
			// sub[1] is the IN(...) group, sub[2] is the ARRAY[...] group.
			region := sub[1]
			if region == "" {
				region = sub[2]
			}
			var vals []string
			for _, mv := range quotedVal.FindAllStringSubmatch(region, -1) {
				if len(mv) > 1 {
					vals = append(vals, mv[1])
				}
			}
			if len(vals) == 0 {
				continue
			}
			// Attach to the matching column in schema.Tables
			for ti := range schema.Tables {
				if schema.Tables[ti].Name != tblName {
					continue
				}
				for ci := range schema.Tables[ti].Columns {
					if schema.Tables[ti].Columns[ci].Name == colName {
						schema.Tables[ti].Columns[ci].AllowedValues = vals
					}
				}
			}
		}
	}

	return schema, nil
}

func (p *PostgresManager) RestoreFromCSV(directory string) error {
	if p.DB == nil {
		return errors.New("no database connection")
	}
	ctx := context.Background()

	schema, err := p.ReadSchemaFromFile(filepath.Join(directory, "schema.json"))
	if err != nil {
		return fmt.Errorf("reading schema: %v", err)
	}

	// Pin one session for the whole restore. session_replication_role is a
	// session-level setting, so it must run on the same connection as every
	// statement that relies on it — p.DB is a pool and gives no such
	// guarantee. A single session also keeps the number of network round
	// trips minimal, which dominates restore time against remote databases.
	conn, err := p.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %v", err)
	}
	defer conn.Close()

	// Disable all triggers/constraints temporarily
	if _, err := conn.ExecContext(ctx, "SET session_replication_role = 'replica';"); err != nil {
		return fmt.Errorf("disabling constraints: %v", err)
	}
	defer conn.ExecContext(context.Background(), "SET session_replication_role = 'origin';")

	// One round trip: fetch existing enums, tables, and FK constraint names
	// up front instead of issuing per-object EXISTS probes.
	existing := map[string]map[string]bool{"enum": {}, "table": {}, "fk": {}}
	metaRows, err := conn.QueryContext(ctx, `
		SELECT 'enum' AS kind, t.typname AS name
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = 'public' AND t.typtype = 'e'
		UNION ALL
		SELECT 'table', table_name
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
		UNION ALL
		SELECT 'fk', con.conname
		FROM pg_constraint con
		JOIN pg_namespace ns ON ns.oid = con.connamespace
		WHERE con.contype = 'f' AND ns.nspname = 'public'
	`)
	if err != nil {
		return fmt.Errorf("querying existing objects: %v", err)
	}
	for metaRows.Next() {
		var kind, name string
		if err := metaRows.Scan(&kind, &name); err != nil {
			metaRows.Close()
			return fmt.Errorf("scanning existing objects: %v", err)
		}
		existing[kind][name] = true
	}
	if err := metaRows.Err(); err != nil {
		metaRows.Close()
		return fmt.Errorf("reading existing objects: %v", err)
	}
	metaRows.Close()

	// Create missing enum types — all in one statement.
	var enumStmts []string
	for _, enum := range schema.Enums {
		if existing["enum"][enum.Name] {
			continue
		}
		enumStmts = append(enumStmts, fmt.Sprintf("CREATE TYPE %s AS ENUM (%s);",
			pq.QuoteIdentifier(enum.Name), joinQuotedStrings(enum.Values)))
	}
	if len(enumStmts) > 0 {
		ui.Step("Creating %d enum type(s)...", len(enumStmts))
		batch := strings.Join(enumStmts, "\n")
		p.logSQL("Create Enums", batch)
		if _, err := conn.ExecContext(ctx, batch); err != nil {
			return fmt.Errorf("creating enum types: %v", err)
		}
	}

	ui.Step("Preparing %d table(s)...", len(schema.Tables))

	// Create missing tables (one statement) and truncate the rest (one
	// combined TRUNCATE — CASCADE makes the order irrelevant).
	var createStmts []string
	var truncateTargets []string
	for _, table := range schema.Tables {
		if existing["table"][table.Name] {
			truncateTargets = append(truncateTargets, pq.QuoteIdentifier(table.Name))
		} else {
			createStmts = append(createStmts, p.buildCreateTableSQL(table)+";")
		}
	}
	if len(createStmts) > 0 {
		batch := strings.Join(createStmts, "\n")
		p.logSQL("Create Tables", batch)
		if _, err := conn.ExecContext(ctx, batch); err != nil {
			return fmt.Errorf("creating tables: %v", err)
		}
	}
	if len(truncateTargets) > 0 {
		truncateSQL := fmt.Sprintf("TRUNCATE TABLE %s CASCADE", strings.Join(truncateTargets, ", "))
		p.logSQL("Truncate Tables", truncateSQL)
		if _, err := conn.ExecContext(ctx, truncateSQL); err != nil {
			return fmt.Errorf("truncating tables: %v", err)
		}
	}

	// Add missing foreign key constraints — all in one statement. The
	// constraint-name convention matches what addForeignKeySQL generates, so
	// the pg_constraint snapshot above tells us which ones already exist.
	if err := p.addMissingForeignKeys(ctx, conn, schema, existing["table"], existing["fk"]); err != nil {
		return err
	}

	// Walk the top-level once and split into function/trigger sidecars. The
	// flat layout (<dir>/<name>_func.sql, <dir>/<table>_<name>_trigger.sql)
	// matches what ExportSchema writes and what sync/seed stage into the
	// restore dir.
	var functionFiles, triggerFiles []string
	if entries, err := os.ReadDir(directory); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			switch {
			case strings.HasSuffix(name, "_func.sql"):
				functionFiles = append(functionFiles, filepath.Join(directory, name))
			case strings.HasSuffix(name, "_trigger.sql"):
				triggerFiles = append(triggerFiles, filepath.Join(directory, name))
			}
		}
	}

	// Restore functions — prefer SQL sidecars, fall back to schema.json entries.
	var fnCount int
	if len(functionFiles) > 0 {
		for _, sqlPath := range functionFiles {
			content, err := os.ReadFile(sqlPath)
			if err != nil {
				return fmt.Errorf("reading function file %s: %v", filepath.Base(sqlPath), err)
			}
			fnName := strings.TrimSuffix(filepath.Base(sqlPath), "_func.sql")
			p.logSQL(fmt.Sprintf("Restore Function %s", fnName), string(content))
			if _, err := conn.ExecContext(ctx, string(content)); err != nil {
				return fmt.Errorf("restoring function %s: %v", fnName, err)
			}
			p.log("Restored function: %s", fnName)
			fnCount++
		}
	} else {
		for _, fn := range schema.Functions {
			p.logSQL(fmt.Sprintf("Restore Function %s", fn.Name), fn.Definition)
			if _, err := conn.ExecContext(ctx, fn.Definition); err != nil {
				return fmt.Errorf("restoring function %s: %v", fn.Name, err)
			}
			p.log("Restored function: %s", fn.Name)
			fnCount++
		}
	}
	if fnCount > 0 {
		ui.Step("Restored %d function(s)", fnCount)
	}

	// Restore triggers — prefer SQL sidecars, fall back to schema.json entries.
	// DROP before CREATE because CREATE OR REPLACE TRIGGER requires PG14+;
	// both statements ship in one round trip.
	var trigCount int
	if len(triggerFiles) > 0 {
		for _, sqlPath := range triggerFiles {
			content, err := os.ReadFile(sqlPath)
			if err != nil {
				return fmt.Errorf("reading trigger file %s: %v", filepath.Base(sqlPath), err)
			}
			name, tableSchema, tableName, definition, parseErr := parseTriggerSQL(string(content))
			if parseErr != nil {
				return fmt.Errorf("parsing trigger file %s: %v", filepath.Base(sqlPath), parseErr)
			}
			tableRef := pq.QuoteIdentifier(tableName)
			if tableSchema != "" && tableSchema != "public" {
				tableRef = pq.QuoteIdentifier(tableSchema) + "." + tableRef
			}
			dropAndCreate := fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s;\n%s",
				pq.QuoteIdentifier(name), tableRef, definition)
			p.logSQL(fmt.Sprintf("Restore Trigger %s", name), dropAndCreate)
			if _, err := conn.ExecContext(ctx, dropAndCreate); err != nil {
				return fmt.Errorf("restoring trigger %s on %s: %v", name, tableRef, err)
			}
			p.log("Restored trigger: %s on %s", name, tableRef)
			trigCount++
		}
	} else {
		for _, trigger := range schema.Triggers {
			tableRef := pq.QuoteIdentifier(trigger.TableName)
			if trigger.TableSchema != "" && trigger.TableSchema != "public" {
				tableRef = pq.QuoteIdentifier(trigger.TableSchema) + "." + tableRef
			}
			dropAndCreate := fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s;\n%s",
				pq.QuoteIdentifier(trigger.Name), tableRef, trigger.Definition)
			p.logSQL(fmt.Sprintf("Restore Trigger %s", trigger.Name), dropAndCreate)
			if _, err := conn.ExecContext(ctx, dropAndCreate); err != nil {
				return fmt.Errorf("restoring trigger %s on %s: %v", trigger.Name, tableRef, err)
			}
			p.log("Restored trigger: %s on %s", trigger.Name, tableRef)
			trigCount++
		}
	}
	if trigCount > 0 {
		ui.Step("Restored %d trigger(s)", trigCount)
	}

	ui.Step("Importing data...")

	// All tables import inside one transaction: one BEGIN/COMMIT for the
	// whole restore instead of one per table, and the seed becomes atomic —
	// either every table lands or none do.
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning import transaction: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var sequenceResets []string
	for _, table := range schema.Tables {
		csvPath := filepath.Join(directory, table.Name+".csv")
		if _, err := os.Stat(csvPath); err != nil {
			p.log("No CSV file found for table: %s", table.Name)
			continue
		}
		p.log("Importing data for table: %s", table.Name)
		if err := p.copyCSVIntoTable(tx, table, csvPath); err != nil {
			return fmt.Errorf("importing data for table %s: %v", table.Name, err)
		}
		p.log("Imported data for table: %s", table.Name)

		// Queue sequence resets for serial/identity columns; they all run
		// in a single statement just before commit.
		for _, col := range table.Columns {
			if strings.Contains(fmt.Sprintf("%v", col.Default), "nextval") ||
				strings.Contains(col.Type, "serial") {
				sequenceResets = append(sequenceResets, fmt.Sprintf(
					"SELECT setval(seq, COALESCE((SELECT MAX(%s) FROM %s), 0) + 1, false) FROM (SELECT pg_get_serial_sequence('%s', '%s') AS seq) q WHERE seq IS NOT NULL;",
					pq.QuoteIdentifier(col.Name), pq.QuoteIdentifier(table.Name),
					table.Name, col.Name))
			}
		}
	}

	if len(sequenceResets) > 0 {
		batch := strings.Join(sequenceResets, "\n")
		p.logSQL("Reset Sequences", batch)
		if _, err := tx.Exec(batch); err != nil {
			// Don't fail the restore over sequence bookkeeping; matches the
			// historical warn-and-continue behaviour.
			p.log("Warning: failed to reset sequences: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing import transaction: %v", err)
	}
	committed = true

	return nil
}

// buildCreateTableSQL renders the CREATE TABLE statement for a table.
// Foreign key constraints are always skipped — they are added afterwards
// once every referenced table exists.
func (p *PostgresManager) buildCreateTableSQL(table Table) string {
	var columnDefs []string
	var primaryKeys []string
	var uniqueConstraints []string

	for _, col := range table.Columns {
		colDef := fmt.Sprintf("%s ", pq.QuoteIdentifier(col.Name))

		defaultStr := columnDefaultString(col.Default)
		isNextval := strings.Contains(strings.ToLower(defaultStr), "nextval(")

		if isNextval {
			// Convert integer+nextval → SERIAL, bigint+nextval → BIGSERIAL.
			// This lets PostgreSQL create the backing sequence automatically.
			switch strings.ToLower(col.Type) {
			case "bigint":
				colDef += "BIGSERIAL"
			case "smallint":
				colDef += "SMALLSERIAL"
			default:
				colDef += "SERIAL"
			}
			if !col.Nullable {
				colDef += " NOT NULL"
			}
		} else {
			if col.Type == "enum" && col.Enum != "" {
				colDef += pq.QuoteIdentifier(col.Enum)
			} else if strings.HasPrefix(col.Type, "ARRAY") {
				colDef += "text[]"
			} else if (col.Type == "character varying" || col.Type == "varchar") && col.Varchar != nil {
				colDef += fmt.Sprintf("varchar(%s)", *col.Varchar)
			} else {
				colDef += col.Type
			}

			if !col.Nullable {
				colDef += " NOT NULL"
			}

			if defaultStr != "" {
				colDef += " DEFAULT " + defaultStr
			}
		}

		columnDefs = append(columnDefs, colDef)

		if col.IsPrimary {
			primaryKeys = append(primaryKeys, col.Name)
		}
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

	return fmt.Sprintf("CREATE TABLE %s (\n  %s\n)",
		pq.QuoteIdentifier(table.Name),
		strings.Join(columnDefs, ",\n  "))
}

// addMissingForeignKeys adds every FK constraint from the schema that is
// not already present, in a single round trip. existingTables is the
// pre-restore table snapshot; tables in schema.Tables that were missing
// have just been created, so a referenced table exists when it is in
// either set. Failures are logged, not fatal — matching the historical
// warn-and-continue behaviour for constraint setup.
func (p *PostgresManager) addMissingForeignKeys(ctx context.Context, conn *sql.Conn, schema *Schema, existingTables, existingFKs map[string]bool) error {
	schemaTables := map[string]bool{}
	for _, t := range schema.Tables {
		schemaTables[t.Name] = true
	}

	var alterStmts []string
	for _, table := range schema.Tables {
		for _, col := range table.Columns {
			if col.ForeignKey == nil {
				continue
			}
			constraintName := fmt.Sprintf("%s_%s_fkey", table.Name, col.Name)
			if existingFKs[constraintName] {
				continue
			}
			if !existingTables[col.ForeignKey.Table] && !schemaTables[col.ForeignKey.Table] {
				p.log("Warning: Cannot add foreign key from %s.%s to non-existent table %s",
					table.Name, col.Name, col.ForeignKey.Table)
				continue
			}
			alterStmts = append(alterStmts, fmt.Sprintf(
				"ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s);",
				pq.QuoteIdentifier(table.Name),
				pq.QuoteIdentifier(constraintName),
				pq.QuoteIdentifier(col.Name),
				pq.QuoteIdentifier(col.ForeignKey.Table),
				pq.QuoteIdentifier(col.ForeignKey.Column)))
		}
	}
	if len(alterStmts) == 0 {
		return nil
	}

	batch := strings.Join(alterStmts, "\n")
	p.logSQL("Add Foreign Keys", batch)
	if _, err := conn.ExecContext(ctx, batch); err == nil {
		return nil
	}

	// The combined statement failed (e.g. one referenced column is gone).
	// Retry one by one so a single bad constraint doesn't block the rest.
	for _, stmt := range alterStmts {
		if _, err := conn.ExecContext(ctx, stmt); err != nil &&
			!strings.Contains(err.Error(), "already exists") {
			p.log("Warning: Failed to add foreign key (%s): %v", stmt, err)
		}
	}
	return nil
}

// copyCSVIntoTable streams one CSV file into a table via COPY, inside the
// caller's transaction. COPY data is pipelined by lib/pq, so the per-table
// network cost is just the prepare + close round trips.
func (p *PostgresManager) copyCSVIntoTable(tx *sql.Tx, table Table, csvPath string) error {
	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("opening CSV file: %v", err)
	}
	defer file.Close()

	columnTypeMap := make(map[string]string)
	for _, col := range table.Columns {
		columnTypeMap[col.Name] = col.Type
	}

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("reading CSV header: %v", err)
	}
	for _, colName := range header {
		if _, exists := columnTypeMap[colName]; !exists {
			p.log("Warning: Column %s in CSV not found in schema for table %s", colName, table.Name)
		}
	}

	stmt, err := tx.Prepare(pq.CopyIn(table.Name, header...))
	if err != nil {
		return fmt.Errorf("preparing COPY statement: %v", err)
	}

	rowCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			stmt.Close()
			return fmt.Errorf("reading CSV record: %v", err)
		}

		if len(record) != len(header) {
			stmt.Close()
			return fmt.Errorf("column count mismatch: expected %d, got %d in row %d", len(header), len(record), rowCount+1)
		}

		values := make([]interface{}, len(record))
		for i, v := range record {
			values[i] = p.processCSVValue(v, columnTypeMap[header[i]])
		}

		if _, err := stmt.Exec(values...); err != nil {
			stmt.Close()
			return fmt.Errorf("executing COPY for table %s row %d: %v\nValues: %v", table.Name, rowCount+1, err, values)
		}
		rowCount++
	}

	// Close the prepared statement to complete the COPY operation
	if err := stmt.Close(); err != nil {
		return fmt.Errorf("closing COPY statement: %v", err)
	}

	ui.Debug("Imported %d rows into %s", rowCount, table.Name)
	return nil
}

// Helper function to process CSV values based on column type
func (p *PostgresManager) processCSVValue(value string, columnType string) interface{} {
	// Explicit NULL markers always map to SQL NULL.
	if value == "NULL" || value == "null" {
		return nil
	}

	// Convert value based on column type
	colType := strings.ToLower(columnType)

	// For text-family types, an empty string is a valid value (""), not NULL.
	isTextType := colType == "text" ||
		strings.HasPrefix(colType, "character varying") ||
		strings.HasPrefix(colType, "varchar") ||
		strings.HasPrefix(colType, "char(") ||
		colType == "char"

	// For all other types, an empty string in a CSV cell means SQL NULL
	// (the value was absent / not set).
	if value == "" && !isTextType {
		return nil
	}

	// Handle JSON and JSONB types
	if colType == "json" || colType == "jsonb" {
		// Try to parse as JSON
		var js interface{}
		if err := json.Unmarshal([]byte(value), &js); err == nil {
			// Valid JSON, return as is
			return value
		}

		// Try to fix common JSON issues
		fixedJSON := value

		// Replace single quotes with double quotes if they appear to be used for JSON
		if strings.Contains(value, "'") && !strings.Contains(value, "\"") {
			fixedJSON = strings.ReplaceAll(value, "'", "\"")
		}

		// Try again with fixed JSON
		if err := json.Unmarshal([]byte(fixedJSON), &js); err == nil {
			return fixedJSON
		}

		// If still invalid, try more aggressive fixing
		fixedJSON = fixSimpleJSON(fixedJSON)

		// Final validation attempt
		if err := json.Unmarshal([]byte(fixedJSON), &js); err == nil {
			return fixedJSON
		}

		// Use original value if all fixes fail
		return value
	}

	// Handle array types
	if strings.HasPrefix(colType, "array") || strings.HasSuffix(colType, "[]") {
		if (strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]")) ||
			(strings.HasPrefix(value, "{") && strings.HasSuffix(value, "}")) {
			// Already in PostgreSQL array format
			if strings.HasPrefix(value, "{") {
				return value
			}

			// Convert JSON array to PostgreSQL array
			var jsonArray []interface{}
			fixedArray := value
			if strings.Contains(value, "'") && !strings.Contains(value, "\"") {
				fixedArray = strings.ReplaceAll(value, "'", "\"")
			}

			if err := json.Unmarshal([]byte(fixedArray), &jsonArray); err == nil {
				// Convert to PostgreSQL array format
				pgArray := make([]string, len(jsonArray))
				for i, elem := range jsonArray {
					switch e := elem.(type) {
					case string:
						pgArray[i] = fmt.Sprintf("\"%s\"", strings.ReplaceAll(e, "\"", "\\\""))
					default:
						pgArray[i] = fmt.Sprintf("%v", e)
					}
				}
				return "{" + strings.Join(pgArray, ",") + "}"
			}

			// Fallback to simpler parsing
			arrayStr := value[1 : len(value)-1]
			elements := parseArrayString(arrayStr)
			return "{" + strings.Join(elements, ",") + "}"
		}

		// Not in array format, use as is
		return value
	}

	// Handle timestamp/date types
	if strings.Contains(colType, "time") || strings.Contains(colType, "date") {
		// Try various time formats
		if strings.Contains(value, "UTC") {
			if t, err := time.Parse("2006-01-02 15:04:05.999999 -0700 MST", value); err == nil {
				return t
			}
		}

		if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return t
		}

		if t, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
			return t
		}

		if t, err := time.Parse("2006-01-02", value); err == nil {
			return t
		}
	}

	// Handle boolean types
	if colType == "boolean" || colType == "bool" {
		lower := strings.ToLower(value)
		if lower == "true" || lower == "t" || lower == "yes" || lower == "y" || lower == "1" {
			return true
		}
		if lower == "false" || lower == "f" || lower == "no" || lower == "n" || lower == "0" {
			return false
		}
	}

	// Handle numeric types
	if strings.Contains(colType, "int") || strings.Contains(colType, "serial") || strings.Contains(colType, "bigserial") {
		// Try to parse as integer
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
	}

	if strings.Contains(colType, "float") || strings.Contains(colType, "numeric") || strings.Contains(colType, "decimal") {
		// Try to parse as float
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
	}

	// Default: return as string
	return value
}

// Helper function to attempt fixing simple JSON formatting issues
func fixSimpleJSON(input string) string {
	// Replace single quotes with double quotes
	result := strings.ReplaceAll(input, "'", "\"")

	// Find unquoted property names and quote them
	// This is a very basic implementation and won't handle all cases
	var buffer strings.Builder
	inQuotes := false

	for i := 0; i < len(result); i++ {
		char := result[i]

		if char == '"' {
			// Toggle quote state
			inQuotes = !inQuotes
			buffer.WriteByte(char)
		} else if char == ':' && !inQuotes {
			// Check if the property name before this colon is quoted
			// If we're not in quotes and found a colon, check if we need to add quotes
			// This is a simplified approach and might not work for all cases
			j := i - 1
			for j >= 0 && (result[j] == ' ' || result[j] == '\t') {
				j--
			}

			if j >= 0 && result[j] != '"' && result[j] != '}' && result[j] != ']' {
				// We found an unquoted property name, need to go back and add quotes
				// This is a very simplified approach and won't handle all cases
				buffer.WriteString("\":")
			} else {
				buffer.WriteByte(char)
			}
		} else {
			buffer.WriteByte(char)
		}
	}

	return buffer.String()
}

// Helper function to parse array strings with quoted elements
func parseArrayString(s string) []string {
	var result []string
	var current string
	inQuotes := false

	for i := 0; i < len(s); i++ {
		char := s[i]

		if char == '"' {
			// Toggle quote state
			inQuotes = !inQuotes
			current += string(char)
		} else if char == ',' && !inQuotes {
			// End of element
			result = append(result, strings.TrimSpace(current))
			current = ""
		} else {
			current += string(char)
		}
	}

	// Add the last element if there is one
	if current != "" {
		result = append(result, strings.TrimSpace(current))
	}

	return result
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

	// Enum creation happens in RestoreFromCSV (batched with the other
	// missing-object DDL) — reading a schema file has no DB side effects.
	return &schema, nil
}

// columnDefaultString extracts the default value as a string, handling the
// untyped interface{} from JSON unmarshalling.
func columnDefaultString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	default:
		return fmt.Sprintf("%v", s)
	}
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

	for _, tableName := range tables {
		if err := p.exportTableToCSV(tableName, outputDir); err != nil {
			return fmt.Errorf("exporting table %s: %v", tableName, err)
		}
		ui.Debug("Exported table: %s", tableName)
	}
	ui.Success("Exported %d table(s)", len(tables))

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

// ExportSchema exports the database schema to outputDir.
//
// Layout:
//
//	<outputDir>/schema.json                       # tables + enums
//	<outputDir>/<fn>_func.sql                     # one per function
//	<outputDir>/<table>_<name>_trigger.sql        # one per trigger (+ header)
//
// Everything is written flat at the top level so the files pass cleanly
// through refreshSchemaFolder → SchemaFiles → sync/zip → server, and back
// through seed/RestoreFromCSV.
func (p *PostgresManager) ExportSchema(outputDir string) error {
	if p.DB == nil {
		return errors.New("no database connection")
	}

	schema, err := p.ExtractSchema()
	if err != nil {
		return fmt.Errorf("extracting schema: %v", err)
	}

	// --- Export functions as flat `<name>_func.sql` sidecars ---
	// The flat layout is what refreshSchemaFolder / SchemaFiles / sync / seed
	// all expect — functions and triggers live alongside schema.json, not in
	// subdirectories, so they ride along cleanly when the schema folder is
	// copied, zipped, or staged for restore.
	for _, fn := range schema.Functions {
		sqlPath := filepath.Join(outputDir, fn.Name+"_func.sql")
		if err := os.WriteFile(sqlPath, []byte(fn.Definition), 0644); err != nil {
			return fmt.Errorf("writing function %s: %v", fn.Name, err)
		}
		ui.Debug("Exported function: %s", fn.Name)
	}
	if len(schema.Functions) > 0 {
		ui.Debug("Exported %d function(s)", len(schema.Functions))
	}

	// --- Export triggers as flat `<table>_<name>_trigger.sql` sidecars ---
	// Each file gets a metadata header so RestoreFromCSV can rebuild the
	// (name, schema, table) tuple without re-parsing CREATE TRIGGER.
	for _, trigger := range schema.Triggers {
		header := fmt.Sprintf("-- seedmancer:trigger\n-- name: %s\n-- table_schema: %s\n-- table_name: %s\n",
			trigger.Name, trigger.TableSchema, trigger.TableName)
		content := header + trigger.Definition
		fileName := fmt.Sprintf("%s_%s_trigger.sql", trigger.TableName, trigger.Name)
		sqlPath := filepath.Join(outputDir, fileName)
		if err := os.WriteFile(sqlPath, []byte(content), 0644); err != nil {
			return fmt.Errorf("writing trigger %s: %v", trigger.Name, err)
		}
		ui.Debug("Exported trigger: %s on %s.%s", trigger.Name, trigger.TableSchema, trigger.TableName)
	}
	if len(schema.Triggers) > 0 {
		ui.Debug("Exported %d trigger(s)", len(schema.Triggers))
	}

	// --- Write schema.json (tables + enums only, no functions/triggers) ---
	schemaForJSON := Schema{
		DatabaseType: schema.DatabaseType,
		Enums:        schema.Enums,
		Tables:       schema.Tables,
	}

	jsonData, err := json.MarshalIndent(schemaForJSON, "", "  ")
	if err != nil {
		return fmt.Errorf("converting schema to JSON: %v", err)
	}

	outputFile := filepath.Join(outputDir, "schema.json")
	if err := os.WriteFile(outputFile, jsonData, 0644); err != nil {
		return fmt.Errorf("writing schema to file: %v", err)
	}

	ui.Debug("Schema exported to: %s", outputFile)
	return nil
}

// parseTriggerSQL reads the seedmancer metadata header written by ExportSchema and
// returns the trigger name, table schema, table name, and the raw SQL definition.
// Header format (lines beginning with "--"):
//
//	-- seedmancer:trigger
//	-- name: <name>
//	-- table_schema: <schema>
//	-- table_name: <table>
func parseTriggerSQL(content string) (name, tableSchema, tableName, definition string, err error) {
	lines := strings.Split(content, "\n")
	var sqlLines []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "-- name:") {
			name = strings.TrimSpace(strings.TrimPrefix(trimmed, "-- name:"))
		} else if strings.HasPrefix(trimmed, "-- table_schema:") {
			tableSchema = strings.TrimSpace(strings.TrimPrefix(trimmed, "-- table_schema:"))
		} else if strings.HasPrefix(trimmed, "-- table_name:") {
			tableName = strings.TrimSpace(strings.TrimPrefix(trimmed, "-- table_name:"))
		} else if !strings.HasPrefix(trimmed, "--") {
			sqlLines = append(sqlLines, line)
		}
	}
	definition = strings.TrimSpace(strings.Join(sqlLines, "\n"))
	if name == "" || tableName == "" || definition == "" {
		err = fmt.Errorf("missing required metadata (name=%q table_name=%q)", name, tableName)
	}
	return
}

// Add a debug query to check character_maximum_length values
func (p *PostgresManager) debugVarcharLengths() {
	query := `
		SELECT 
			table_name, 
			column_name, 
			data_type, 
			character_maximum_length
		FROM 
			information_schema.columns 
		WHERE 
			table_schema = 'public' 
			AND (data_type = 'character varying' OR data_type = 'varchar')
		ORDER BY 
			table_name, ordinal_position;
	`

	rows, err := p.DB.Query(query)
	if err != nil {
		p.log("Error querying varchar lengths: %v", err)
		return
	}
	defer rows.Close()

	p.log("Debugging varchar lengths:")
	for rows.Next() {
		var tableName, columnName, dataType string
		var maxLength sql.NullInt64

		if err := rows.Scan(&tableName, &columnName, &dataType, &maxLength); err != nil {
			p.log("Error scanning row: %v", err)
			continue
		}

		if maxLength.Valid {
			p.log("Table: %s, Column: %s, Type: %s, MaxLength: %d",
				tableName, columnName, dataType, maxLength.Int64)
		} else {
			p.log("Table: %s, Column: %s, Type: %s, MaxLength: NULL",
				tableName, columnName, dataType)
		}
	}
}
