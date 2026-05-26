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

	_ "github.com/go-sql-driver/mysql"

	"github.com/KazanKK/seedmancer/internal/ui"
)

type MySQLManager struct {
	DB *sql.DB
}

func (m *MySQLManager) log(format string, args ...interface{}) {
	ui.Debug("[mysql] "+format, args...)
}

func (m *MySQLManager) logSQL(operation, query string) {
	ui.Debug("[mysql] %s:\n%s", operation, query)
}

// quoteIdent wraps a MySQL identifier in backticks, escaping any backticks
// already present.
func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func (m *MySQLManager) ConnectWithDSN(dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	m.DB = db
	return nil
}

// ExecSQL runs sqlText inside a single transaction. On any error the
// transaction is rolled back. The DSN normalization in database/factory.go
// enables `multiStatements=true` so callers can pass agent-written DML
// chains (INSERT/UPDATE/DELETE) as a single string.
func (m *MySQLManager) ExecSQL(sqlText string) error {
	if m.DB == nil {
		return errors.New("no database connection")
	}
	tx, err := m.DB.Begin()
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

// ExtractSchema reads tables, columns, constraints, routines, and triggers
// from information_schema for the current database.
func (m *MySQLManager) ExtractSchema() (*Schema, error) {
	if m.DB == nil {
		return nil, errors.New("no database connection")
	}

	// ── Columns ──────────────────────────────────────────────────────────────
	colRows, err := m.DB.Query(`
		SELECT
			TABLE_NAME,
			COLUMN_NAME,
			DATA_TYPE,
			COLUMN_TYPE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			COLUMN_KEY,
			EXTRA,
			CHARACTER_MAXIMUM_LENGTH
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE()
		ORDER BY TABLE_NAME, ORDINAL_POSITION
	`)
	if err != nil {
		return nil, fmt.Errorf("querying columns: %v", err)
	}
	defer colRows.Close()

	type rawColumn struct {
		tableName  string
		columnName string
		dataType   string
		columnType string
		isNullable string
		colDefault sql.NullString
		columnKey  string
		extra      string
		charMaxLen sql.NullInt64
	}

	var rawCols []rawColumn
	for colRows.Next() {
		var rc rawColumn
		if err := colRows.Scan(
			&rc.tableName, &rc.columnName, &rc.dataType, &rc.columnType,
			&rc.isNullable, &rc.colDefault, &rc.columnKey, &rc.extra,
			&rc.charMaxLen,
		); err != nil {
			return nil, fmt.Errorf("scanning column row: %v", err)
		}
		rawCols = append(rawCols, rc)
	}

	// ── Foreign keys ─────────────────────────────────────────────────────────
	fkRows, err := m.DB.Query(`
		SELECT
			kcu.TABLE_NAME,
			kcu.COLUMN_NAME,
			kcu.REFERENCED_TABLE_NAME,
			kcu.REFERENCED_COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE kcu
		JOIN information_schema.TABLE_CONSTRAINTS tc
			ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
		WHERE kcu.TABLE_SCHEMA = DATABASE()
		AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
		AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
	`)
	if err != nil {
		return nil, fmt.Errorf("querying foreign keys: %v", err)
	}
	defer fkRows.Close()

	type fkKey struct{ table, col string }
	fkMap := map[fkKey]*ForeignKey{}
	for fkRows.Next() {
		var tbl, col, refTbl, refCol string
		if err := fkRows.Scan(&tbl, &col, &refTbl, &refCol); err != nil {
			return nil, fmt.Errorf("scanning FK row: %v", err)
		}
		fkMap[fkKey{tbl, col}] = &ForeignKey{Table: refTbl, Column: refCol}
	}

	// ── Build Schema ─────────────────────────────────────────────────────────
	schema := &Schema{
		DatabaseType: MySQL,
		Tables:       []Table{},
	}

	var syntheticEnums []EnumItem
	currentTable := ""
	var currentColumns []Column

	flush := func() {
		if currentTable != "" {
			schema.Tables = append(schema.Tables, Table{
				Name:    currentTable,
				Columns: currentColumns,
			})
		}
	}

	for _, rc := range rawCols {
		if rc.tableName != currentTable {
			flush()
			currentTable = rc.tableName
			currentColumns = []Column{}
		}

		col := Column{
			Name:      rc.columnName,
			Type:      rc.dataType,
			Nullable:  rc.isNullable == "YES",
			IsPrimary: rc.columnKey == "PRI",
			IsUnique:  rc.columnKey == "UNI",
		}

		// varchar length
		if rc.charMaxLen.Valid && (rc.dataType == "varchar" || rc.dataType == "char") {
			s := strconv.FormatInt(rc.charMaxLen.Int64, 10)
			col.Varchar = &s
		}

		// auto_increment stored as a sentinel default so createTable can detect it
		if strings.Contains(strings.ToLower(rc.extra), "auto_increment") {
			col.Default = "AUTO_INCREMENT"
		} else if rc.colDefault.Valid {
			col.Default = rc.colDefault.String
		}

		// foreign key
		if fk, ok := fkMap[fkKey{rc.tableName, rc.columnName}]; ok {
			col.ForeignKey = fk
		}

		// MySQL ENUM: "enum('a','b','c')" — synthesise a named EnumItem
		if rc.dataType == "enum" {
			enumName := rc.tableName + "_" + rc.columnName
			values := parseMySQLEnum(rc.columnType)
			syntheticEnums = append(syntheticEnums, EnumItem{
				Name:   enumName,
				Values: values,
			})
			col.Type = "enum"
			col.Enum = enumName
		}

		currentColumns = append(currentColumns, col)
	}
	flush()

	schema.Enums = syntheticEnums

	// ── Stored functions and procedures ──────────────────────────────────────
	routineRows, err := m.DB.Query(`
		SELECT ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION
		FROM information_schema.ROUTINES
		WHERE ROUTINE_SCHEMA = DATABASE()
		ORDER BY ROUTINE_NAME
	`)
	if err != nil {
		m.log("Warning: could not query routines: %v", err)
	} else {
		defer routineRows.Close()
		for routineRows.Next() {
			var name, rType string
			var def sql.NullString
			if err := routineRows.Scan(&name, &rType, &def); err != nil {
				return nil, fmt.Errorf("scanning routine: %v", err)
			}
			// Use SHOW CREATE to get the full definition including DELIMITER logic
			full, err := m.showCreateRoutine(name, rType)
			if err != nil {
				m.log("Warning: could not get definition for %s %s: %v", rType, name, err)
				continue
			}
			schema.Functions = append(schema.Functions, Function{
				Name:       name,
				Definition: full,
			})
		}
	}

	// ── Triggers ─────────────────────────────────────────────────────────────
	trigRows, err := m.DB.Query(`
		SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_STATEMENT,
		       EVENT_MANIPULATION, ACTION_TIMING
		FROM information_schema.TRIGGERS
		WHERE TRIGGER_SCHEMA = DATABASE()
		ORDER BY TRIGGER_NAME
	`)
	if err != nil {
		m.log("Warning: could not query triggers: %v", err)
	} else {
		defer trigRows.Close()
		for trigRows.Next() {
			var trigName, tblName, actionStmt, eventManip, actionTiming string
			if err := trigRows.Scan(&trigName, &tblName, &actionStmt, &eventManip, &actionTiming); err != nil {
				return nil, fmt.Errorf("scanning trigger: %v", err)
			}
			full, err := m.showCreateTrigger(trigName)
			if err != nil {
				m.log("Warning: could not get definition for trigger %s: %v", trigName, err)
				continue
			}
			schema.Triggers = append(schema.Triggers, Trigger{
				Name:        trigName,
				TableName:   tblName,
				TableSchema: "",
				Definition:  full,
			})
		}
	}

	return schema, nil
}

// showCreateRoutine retrieves the full CREATE FUNCTION/PROCEDURE statement.
func (m *MySQLManager) showCreateRoutine(name, routineType string) (string, error) {
	var row *sql.Row
	switch strings.ToUpper(routineType) {
	case "FUNCTION":
		row = m.DB.QueryRow("SHOW CREATE FUNCTION " + quoteIdent(name))
	default:
		row = m.DB.QueryRow("SHOW CREATE PROCEDURE " + quoteIdent(name))
	}
	// SHOW CREATE FUNCTION returns: Name, sql_mode, Create Function, char_set_client, collation_connection, Database Collation
	var n, mode, def, cs, cc, dc sql.NullString
	if err := row.Scan(&n, &mode, &def, &cs, &cc, &dc); err != nil {
		return "", err
	}
	if !def.Valid {
		return "", fmt.Errorf("NULL definition for %s %s", routineType, name)
	}
	return def.String, nil
}

// showCreateTrigger retrieves the full CREATE TRIGGER statement.
func (m *MySQLManager) showCreateTrigger(name string) (string, error) {
	row := m.DB.QueryRow("SHOW CREATE TRIGGER " + quoteIdent(name))
	// SHOW CREATE TRIGGER returns: Trigger, sql_mode, SQL Original Statement, character_set_client, collation_connection, Database Collation, Created
	var trig, mode, def, cs, cc, dc sql.NullString
	var created sql.NullString
	if err := row.Scan(&trig, &mode, &def, &cs, &cc, &dc, &created); err != nil {
		return "", err
	}
	if !def.Valid {
		return "", fmt.Errorf("NULL definition for trigger %s", name)
	}
	return def.String, nil
}

// ExportSchema writes schema.json and sidecar SQL files to outputDir.
func (m *MySQLManager) ExportSchema(outputDir string) error {
	if m.DB == nil {
		return errors.New("no database connection")
	}

	schema, err := m.ExtractSchema()
	if err != nil {
		return fmt.Errorf("extracting schema: %v", err)
	}

	// Write function sidecars (<name>_func.sql)
	for _, fn := range schema.Functions {
		sqlPath := filepath.Join(outputDir, fn.Name+"_func.sql")
		if err := os.WriteFile(sqlPath, []byte(fn.Definition), 0644); err != nil {
			return fmt.Errorf("writing function %s: %v", fn.Name, err)
		}
		ui.Debug("Exported function: %s", fn.Name)
	}
	if len(schema.Functions) > 0 {
		ui.Success("Exported %d function(s)", len(schema.Functions))
	}

	// Write trigger sidecars (<table>_<name>_trigger.sql) with metadata header
	for _, trigger := range schema.Triggers {
		header := fmt.Sprintf("-- seedmancer:trigger\n-- name: %s\n-- table_schema: %s\n-- table_name: %s\n",
			trigger.Name, trigger.TableSchema, trigger.TableName)
		content := header + trigger.Definition
		fileName := fmt.Sprintf("%s_%s_trigger.sql", trigger.TableName, trigger.Name)
		sqlPath := filepath.Join(outputDir, fileName)
		if err := os.WriteFile(sqlPath, []byte(content), 0644); err != nil {
			return fmt.Errorf("writing trigger %s: %v", trigger.Name, err)
		}
		ui.Debug("Exported trigger: %s on %s", trigger.Name, trigger.TableName)
	}
	if len(schema.Triggers) > 0 {
		ui.Success("Exported %d trigger(s)", len(schema.Triggers))
	}

	// Write schema.json (tables + enums only)
	schemaForJSON := Schema{
		DatabaseType: schema.DatabaseType,
		Enums:        schema.Enums,
		Tables:       schema.Tables,
	}
	jsonData, err := json.MarshalIndent(schemaForJSON, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling schema: %v", err)
	}
	outputFile := filepath.Join(outputDir, "schema.json")
	if err := os.WriteFile(outputFile, jsonData, 0644); err != nil {
		return fmt.Errorf("writing schema.json: %v", err)
	}
	ui.Debug("Schema exported to: %s", outputFile)
	return nil
}

// ExportToCSV exports each table to a CSV file in outputDir.
func (m *MySQLManager) ExportToCSV(outputDir string) error {
	if m.DB == nil {
		return errors.New("no database connection")
	}

	rows, err := m.DB.Query(`
		SELECT TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE()
		AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME
	`)
	if err != nil {
		return fmt.Errorf("querying tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return fmt.Errorf("scanning table name: %v", err)
		}
		tables = append(tables, t)
	}

	for _, tbl := range tables {
		if err := m.exportTableToCSV(tbl, outputDir); err != nil {
			return fmt.Errorf("exporting table %s: %v", tbl, err)
		}
		ui.Debug("Exported table: %s", tbl)
	}
	ui.Success("Exported %d table(s)", len(tables))
	return nil
}

func (m *MySQLManager) exportTableToCSV(tableName, outputDir string) error {
	csvPath := filepath.Join(outputDir, tableName+".csv")
	file, err := os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("creating CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Column names in ordinal order
	colRows, err := m.DB.Query(`
		SELECT COLUMN_NAME
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`, tableName)
	if err != nil {
		return fmt.Errorf("querying columns: %v", err)
	}
	defer colRows.Close()

	var columns []string
	for colRows.Next() {
		var c string
		if err := colRows.Scan(&c); err != nil {
			return fmt.Errorf("scanning column name: %v", err)
		}
		columns = append(columns, c)
	}

	if err := writer.Write(columns); err != nil {
		return fmt.Errorf("writing CSV header: %v", err)
	}

	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = quoteIdent(c)
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(quotedCols, ", "), quoteIdent(tableName))
	m.logSQL("Export "+tableName, query)

	dataRows, err := m.DB.Query(query)
	if err != nil {
		return fmt.Errorf("querying data: %v", err)
	}
	defer dataRows.Close()

	vals := make([]interface{}, len(columns))
	valPtrs := make([]interface{}, len(columns))
	for i := range vals {
		valPtrs[i] = &vals[i]
	}

	for dataRows.Next() {
		if err := dataRows.Scan(valPtrs...); err != nil {
			return fmt.Errorf("scanning row: %v", err)
		}
		row := make([]string, len(columns))
		for i, v := range vals {
			if v == nil {
				row[i] = "NULL"
			} else {
				switch val := v.(type) {
				case []byte:
					row[i] = string(val)
				case time.Time:
					row[i] = val.Format("2006-01-02 15:04:05.999999 -0700 UTC")
				default:
					row[i] = fmt.Sprintf("%v", val)
				}
			}
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("writing CSV row: %v", err)
		}
	}
	return nil
}

// RestoreFromCSV restores the database from schema.json + CSV files in directory.
func (m *MySQLManager) RestoreFromCSV(directory string) error {
	if m.DB == nil {
		return errors.New("no database connection")
	}

	if _, err := m.DB.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return fmt.Errorf("disabling FK checks: %v", err)
	}
	defer m.DB.Exec("SET FOREIGN_KEY_CHECKS = 1")

	schema, err := m.readSchemaFromFile(filepath.Join(directory, "schema.json"))
	if err != nil {
		return fmt.Errorf("reading schema: %v", err)
	}

	ui.Step("Preparing %d table(s)...", len(schema.Tables))
	for _, table := range schema.Tables {
		exists, err := m.tableExists(table.Name)
		if err != nil {
			return err
		}
		if !exists {
			if err := m.createTable(table); err != nil {
				return fmt.Errorf("creating table %s: %v", table.Name, err)
			}
		} else {
			truncSQL := "TRUNCATE TABLE " + quoteIdent(table.Name)
			m.logSQL("Truncate "+table.Name, truncSQL)
			if _, err := m.DB.Exec(truncSQL); err != nil {
				return fmt.Errorf("truncating table %s: %v", table.Name, err)
			}
		}
	}

	// Add FK constraints (only for newly created tables; existing ones keep theirs)
	for _, table := range schema.Tables {
		if err := m.addForeignKeys(table); err != nil {
			return fmt.Errorf("adding FKs for %s: %v", table.Name, err)
		}
	}

	// Restore functions from sidecars
	var functionFiles, triggerFiles []string
	if entries, err := os.ReadDir(directory); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			n := entry.Name()
			switch {
			case strings.HasSuffix(n, "_func.sql"):
				functionFiles = append(functionFiles, filepath.Join(directory, n))
			case strings.HasSuffix(n, "_trigger.sql"):
				triggerFiles = append(triggerFiles, filepath.Join(directory, n))
			}
		}
	}

	var fnCount int
	if len(functionFiles) > 0 {
		for _, sqlPath := range functionFiles {
			content, err := os.ReadFile(sqlPath)
			if err != nil {
				return fmt.Errorf("reading function file %s: %v", filepath.Base(sqlPath), err)
			}
			fnName := strings.TrimSuffix(filepath.Base(sqlPath), "_func.sql")
			dropSQL := "DROP FUNCTION IF EXISTS " + quoteIdent(fnName)
			m.logSQL("Drop Function "+fnName, dropSQL)
			if _, err := m.DB.Exec(dropSQL); err != nil {
				m.log("Warning: dropping function %s: %v", fnName, err)
			}
			m.logSQL("Restore Function "+fnName, string(content))
			if _, err := m.DB.Exec(string(content)); err != nil {
				return fmt.Errorf("restoring function %s: %v", fnName, err)
			}
			fnCount++
		}
	} else {
		for _, fn := range schema.Functions {
			dropSQL := "DROP FUNCTION IF EXISTS " + quoteIdent(fn.Name)
			m.logSQL("Drop Function "+fn.Name, dropSQL)
			if _, err := m.DB.Exec(dropSQL); err != nil {
				m.log("Warning: dropping function %s: %v", fn.Name, err)
			}
			m.logSQL("Restore Function "+fn.Name, fn.Definition)
			if _, err := m.DB.Exec(fn.Definition); err != nil {
				return fmt.Errorf("restoring function %s: %v", fn.Name, err)
			}
			fnCount++
		}
	}
	if fnCount > 0 {
		ui.Step("Restored %d function(s)", fnCount)
	}

	var trigCount int
	if len(triggerFiles) > 0 {
		for _, sqlPath := range triggerFiles {
			content, err := os.ReadFile(sqlPath)
			if err != nil {
				return fmt.Errorf("reading trigger file %s: %v", filepath.Base(sqlPath), err)
			}
			name, _, tableName, definition, parseErr := parseTriggerSQL(string(content))
			if parseErr != nil {
				return fmt.Errorf("parsing trigger file %s: %v", filepath.Base(sqlPath), parseErr)
			}
			dropSQL := fmt.Sprintf("DROP TRIGGER IF EXISTS %s", quoteIdent(name))
			m.logSQL("Drop Trigger "+name, dropSQL)
			if _, err := m.DB.Exec(dropSQL); err != nil {
				m.log("Warning: dropping trigger %s on %s: %v", name, tableName, err)
			}
			m.logSQL("Restore Trigger "+name, definition)
			if _, err := m.DB.Exec(definition); err != nil {
				return fmt.Errorf("restoring trigger %s: %v", name, err)
			}
			trigCount++
		}
	} else {
		for _, trigger := range schema.Triggers {
			dropSQL := fmt.Sprintf("DROP TRIGGER IF EXISTS %s", quoteIdent(trigger.Name))
			m.logSQL("Drop Trigger "+trigger.Name, dropSQL)
			if _, err := m.DB.Exec(dropSQL); err != nil {
				m.log("Warning: dropping trigger %s: %v", trigger.Name, err)
			}
			m.logSQL("Restore Trigger "+trigger.Name, trigger.Definition)
			if _, err := m.DB.Exec(trigger.Definition); err != nil {
				return fmt.Errorf("restoring trigger %s: %v", trigger.Name, err)
			}
			trigCount++
		}
	}
	if trigCount > 0 {
		ui.Step("Restored %d trigger(s)", trigCount)
	}

	ui.Step("Importing data...")
	for _, table := range schema.Tables {
		csvPath := filepath.Join(directory, table.Name+".csv")
		if _, err := os.Stat(csvPath); err == nil {
			if err := m.importCSV(table, csvPath); err != nil {
				return fmt.Errorf("importing %s: %v", table.Name, err)
			}
		} else {
			m.log("No CSV file found for table: %s", table.Name)
		}
	}

	return nil
}

// createTable builds and executes a CREATE TABLE statement for MySQL.
func (m *MySQLManager) createTable(table Table) error {
	var cols []string
	var pks []string
	var uniques []string

	for _, col := range table.Columns {
		def := quoteIdent(col.Name) + " "

		defaultStr := fmt.Sprintf("%v", col.Default)
		if col.Default == nil {
			defaultStr = ""
		}
		isAutoInc := strings.ToUpper(strings.TrimSpace(defaultStr)) == "AUTO_INCREMENT"

		if isAutoInc {
			switch strings.ToLower(col.Type) {
			case "bigint":
				def += "BIGINT"
			case "smallint", "tinyint":
				def += "SMALLINT"
			default:
				def += "INT"
			}
			def += " AUTO_INCREMENT"
			if !col.Nullable {
				def += " NOT NULL"
			}
		} else {
			def += m.columnTypeDDL(col)
			if !col.Nullable {
				def += " NOT NULL"
			}
			if defaultStr != "" {
				def += " DEFAULT " + defaultStr
			}
		}

		cols = append(cols, def)
		if col.IsPrimary {
			pks = append(pks, col.Name)
		}
		if col.IsUnique && !col.IsPrimary {
			uniques = append(uniques, col.Name)
		}
	}

	if len(pks) > 0 {
		quotedPKs := make([]string, len(pks))
		for i, p := range pks {
			quotedPKs[i] = quoteIdent(p)
		}
		cols = append(cols, "PRIMARY KEY ("+strings.Join(quotedPKs, ", ")+")")
	}
	for _, u := range uniques {
		cols = append(cols, "UNIQUE ("+quoteIdent(u)+")")
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (\n  %s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		quoteIdent(table.Name), strings.Join(cols, ",\n  "))
	m.logSQL("Create Table "+table.Name, createSQL)
	_, err := m.DB.Exec(createSQL)
	return err
}

// columnTypeDDL converts a schema Column type into a MySQL DDL fragment.
func (m *MySQLManager) columnTypeDDL(col Column) string {
	t := strings.ToLower(col.Type)

	if col.Type == "enum" && col.Enum != "" {
		return col.Enum // will be replaced by actual ENUM(...) at restore if enum info available
	}

	switch {
	case (t == "varchar" || t == "character varying") && col.Varchar != nil:
		return fmt.Sprintf("VARCHAR(%s)", *col.Varchar)
	case t == "varchar" || t == "character varying":
		return "VARCHAR(255)"
	case t == "char" && col.Varchar != nil:
		return fmt.Sprintf("CHAR(%s)", *col.Varchar)
	case t == "text", t == "longtext", t == "mediumtext", t == "tinytext":
		return strings.ToUpper(t)
	case t == "integer" || t == "int4":
		return "INT"
	case t == "bigint" || t == "int8":
		return "BIGINT"
	case t == "smallint" || t == "int2":
		return "SMALLINT"
	case t == "boolean" || t == "bool":
		return "TINYINT(1)"
	case t == "numeric" || t == "decimal":
		return "DECIMAL(18,6)"
	case t == "real" || t == "float4":
		return "FLOAT"
	case t == "double precision" || t == "float8":
		return "DOUBLE"
	case strings.Contains(t, "timestamp"):
		return "DATETIME(6)"
	case t == "date":
		return "DATE"
	case t == "time":
		return "TIME"
	case t == "json" || t == "jsonb":
		return "JSON"
	case t == "uuid":
		return "CHAR(36)"
	case t == "bytea":
		return "BLOB"
	default:
		return strings.ToUpper(col.Type)
	}
}

func (m *MySQLManager) tableExists(name string) (bool, error) {
	var count int
	err := m.DB.QueryRow(`
		SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
	`, name).Scan(&count)
	return count > 0, err
}

func (m *MySQLManager) addForeignKeys(table Table) error {
	for _, col := range table.Columns {
		if col.ForeignKey == nil {
			continue
		}
		constraintName := fmt.Sprintf("%s_%s_fk", table.Name, col.Name)
		alterSQL := fmt.Sprintf(
			"ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)",
			quoteIdent(table.Name),
			quoteIdent(constraintName),
			quoteIdent(col.Name),
			quoteIdent(col.ForeignKey.Table),
			quoteIdent(col.ForeignKey.Column),
		)
		m.logSQL("Add FK "+constraintName, alterSQL)
		if _, err := m.DB.Exec(alterSQL); err != nil {
			if isDuplicateConstraint(err) {
				continue
			}
			m.log("Warning: adding FK %s: %v", constraintName, err)
		}
	}
	return nil
}

// importCSV loads CSV data into a table using batched INSERT statements.
const mysqlBatchSize = 500

func (m *MySQLManager) importCSV(table Table, csvPath string) error {
	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("opening CSV: %v", err)
	}
	defer file.Close()

	colTypeMap := map[string]string{}
	for _, c := range table.Columns {
		colTypeMap[c.Name] = c.Type
	}

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("reading header: %v", err)
	}

	quotedHeader := make([]string, len(header))
	for i, h := range header {
		quotedHeader[i] = quoteIdent(h)
	}
	placeholders := "(" + strings.Repeat("?,", len(header)-1) + "?)"

	insertPrefix := fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		quoteIdent(table.Name), strings.Join(quotedHeader, ", "))

	var batch [][]interface{}
	rowCount := 0

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		rowPlaceholders := make([]string, len(batch))
		var flatVals []interface{}
		for i, row := range batch {
			rowPlaceholders[i] = placeholders
			flatVals = append(flatVals, row...)
		}
		query := insertPrefix + strings.Join(rowPlaceholders, ", ")
		m.logSQL(fmt.Sprintf("Insert batch %d rows into %s", len(batch), table.Name), query)
		if _, err := m.DB.Exec(query, flatVals...); err != nil {
			return fmt.Errorf("batch insert into %s: %v", table.Name, err)
		}
		batch = batch[:0]
		return nil
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row: %v", err)
		}
		if len(record) != len(header) {
			return fmt.Errorf("column count mismatch at row %d", rowCount+1)
		}
		vals := make([]interface{}, len(record))
		for i, v := range record {
			vals[i] = m.processCSVValue(v, colTypeMap[header[i]])
		}
		batch = append(batch, vals)
		rowCount++
		if len(batch) >= mysqlBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}

	// Reset AUTO_INCREMENT
	for _, col := range table.Columns {
		defaultStr := fmt.Sprintf("%v", col.Default)
		if col.Default == nil {
			defaultStr = ""
		}
		if strings.ToUpper(strings.TrimSpace(defaultStr)) == "AUTO_INCREMENT" {
			resetSQL := fmt.Sprintf(
				"ALTER TABLE %s AUTO_INCREMENT = (SELECT IFNULL(MAX(%s), 0) + 1 FROM %s)",
				quoteIdent(table.Name), quoteIdent(col.Name), quoteIdent(table.Name),
			)
			m.logSQL("Reset AUTO_INCREMENT "+table.Name, resetSQL)
			if _, err := m.DB.Exec(resetSQL); err != nil {
				m.log("Warning: resetting AUTO_INCREMENT for %s.%s: %v", table.Name, col.Name, err)
			}
		}
	}

	ui.Debug("Imported %d rows into %s", rowCount, table.Name)
	return nil
}

// processCSVValue converts a raw CSV string to a typed Go value for MySQL.
func (m *MySQLManager) processCSVValue(value, columnType string) interface{} {
	if value == "" || value == "NULL" || value == "null" {
		return nil
	}
	ct := strings.ToLower(columnType)

	if ct == "json" {
		var js interface{}
		if json.Unmarshal([]byte(value), &js) == nil {
			return value
		}
		return value
	}

	// tinyint(1) is MySQL's canonical boolean — check before the general
	// int path so "0"/"1"/"true"/"false" map to int rather than int64.
	if ct == "tinyint" || ct == "bool" || ct == "boolean" {
		lower := strings.ToLower(value)
		if lower == "true" || lower == "t" || lower == "yes" || lower == "1" {
			return 1
		}
		if lower == "false" || lower == "f" || lower == "no" || lower == "0" {
			return 0
		}
	}

	if strings.Contains(ct, "int") {
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
	}

	if strings.Contains(ct, "float") || strings.Contains(ct, "double") ||
		strings.Contains(ct, "decimal") || strings.Contains(ct, "numeric") {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
	}

	if strings.Contains(ct, "datetime") || strings.Contains(ct, "timestamp") {
		for _, layout := range []string{
			"2006-01-02 15:04:05.999999 -0700 UTC",
			time.RFC3339Nano,
			"2006-01-02 15:04:05",
		} {
			if t, err := time.Parse(layout, value); err == nil {
				return t
			}
		}
	}

	if ct == "date" {
		if t, err := time.Parse("2006-01-02", value); err == nil {
			return t
		}
	}

	return value
}

// readSchemaFromFile parses schema.json without executing any DB statements.
func (m *MySQLManager) readSchemaFromFile(filename string) (*Schema, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var schema Schema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("parsing schema.json: %v", err)
	}
	if schema.DatabaseType != "" && schema.DatabaseType != MySQL {
		m.log("Warning: schema was created for %s, restoring into MySQL", schema.DatabaseType)
	}
	return &schema, nil
}

// parseMySQLEnum extracts values from a MySQL enum type string such as
// "enum('a','b','c')". Returns nil for any non-enum column type.
func parseMySQLEnum(columnType string) []string {
	lower := strings.ToLower(columnType)
	if !strings.HasPrefix(lower, "enum") {
		return nil
	}
	start := strings.Index(lower, "(")
	end := strings.LastIndex(lower, ")")
	if start == -1 || end == -1 || end <= start {
		return nil
	}
	inner := columnType[start+1 : end]
	var values []string
	for _, part := range strings.Split(inner, ",") {
		v := strings.TrimSpace(part)
		v = strings.Trim(v, "'")
		if v != "" {
			values = append(values, v)
		}
	}
	return values
}

// isDuplicateConstraint returns true when the MySQL error indicates the
// foreign key constraint name already exists.
func isDuplicateConstraint(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key name") ||
		strings.Contains(msg, "already exists")
}
