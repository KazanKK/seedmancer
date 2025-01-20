package db

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-faker/faker/v4"
)

// Store generated primary keys for referential integrity
var (
	generatedValues     = make(map[string][]string)
	valueMutex         sync.RWMutex
	idCounters         = make(map[string]int) // Track IDs for each table
	uniqueValueSets    = make(map[string]map[string]bool)
	uniqueValueMutex   sync.RWMutex
)

func init() {
	rand.Seed(time.Now().UnixNano())
	uniqueValueSets = make(map[string]map[string]bool)
}

func randomInt(min, max int) int {
	return rand.Intn(max-min+1) + min
}

func randomFloat(min, max float64, precision int) float64 {
	val := min + rand.Float64()*(max-min)
	factor := math.Pow10(precision)
	return math.Round(val*factor) / factor
}

func (p *PostgresManager) GenerateFakeData(outputDir string, rowCount int) error {
	// Extract schema first
	schema, err := p.ExtractSchema()
	if err != nil {
		return fmt.Errorf("failed to extract schema: %v", err)
	}

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
	}

	return nil
}

func generateTableData(table Table, outputDir string, rowCount int) error {
	filename := filepath.Join(outputDir, fmt.Sprintf("%s.csv", table.Name))
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("creating CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	headers := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		headers[i] = col.Name
	}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("writing CSV header: %v", err)
	}

	// Generate rows
	for i := 0; i < rowCount; i++ {
		row := make([]string, len(table.Columns))
		for j, col := range table.Columns {
			// Handle foreign keys first
			if col.ForeignKey != nil {
				key := fmt.Sprintf("%s.%s", col.ForeignKey.Table, col.ForeignKey.Column)
				valueMutex.RLock()
				values := generatedValues[key]
				valueMutex.RUnlock()
				if len(values) > 0 {
					row[j] = values[i%len(values)]
					continue
				}
			}

			// Generate value based on column type
			value := generateFakeValue(col, i)
			if col.IsPrimary || col.IsUnique {
				value = generateUniqueValue(col, i)
			}
			row[j] = value
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("writing CSV row: %v", err)
		}
	}

	return nil
}

func generateFakeValue(col Column, rowIndex int) string {
	// Handle nullable fields
	if col.Nullable && rand.Float32() < 0.1 { // 10% chance of NULL
		return "NULL"
	}

	switch strings.ToLower(col.Type) {
	case "uuid":
		return faker.UUIDHyphenated()
	
	case "text", "varchar", "char":
		if strings.Contains(strings.ToLower(col.Name), "email") {
			return fmt.Sprintf("user%d@example.com", rowIndex)
		}
		if strings.Contains(strings.ToLower(col.Name), "name") {
			return faker.Name()
		}
		if strings.Contains(strings.ToLower(col.Name), "phone") {
			return faker.Phonenumber()
		}
		return faker.Word()
	
	case "int", "integer", "bigint", "smallint":
		return fmt.Sprintf("%d", randomInt(1, 1000000))
	
	case "decimal", "numeric", "real", "double precision":
		return fmt.Sprintf("%.2f", randomFloat(0, 1000, 2))
	
	case "boolean", "bool":
		return fmt.Sprintf("%v", rand.Intn(2) == 1)
	
	case "date":
		days := rand.Intn(365*5) // Random date within 5 years
		return time.Now().AddDate(0, 0, -days).Format("2006-01-02")
	
	case "timestamp", "timestamptz":
		days := rand.Intn(365*5) // Random timestamp within 5 years
		hours := rand.Intn(24)
		minutes := rand.Intn(60)
		return time.Now().
			AddDate(0, 0, -days).
			Add(time.Duration(hours)*time.Hour).
			Add(time.Duration(minutes)*time.Minute).
			Format("2006-01-02 15:04:05")
	
	case "json", "jsonb":
		data := map[string]interface{}{
			"id": rowIndex + 1,
			"value": faker.Word(),
			"timestamp": time.Now().Unix(),
		}
		jsonBytes, _ := json.Marshal(data)
		return string(jsonBytes)
	
	default:
		if len(col.Values) > 0 { // Handle enum types
			return col.Values[rowIndex%len(col.Values)]
		}
		return fmt.Sprintf("value_%d", rowIndex)
	}
}

// Helper function to generate a simple JSON object
func generateSimpleJSON() string {
	data := map[string]interface{}{
		"id": randomInt(1, 1000000),
		"value": faker.Word(),
		"timestamp": time.Now().Unix(),
	}
	jsonBytes, _ := json.Marshal(data)
	return string(jsonBytes)
}

// Helper function to get minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func generatePrimaryKeyValue(col Column, tableName string) string {
	dataType := strings.ToLower(col.Type)
	if strings.Contains(dataType, "uuid") {
		return faker.UUIDHyphenated()
	}
	if strings.Contains(dataType, "serial") || strings.Contains(dataType, "int") {
		idCounters[tableName]++
		return fmt.Sprintf("%d", idCounters[tableName])
	}
	return faker.Word()
}

func generateUniqueValue(col Column, rowIndex int) string {
	uniqueValueMutex.Lock()
	defer uniqueValueMutex.Unlock()

	// Initialize unique value set if not exists
	if _, exists := uniqueValueSets[col.Name]; !exists {
		uniqueValueSets[col.Name] = make(map[string]bool)
	}

	var value string
	maxAttempts := 1000
	attempt := 0

	for attempt < maxAttempts {
		value = generateUniqueValueByType(col, rowIndex, attempt)
		
		// Check if value is unique
		if !uniqueValueSets[col.Name][value] {
			uniqueValueSets[col.Name][value] = true
			return value
		}
		attempt++
	}

	// If we couldn't generate a unique value after max attempts,
	// append the rowIndex to make it unique
	value = fmt.Sprintf("%s_%d", value, rowIndex)
	uniqueValueSets[col.Name][value] = true
	return value
}

func generateUniqueValueByType(col Column, rowIndex, attempt int) string {
	dataType := strings.ToLower(col.Type)
	
	// Handle sequence types
	if strings.Contains(dataType, "serial") ||
	   strings.Contains(fmt.Sprintf("%v", col.Default), "nextval") {
		return fmt.Sprintf("%d", rowIndex+1+attempt)
	}

	switch {
	case strings.Contains(dataType, "uuid"):
		return faker.UUIDHyphenated()
		
	case strings.Contains(dataType, "int"):
		if strings.Contains(dataType, "small") {
			return fmt.Sprintf("%d", rowIndex+1+attempt)
		}
		if strings.Contains(dataType, "big") {
			return fmt.Sprintf("%d", (rowIndex+1)*1000+attempt)
		}
		return fmt.Sprintf("%d", rowIndex+1+attempt)
		
	case strings.Contains(dataType, "varchar"), strings.Contains(dataType, "text"):
		if strings.Contains(strings.ToLower(col.Name), "email") {
			return fmt.Sprintf("user%d_%d@example.com", rowIndex+1, attempt)
		}
		if strings.Contains(strings.ToLower(col.Name), "name") {
			return fmt.Sprintf("%s_%d_%d", faker.Name(), rowIndex+1, attempt)
		}
		return fmt.Sprintf("%s_%d_%d", faker.Word(), rowIndex+1, attempt)
		
	case strings.Contains(dataType, "timestamp"):
		// Generate unique timestamps by adding seconds
		return time.Now().Add(time.Duration(rowIndex+attempt) * time.Second).Format("2006-01-02 15:04:05")
		
	case strings.Contains(dataType, "date"):
		// Generate unique dates by adding days
		return time.Now().AddDate(0, 0, rowIndex+attempt).Format("2006-01-02")
		
	case strings.Contains(dataType, "json"), strings.Contains(dataType, "jsonb"):
		data := map[string]interface{}{
			"id":        rowIndex + 1 + attempt,
			"uniqueKey": fmt.Sprintf("key_%d_%d", rowIndex+1, attempt),
			"createdAt": time.Now().Format(time.RFC3339),
		}
		jsonBytes, _ := json.Marshal(data)
		return string(jsonBytes)
		
	default:
		return fmt.Sprintf("unique_%d_%d", rowIndex+1, attempt)
	}
}