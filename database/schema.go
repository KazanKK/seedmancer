package db

type Column struct {
    Name       string      `json:"name"`
    Type       string      `json:"type"`        // Database type (e.g. text, integer, timestamp)
    Nullable   bool        `json:"nullable"`
    Default    interface{} `json:"default,omitempty"`
    IsPrimary  bool        `json:"isPrimary"`
    IsUnique   bool        `json:"isUnique"`
    ForeignKey *ForeignKey `json:"foreignKey,omitempty"`
    Enum       string      `json:"enum,omitempty"`
}

type ForeignKey struct {
    Table  string `json:"table"`  // Referenced table
    Column string `json:"column"` // Referenced column
}

type Table struct {
    Name    string   `json:"name"`
    Columns []Column `json:"columns"`
}

type EnumItem struct {
    Name   string   `json:"name"`
    Values []string `json:"values"`
}

type DatabaseType string

const (
	Postgres DatabaseType = "postgres"
	MySQL    DatabaseType = "mysql"
)

// Updated Schema struct to include database type
type Schema struct {
    DatabaseType DatabaseType `json:"databaseType"` // "postgres", "mysql", etc.
    Enums        []EnumItem   `json:"enums"`
    Tables       []Table      `json:"tables,omitempty"`
}

type SchemaExtractor interface {
    ExtractSchema() (*Schema, error)
} 