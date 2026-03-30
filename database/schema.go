package db

type Column struct {
    Name       string      `json:"name"`
    Type       string      `json:"type"`        // Database type (e.g. text, integer, timestamp)
    Varchar    *string      `json:"varchar,omitempty"`
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

type Function struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
}

type Trigger struct {
	Name       string `json:"name"`
	TableName  string `json:"tableName"`
	Definition string `json:"definition"`
}

type DatabaseType string

const (
	Postgres DatabaseType = "postgres"
)

type Schema struct {
	DatabaseType DatabaseType `json:"databaseType"`
	Enums        []EnumItem   `json:"enums"`
	Tables       []Table      `json:"tables,omitempty"`
	Functions    []Function   `json:"functions,omitempty"`
	Triggers     []Trigger    `json:"triggers,omitempty"`
}

type SchemaExtractor interface {
    ExtractSchema() (*Schema, error)
} 