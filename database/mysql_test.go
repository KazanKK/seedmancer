package db

import (
	"strings"
	"testing"
	"time"
)

// ── parseMySQLEnum ────────────────────────────────────────────────────────────

func TestParseMySQLEnum(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"enum('active','inactive','pending')", []string{"active", "inactive", "pending"}},
		{"ENUM('a','b')", []string{"a", "b"}},
		{"enum('only')", []string{"only"}},
		{"enum()", nil},
		{"varchar(255)", nil},
		{"", nil},
	}
	for _, c := range cases {
		got := parseMySQLEnum(c.in)
		if len(got) != len(c.want) {
			t.Errorf("parseMySQLEnum(%q) = %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range c.want {
			if got[i] != c.want[i] {
				t.Errorf("parseMySQLEnum(%q)[%d] = %q, want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}

func TestParseMySQLEnum_singleQuotesStripped(t *testing.T) {
	got := parseMySQLEnum("enum('hello world','foo')")
	if len(got) != 2 || got[0] != "hello world" || got[1] != "foo" {
		t.Errorf("unexpected result: %v", got)
	}
}

// ── quoteIdent ────────────────────────────────────────────────────────────────

func TestQuoteIdent(t *testing.T) {
	cases := []struct{ in, want string }{
		{"users", "`users`"},
		{"my table", "`my table`"},
		{"back`tick", "`back``tick`"},
		{"", "``"},
	}
	for _, c := range cases {
		if got := quoteIdent(c.in); got != c.want {
			t.Errorf("quoteIdent(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// ── isDuplicateConstraint ─────────────────────────────────────────────────────

func TestIsDuplicateConstraint(t *testing.T) {
	if isDuplicateConstraint(nil) {
		t.Error("nil error should return false")
	}

	type fakeErr struct{ msg string }
	makeErr := func(msg string) error {
		return &fakeError{msg}
	}

	if !isDuplicateConstraint(makeErr("Duplicate key name 'idx_foo'")) {
		t.Error("'duplicate key name' should be recognised")
	}
	if !isDuplicateConstraint(makeErr("constraint already exists")) {
		t.Error("'already exists' should be recognised")
	}
	if isDuplicateConstraint(makeErr("Table not found")) {
		t.Error("unrelated error should not be recognised as duplicate")
	}
}

type fakeError struct{ msg string }

func (e *fakeError) Error() string { return e.msg }

// ── MySQLManager.processCSVValue ──────────────────────────────────────────────

func TestMySQLProcessCSVValue_nulls(t *testing.T) {
	m := &MySQLManager{}
	// Explicit NULL markers always map to SQL NULL for any type.
	for _, v := range []string{"NULL", "null"} {
		if got := m.processCSVValue(v, "varchar"); got != nil {
			t.Errorf("processCSVValue(%q) = %v, want nil", v, got)
		}
	}
	// Empty string is SQL NULL for non-text types.
	if got := m.processCSVValue("", "int"); got != nil {
		t.Errorf("processCSVValue(%q, int) = %v, want nil", "", got)
	}
	// Empty string is a valid value for text-family types.
	for _, colType := range []string{"text", "tinytext", "mediumtext", "longtext", "varchar(255)", "char"} {
		if got := m.processCSVValue("", colType); got != "" {
			t.Errorf("processCSVValue(%q, %s) = %v, want empty string", "", colType, got)
		}
	}
}

func TestMySQLProcessCSVValue_integers(t *testing.T) {
	m := &MySQLManager{}
	if got := m.processCSVValue("42", "int"); got != int64(42) {
		t.Errorf("int = %v (%T), want int64(42)", got, got)
	}
	if got := m.processCSVValue("99", "bigint"); got != int64(99) {
		t.Errorf("bigint = %v (%T), want int64(99)", got, got)
	}
	if got := m.processCSVValue("abc", "int"); got != "abc" {
		t.Errorf("non-numeric int should pass through, got %v", got)
	}
}

func TestMySQLProcessCSVValue_booleans(t *testing.T) {
	m := &MySQLManager{}
	for _, v := range []string{"true", "TRUE", "yes", "1", "t"} {
		if got := m.processCSVValue(v, "tinyint"); got != 1 {
			t.Errorf("processCSVValue(%q, tinyint) = %v, want 1", v, got)
		}
	}
	for _, v := range []string{"false", "FALSE", "no", "0", "f"} {
		if got := m.processCSVValue(v, "tinyint"); got != 0 {
			t.Errorf("processCSVValue(%q, tinyint) = %v, want 0", v, got)
		}
	}
}

func TestMySQLProcessCSVValue_floats(t *testing.T) {
	m := &MySQLManager{}
	if got := m.processCSVValue("3.14", "decimal"); got != 3.14 {
		t.Errorf("decimal = %v", got)
	}
	if got := m.processCSVValue("2.71", "double"); got != 2.71 {
		t.Errorf("double = %v", got)
	}
}

func TestMySQLProcessCSVValue_timestamps(t *testing.T) {
	m := &MySQLManager{}
	got := m.processCSVValue("2026-04-18 00:15:14", "datetime")
	if _, ok := got.(time.Time); !ok {
		t.Errorf("want time.Time for datetime, got %T (%v)", got, got)
	}

	gotDate := m.processCSVValue("2026-04-18", "date")
	if _, ok := gotDate.(time.Time); !ok {
		t.Errorf("want time.Time for date, got %T (%v)", gotDate, gotDate)
	}

	gotRFC := m.processCSVValue("2026-04-18T00:15:14Z", "timestamp")
	if _, ok := gotRFC.(time.Time); !ok {
		t.Errorf("want time.Time for RFC3339 timestamp, got %T (%v)", gotRFC, gotRFC)
	}
}

func TestMySQLProcessCSVValue_json(t *testing.T) {
	m := &MySQLManager{}
	in := `{"key":"value"}`
	if got := m.processCSVValue(in, "json"); got != in {
		t.Errorf("valid JSON should round-trip, got %v", got)
	}
}

func TestMySQLProcessCSVValue_strings(t *testing.T) {
	m := &MySQLManager{}
	if got := m.processCSVValue("hello", "varchar"); got != "hello" {
		t.Errorf("varchar = %v, want 'hello'", got)
	}
	if got := m.processCSVValue("some text", "text"); got != "some text" {
		t.Errorf("text = %v, want 'some text'", got)
	}
}

// ── MySQLManager.columnTypeDDL ────────────────────────────────────────────────

func TestMySQLColumnTypeDDL(t *testing.T) {
	m := &MySQLManager{}

	varcharLen := "100"
	cases := []struct {
		col  Column
		want string
	}{
		{Column{Type: "varchar", Varchar: &varcharLen}, "VARCHAR(100)"},
		{Column{Type: "character varying", Varchar: &varcharLen}, "VARCHAR(100)"},
		{Column{Type: "varchar"}, "VARCHAR(255)"},
		{Column{Type: "text"}, "TEXT"},
		{Column{Type: "integer"}, "INT"},
		{Column{Type: "int4"}, "INT"},
		{Column{Type: "bigint"}, "BIGINT"},
		{Column{Type: "smallint"}, "SMALLINT"},
		{Column{Type: "boolean"}, "TINYINT(1)"},
		{Column{Type: "bool"}, "TINYINT(1)"},
		{Column{Type: "numeric"}, "DECIMAL(18,6)"},
		{Column{Type: "decimal"}, "DECIMAL(18,6)"},
		{Column{Type: "real"}, "FLOAT"},
		{Column{Type: "double precision"}, "DOUBLE"},
		{Column{Type: "date"}, "DATE"},
		{Column{Type: "json"}, "JSON"},
		{Column{Type: "jsonb"}, "JSON"},
		{Column{Type: "uuid"}, "CHAR(36)"},
		{Column{Type: "bytea"}, "BLOB"},
	}
	for _, c := range cases {
		got := m.columnTypeDDL(c.col)
		if !strings.EqualFold(got, c.want) {
			t.Errorf("columnTypeDDL(%q) = %q, want %q", c.col.Type, got, c.want)
		}
	}
}

func TestMySQLColumnTypeDDL_timestamp(t *testing.T) {
	m := &MySQLManager{}
	got := m.columnTypeDDL(Column{Type: "timestamp without time zone"})
	if !strings.Contains(strings.ToUpper(got), "DATETIME") {
		t.Errorf("timestamp type should map to DATETIME variant, got %q", got)
	}
}
