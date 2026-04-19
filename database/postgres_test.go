package db

import (
	"strings"
	"testing"
	"time"
)

func TestParseTriggerSQL(t *testing.T) {
	content := `-- seedmancer:trigger
-- name: users_updated_at
-- table_schema: public
-- table_name: users
CREATE TRIGGER users_updated_at
BEFORE UPDATE ON public.users
FOR EACH ROW EXECUTE FUNCTION set_updated_at();`

	name, schema, table, def, err := parseTriggerSQL(content)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if name != "users_updated_at" || schema != "public" || table != "users" {
		t.Fatalf("metadata mismatch: name=%q schema=%q table=%q", name, schema, table)
	}
	if !strings.HasPrefix(def, "CREATE TRIGGER") {
		t.Fatalf("def should start with CREATE TRIGGER, got %q", def)
	}
	if strings.Contains(def, "seedmancer:trigger") {
		t.Fatal("definition should not contain header markers")
	}
}

func TestParseTriggerSQL_missingMetadataErrors(t *testing.T) {
	_, _, _, _, err := parseTriggerSQL("CREATE TRIGGER x ...;")
	if err == nil {
		t.Fatal("expected error when metadata missing")
	}
}

func TestColumnDefaultString(t *testing.T) {
	cases := []struct {
		in   interface{}
		want string
	}{
		{nil, ""},
		{"hello", "hello"},
		{42, "42"},
		{3.14, "3.14"},
	}
	for _, c := range cases {
		if got := columnDefaultString(c.in); got != c.want {
			t.Errorf("columnDefaultString(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestJoinQuotedStrings(t *testing.T) {
	got := joinQuotedStrings([]string{"a", "b", "it's"})
	want := "'a', 'b', 'it''s'"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got := joinQuotedStrings(nil); got != "" {
		t.Errorf("empty input should produce empty string, got %q", got)
	}
}

func TestFixSimpleJSON(t *testing.T) {
	// Single-quote to double-quote swap.
	out := fixSimpleJSON(`{'k':'v'}`)
	if !strings.Contains(out, `"k"`) || !strings.Contains(out, `"v"`) {
		t.Fatalf("fixSimpleJSON did not convert quotes: %q", out)
	}
}

func TestParseArrayString(t *testing.T) {
	got := parseArrayString(`a,b,"c,d"`)
	want := []string{"a", "b", `"c,d"`}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestProcessCSVValue_nulls(t *testing.T) {
	p := &PostgresManager{}
	for _, v := range []string{"", "NULL", "null"} {
		if got := p.processCSVValue(v, "text"); got != nil {
			t.Errorf("processCSVValue(%q) = %v, want nil", v, got)
		}
	}
}

func TestProcessCSVValue_booleans(t *testing.T) {
	p := &PostgresManager{}
	trueValues := []string{"true", "TRUE", "t", "yes", "1"}
	for _, v := range trueValues {
		if got := p.processCSVValue(v, "boolean"); got != true {
			t.Errorf("processCSVValue(%q) = %v, want true", v, got)
		}
	}
	falseValues := []string{"false", "FALSE", "f", "no", "0"}
	for _, v := range falseValues {
		if got := p.processCSVValue(v, "boolean"); got != false {
			t.Errorf("processCSVValue(%q) = %v, want false", v, got)
		}
	}
}

func TestProcessCSVValue_integers(t *testing.T) {
	p := &PostgresManager{}
	if got := p.processCSVValue("42", "integer"); got != int64(42) {
		t.Errorf("int = %v (%T)", got, got)
	}
	if got := p.processCSVValue("abc", "integer"); got != "abc" {
		t.Errorf("non-numeric should pass through as string, got %v", got)
	}
}

func TestProcessCSVValue_floats(t *testing.T) {
	p := &PostgresManager{}
	if got := p.processCSVValue("3.14", "numeric"); got != 3.14 {
		t.Errorf("float = %v", got)
	}
}

func TestProcessCSVValue_timestamps(t *testing.T) {
	p := &PostgresManager{}
	got := p.processCSVValue("2026-04-18 00:15:14", "timestamp")
	if _, ok := got.(time.Time); !ok {
		t.Errorf("want time.Time, got %T (%v)", got, got)
	}

	gotDate := p.processCSVValue("2026-04-18", "date")
	if _, ok := gotDate.(time.Time); !ok {
		t.Errorf("want time.Time for date, got %T (%v)", gotDate, gotDate)
	}
}

func TestProcessCSVValue_jsonValid(t *testing.T) {
	p := &PostgresManager{}
	in := `{"k":"v"}`
	if got := p.processCSVValue(in, "jsonb"); got != in {
		t.Errorf("valid json should round-trip, got %v", got)
	}
}
