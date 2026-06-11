package schemadiff

import (
	"strings"
	"testing"
)

func TestDiff_identicalSchemas(t *testing.T) {
	body := `{"tables":[{"name":"User","columns":[{"name":"id","type":"uuid"}]}]}`
	got, err := Diff([]byte(body), []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no changes, got %v", got)
	}
}

func TestDiff_addedAndRemovedTables(t *testing.T) {
	old := `{"tables":[{"name":"User","columns":[{"name":"id","type":"uuid"}]}]}`
	new := `{"tables":[
		{"name":"User","columns":[{"name":"id","type":"uuid"}]},
		{"name":"Plan","columns":[{"name":"id","type":"uuid"}]}
	]}`
	got, err := Diff([]byte(old), []byte(new))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Kind != TableAdded || got[0].Table != "Plan" {
		t.Fatalf("unexpected diff: %+v", got)
	}

	got, err = Diff([]byte(new), []byte(old))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Kind != TableRemoved || got[0].Table != "Plan" {
		t.Fatalf("unexpected diff: %+v", got)
	}
}

func TestDiff_columnAddRemoveChange(t *testing.T) {
	old := `{"tables":[{"name":"User","columns":[
		{"name":"id","type":"uuid"},
		{"name":"email","type":"varchar(255)","nullable":true},
		{"name":"fullName","type":"text"}
	]}]}`
	new := `{"tables":[{"name":"User","columns":[
		{"name":"id","type":"uuid"},
		{"name":"email","type":"text","nullable":false},
		{"name":"status","type":"text","nullable":false,"default":"active"}
	]}]}`
	got, err := Diff([]byte(old), []byte(new))
	if err != nil {
		t.Fatal(err)
	}
	rendered := make([]string, len(got))
	for i, c := range got {
		rendered[i] = c.String()
	}
	joined := strings.Join(rendered, "\n")

	for _, wantSub := range []string{
		"~ User.email type changed varchar(255) -> text",
		"nullable true -> false",
		"- User.fullName removed",
		"+ User.status added text",
	} {
		if !strings.Contains(joined, wantSub) {
			t.Errorf("diff missing %q\nfull:\n%s", wantSub, joined)
		}
	}
}

func TestDiff_invalidJSON(t *testing.T) {
	if _, err := Diff([]byte("not json"), []byte("{}")); err == nil {
		t.Fatal("expected error for invalid old JSON")
	}
	if _, err := Diff([]byte("{}"), []byte("not json")); err == nil {
		t.Fatal("expected error for invalid new JSON")
	}
}
