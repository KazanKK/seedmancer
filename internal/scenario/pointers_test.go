package scenario

import (
	"os"
	"testing"
)

func TestPointersRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if err := WritePointers(dir, Pointers{Latest: "r003", Stable: "r002"}); err != nil {
		t.Fatal(err)
	}
	got, err := ReadPointers(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.Latest != "r003" || got.Stable != "r002" {
		t.Fatalf("unexpected pointers: %+v", got)
	}
}

func TestReadPointers_missing(t *testing.T) {
	dir := t.TempDir()
	got, err := ReadPointers(dir)
	if err != nil {
		t.Fatalf("expected nil error for missing file, got %v", err)
	}
	if got.Latest != "" || got.Stable != "" {
		t.Fatalf("expected zero-value, got %+v", got)
	}
}

func TestReadPointers_corrupt(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(PointersPath(dir), []byte("not json"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := ReadPointers(dir)
	if err == nil {
		t.Fatal("expected error reading corrupt pointers")
	}
}
