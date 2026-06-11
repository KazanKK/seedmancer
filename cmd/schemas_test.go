package cmd

import (
	"testing"
	"time"
)

func TestFormatBytes(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
	}
	for _, c := range cases {
		if got := formatBytes(c.in); got != c.want {
			t.Errorf("formatBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestSchemaRecency(t *testing.T) {
	updated := "2026-03-01T00:00:00Z"
	synced := "2026-04-01T00:00:00Z"

	t.Run("prefers lastSyncedAt", func(t *testing.T) {
		s := schemaSummary{LastSyncedAt: &synced, UpdatedAt: updated}
		got := schemaRecency(s)
		want, _ := time.Parse(time.RFC3339, synced)
		if !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("falls back to updatedAt", func(t *testing.T) {
		s := schemaSummary{UpdatedAt: updated}
		got := schemaRecency(s)
		want, _ := time.Parse(time.RFC3339, updated)
		if !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("unparseable returns zero time", func(t *testing.T) {
		s := schemaSummary{UpdatedAt: "nope"}
		if !schemaRecency(s).IsZero() {
			t.Fatal("want zero time for unparseable timestamp")
		}
	})
}
