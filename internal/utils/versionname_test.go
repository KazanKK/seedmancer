package utils

import "testing"

func TestSanitizeDatasetSegment(t *testing.T) {
	cases := map[string]string{
		"my/db":  "my_db",
		"a(b)":   "a(b)",
		"   ":    "dataset",
		"plain":  "plain",
		"a:b*c?": "a_b_c_",
	}
	for in, want := range cases {
		if got := SanitizeDatasetSegment(in); got != want {
			t.Errorf("SanitizeDatasetSegment(%q) = %q, want %q", in, got, want)
		}
	}
}
