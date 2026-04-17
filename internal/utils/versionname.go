package utils

import (
	"strings"
)

// SanitizeDatasetSegment strips characters that would break a directory name.
// Used when we build dataset folders from user-provided labels.
func SanitizeDatasetSegment(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Map(func(r rune) rune {
		switch r {
		case '/', '\\':
			return '_'
		case ':', '*', '?', '"', '<', '>', '|':
			return '_'
		case 0:
			return -1
		default:
			return r
		}
	}, s)
	if s == "" {
		return "dataset"
	}
	return s
}
