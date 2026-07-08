package object

import (
	"strconv"
	"strings"
)

// formatIdx converts an int64 index to a string key for the RDB hash map.
// Uses strconv.FormatInt which returns a freshly allocated string (safe for
// concurrent use, unlike the previous shared-buffer approach).
func formatIdx(idx int64) string {
	return strconv.FormatInt(idx, 10)
}

// parseIdx parses an index string back to int64.
// Returns the parsed value and whether parsing succeeded.
func parseIdx(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func sortForward(a []int64) {
	for i := 0; i < len(a); i++ {
		for j := i + 1; j < len(a); j++ {
			if a[j] < a[i] {
				a[i], a[j] = a[j], a[i]
			}
		}
	}
}

func sortReverse(a []int64) {
	for i := 0; i < len(a); i++ {
		for j := i + 1; j < len(a); j++ {
			if a[j] > a[i] {
				a[i], a[j] = a[j], a[i]
			}
		}
	}
}

func reverseSlice(s []string) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func stringsEqualFold(a, b string) bool {
	return strings.EqualFold(a, b)
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

func containsFold(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
