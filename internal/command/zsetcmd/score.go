package zsetcmd

import (
	"math"
	"strconv"
)

// formatScore converts a sorted-set score to its string representation,
// matching Redis's d2string / fpconv_dtoa behaviour:
//
//   - NaN / +Inf / -Inf map to "nan" / "inf" / "-inf".
//   - Integral doubles that fit in an int64 are printed without a decimal
//     point (e.g. 3 -> "3", not "3.0").
//   - All other values use the shortest decimal string that round-trips
//     back to the exact same IEEE 754 double (strconv precision -1). This
//     guarantees ZSCORE/ZADD round-trip bit-exactly, which the Redis test
//     suite relies on (17-19 significant digit mantissas must survive).
//
// Using precision -1 (shortest round-trip) rather than a fixed 15/17 digit
// count is both correct and futureproof: it never loses precision and never
// emits redundant digits.
func formatScore(score float64) string {
	if math.IsNaN(score) {
		return "nan"
	}
	if math.IsInf(score, 1) {
		return "inf"
	}
	if math.IsInf(score, -1) {
		return "-inf"
	}
	if score == 0 {
		// Avoid signed zero "-0".
		return "0"
	}
	// Print integral doubles that fit exactly in an int64 as integers.
	if iv := int64(score); float64(iv) == score {
		return strconv.FormatInt(iv, 10)
	}
	// Shortest representation that parses back to the identical double.
	return strconv.FormatFloat(score, 'g', -1, 64)
}
