package generic

import (
	"math"
	"strconv"
	"strings"

	"frostdb/internal/resp/types"
)

// ParseTimeout parses the timeout argument and returns it as a float64.
// If there is an error, it returns a types.Value of error type.
func ParseTimeout(val types.Value) (float64, types.Value) {
	s := strings.TrimSpace(string(val.Str))
	var f float64
	if strings.HasPrefix(strings.ToLower(s), "0x") {
		u, err := strconv.ParseUint(s, 0, 64)
		if err != nil {
			return 0, types.NewError("ERR timeout is out of range")
		}
		f = float64(u)
	} else {
		var err error
		f, err = strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, types.NewError("ERR timeout is not a float or out of range")
		}
	}
	if f < 0 {
		return 0, types.NewError("ERR timeout is negative")
	}
	if math.IsNaN(f) || math.IsInf(f, 0) || f > 9223372036854775 {
		return 0, types.NewError("ERR timeout is out of range")
	}
	return f, types.Value{}
}
