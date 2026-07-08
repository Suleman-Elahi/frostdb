package hashcmd

import (
	"math"
	"math/rand"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// HRandFieldCommand implements HRANDFIELD key [count [WITHVALUES]]
type HRandFieldCommand struct{}

func (c HRandFieldCommand) Name() string { return "HRANDFIELD" }

func (c HRandFieldCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 || len(args) > 3 {
		return types.NewError("ERR wrong number of arguments for 'hrandfield' command")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		if len(args) >= 2 {
			return types.NewArray([]types.Value{})
		}
		return types.RespNil
	}

	h, ok := obj.Value.(*object.Hash)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	key := string(args[0].Str)
	ExpireIfNeeded(ctx, key, h, nil, ctx.NowMs(), false, true)

	dict := h.GetAll()
	n := len(dict)
	if n == 0 {
		if len(args) >= 2 {
			return types.NewArray([]types.Value{})
		}
		return types.RespNil
	}

	// Collect keys and values into parallel slices for indexed access.
	keys := make([]string, 0, n)
	vals := make([]string, 0, n)
	for k, v := range dict {
		keys = append(keys, k)
		vals = append(vals, v)
	}

	// No count argument: return a single random field name.
	if len(args) < 2 {
		return types.NewString(keys[rand.Intn(n)])
	}

	count, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil || count == math.MinInt64 {
		if err != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}
		return types.NewError("ERR value is out of range")
	}

	// Detect overflow for very large negative values.
	// Redis rejects values whose absolute value overflows when negated.
	if count < 0 && count > math.MinInt64 {
		abs := -count
		// Multiplied-by-2 overflow check (for WITHVALUES allocation).
		if abs > math.MaxInt64/2 {
			return types.NewError("ERR value is out of range")
		}
	}

	withValues := false
	if len(args) >= 3 {
		if strings.EqualFold(string(args[2].Str), "withvalues") {
			withValues = true
		} else {
			return types.NewError("ERR syntax error")
		}
	}

	if count == 0 {
		return types.NewArray([]types.Value{})
	}

	allowDuplicates := count < 0
	if count < 0 {
		count = -count
	}

	var result []types.Value

	if allowDuplicates {
		// Negative count: allow duplicate fields, return exactly |count| elements.
		for i := int64(0); i < count; i++ {
			idx := rand.Intn(n)
			if withValues {
				if ctx.Protocol >= 3 {
					result = append(result, types.NewArray([]types.Value{
						types.NewString(keys[idx]),
						types.NewString(vals[idx]),
					}))
				} else {
					result = append(result, types.NewString(keys[idx]))
					result = append(result, types.NewString(vals[idx]))
				}
			} else {
				result = append(result, types.NewString(keys[idx]))
			}
		}
	} else {
		// Positive count: unique fields only, capped at hash size.
		want := int(count)
		if want > n {
			want = n
		}

		// Fisher-Yates partial shuffle for unique selection.
		shuffled := make([]int, n)
		for i := range shuffled {
			shuffled[i] = i
		}
		for i := 0; i < want; i++ {
			j := i + rand.Intn(n-i)
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		}

		for i := 0; i < want; i++ {
			idx := shuffled[i]
			if withValues {
				if ctx.Protocol >= 3 {
					result = append(result, types.NewArray([]types.Value{
						types.NewString(keys[idx]),
						types.NewString(vals[idx]),
					}))
				} else {
					result = append(result, types.NewString(keys[idx]))
					result = append(result, types.NewString(vals[idx]))
				}
			} else {
				result = append(result, types.NewString(keys[idx]))
			}
		}
	}

	return types.NewArray(result)
}
