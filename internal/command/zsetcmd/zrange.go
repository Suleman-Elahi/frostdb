package zsetcmd

import (
	"math"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ZRangeCommand struct{}

func (c ZRangeCommand) Name() string { return "ZRANGE" }

func (c ZRangeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'zrange' command")
	}

	key := string(args[0].Str)
	startStr := string(args[1].Str)
	stopStr := string(args[2].Str)

	// Parse optional flags
	byScore := false
	byLex := false
	withScores := false
	rev := false
	hasLimit := false
	limitOffset := 0
	limitCount := -1

	for i := 3; i < len(args); i++ {
		flag := strings.ToUpper(string(args[i].Str))
		switch flag {
		case "BYSCORE":
			byScore = true
		case "BYLEX":
			byLex = true
		case "WITHSCORES":
			withScores = true
		case "REV":
			rev = true
		case "LIMIT":
			if i+2 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			var err error
			limitOffset, err = strconv.Atoi(string(args[i+1].Str))
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			limitCount, err = strconv.Atoi(string(args[i+2].Str))
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			hasLimit = true
			i += 2
		default:
			return types.NewError("ERR syntax error")
		}
	}

	// LIMIT is only valid with BYSCORE or BYLEX
	if hasLimit && !byScore && !byLex {
		return types.NewError("ERR syntax error")
	}

	// BYLEX + WITHSCORES is invalid
	if byLex && withScores {
		return types.NewError("ERR syntax error")
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewArray([]types.Value{})
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	var entries []object.ZSetEntry

	if byLex {
		if rev {
			max, maxExcl, err := object.ParseLexBound(startStr)
			if err != nil {
				return types.NewError("ERR min or max not valid string range item")
			}
			min, minExcl, err := object.ParseLexBound(stopStr)
			if err != nil {
				return types.NewError("ERR min or max not valid string range item")
			}
			entries = zset.RangeByLex(min, max, minExcl, maxExcl)
			reverseEntries(entries)
		} else {
			min, minExcl, err := object.ParseLexBound(startStr)
			if err != nil {
				return types.NewError("ERR min or max not valid string range item")
			}
			max, maxExcl, err := object.ParseLexBound(stopStr)
			if err != nil {
				return types.NewError("ERR min or max not valid string range item")
			}
			entries = zset.RangeByLex(min, max, minExcl, maxExcl)
		}
	} else if byScore {
		if rev {
			// When REV + BYSCORE, the first range bound is the max and the second is the min.
			max, maxExcl := parseScoreBound(startStr, true)
			min, minExcl := parseScoreBound(stopStr, false)
			entries = zset.RangeByScore(min, max, minExcl, maxExcl)
			reverseEntries(entries)
		} else {
			min, minExcl := parseScoreBound(startStr, false)
			max, maxExcl := parseScoreBound(stopStr, true)
			entries = zset.RangeByScore(min, max, minExcl, maxExcl)
		}
	} else {
		start, err1 := strconv.Atoi(startStr)
		stop, err2 := strconv.Atoi(stopStr)
		if err1 != nil || err2 != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}

		if rev {
			// Reverse the entire set then slice by index
			all := zset.RangeByIndex(0, -1)
			reverseEntries(all)
			length := len(all)
			if start < 0 {
				start = length + start
			}
			if stop < 0 {
				stop = length + stop
			}
			if start < 0 {
				start = 0
			}
			if stop >= length {
				stop = length - 1
			}
			if start > stop || start >= length {
				entries = nil
			} else {
				entries = all[start : stop+1]
			}
		} else {
			entries = zset.RangeByIndex(start, stop)
		}
	}

	// Apply LIMIT for BYSCORE/BYLEX
	if hasLimit {
		if limitOffset < 0 || limitCount < 0 {
			entries = nil
		} else {
			if limitOffset > 0 {
				if limitOffset >= len(entries) {
					entries = nil
				} else {
					entries = entries[limitOffset:]
				}
			}
			if limitCount >= 0 && limitCount < len(entries) {
				entries = entries[:limitCount]
			}
		}
	}

	if withScores {
		// RESP3: return array of [member, score_double] pairs
		if ctx.Protocol >= 3 {
			result := make([]types.Value, 0, len(entries))
			for _, e := range entries {
				result = append(result, types.NewArray([]types.Value{
					types.NewString(e.Member),
					newDouble(e.Score),
				}))
			}
			return types.NewArray(result)
		}

		result := make([]types.Value, 0, len(entries)*2)
		for _, e := range entries {
			result = append(result, types.NewString(e.Member))
			result = append(result, types.NewString(formatScore(e.Score)))
		}
		return types.NewArray(result)
	}

	result := make([]types.Value, 0, len(entries))
	for _, e := range entries {
		result = append(result, types.NewString(e.Member))
	}
	return types.NewArray(result)
}

func parseScoreBound(s string, isMax bool) (float64, bool) {
	exclusive := false
	if strings.HasPrefix(s, "(") {
		exclusive = true
		s = s[1:]
	}

	switch strings.ToLower(s) {
	case "+inf", "inf":
		return math.Inf(1), exclusive
	case "-inf":
		return math.Inf(-1), exclusive
	default:
		val, err := strconv.ParseFloat(s, 64)
		if err != nil {
			if isMax {
				return math.Inf(1), false
			}
			return math.Inf(-1), false
		}
		return val, exclusive
	}
}

func reverseEntries(entries []object.ZSetEntry) {
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
}
