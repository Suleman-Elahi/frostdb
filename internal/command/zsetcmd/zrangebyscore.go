package zsetcmd

import (
	"math"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ZRangeByScoreCommand struct{}

func (c ZRangeByScoreCommand) Name() string { return "ZRANGEBYSCORE" }

func (c ZRangeByScoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'zrangebyscore' command")
	}

	key := string(args[0].Str)
	minStr := string(args[1].Str)
	maxStr := string(args[2].Str)

	min, minExcl, minErr := parseScoreBoundStrict(minStr)
	if minErr != nil {
		return types.NewError("ERR min or max is not a float")
	}
	max, maxExcl, maxErr := parseScoreBoundStrict(maxStr)
	if maxErr != nil {
		return types.NewError("ERR min or max is not a float")
	}

	limitSet := false
	withScores := false
	offset := 0
	count := -1

	for i := 3; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i].Str))
		switch opt {
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			var err error
			offset, err = strconv.Atoi(string(args[i+1].Str))
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			count, err = strconv.Atoi(string(args[i+2].Str))
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			limitSet = true
			i += 2
		default:
			return types.NewError("ERR syntax error")
		}
	}

	if limitSet && (offset < 0 || count < 0) {
		return types.NewArray([]types.Value{})
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewArray([]types.Value{})
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	entries := zset.RangeByScore(min, max, minExcl, maxExcl)

	if offset > 0 {
		if offset >= len(entries) {
			entries = nil
		} else {
			entries = entries[offset:]
		}
	}
	if count >= 0 && count < len(entries) {
		entries = entries[:count]
	}

	if withScores {
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

func parseScoreBoundStrict(s string) (float64, bool, error) {
	exclusive := false
	if strings.HasPrefix(s, "(") {
		exclusive = true
		s = s[1:]
	}
	switch strings.ToLower(s) {
	case "+inf", "inf":
		return math.Inf(1), exclusive, nil
	case "-inf":
		return math.Inf(-1), exclusive, nil
	default:
		val, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsNaN(val) {
			return 0, false, &floatParseError{}
		}
		return val, exclusive, nil
	}
}
