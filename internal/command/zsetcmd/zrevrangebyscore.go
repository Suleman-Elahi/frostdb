package zsetcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ZRevRangeByScoreCommand struct{}

func (c ZRevRangeByScoreCommand) Name() string { return "ZREVRANGEBYSCORE" }

func (c ZRevRangeByScoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'zrevrangebyscore' command")
	}

	key := string(args[0].Str)
	maxStr := string(args[1].Str)
	minStr := string(args[2].Str)

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
	reverseEntries(entries)

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
