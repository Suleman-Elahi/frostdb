package zsetcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ZRevRangeCommand struct{}

func (c ZRevRangeCommand) Name() string { return "ZREVRANGE" }

func (c ZRevRangeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'zrevrange' command")
	}

	key := string(args[0].Str)
	startStr := string(args[1].Str)
	stopStr := string(args[2].Str)

	withScores := false
	for i := 3; i < len(args); i++ {
		if strings.ToUpper(string(args[i].Str)) == "WITHSCORES" {
			withScores = true
		} else {
			return types.NewError("ERR syntax error")
		}
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewArray([]types.Value{})
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)

	start, err1 := strconv.Atoi(startStr)
	stop, err2 := strconv.Atoi(stopStr)
	if err1 != nil || err2 != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

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
		return types.NewArray([]types.Value{})
	}
	entries := all[start : stop+1]

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
