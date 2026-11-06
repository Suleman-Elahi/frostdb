package zsetcmd

import (
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ZPopMinCommand struct{}

func (c ZPopMinCommand) Name() string { return "ZPOPMIN" }

func (c ZPopMinCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'zpopmin' command")
	}

	key := string(args[0].Str)
	count := 1

	if len(args) >= 2 {
		n, err := strconv.Atoi(string(args[1].Str))
		if err != nil || n < 0 {
			return types.NewError("ERR value is out of range, must be positive")
		}
		count = n
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		ctx.SkipReplication = true
		return types.NewArray([]types.Value{})
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	entries := zset.PopMin(count)
	if len(entries) == 0 {
		ctx.SkipReplication = true
		return types.NewArray([]types.Value{})
	}

	if zset.Len() == 0 {
		ctx.Engine.Del(key)
		ctx.ReplicateAs = [][]byte{[]byte("DEL"), []byte(key)}
	}

	if ctx.Protocol >= 3 {
		if len(args) < 2 {
			// No COUNT argument: flat array [member, double]
			result := make([]types.Value, 0, len(entries)*2)
			for _, e := range entries {
				result = append(result, types.NewString(e.Member))
				result = append(result, newDouble(e.Score))
			}
			return types.NewArray(result)
		} else {
			// COUNT argument present: array of arrays [[member, double], ...]
			result := make([]types.Value, 0, len(entries))
			for _, e := range entries {
				result = append(result, types.NewArray([]types.Value{
					types.NewString(e.Member),
					newDouble(e.Score),
				}))
			}
			return types.NewArray(result)
		}
	}

	result := make([]types.Value, 0, len(entries)*2)
	for _, e := range entries {
		result = append(result, types.NewString(e.Member))
		result = append(result, types.NewString(formatScore(e.Score)))
	}
	return types.NewArray(result)
}
