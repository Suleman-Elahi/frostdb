package zsetcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ZMScoreCommand implements ZMSCORE key member [member ...]
type ZMScoreCommand struct{}

func (c ZMScoreCommand) Name() string { return "ZMSCORE" }

func (c ZMScoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'zmscore' command")
	}

	key := string(args[0].Str)

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		// Return array of nils for each requested member
		result := make([]types.Value, len(args)-1)
		for i := range result {
			result[i] = types.RespNil
		}
		return types.NewArray(result)
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	result := make([]types.Value, 0, len(args)-1)

	for _, a := range args[1:] {
		member := string(a.Str)
		score, exists := zset.Score(member)
		if exists {
			result = append(result, types.NewString(formatScore(score)))
		} else {
			result = append(result, types.RespNil)
		}
	}

	return types.NewArray(result)
}
