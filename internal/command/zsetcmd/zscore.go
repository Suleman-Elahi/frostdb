package zsetcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ZScoreCommand implements ZSCORE key member.
type ZScoreCommand struct{}

func (c ZScoreCommand) Name() string { return "ZSCORE" }

func (c ZScoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'zscore' command")
	}

	key := string(args[0].Str)
	member := string(args[1].Str)

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespNil
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	z := obj.Value.(*object.ZSet)
	score, exists := z.Score(member)
	if !exists {
		return types.RespNil
	}

	return types.NewString(formatScore(score))
}
