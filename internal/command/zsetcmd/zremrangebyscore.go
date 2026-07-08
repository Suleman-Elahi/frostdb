package zsetcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ZRemRangeByScoreCommand struct{}

func (c ZRemRangeByScoreCommand) Name() string { return "ZREMRANGEBYSCORE" }

func (c ZRemRangeByScoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'zremrangebyscore' command")
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

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}
	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	removed := zset.RemRangeByScore(min, max, minExcl, maxExcl)

	if zset.Len() == 0 {
		ctx.Engine.Del(key)
	}

	if removed > 0 {
		rawArgs := make([][]byte, 0, 1+len(args))
		rawArgs = append(rawArgs, []byte("ZREMRANGEBYSCORE"))
		for _, arg := range args {
			rawArgs = append(rawArgs, arg.Str)
		}
		ctx.Engine.LogCommand(rawArgs) //nolint:errcheck
		ctx.SkipReplication = true
	}

	return types.NewInt(int64(removed))
}
