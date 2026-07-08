package zsetcmd

import (
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ZRemRangeByRankCommand struct{}

func (c ZRemRangeByRankCommand) Name() string { return "ZREMRANGEBYRANK" }

func (c ZRemRangeByRankCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'zremrangebyrank' command")
	}

	key := string(args[0].Str)
	start, err1 := strconv.Atoi(string(args[1].Str))
	stop, err2 := strconv.Atoi(string(args[2].Str))
	if err1 != nil || err2 != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	removed := zset.RemRangeByRank(start, stop)

	if zset.Len() == 0 {
		ctx.Engine.Del(key)
	}

	return types.NewInt(int64(removed))
}

type ZRemRangeByLexCommand struct{}

func (c ZRemRangeByLexCommand) Name() string { return "ZREMRANGEBYLEX" }

func (c ZRemRangeByLexCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'zremrangebylex' command")
	}

	key := string(args[0].Str)
	minStr := string(args[1].Str)
	maxStr := string(args[2].Str)

	if !validLexBound(minStr) || !validLexBound(maxStr) {
		return types.NewError("ERR min or max not valid string range item")
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	min, minExcl, _ := object.ParseLexBound(minStr)
	max, maxExcl, _ := object.ParseLexBound(maxStr)

	removed := zset.RemRangeByLex(min, max, minExcl, maxExcl)

	if zset.Len() == 0 {
		ctx.Engine.Del(key)
	}

	return types.NewInt(int64(removed))
}
