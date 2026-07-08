package zsetcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ZCountCommand implements ZCOUNT key min max.
type ZCountCommand struct{}

func (c ZCountCommand) Name() string { return "ZCOUNT" }

func (c ZCountCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'zcount' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	min, minExcl := parseScoreBound(string(args[1].Str), false)
	max, maxExcl := parseScoreBound(string(args[2].Str), true)

	z := obj.Value.(*object.ZSet)
	entries := z.RangeByScore(min, max, minExcl, maxExcl)
	return types.NewInt(int64(len(entries)))
}
