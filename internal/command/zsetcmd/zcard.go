package zsetcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ZCardCommand implements ZCARD key.
type ZCardCommand struct{}

func (c ZCardCommand) Name() string { return "ZCARD" }

func (c ZCardCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'zcard' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	z := obj.Value.(*object.ZSet)
	return types.NewInt(int64(z.Len()))
}
