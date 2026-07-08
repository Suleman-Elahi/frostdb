package zsetcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ZRemCommand struct{}

func (c ZRemCommand) Name() string { return "ZREM" }

func (c ZRemCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'zrem' command")
	}

	key := string(args[0].Str)

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}
	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	removed := 0

	for i := 1; i < len(args); i++ {
		member := string(args[i].Str)
		if zset.Remove(member) {
			removed++
		}
	}

	if zset.Len() == 0 {
		ctx.Engine.Del(key)
	}

	if removed > 0 {
		rawArgs := make([][]byte, 0, 1+len(args))
		rawArgs = append(rawArgs, []byte("ZREM"))
		for _, arg := range args {
			rawArgs = append(rawArgs, arg.Str)
		}
		ctx.Engine.LogCommand(rawArgs) //nolint:errcheck // error logged internally
		ctx.SkipReplication = true
	}

	return types.NewInt(int64(removed))
}
