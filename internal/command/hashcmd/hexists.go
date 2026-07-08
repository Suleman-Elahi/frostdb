package hashcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type HExistsCommand struct{}

func (c HExistsCommand) Name() string { return "HEXISTS" }

func (c HExistsCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'hexists' command")
	}

	key := string(args[0].Str)
	field := string(args[1].Str)
	
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}
	
	if obj.Type != object.TypeHash {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	
	h := obj.Value.(*object.Hash)
	ExpireIfNeeded(ctx, key, h, []string{field}, ctx.NowMs(), false, true)
	_, exists := h.Get(field)
	if exists {
		return types.NewInt(1)
	}
	return types.NewInt(0)
}
