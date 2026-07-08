package hashcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type HGetCommand struct{}

func (c HGetCommand) Name() string { return "HGET" }

func (c HGetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'hget' command")
	}

	key := string(args[0].Str)
	field := string(args[1].Str)
	
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespNil
	}
	
	if obj.Type != object.TypeHash {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	h := obj.Value.(*object.Hash)
	
	// Lazy expire before reading
	ExpireIfNeeded(ctx, key, h, []string{field}, ctx.NowMs(), false, true)

	val, exists := h.Get(field)
	if !exists {
		return types.RespNil
	}
	
	return types.NewString(val)
}
