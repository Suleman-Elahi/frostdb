package hashcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// HStrLenCommand implements HSTRLEN key field.
type HStrLenCommand struct{}

func (c HStrLenCommand) Name() string { return "HSTRLEN" }

func (c HStrLenCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'hstrlen' command")
	}

	key := string(args[0].Str)
	field := string(args[1].Str)

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespZero
	}
	if obj.Type != object.TypeHash {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	h := obj.Value.(*object.Hash)
	ExpireIfNeeded(ctx, key, h, []string{field}, ctx.NowMs(), false, true)
	v, ok := h.Get(field)
	if !ok {
		return types.RespZero
	}
	return types.NewInt(int64(len(v)))
}
