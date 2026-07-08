package hashcmd

import (
	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// HSetNXCommand implements HSETNX key field value
type HSetNXCommand struct{}

func (c HSetNXCommand) Name() string { return "HSETNX" }

func (c HSetNXCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'hsetnx' command")
	}

	key := string(args[0].Str)
	field := string(args[1].Str)
	value := string(args[2].Str)

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		h := object.NewHash()
		h.Set(field, value)
		ctx.Engine.Set(key, &object.Object{Type: object.TypeHash, Value: h})
		ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hset", key, []string{field})
		return types.RespOne
	}

	h, ok := obj.Value.(*object.Hash)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	ExpireIfNeeded(ctx, key, h, []string{field}, ctx.NowMs(), false, true)

	if _, exists := h.Get(field); exists {
		return types.RespZero
	}

	h.Set(field, value)
	ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hset", key, []string{field})
	return types.RespOne
}
