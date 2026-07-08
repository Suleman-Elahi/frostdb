package hashcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type HLenCommand struct{}

func (c HLenCommand) Name() string { return "HLEN" }

func (c HLenCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'hlen' command")
	}

	key := string(args[0].Str)
	
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespZero
	}
	
	if obj.Type != object.TypeHash {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	
	h := obj.Value.(*object.Hash)
	ExpireIfNeeded(ctx, key, h, nil, ctx.NowMs(), false, true)
	
	return types.NewInt(int64(h.Len()))
}
