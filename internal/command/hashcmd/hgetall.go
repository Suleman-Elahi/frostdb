package hashcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type HGetAllCommand struct{}

func (c HGetAllCommand) Name() string { return "HGETALL" }

func (c HGetAllCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'hgetall' command")
	}

	key := string(args[0].Str)
	
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespEmptyArr
	}
	
	if obj.Type != object.TypeHash {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	
	h := obj.Value.(*object.Hash)
	ExpireIfNeeded(ctx, key, h, nil, ctx.NowMs(), false, true)
	keys := h.Keys()
	
	result := make([]types.Value, 0, len(keys)*2)
	for _, field := range keys {
		val, _ := h.Get(field)
		result = append(result, types.NewString(field))
		result = append(result, types.NewString(val))
	}
	
	if ctx.Protocol >= 3 {
		return types.Value{
			Type:  types.TypeMap,
			Array: result,
		}
	}
	
	return types.NewArray(result)
}
