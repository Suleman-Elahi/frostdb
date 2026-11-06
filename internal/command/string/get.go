package str

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type GetCommand struct{}

func (c GetCommand) Name() string {
	return "GET"
}

func (c GetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'get' command")
	}

	keyBytes := args[0].Str
	obj, ok := ctx.Engine.GetBytes(keyBytes)
	if !ok {
		return types.RespNil
	}

	if b, ok := object.StringBytes(obj); ok {
		return types.NewBytes(b)
	}

	return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
}
