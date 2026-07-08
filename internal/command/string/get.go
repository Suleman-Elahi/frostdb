package str

import (
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

	// For Phase 1, assume everything is a string that we can cast
	if strVal, ok := obj.Value.(string); ok {
		return types.NewString(strVal)
	}

	return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
}
