package str

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// MGetCommand implements MGET key [key ...]
type MGetCommand struct{}

func (c MGetCommand) Name() string { return "MGET" }

func (c MGetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'mget' command")
	}

	result := make([]types.Value, len(args))
	for i, arg := range args {
		obj, ok := ctx.Engine.GetBytes(arg.Str)
		if !ok {
			result[i] = types.RespNil
			continue
		}
		strVal, isStr := obj.Value.(string)
		if !isStr {
			result[i] = types.RespNil
			continue
		}
		result[i] = types.NewString(strVal)
	}
	return types.NewArray(result)
}
