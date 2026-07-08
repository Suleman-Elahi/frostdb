package str

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// MSetCommand implements MSET key value [key value ...]
type MSetCommand struct{}

func (c MSetCommand) Name() string { return "MSET" }

func (c MSetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return types.NewError("ERR wrong number of arguments for 'mset' command")
	}

	for i := 0; i < len(args); i += 2 {
		key := string(args[i].Str)
		val := string(args[i+1].Str)
		ctx.Engine.Set(key, object.AcquireStringObject(val))
	}
	return types.RespOK
}

// MSetNXCommand implements MSETNX key value [key value ...]
type MSetNXCommand struct{}

func (c MSetNXCommand) Name() string { return "MSETNX" }

func (c MSetNXCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return types.NewError("ERR wrong number of arguments for 'msetnx' command")
	}

	// Check all keys first — atomic: set all or none
	for i := 0; i < len(args); i += 2 {
		if _, ok := ctx.Engine.GetBytes(args[i].Str); ok {
			return types.RespZero
		}
	}

	for i := 0; i < len(args); i += 2 {
		key := string(args[i].Str)
		val := string(args[i+1].Str)
		ctx.Engine.Set(key, object.AcquireStringObject(val))
	}
	return types.RespOne
}
