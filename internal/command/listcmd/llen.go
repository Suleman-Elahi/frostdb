package listcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// LLenCommand implements LLEN key
type LLenCommand struct{}

func (c LLenCommand) Name() string { return "LLEN" }

func (c LLenCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'llen' command")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		return types.RespZero
	}

	l, ok := obj.Value.(*object.List)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return types.NewInt(int64(l.LLen()))
}
