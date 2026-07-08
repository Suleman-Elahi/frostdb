package setcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type SCardCommand struct{}

func (c SCardCommand) Name() string { return "SCARD" }

func (c SCardCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'scard' command")
	}

	key := string(args[0].Str)
	
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}
	
	if obj.Type != object.TypeSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	
	s := obj.Value.(*object.Set)
	return types.NewInt(int64(s.Len()))
}
