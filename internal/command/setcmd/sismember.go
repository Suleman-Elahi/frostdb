package setcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type SIsMemberCommand struct{}

func (c SIsMemberCommand) Name() string { return "SISMEMBER" }

func (c SIsMemberCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'sismember' command")
	}

	key := string(args[0].Str)
	member := string(args[1].Str)
	
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}
	
	if obj.Type != object.TypeSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	
	s := obj.Value.(*object.Set)
	if s.Contains(member) {
		return types.NewInt(1)
	}
	return types.NewInt(0)
}
