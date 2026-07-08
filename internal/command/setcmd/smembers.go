package setcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type SMembersCommand struct{}

func (c SMembersCommand) Name() string { return "SMEMBERS" }

func (c SMembersCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'smembers' command")
	}

	key := string(args[0].Str)
	
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewArray([]types.Value{})
	}
	
	if obj.Type != object.TypeSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	
	s := obj.Value.(*object.Set)
	members := s.Members()
	
	// RESP3 check for Sets
	isResp3 := ctx.Protocol >= 3
	
	vals := make([]types.Value, 0, len(members))
	for _, member := range members {
		vals = append(vals, types.NewString(member))
	}
	
	if isResp3 {
		return types.Value{Type: types.TypeSet, Array: vals}
	}
	return types.NewArray(vals)
}
