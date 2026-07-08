package setcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// SMisMemberCommand implements SMISMEMBER key member [member ...].
type SMisMemberCommand struct{}

func (c SMisMemberCommand) Name() string { return "SMISMEMBER" }

func (c SMisMemberCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'smismember' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)

	result := make([]types.Value, len(args)-1)

	if !ok {
		for i := range result {
			result[i] = types.RespZero
		}
		return types.NewArray(result)
	}

	if obj.Type != object.TypeSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	s := obj.Value.(*object.Set)
	for i, arg := range args[1:] {
		if s.Contains(string(arg.Str)) {
			result[i] = types.RespOne
		} else {
			result[i] = types.RespZero
		}
	}
	return types.NewArray(result)
}
