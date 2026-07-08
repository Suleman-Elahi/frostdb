package setcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type SRemCommand struct{}

func (c SRemCommand) Name() string { return "SREM" }

func (c SRemCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'srem' command")
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

	members := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		members = append(members, string(args[i].Str))
	}

	removed := s.Remove(members...)

	if s.Len() == 0 {
		ctx.Engine.Del(key)
	}

	if removed > 0 {
		rawArgs := make([][]byte, 0, 1+len(args))
		rawArgs = append(rawArgs, []byte("SREM"))
		for _, arg := range args {
			rawArgs = append(rawArgs, arg.Str)
		}
		ctx.Engine.LogCommand(rawArgs) //nolint:errcheck // error logged internally
		ctx.SkipReplication = true
	}

	return types.NewInt(int64(removed))
}
