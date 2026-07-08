package streamcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// XDelCommand implements XDEL key id [id ...].
type XDelCommand struct{}

func (c XDelCommand) Name() string { return "XDEL" }

func (c XDelCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'xdel' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)
	ids := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		ids[i] = string(arg.Str)
	}

	deleted := stream.Delete(ids...)
	return types.NewInt(int64(deleted))
}
