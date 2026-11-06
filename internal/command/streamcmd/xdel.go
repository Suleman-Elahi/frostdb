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
	ids := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		idStr := string(arg.Str)
		if _, err := object.ParseStreamID(idStr); err != nil {
			return types.NewError("ERR Invalid stream ID specified as stream command argument")
		}
		ids[i] = idStr
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)
	deleted := stream.Delete(ids...)
	return types.NewInt(int64(deleted))
}
