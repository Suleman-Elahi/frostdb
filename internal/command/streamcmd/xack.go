package streamcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type XAckCommand struct{}

func (c XAckCommand) Name() string { return "XACK" }

func (c XAckCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'xack' command")
	}

	key := string(args[0].Str)
	groupName := string(args[1].Str)

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)
	ids := make([]string, 0, len(args)-2)
	for _, arg := range args[2:] {
		ids = append(ids, string(arg.Str))
	}

	return types.NewInt(stream.Ack(groupName, ids...))
}

