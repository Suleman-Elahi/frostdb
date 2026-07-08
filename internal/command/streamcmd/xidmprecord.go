package streamcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type XIDMPRecordCommand struct{}

func (c XIDMPRecordCommand) Name() string { return "XIDMPRECORD" }

func (c XIDMPRecordCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 4 {
		return types.NewError("ERR wrong number of arguments for 'xidmprecord' command")
	}

	key := string(args[0].Str)
	producer := string(args[1].Str)
	iid := string(args[2].Str)
	id := string(args[3].Str)

	if producer == "" {
		return types.NewError("ERR producer ID must be non-empty")
	}
	if iid == "" {
		return types.NewError("ERR idempotent ID must be non-empty")
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewError("ERR no such key")
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)
	if err := stream.RecordIDMP(producer, iid, id); err != nil {
		return types.NewError(err.Error())
	}
	return types.RespOK
}

