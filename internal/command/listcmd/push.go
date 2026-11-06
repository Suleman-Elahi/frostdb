package listcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type PushCommand struct {
	IsLeft bool
}

func (c PushCommand) Name() string {
	if c.IsLeft {
		return "LPUSH"
	}
	return "RPUSH"
}

func (c PushCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for '" + c.Name() + "' command")
	}

	key := string(args[0].Str)
	
	obj, ok := ctx.Engine.Get(key)
	var list *object.List

	if ok {
		if obj.Type != object.TypeList {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		list = obj.Value.(*object.List)
	} else {
		list = object.NewList()
		obj = &object.Object{
			Type:     object.TypeList,
			Encoding: object.EncodingRaw,
			Value:    list,
		}
		ctx.Engine.Set(key, obj)
	}

	for i := 1; i < len(args); i++ {
		if c.IsLeft {
			list.PushFront(args[i].Str)
		} else {
			list.PushBack(args[i].Str)
		}
	}

	// Wake up any blocking clients after command lock release / EXEC
	ctx.SignalKey(key)

	return types.NewInt(int64(list.LLen()))
}
