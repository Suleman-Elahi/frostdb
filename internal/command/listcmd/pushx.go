package listcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// PushXCommand implements LPUSHX/RPUSHX.
type PushXCommand struct {
	IsLeft bool
}

func (c PushXCommand) Name() string {
	if c.IsLeft {
		return "LPUSHX"
	}
	return "RPUSHX"
}

func (c PushXCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		if c.IsLeft {
			return types.NewError("ERR wrong number of arguments for 'lpushx' command")
		}
		return types.NewError("ERR wrong number of arguments for 'rpushx' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}
	if obj.Type != object.TypeList {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := obj.Value.(*object.List)
	for i := 1; i < len(args); i++ {
		if c.IsLeft {
			list.PushFront(args[i].Str)
		} else {
			list.PushBack(args[i].Str)
		}
	}

	ctx.SignalKey(key)
	return types.NewInt(int64(list.LLen()))
}
