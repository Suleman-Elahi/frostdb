package listcmd

import (
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// LTrimCommand implements LTRIM key start stop.
type LTrimCommand struct{}

func (c LTrimCommand) Name() string { return "LTRIM" }

func (c LTrimCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'ltrim' command")
	}

	key := string(args[0].Str)
	start, err := strconv.Atoi(string(args[1].Str))
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(string(args[2].Str))
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespOK
	}
	if obj.Type != object.TypeList {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := obj.Value.(*object.List)
	if list.Trim(start, stop) == 0 {
		ctx.Engine.Del(key)
	}

	return types.RespOK
}
