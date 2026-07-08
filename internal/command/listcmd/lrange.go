package listcmd

import (
	"strconv"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type LRangeCommand struct{}

func (c LRangeCommand) Name() string { return "LRANGE" }

func (c LRangeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'lrange' command")
	}

	key := string(args[0].Str)
	startStr := string(args[1].Str)
	endStr := string(args[2].Str)

	start, err := strconv.Atoi(startStr)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	end, err := strconv.Atoi(endStr)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewArray([]types.Value{})
	}

	if obj.Type != object.TypeList {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := obj.Value.(*object.List)
	
	values := list.Range(start, end)
	result := make([]types.Value, len(values))
	for i, v := range values {
		result[i] = types.NewString(string(v))
	}
	return types.NewArray(result)
}
