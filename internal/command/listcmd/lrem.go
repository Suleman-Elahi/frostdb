package listcmd

import (
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type LRemCommand struct{}

func (c LRemCommand) Name() string { return "LREM" }

func (c LRemCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'lrem' command")
	}

	key := string(args[0].Str)
	countStr := string(args[1].Str)
	element := args[2].Str

	count, err := strconv.Atoi(countStr)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}

	if obj.Type != object.TypeList {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := obj.Value.(*object.List)
	removed := list.Remove(element, count)

	if list.LLen() == 0 {
		ctx.Engine.Del(key)
	}

	if removed > 0 {
		rawArgs := make([][]byte, 0, 1+len(args))
		rawArgs = append(rawArgs, []byte("LREM"))
		for _, arg := range args {
			rawArgs = append(rawArgs, arg.Str)
		}
		ctx.Engine.LogCommand(rawArgs) //nolint:errcheck // error logged internally
		ctx.SkipReplication = true
	}

	return types.NewInt(int64(removed))
}
