package generic

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type PersistCommand struct{}

func (c PersistCommand) Name() string { return "PERSIST" }

func (c PersistCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'persist' command")
	}

	key := string(args[0].Str)
	
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}

	if obj.ExpireAt == 0 {
		return types.NewInt(0)
	}

	obj.ExpireAt = 0
	
	// WAL logging
	ctx.Engine.LogCommand([][]byte{
		[]byte("PERSIST"),
		args[0].Str,
	}) //nolint:errcheck // error logged internally
	ctx.SkipReplication = true

	return types.NewInt(1)
}
