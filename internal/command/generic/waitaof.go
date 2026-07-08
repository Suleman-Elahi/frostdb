package generic

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type WaitAofCommand struct{}

func (c WaitAofCommand) Name() string { return "WAITAOF" }

func (c WaitAofCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'waitaof' command")
	}

	return types.NewArray([]types.Value{
		types.RespZero,
		types.RespZero,
	})
}
