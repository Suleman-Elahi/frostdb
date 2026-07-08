package tx

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type MultiCommand struct{}

func (c MultiCommand) Name() string { return "MULTI" }

func (c MultiCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if ctx.InMulti {
		return types.NewError("ERR MULTI calls can not be nested")
	}
	ctx.InMulti = true
	ctx.TxQueue = nil
	ctx.TxErrors = false
	return types.NewSimpleString("OK")
}
