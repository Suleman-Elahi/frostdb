package tx

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type DiscardCommand struct{}

func (c DiscardCommand) Name() string { return "DISCARD" }

func (c DiscardCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if !ctx.InMulti {
		return types.NewError("ERR DISCARD without MULTI")
	}
	defer GlobalWatchManager.Unwatch(ctx)
	ctx.InMulti = false
	ctx.TxQueue = nil
	ctx.TxErrors = false
	ctx.WatchAborted = false
	return types.NewSimpleString("OK")
}
