package generic

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ResetCommand implements RESET — resets connection state (Redis 6.2+).
type ResetCommand struct{}

func (c ResetCommand) Name() string { return "RESET" }

func (c ResetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	// Reset connection state
	ctx.Protocol = 2
	ctx.ClientName = ""
	ctx.InMulti = false
	ctx.TxQueue = nil
	ctx.TxErrors = false
	ctx.Subscriptions = nil
	ctx.PubSubMode = false

	return types.NewSimpleString("RESET")
}
