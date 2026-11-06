package tx

import (
	"frostdb/internal/command/registry"
	"frostdb/internal/engine/api"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ExecCommand struct{}

func (c ExecCommand) Name() string { return "EXEC" }

func (c ExecCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if !ctx.InMulti {
		return types.NewError("ERR EXEC without MULTI")
	}
	defer GlobalWatchManager.Unwatch(ctx)

	if ctx.WatchAborted {
		ctx.InMulti = false
		ctx.TxQueue = nil
		ctx.TxErrors = false
		ctx.WatchAborted = false
		if ctx.Protocol >= 3 {
			return types.NewNull()
		}
		return types.Value{Type: types.TypeArray, IsNull: true}
	}

	if ctx.TxErrors {
		ctx.InMulti = false
		ctx.TxQueue = nil
		ctx.TxErrors = false
		return types.NewError("EXECABORT Transaction discarded because of previous errors.")
	}

	// Save and clear multi state so dispatched commands execute normally
	queue := ctx.TxQueue
	ctx.InMulti = false
	ctx.TxQueue = nil
	ctx.TxErrors = false
	ctx.WatchAborted = false
	ctx.ExecutingMulti = true

	// Propagate MULTI to replicas
	ctx.Engine.PropagateToReplicas([][]byte{[]byte("MULTI")})

	results := make([]types.Value, 0, len(queue))
	for _, cmdArgs := range queue {
		result := registry.Dispatch(ctx, cmdArgs)
		results = append(results, result)
	}
	ctx.ExecutingMulti = false

	// Propagate EXEC to replicas
	ctx.Engine.PropagateToReplicas([][]byte{[]byte("EXEC")})

	if ctx.UnlockKeys != nil {
		ctx.UnlockKeys()
	}

	// Flush deferred signals: wake blocked clients now that the
	// transaction has fully applied (including expiry, deletes, etc).
	if signals := ctx.TakeSignals(); len(signals) > 0 {
		if be, ok := ctx.Engine.(api.BlockingEngine); ok {
			be.FlushSignals(signals)
		} else {
			for _, key := range signals {
				ctx.Engine.SignalKey(key)
			}
		}
	}

	return types.NewArray(results)
}
