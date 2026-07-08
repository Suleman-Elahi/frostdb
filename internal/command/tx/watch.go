package tx

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type WatchCommand struct{}

func (c WatchCommand) Name() string { return "WATCH" }

func (c WatchCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'watch' command")
	}
	if ctx.WatchedKeys == nil {
		ctx.WatchedKeys = make(map[string]struct{})
	}

	for _, arg := range args {
		key := string(arg.Str)
		if _, ok := ctx.WatchedKeys[key]; !ok {
			ctx.WatchedKeys[key] = struct{}{}
			GlobalWatchManager.Watch(ctx, key)
		}
	}

	return types.RespOK
}

type UnwatchCommand struct{}

func (c UnwatchCommand) Name() string { return "UNWATCH" }

func (c UnwatchCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	GlobalWatchManager.Unwatch(ctx)
	// Clear the WatchAborted flag - if the client explicitly unwatches,
	// any previous aborted state should be cleared so future transactions work.
	ctx.WatchAborted = false
	return types.RespOK
}
