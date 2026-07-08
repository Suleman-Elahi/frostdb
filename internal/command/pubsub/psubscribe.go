package pubsub

import (
	"frostdb/internal/engine/api"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/encoder"
	"frostdb/internal/resp/types"
)

// PSubscribeCommand implements PSUBSCRIBE pattern [pattern ...].
// Uses glob-style pattern matching on channel names.
type PSubscribeCommand struct{}

func (c PSubscribeCommand) Name() string { return "PSUBSCRIBE" }

func (c PSubscribeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'psubscribe' command")
	}

	if ctx.Subscriptions == nil {
		ctx.Subscriptions = make(map[string]func())
	}
	if ctx.PatternSubs == nil {
		ctx.PatternSubs = make(map[string]func())
	}
	if ctx.PubSubCh == nil {
		ctx.PubSubCh = make(chan api.PubSubMessage, 256)
	}
	ctx.PubSubMode = true

	for _, arg := range args {
		pattern := string(arg.Str)
		
		if _, exists := ctx.PatternSubs[pattern]; !exists {
			cleanup := ctx.Engine.PubSubSubscribe("__pattern:"+pattern, ctx.PubSubCh)
			ctx.PatternSubs[pattern] = cleanup
		}

		// Send psubscribe confirmation
		total := int64(len(ctx.Subscriptions) + len(ctx.PatternSubs))
		confirm := types.NewArray([]types.Value{
			types.NewString("psubscribe"),
			types.NewString(pattern),
			types.NewInt(total),
		})
		encoder.Encode(ctx.Conn, confirm)
	}

	return types.Value{Type: types.TypeNoReply}
}
