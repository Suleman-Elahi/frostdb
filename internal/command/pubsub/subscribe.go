package pubsub

import (
	"frostdb/internal/engine/api"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/encoder"
	"frostdb/internal/resp/types"
)

type SubscribeCommand struct{}

func (c SubscribeCommand) Name() string { return "SUBSCRIBE" }

func (c SubscribeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'subscribe' command")
	}

	if ctx.Subscriptions == nil {
		ctx.Subscriptions = make(map[string]func())
	}
	if ctx.PubSubCh == nil {
		ctx.PubSubCh = make(chan api.PubSubMessage, 256)
	}
	ctx.PubSubMode = true

	for _, arg := range args {
		channel := string(arg.Str)
		
		// If already subscribed, skip adding another subscription but we still
		// need to reply with the current number of subscriptions.
		if _, exists := ctx.Subscriptions[channel]; !exists {
			cleanup := ctx.Engine.PubSubSubscribe(channel, ctx.PubSubCh)
			ctx.Subscriptions[channel] = cleanup
		}

		// Send subscribe confirmation
		confirm := types.NewArray([]types.Value{
			types.NewString("subscribe"),
			types.NewString(channel),
			types.NewInt(int64(len(ctx.Subscriptions) + len(ctx.PatternSubs))),
		})
		encoder.Encode(ctx.Conn, confirm)
	}

	// Return NoReply since we already wrote responses directly
	return types.Value{Type: types.TypeNoReply}
}
