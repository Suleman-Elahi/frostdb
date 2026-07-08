package pubsub

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/encoder"
	"frostdb/internal/resp/types"
)

type UnsubscribeCommand struct{}

func (c UnsubscribeCommand) Name() string { return "UNSUBSCRIBE" }

func (c UnsubscribeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if ctx.Subscriptions == nil {
		ctx.Subscriptions = make(map[string]func())
	}

	channelsToUnsubscribe := make([]string, 0)

	if len(args) == 0 {
		// Unsubscribe from all
		for channel := range ctx.Subscriptions {
			channelsToUnsubscribe = append(channelsToUnsubscribe, channel)
		}
	} else {
		for _, arg := range args {
			channelsToUnsubscribe = append(channelsToUnsubscribe, string(arg.Str))
		}
	}

	// If no channels to unsubscribe from, and no arguments were provided,
	// Redis replies with a single unsubscribe message with null channel.
	if len(args) == 0 && len(channelsToUnsubscribe) == 0 {
		reply := types.NewArray([]types.Value{
			types.NewString("unsubscribe"),
			{Type: types.TypeBulkString, IsNull: true},
			types.NewInt(int64(len(ctx.PatternSubs))),
		})
		encoder.Encode(ctx.Conn, reply)
		return types.Value{Type: types.TypeNoReply}
	}

	for _, channel := range channelsToUnsubscribe {
		if cleanup, exists := ctx.Subscriptions[channel]; exists {
			cleanup()
			delete(ctx.Subscriptions, channel)
		}

		reply := types.NewArray([]types.Value{
			types.NewString("unsubscribe"),
			types.NewString(channel),
			types.NewInt(int64(len(ctx.Subscriptions) + len(ctx.PatternSubs))),
		})
		encoder.Encode(ctx.Conn, reply)
	}

	if len(ctx.Subscriptions) == 0 && len(ctx.PatternSubs) == 0 {
		ctx.PubSubMode = false
	}

	return types.Value{Type: types.TypeNoReply}
}
