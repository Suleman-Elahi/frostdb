package pubsub

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/encoder"
	"frostdb/internal/resp/types"
)

// PUnsubscribeCommand implements PUNSUBSCRIBE [pattern ...].
type PUnsubscribeCommand struct{}

func (c PUnsubscribeCommand) Name() string { return "PUNSUBSCRIBE" }

func (c PUnsubscribeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if ctx.PatternSubs == nil {
		ctx.PatternSubs = make(map[string]func())
	}

	patternsToUnsubscribe := make([]string, 0)

	if len(args) == 0 {
		// Unsubscribe from all patterns
		for pattern := range ctx.PatternSubs {
			patternsToUnsubscribe = append(patternsToUnsubscribe, pattern)
		}
	} else {
		for _, arg := range args {
			patternsToUnsubscribe = append(patternsToUnsubscribe, string(arg.Str))
		}
	}

	if len(args) == 0 && len(patternsToUnsubscribe) == 0 {
		reply := types.NewArray([]types.Value{
			types.NewString("punsubscribe"),
			{Type: types.TypeBulkString, IsNull: true},
			types.NewInt(int64(len(ctx.Subscriptions))),
		})
		encoder.Encode(ctx.Conn, reply)
		return types.Value{Type: types.TypeNoReply}
	}

	for _, pattern := range patternsToUnsubscribe {
		if cleanup, exists := ctx.PatternSubs[pattern]; exists {
			cleanup()
			delete(ctx.PatternSubs, pattern)
		}

		reply := types.NewArray([]types.Value{
			types.NewString("punsubscribe"),
			types.NewString(pattern),
			types.NewInt(int64(len(ctx.Subscriptions) + len(ctx.PatternSubs))),
		})
		encoder.Encode(ctx.Conn, reply)
	}

	if len(ctx.PatternSubs) == 0 && len(ctx.Subscriptions) == 0 {
		ctx.PubSubMode = false
	}

	return types.Value{Type: types.TypeNoReply}
}
