package pubsub

import (
	"frostdb/internal/engine/api"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/encoder"
	"frostdb/internal/resp/types"
	"frostdb/internal/stats"
)

func checkPubSubOOM(ctx *context.Context, cmdName string, args []types.Value) bool {
	if ctx.AllowOOM {
		return false
	}
	oomEng, ok := ctx.Engine.(api.PubSubOOMEngine)
	if !ok {
		return false
	}
	var argBytes int64 = 1024
	for _, arg := range args {
		argBytes += int64(len(arg.Str))
	}
	if oomEng.CheckPubSubOOM(argBytes) {
		stats.Get().RecordRejectedCommand(cmdName)
		return true
	}
	return false
}

// SSubscribeCommand implements SSUBSCRIBE shardchannel [shardchannel ...].
type SSubscribeCommand struct{}

func (c SSubscribeCommand) Name() string { return "SSUBSCRIBE" }

func (c SSubscribeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'ssubscribe' command")
	}
	if checkPubSubOOM(ctx, "SSUBSCRIBE", args) {
		return types.NewError("OOM command not allowed when used memory > 'maxmemory'.")
	}

	if ctx.ShardSubs == nil {
		ctx.ShardSubs = make(map[string]func())
	}
	if ctx.PubSubCh == nil {
		ctx.PubSubCh = make(chan api.PubSubMessage, 256)
	}
	ctx.PubSubMode = true

	var lastConfirm types.Value
	for _, arg := range args {
		channel := string(arg.Str)
		if _, exists := ctx.ShardSubs[channel]; !exists {
			cleanup := ctx.Engine.PubSubSubscribe("__shard:"+channel, ctx.PubSubCh)
			ctx.ShardSubs[channel] = cleanup
		}

		confirm := types.NewArray([]types.Value{
			types.NewString("ssubscribe"),
			types.NewString(channel),
			types.NewInt(int64(len(ctx.ShardSubs))),
		})
		lastConfirm = confirm
		if !ctx.ExecutingMulti || len(args) > 1 {
			encoder.Encode(ctx.Conn, confirm)
		}
	}

	if ctx.ExecutingMulti && len(args) == 1 {
		return lastConfirm
	}
	return types.Value{Type: types.TypeNoReply}
}

// SUnsubscribeCommand implements SUNSUBSCRIBE [shardchannel ...].
type SUnsubscribeCommand struct{}

func (c SUnsubscribeCommand) Name() string { return "SUNSUBSCRIBE" }

func (c SUnsubscribeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if ctx.ShardSubs == nil {
		ctx.ShardSubs = make(map[string]func())
	}

	channelsToUnsubscribe := make([]string, 0)
	if len(args) == 0 {
		for channel := range ctx.ShardSubs {
			channelsToUnsubscribe = append(channelsToUnsubscribe, channel)
		}
	} else {
		for _, arg := range args {
			channelsToUnsubscribe = append(channelsToUnsubscribe, string(arg.Str))
		}
	}

	if len(args) == 0 && len(channelsToUnsubscribe) == 0 {
		reply := types.NewArray([]types.Value{
			types.NewString("sunsubscribe"),
			{Type: types.TypeBulkString, IsNull: true},
			types.NewInt(0),
		})
		if ctx.ExecutingMulti {
			return reply
		}
		encoder.Encode(ctx.Conn, reply)
		return types.Value{Type: types.TypeNoReply}
	}

	var lastReply types.Value
	for _, channel := range channelsToUnsubscribe {
		if cleanup, exists := ctx.ShardSubs[channel]; exists {
			cleanup()
			delete(ctx.ShardSubs, channel)
		}

		reply := types.NewArray([]types.Value{
			types.NewString("sunsubscribe"),
			types.NewString(channel),
			types.NewInt(int64(len(ctx.ShardSubs))),
		})
		lastReply = reply
		if !ctx.ExecutingMulti || len(channelsToUnsubscribe) > 1 {
			encoder.Encode(ctx.Conn, reply)
		}
	}

	if len(ctx.Subscriptions) == 0 && len(ctx.PatternSubs) == 0 && len(ctx.ShardSubs) == 0 {
		ctx.PubSubMode = false
	}

	if ctx.ExecutingMulti && len(channelsToUnsubscribe) == 1 {
		return lastReply
	}
	return types.Value{Type: types.TypeNoReply}
}

// SPublishCommand implements SPUBLISH shardchannel message.
type SPublishCommand struct{}

func (c SPublishCommand) Name() string { return "SPUBLISH" }

func (c SPublishCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'spublish' command")
	}

	channel := string(args[0].Str)
	message := string(args[1].Str)

	count := ctx.Engine.PubSubPublish("__shard:"+channel, message)
	return types.NewInt(count)
}
