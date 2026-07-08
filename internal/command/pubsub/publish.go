package pubsub

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type PublishCommand struct{}

func (c PublishCommand) Name() string { return "PUBLISH" }

func (c PublishCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'publish' command")
	}

	channel := string(args[0].Str)
	message := string(args[1].Str)

	count := ctx.Engine.PubSubPublish(channel, message)
	return types.NewInt(count)
}
