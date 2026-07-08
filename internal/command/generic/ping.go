package generic

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type PingCommand struct{}

func (c PingCommand) Name() string {
	return "PING"
}

func (c PingCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) > 1 {
		return types.NewError("ERR wrong number of arguments for 'ping' command")
	}

	if ctx.PubSubMode && ctx.Protocol < 3 {
		var arg types.Value
		if len(args) == 0 {
			arg = types.NewString("")
		} else {
			arg = types.NewString(string(args[0].Str))
		}
		return types.NewArray([]types.Value{
			types.NewString("pong"),
			arg,
		})
	}

	if len(args) == 0 {
		return types.NewSimpleString("PONG")
	}
	return types.NewString(string(args[0].Str))
}
