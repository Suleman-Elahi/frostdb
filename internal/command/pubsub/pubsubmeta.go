package pubsub

import (
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// PubSubMetaCommand implements PUBSUB subcommands used by test clients.
type PubSubMetaCommand struct{}

func (c PubSubMetaCommand) Name() string { return "PUBSUB" }

func (c PubSubMetaCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewError("ERR wrong number of arguments for 'pubsub' command")
	}

	switch strings.ToUpper(string(args[0].Str)) {
	case "CHANNELS":
		return types.RespEmptyArr
	case "NUMSUB":
		channels := make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			channels = append(channels, string(args[i].Str))
		}
		counts := ctx.Engine.PubSubNumSub(channels)
		
		out := make([]types.Value, 0, len(channels)*2)
		for i, ch := range channels {
			out = append(out, types.NewString(ch), types.NewInt(counts[i]))
		}
		return types.NewArray(out)
	case "NUMPAT":
		return types.NewInt(ctx.Engine.PubSubNumPat())
	case "SHARDCHANNELS":
		return types.RespEmptyArr
	case "SHARDNUMSUB":
		out := make([]types.Value, 0, (len(args)-1)*2)
		for i := 1; i < len(args); i++ {
			out = append(out, types.NewString(string(args[i].Str)), types.RespZero)
		}
		return types.NewArray(out)
	default:
		return types.NewError("ERR unknown subcommand")
	}
}
