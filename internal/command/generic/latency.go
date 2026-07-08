package generic

import (
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// LatencyCommand implements LATENCY LATEST|HISTORY|RESET|HELP — stub.
type LatencyCommand struct{}

func (c LatencyCommand) Name() string { return "LATENCY" }

func (c LatencyCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewError("ERR wrong number of arguments for 'latency' command")
	}

	sub := strings.ToUpper(string(args[0].Str))
	switch sub {
	case "LATEST":
		return types.RespEmptyArr
	case "HISTORY":
		return types.RespEmptyArr
	case "RESET":
		return types.RespOK
	case "HELP":
		return types.NewArray([]types.Value{
			types.NewString("LATENCY LATEST - Return the latest latency samples"),
			types.NewString("LATENCY HISTORY <event> - Return latency time series"),
			types.NewString("LATENCY RESET [event ...] - Reset latency data"),
		})
	default:
		return types.NewError("ERR unknown subcommand '" + sub + "'")
	}
}
