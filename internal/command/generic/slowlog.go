package generic

import (
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// SlowlogCommand implements SLOWLOG GET|LEN|RESET.
// Stub implementation — FrostDB doesn't track slow queries yet.
type SlowlogCommand struct{}

func (c SlowlogCommand) Name() string { return "SLOWLOG" }

func (c SlowlogCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewError("ERR wrong number of arguments for 'slowlog' command")
	}

	sub := strings.ToUpper(string(args[0].Str))
	switch sub {
	case "GET":
		return types.RespEmptyArr
	case "LEN":
		return types.NewInt(0)
	case "RESET":
		return types.RespOK
	case "HELP":
		return types.NewArray([]types.Value{
			types.NewString("SLOWLOG GET [count] - Return top entries from the slowlog"),
			types.NewString("SLOWLOG LEN - Return the number of entries in the slowlog"),
			types.NewString("SLOWLOG RESET - Reset the slowlog"),
		})
	default:
		return types.NewError("ERR unknown subcommand '" + sub + "'")
	}
}
