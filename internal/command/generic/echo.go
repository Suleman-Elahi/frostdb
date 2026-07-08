package generic

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type EchoCommand struct{}

func (c EchoCommand) Name() string {
	return "ECHO"
}

func (c EchoCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'echo' command")
	}
	return types.NewString(string(args[0].Str))
}
