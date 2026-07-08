package generic

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ExistsCommand struct{}

func (c ExistsCommand) Name() string { return "EXISTS" }

func (c ExistsCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'exists' command")
	}

	count := 0
	for _, arg := range args {
		_, ok := ctx.Engine.GetWithTime(string(arg.Str), ctx.NowMs())
		if ok {
			count++
		}
	}
	return types.NewInt(int64(count))
}
