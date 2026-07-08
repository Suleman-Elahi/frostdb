package generic

import (
	"frostdb/internal/engine/keyevent"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type DelCommand struct{}

func (c DelCommand) Name() string {
	return "DEL"
}

func (c DelCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'del' command")
	}

	count := 0
	for _, arg := range args {
		key := string(arg.Str)
		if ctx.Engine.Del(key) {
			count++
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
		}
	}

	return types.NewInt(int64(count))
}
