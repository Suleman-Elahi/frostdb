package generic

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// TouchCommand implements TOUCH — updates LRU time, returns count of existing keys.
type TouchCommand struct{}

func (c TouchCommand) Name() string { return "TOUCH" }

func (c TouchCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewError("ERR wrong number of arguments for 'touch' command")
	}

	count := int64(0)
	for _, arg := range args {
		// Get triggers LRU/LFU touch internally
		if _, ok := ctx.Engine.Get(string(arg.Str)); ok {
			count++
		}
	}
	return types.NewInt(count)
}
