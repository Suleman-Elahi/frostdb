package hashcmd

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// HMSetCommand implements HMSET as a compatibility alias for HSET.
type HMSetCommand struct{}

func (c HMSetCommand) Name() string { return "HMSET" }

func (c HMSetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return types.NewError("ERR wrong number of arguments for 'hmset' command")
	}
	res := (HSetCommand{}).Execute(ctx, args)
	if res.Type == types.TypeError {
		return res
	}
	// Redis HMSET returns OK (deprecated alias behavior).
	return types.RespOK
}

