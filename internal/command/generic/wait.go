package generic

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// WaitCommand implements WAIT numreplicas timeout.
// Since FrostDB is single-node, this always returns 0 replicas immediately.
type WaitCommand struct{}

func (c WaitCommand) Name() string { return "WAIT" }

func (c WaitCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'wait' command")
	}
	// No replication — always 0 replicas acknowledged
	return types.NewInt(0)
}
