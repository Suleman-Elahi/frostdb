package generic

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// BgRewriteAofCommand implements BGREWRITEAOF — triggers AOF rewrite in background.
// In FrostDB, this creates a checkpoint and truncates the WAL.
type BgRewriteAofCommand struct{}

func (c BgRewriteAofCommand) Name() string { return "BGREWRITEAOF" }

func (c BgRewriteAofCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if err := ctx.Engine.RewriteAOFAsync(); err != nil {
		return types.NewError("ERR " + err.Error())
	}
	return types.NewSimpleString("Background append only file rewriting started")
}
