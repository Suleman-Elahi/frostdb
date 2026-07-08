package generic

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type XClaimDummy struct{}
func (c XClaimDummy) Name() string { return "XCLAIM" }
func (c XClaimDummy) Execute(ctx *context.Context, args []types.Value) types.Value {
	return types.NewArray([]types.Value{})
}
