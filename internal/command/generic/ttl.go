package generic

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type TtlCommand struct{}

func (c TtlCommand) Name() string {
	return "TTL"
}

func (c TtlCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'ttl' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(-2) // Key does not exist
	}

	if obj.ExpireAt == 0 {
		return types.NewInt(-1) // Key exists but has no associated expire
	}

	ttlMs := obj.ExpireAt - ctx.NowMs()
	if ttlMs < 0 {
		return types.NewInt(-2) // Already expired (though Get shouldn't return it)
	}

	return types.NewInt(ttlMs / 1000)
}
