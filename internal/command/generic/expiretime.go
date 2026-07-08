package generic

import (
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ExpireTimeCommand struct{}

func (c ExpireTimeCommand) Name() string { return "EXPIRETIME" }

func (c ExpireTimeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'expiretime' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.GetWithTime(key, ctx.NowMs())
	if !ok {
		return types.NewInt(-2)
	}
	if obj.ExpireAt == 0 {
		return types.NewInt(-1)
	}

	return types.NewInt(obj.ExpireAt / 1000)
}

type PExpireTimeCommand struct{}

func (c PExpireTimeCommand) Name() string { return "PEXPIRETIME" }

func (c PExpireTimeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'pexpiretime' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.GetWithTime(key, ctx.NowMs())
	if !ok {
		return types.NewInt(-2)
	}
	if obj.ExpireAt == 0 {
		return types.NewInt(-1)
	}

	return types.NewInt(obj.ExpireAt)
}
