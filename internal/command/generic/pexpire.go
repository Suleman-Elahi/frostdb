package generic

import (
	"strconv"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// PExpireCommand implements PEXPIRE key milliseconds
type PExpireCommand struct{}

func (c PExpireCommand) Name() string { return "PEXPIRE" }

func (c PExpireCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'pexpire' command")
	}

	ms, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		ctx.SkipReplication = true
		return types.RespZero
	}

	expireAt := ctx.NowMs() + ms
	obj.ExpireAt = expireAt
	ctx.ReplicateAs = [][]byte{
		[]byte("PEXPIREAT"),
		args[0].Str,
		[]byte(strconv.FormatInt(expireAt, 10)),
	}

	return types.RespOne
}

// ExpireAtCommand implements EXPIREAT key unix-time-seconds
type ExpireAtCommand struct{}

func (c ExpireAtCommand) Name() string { return "EXPIREAT" }

func (c ExpireAtCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'expireat' command")
	}

	secs, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		ctx.SkipReplication = true
		return types.RespZero
	}

	expireAt := secs * 1000
	obj.ExpireAt = expireAt
	ctx.ReplicateAs = [][]byte{
		[]byte("PEXPIREAT"),
		args[0].Str,
		[]byte(strconv.FormatInt(expireAt, 10)),
	}

	return types.RespOne
}

// PExpireAtCommand implements PEXPIREAT key unix-time-milliseconds
type PExpireAtCommand struct{}

func (c PExpireAtCommand) Name() string { return "PEXPIREAT" }

func (c PExpireAtCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'pexpireat' command")
	}

	ms, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		ctx.SkipReplication = true
		return types.RespZero
	}

	obj.ExpireAt = ms
	return types.RespOne
}

// PTtlCommand implements PTTL key (returns TTL in milliseconds)
type PTtlCommand struct{}

func (c PTtlCommand) Name() string { return "PTTL" }

func (c PTtlCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'pttl' command")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		return types.NewInt(-2) // key does not exist
	}

	if obj.ExpireAt == 0 {
		return types.NewInt(-1) // no TTL
	}

	remaining := obj.ExpireAt - ctx.NowMs()
	if remaining < 0 {
		return types.NewInt(-2)
	}
	return types.NewInt(remaining)
}
