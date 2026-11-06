package generic

import (
	"strconv"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ExpireCommand struct{}

func (c ExpireCommand) Name() string {
	return "EXPIRE"
}

func (c ExpireCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'expire' command")
	}

	key := string(args[0].Str)
	seconds, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		ctx.SkipReplication = true
		return types.NewInt(0)
	}

	expireAt := ctx.NowMs() + (seconds * 1000)
	obj.ExpireAt = expireAt
	ctx.Engine.Set(key, obj)

	ctx.ReplicateAs = [][]byte{
		[]byte("PEXPIREAT"),
		[]byte(key),
		[]byte(strconv.FormatInt(expireAt, 10)),
	}

	return types.NewInt(1)
}
