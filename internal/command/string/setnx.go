package str

import (
	"fmt"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// SetNXCommand implements SETNX key value
type SetNXCommand struct{}

func (c SetNXCommand) Name() string { return "SETNX" }

func (c SetNXCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'setnx' command")
	}

	if _, ok := ctx.Engine.GetBytes(args[0].Str); ok {
		return types.RespZero
	}

	key := string(args[0].Str)
	val := string(args[1].Str)
	ctx.Engine.Set(key, object.AcquireStringObject(val))
	return types.RespOne
}

// SetEXCommand implements SETEX key seconds value
type SetEXCommand struct{}

func (c SetEXCommand) Name() string { return "SETEX" }

func (c SetEXCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'setex' command")
	}

	secs, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil || secs <= 0 {
		return types.NewError("ERR invalid expire time in 'setex' command")
	}

	key := string(args[0].Str)
	val := string(args[2].Str)
	obj := object.AcquireStringObject(val)
	obj.ExpireAt = ctx.NowMs() + secs*1000
	ctx.Engine.Set(key, obj)
	return types.RespOK
}

// PSetEXCommand implements PSETEX key milliseconds value
type PSetEXCommand struct{}

func (c PSetEXCommand) Name() string { return "PSETEX" }

func (c PSetEXCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'psetex' command")
	}

	ms, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil || ms <= 0 {
		return types.NewError("ERR invalid expire time in 'psetex' command")
	}

	key := string(args[0].Str)
	val := string(args[2].Str)
	obj := object.AcquireStringObject(val)
	obj.ExpireAt = ctx.NowMs() + ms
	ctx.Engine.Set(key, obj)
	return types.RespOK
}

// parseTTLArgs parses EX/PX/EXAT/PXAT/PERSIST from args, returns ExpireAt in ms.
// Returns -1 for PERSIST (clear TTL), 0 for no TTL change, positive for TTL.
func parseTTLArgs(ctx *context.Context, args []types.Value) int64 {
	ttl, _ := parseTTLArgsWithError(ctx, args)
	return ttl
}

// parseTTLArgsWithError parses EX/PX/EXAT/PXAT/PERSIST from args.
// Returns (ttl, nil) on success, (0, error) on syntax error.
// ttl is -1 for PERSIST, 0 for no TTL specified, positive for expiry time in ms.
func parseTTLArgsWithError(ctx *context.Context, args []types.Value) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	opt := strings.ToUpper(string(args[0].Str))
	switch opt {
	case "EX":
		if len(args) < 2 {
			return 0, fmt.Errorf("ERR syntax error")
		}
		secs, err := strconv.ParseInt(string(args[1].Str), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer or out of range")
		}
		return ctx.NowMs() + secs*1000, nil
	case "PX":
		if len(args) < 2 {
			return 0, fmt.Errorf("ERR syntax error")
		}
		ms, err := strconv.ParseInt(string(args[1].Str), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer or out of range")
		}
		return ctx.NowMs() + ms, nil
	case "EXAT":
		if len(args) < 2 {
			return 0, fmt.Errorf("ERR syntax error")
		}
		secs, err := strconv.ParseInt(string(args[1].Str), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer or out of range")
		}
		return secs * 1000, nil
	case "PXAT":
		if len(args) < 2 {
			return 0, fmt.Errorf("ERR syntax error")
		}
		ms, err := strconv.ParseInt(string(args[1].Str), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer or out of range")
		}
		return ms, nil
	case "PERSIST":
		return -1, nil // -1 signals PERSIST (clear TTL)
	default:
		return 0, fmt.Errorf("ERR syntax error")
	}
}
