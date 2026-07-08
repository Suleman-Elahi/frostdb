package hashcmd

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// HKeysCommand implements HKEYS key
type HKeysCommand struct{}

func (c HKeysCommand) Name() string { return "HKEYS" }

func (c HKeysCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'hkeys' command")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		return types.RespEmptyArr
	}

	h, ok := obj.Value.(*object.Hash)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	key := string(args[0].Str)
	ExpireIfNeeded(ctx, key, h, nil, ctx.NowMs(), false, true)

	keys := h.Keys()
	result := make([]types.Value, 0, len(keys))
	for _, k := range keys {
		result = append(result, types.NewString(k))
	}
	return types.NewArray(result)
}

// HValsCommand implements HVALS key
type HValsCommand struct{}

func (c HValsCommand) Name() string { return "HVALS" }

func (c HValsCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'hvals' command")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		return types.RespEmptyArr
	}

	h, ok := obj.Value.(*object.Hash)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	key := string(args[0].Str)
	ExpireIfNeeded(ctx, key, h, nil, ctx.NowMs(), false, true)

	keys := h.Keys()
	result := make([]types.Value, 0, len(keys))
	for _, k := range keys {
		v, _ := h.Get(k)
		result = append(result, types.NewString(v))
	}
	return types.NewArray(result)
}

// HMGetCommand implements HMGET key field [field ...]
type HMGetCommand struct{}

func (c HMGetCommand) Name() string { return "HMGET" }

func (c HMGetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'hmget' command")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	result := make([]types.Value, len(args)-1)

	if !ok {
		for i := range result {
			result[i] = types.RespNil
		}
		return types.NewArray(result)
	}

	h, ok := obj.Value.(*object.Hash)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	key := string(args[0].Str)
	fields := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		fields[i] = string(arg.Str)
	}
	ExpireIfNeeded(ctx, key, h, fields, ctx.NowMs(), true, true)

	for i, arg := range args[1:] {
		val, exists := h.Get(string(arg.Str))
		if exists {
			result[i] = types.NewString(val)
		} else {
			result[i] = types.RespNil
		}
	}
	return types.NewArray(result)
}
