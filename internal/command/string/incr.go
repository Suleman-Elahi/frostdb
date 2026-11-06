package str

import (
	"math"
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// IncrCommand implements INCR key
type IncrCommand struct{}

func (c IncrCommand) Name() string { return "INCR" }

func (c IncrCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'incr' command")
	}
	return incrBy(ctx, args[0].Str, 1)
}

// DecrCommand implements DECR key
type DecrCommand struct{}

func (c DecrCommand) Name() string { return "DECR" }

func (c DecrCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'decr' command")
	}
	return incrBy(ctx, args[0].Str, -1)
}

// IncrByCommand implements INCRBY key increment
type IncrByCommand struct{}

func (c IncrByCommand) Name() string { return "INCRBY" }

func (c IncrByCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'incrby' command")
	}
	incr, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}
	return incrBy(ctx, args[0].Str, incr)
}

// DecrByCommand implements DECRBY key decrement
type DecrByCommand struct{}

func (c DecrByCommand) Name() string { return "DECRBY" }

func (c DecrByCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'decrby' command")
	}
	decr, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}
	if decr == math.MinInt64 {
		return types.NewError("ERR increment or decrement would overflow")
	}
	return incrBy(ctx, args[0].Str, -decr)
}

func incrBy(ctx *context.Context, keyBytes []byte, delta int64) types.Value {
	key := string(keyBytes)
	var current int64

	obj, ok := ctx.Engine.GetBytes(keyBytes)
	if ok {
		strVal, isStr := obj.Value.([]byte)
		if !isStr {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		var err error
		current, err = strconv.ParseInt(string(strVal), 10, 64)
		if err != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}
	}

	current += delta
	newObj := object.AcquireStringObject(strconv.FormatInt(current, 10))
	if ok {
		newObj.ExpireAt = obj.ExpireAt
	}
	ctx.Engine.Set(key, newObj)
	return types.NewInt(current)
}
