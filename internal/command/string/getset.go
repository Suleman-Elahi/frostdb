package str

import (
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
	"frostdb/internal/stats"
)

// GetSetCommand implements GETSET key value (deprecated but still used)
type GetSetCommand struct{}

func (c GetSetCommand) Name() string { return "GETSET" }

func (c GetSetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'getset' command")
	}

	key := string(args[0].Str)
	newVal := string(args[1].Str)

	// Get old value
	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	var result types.Value
	if !ok {
		result = types.RespNil
	} else {
		strVal, isStr := obj.Value.(string)
		if !isStr {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		result = types.NewString(strVal)
	}

	// Set new value (clears TTL like Redis)
	ctx.Engine.Set(key, object.AcquireStringObject(newVal))
	return result
}

// GetExCommand implements GETEX key [EX|PX|EXAT|PXAT|PERSIST]
type GetExCommand struct{}

func (c GetExCommand) Name() string { return "GETEX" }

func (c GetExCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'getex' command")
	}

	// Validate syntax FIRST (before key lookup - Redis behavior)
	var ttl int64
	if len(args) > 1 {
		var err error
		ttl, err = parseTTLArgsWithError(ctx, args[1:])
		if err != nil {
			return types.NewError(err.Error())
		}
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		return types.RespNil
	}

	strVal, isStr := obj.Value.(string)
	if !isStr {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Apply TTL changes and replicate accordingly
	if len(args) > 1 {
		if ttl == -1 {
			// PERSIST - clear TTL
			if obj.ExpireAt > 0 {
				obj.ExpireAt = 0
				ctx.ReplicateAs = [][]byte{[]byte("PERSIST"), args[0].Str}
			} else {
				ctx.SkipReplication = true
			}
		} else if ttl > 0 {
			now := ctx.NowMs()
			if ttl <= now {
				// Past expiration: delete key and replicate as DEL (or UNLINK)
				ctx.Engine.Del(string(args[0].Str))
				stats.Get().ExpiredKeys.Add(1)
				delCmd := "DEL"
				if ctx.Engine.GetEvictionConfig().LazyfreeLazyServerDel {
					delCmd = "UNLINK"
				}
				ctx.ReplicateAs = [][]byte{[]byte(delCmd), args[0].Str}
			} else {
				// Future expiration: update TTL and replicate as PEXPIREAT
				obj.ExpireAt = ttl
				ctx.ReplicateAs = [][]byte{[]byte("PEXPIREAT"), args[0].Str, []byte(strconv.FormatInt(ttl, 10))}
			}
		}
	} else {
		// No TTL arguments, just GET
		ctx.SkipReplication = true
	}

	return types.NewString(strVal)
}

// GetDelCommand implements GETDEL key
type GetDelCommand struct{}

func (c GetDelCommand) Name() string { return "GETDEL" }

func (c GetDelCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'getdel' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		ctx.SkipReplication = true
		return types.RespNil
	}

	strVal, isStr := obj.Value.(string)
	if !isStr {
		ctx.SkipReplication = true
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	ctx.Engine.Del(key)
	ctx.ReplicateAs = [][]byte{[]byte("DEL"), args[0].Str}
	return types.NewString(strVal)
}
