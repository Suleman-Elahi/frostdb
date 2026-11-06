package zsetcmd

import (
	"frostdb/internal/command/generic"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// BZPopMaxCommand implements BZPOPMAX key [key ...] timeout.
type BZPopMaxCommand struct{}

func (c BZPopMaxCommand) Name() string { return "BZPOPMAX" }

func (c BZPopMaxCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'bzpopmax' command")
	}

	keys := make([]string, 0, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys = append(keys, string(args[i].Str))
	}

	timeoutSecs, errVal := generic.ParseTimeout(args[len(args)-1])
	if errVal.Type == types.TypeError {
		return errVal
	}

	// Initial type check
	for _, key := range keys {
		obj, ok := ctx.Engine.Get(key)
		if ok && obj.Type != object.TypeZSet {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	tryOnce := func() (types.Value, bool, bool) {
		for _, key := range keys {
			obj, ok := ctx.Engine.Get(key)
			if !ok {
				continue
			}
			if obj.Type != object.TypeZSet {
				return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value"), true, false
			}
		}
		val := tryZPopMax(ctx, keys)
		if !val.IsNull {
			return val, true, true
		}
		return val, false, false
	}

	return blockOnZSetKeys(ctx, keys, timeoutSecs, tryOnce)
}

func tryZPopMax(ctx *context.Context, keys []string) types.Value {
	for _, key := range keys {
		obj, ok := ctx.Engine.Get(key)
		if !ok || obj.Type != object.TypeZSet {
			continue
		}
		z := obj.Value.(*object.ZSet)
		if z.Len() == 0 {
			continue
		}
		entries := z.PopMax(1)
		if len(entries) == 0 {
			continue
		}
		// Delete sorted set if last element was popped
		if z.Len() == 0 {
			ctx.Engine.Del(key)
		}
		e := entries[0]
		scoreVal := types.NewString(formatScore(e.Score))
		if ctx.Protocol >= 3 {
			scoreVal = newDouble(e.Score)
		}

		// Rewrite to ZPOPMAX for replication
		ctx.ReplicateAs = [][]byte{[]byte("ZPOPMAX"), []byte(key), []byte("1")}
		ctx.SkipReplication = false

		return types.NewArray([]types.Value{
			types.NewString(key),
			types.NewString(e.Member),
			scoreVal,
		})
	}
	if ctx.Protocol >= 3 {
		return types.NewNull()
	}
	return types.RespNilArr
}

