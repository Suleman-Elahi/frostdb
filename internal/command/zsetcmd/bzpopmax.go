package zsetcmd

import (
	"strconv"
	"time"

	"frostdb/internal/blocking"
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

	timeoutSecs, err := strconv.ParseFloat(string(args[len(args)-1].Str), 64)
	if err != nil || timeoutSecs < 0 {
		return types.NewError("ERR timeout is not a float or out of range")
	}

	// Initial type check
	for _, key := range keys {
		obj, ok := ctx.Engine.Get(key)
		if ok && obj.Type != object.TypeZSet {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}
	
	if ctx.ExecutingMulti || ctx.InScript {
		val := tryZPopMax(ctx, keys)
		if val.IsNull {
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return types.RespNilArr
		}
		for _, k := range keys {
			if obj, ok := ctx.Engine.Get(k); ok && obj.Type == object.TypeZSet {
				if obj.Value.(*object.ZSet).Len() > 0 {
					ctx.DeferredSignals = append(ctx.DeferredSignals, k)
				}
			}
		}
		return val
	}

	defer func() {
		for _, k := range keys {
			if obj, ok := ctx.Engine.Get(k); ok && obj.Type == object.TypeZSet {
				if obj.Value.(*object.ZSet).Len() > 0 {
					ctx.Engine.SignalKey(k)
				}
			}
		}
	}()

	// Try immediate pop
	if val := tryZPopMax(ctx, keys); !val.IsNull {
		return val
	}

	// Subscribe and wait
	waitCh, cleanup := ctx.Engine.Subscribe(keys)
	defer cleanup()

	blocking.Inc()
	defer blocking.Dec()

	var timeoutCh <-chan time.Time
	if timeoutSecs > 0 {
		timeoutCh = time.After(time.Duration(timeoutSecs * float64(time.Second)))
	}

	// Release the command lock before blocking.
	if ctx.CmdMu != nil {
		ctx.CmdMu.Unlock()
	}
	defer func() {
		if ctx.CmdMu != nil {
			ctx.CmdMu.Lock()
		}
	}()

	for {
		if val := tryZPopMax(ctx, keys); !val.IsNull {
			return val
		}

		select {
		case <-waitCh:
			if val := tryZPopMax(ctx, keys); !val.IsNull {
				return val
			}
		case <-timeoutCh:
			ctx.SkipReplication = true
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return types.RespNilArr
		case <-ctx.Done:
			ctx.SkipReplication = true
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return types.RespNilArr
		case <-blocking.StopSignal():
			ctx.SkipReplication = true
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return types.RespNilArr
		}
	}
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
