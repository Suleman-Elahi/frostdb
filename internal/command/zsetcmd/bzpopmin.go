package zsetcmd

import (
	"time"

	"frostdb/internal/blocking"
	"frostdb/internal/command/generic"
	"frostdb/internal/engine/api"
	"frostdb/internal/engine/object"
	"frostdb/internal/engine/shard"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// BZPopMinCommand implements BZPOPMIN key [key ...] timeout.
type BZPopMinCommand struct{}

func (c BZPopMinCommand) Name() string { return "BZPOPMIN" }

func (c BZPopMinCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'bzpopmin' command")
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
		val := tryZPopMin(ctx, keys)
		if !val.IsNull {
			return val, true, true
		}
		return val, false, false
	}

	return blockOnZSetKeys(ctx, keys, timeoutSecs, tryOnce)
}

func blockOnZSetKeys(
	ctx *context.Context,
	keys []string,
	timeoutSecs float64,
	tryOnce func() (val types.Value, done bool, consumed bool),
) types.Value {
	nullResp := func() types.Value {
		ctx.SkipReplication = true
		if ctx.Protocol >= 3 {
			return types.NewNull()
		}
		return types.RespNilArr
	}

	if ctx.ExecutingMulti || ctx.InScript {
		val, done, _ := tryOnce()
		if !done {
			return nullResp()
		}
		for _, k := range keys {
			if obj, ok := ctx.Engine.Get(k); ok && obj.Type == object.TypeZSet {
				if obj.Value.(*object.ZSet).Len() > 0 {
					ctx.SignalKey(k)
				}
			}
		}
		return val
	}

	// Try immediate pop before subscribing
	if val, done, _ := tryOnce(); done {
		for _, k := range keys {
			if obj, ok := ctx.Engine.Get(k); ok && obj.Type == object.TypeZSet {
				if obj.Value.(*object.ZSet).Len() > 0 {
					ctx.SignalKey(k)
				}
			}
		}
		return val
	}

	be, hasFair := ctx.Engine.(api.BlockingEngine)
	var waiter *shard.Waiter
	var waitCh <-chan struct{}
	var cleanup func()
	if hasFair {
		waiter, cleanup = be.SubscribeWaiter(keys)
	} else {
		waitCh, cleanup = ctx.Engine.Subscribe(keys)
	}
	defer cleanup()

	unblockCh := make(chan generic.UnblockType, 1)
	if ctx.ClientID > 0 {
		generic.RegisterClientUnblockCh(ctx.ClientID, unblockCh)
		defer generic.UnregisterClientUnblockCh(ctx.ClientID)
	}

	disconnectCh, stopDisconnectWatch := ctx.WatchDisconnect()
	defer stopDisconnectWatch()

	blocking.Inc()
	defer func() {
		if ctx.RecordStatsNow != nil {
			ctx.RecordStatsNow(false)
		}
		blocking.Dec()
	}()

	var timeoutCh <-chan time.Time
	if timeoutSecs > 0 {
		timer := time.NewTimer(time.Duration(timeoutSecs * float64(time.Second)))
		defer timer.Stop()
		timeoutCh = timer.C
	}

	if ctx.FlushWriter != nil {
		ctx.FlushWriter()
	}

	if ctx.UnlockKeys != nil {
		ctx.UnlockKeys()
	}
	defer func() {
		if ctx.LockKeys != nil {
			ctx.LockKeys()
		}
	}()

	for {
		if hasFair {
			select {
			case ackCh := <-waiter.WakeCh:
				if ctx.LockKeys != nil {
					ctx.LockKeys()
				}
				ctx.InWakeup = true
				val, done, consumed := tryOnce()
				ctx.InWakeup = false
				cascaded := ctx.TakeSignals()
				if ctx.UnlockKeys != nil {
					ctx.UnlockKeys()
				}
				ackCh <- shard.SignalAck{
					Consumed: consumed,
					Cascaded: cascaded,
				}
				if done {
					return val
				}
			case ut := <-unblockCh:
				ctx.SkipReplication = true
				if ut == generic.UnblockError {
					return types.NewError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
				}
				return nullResp()
			case <-timeoutCh:
				return nullResp()
			case <-ctx.Done:
				return nullResp()
			case <-disconnectCh:
				return nullResp()
			case <-blocking.StopSignal():
				return nullResp()
			}
		} else {
			select {
			case <-waitCh:
				if ctx.LockKeys != nil {
					ctx.LockKeys()
				}
				val, done, _ := tryOnce()
				if ctx.UnlockKeys != nil {
					ctx.UnlockKeys()
				}
				if done {
					return val
				}
			case ut := <-unblockCh:
				ctx.SkipReplication = true
				if ut == generic.UnblockError {
					return types.NewError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
				}
				return nullResp()
			case <-timeoutCh:
				return nullResp()
			case <-ctx.Done:
				return nullResp()
			case <-disconnectCh:
				return nullResp()
			case <-blocking.StopSignal():
				return nullResp()
			}
		}
	}
}

func tryZPopMin(ctx *context.Context, keys []string) types.Value {
	for _, key := range keys {
		obj, ok := ctx.Engine.Get(key)
		if !ok || obj.Type != object.TypeZSet {
			continue
		}
		z := obj.Value.(*object.ZSet)
		if z.Len() == 0 {
			continue
		}
		entries := z.PopMin(1)
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

		// Rewrite to ZPOPMIN for replication
		ctx.ReplicateAs = [][]byte{[]byte("ZPOPMIN"), []byte(key), []byte("1")}
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

