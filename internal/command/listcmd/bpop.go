package listcmd

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

type BPopCommand struct {
	IsLeft bool
}

func (c BPopCommand) Name() string {
	if c.IsLeft {
		return "BLPOP"
	}
	return "BRPOP"
}

func (c BPopCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for '" + c.Name() + "' command")
	}

	keys := make([]string, 0, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys = append(keys, string(args[i].Str))
	}

	timeoutSecs, errVal := generic.ParseTimeout(args[len(args)-1])
	if errVal.Type == types.TypeError {
		return errVal
	}

	if ctx.ExecutingMulti || ctx.InScript {
		val := c.tryPop(ctx, keys, false)
		if val.IsNull {
			ctx.SkipReplication = true
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return types.RespNil
		}
		return val
	}

	// Try popping first (strict: wrong type should error immediately).
	if val := c.tryPop(ctx, keys, false); !val.IsNull {
		return val
	}

	return blockOnListKeys(ctx, keys, timeoutSecs, func() (types.Value, bool) {
		val := c.tryPop(ctx, keys, true)
		return val, !val.IsNull
	})
}

func (c BPopCommand) tryPop(ctx *context.Context, keys []string, ignoreWrongType bool) types.Value {
	for _, key := range keys {
		obj, ok := ctx.Engine.GetCheckExpired(key)
		if !ok {
			continue
		}
		if obj.Type != object.TypeList {
			if ignoreWrongType {
				continue
			}
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		list := obj.Value.(*object.List)
		var val []byte
		var popped bool

		if c.IsLeft {
			val, popped = list.PopFront()
		} else {
			val, popped = list.PopBack()
		}

		if popped {
			if list.LLen() == 0 {
				ctx.Engine.Del(key)
			} else {
				ctx.SignalKey(key)
			}
			popCmd := "LPOP"
			if !c.IsLeft {
				popCmd = "RPOP"
			}
			ctx.ReplicateAs = [][]byte{[]byte(popCmd), []byte(key)}
			return types.NewArray([]types.Value{
				types.NewString(key),
				types.NewString(string(val)),
			})
		}
	}
	return types.RespNil
}

func blockOnListKeys(ctx *context.Context, keys []string, timeoutSecs float64, tryWakeFn func() (types.Value, bool)) types.Value {
	var waiter *shard.Waiter
	var legacyCh <-chan struct{}
	var cleanup func()
	if be, ok := ctx.Engine.(api.BlockingEngine); ok {
		waiter, cleanup = be.SubscribeWaiter(keys)
	} else {
		legacyCh, cleanup = ctx.Engine.Subscribe(keys)
	}

	blocking.Inc()
	generic.UpdateClientBlocked(ctx, true)
	unblockCh := make(chan generic.UnblockType, 1)
	generic.RegisterClientUnblockCh(ctx.ClientID, unblockCh)

	blockedActive := true
	finishBlocked := func(failed bool) {
		if blockedActive {
			blockedActive = false
			if ctx.RecordStatsNow != nil {
				ctx.RecordStatsNow(failed)
			}
			blocking.Dec()
			generic.UpdateClientBlocked(ctx, false)
			generic.UnregisterClientUnblockCh(ctx.ClientID)
		}
	}
	defer func() {
		finishBlocked(false)
		cleanup()
	}()

	discCh, stopDisc := ctx.WatchDisconnect()
	defer stopDisc()

	var timeoutCh <-chan time.Time
	if timeoutSecs > 0 {
		timeoutCh = time.After(time.Duration(timeoutSecs * float64(time.Second)))
	}

	// Flush any buffered pipeline replies before blocking this connection.
	if ctx.FlushWriter != nil {
		ctx.FlushWriter()
	}

	// Release striped key locks while waiting so other connections can push.
	if ctx.UnlockKeys != nil {
		ctx.UnlockKeys()
	}

	var wakeCh <-chan chan shard.SignalAck
	if waiter != nil {
		wakeCh = waiter.WakeCh
	}

	for {
		select {
		case ackCh := <-wakeCh:
			if ctx.LockKeys != nil {
				ctx.LockKeys()
			}
			ctx.InWakeup = true
			dirtyBefore := ctx.Engine.GetDirty()
			val, ok := tryWakeFn()
			dirtyAfter := ctx.Engine.GetDirty()
			ctx.InWakeup = false
			if ok {
				failed := val.Type == types.TypeError
				if !failed && !ctx.SkipReplication && len(ctx.ReplicateAs) > 0 {
					if dirtyAfter == dirtyBefore {
						ctx.Engine.AddDirty(1)
					}
					_ = ctx.Engine.LogCommand(ctx.ReplicateAs)
					ctx.SkipReplication = true
				} else {
					ctx.SkipReplication = true
				}
				cascaded := ctx.TakeSignals()
				finishBlocked(failed)
				if ctx.UnlockKeys != nil {
					ctx.UnlockKeys()
				}
				ackCh <- shard.SignalAck{
					Consumed: true,
					Cascaded: cascaded,
				}
				return val
			}
			if ctx.UnlockKeys != nil {
				ctx.UnlockKeys()
			}
			ackCh <- shard.SignalAck{Consumed: false}

		case <-legacyCh:
			if ctx.LockKeys != nil {
				ctx.LockKeys()
			}
			val, ok := tryWakeFn()
			if ok {
				failed := val.Type == types.TypeError
				finishBlocked(failed)
				if ctx.UnlockKeys != nil {
					ctx.UnlockKeys()
				}
				return val
			}
			if ctx.UnlockKeys != nil {
				ctx.UnlockKeys()
			}

		case <-timeoutCh:
			ctx.SkipReplication = true
			finishBlocked(false)
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return types.RespNil

		case <-discCh:
			ctx.SkipReplication = true
			finishBlocked(false)
			return types.RespNil

		case <-blocking.StopSignal():
			ctx.SkipReplication = true
			finishBlocked(false)
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return types.RespNil

		case ut := <-unblockCh:
			ctx.SkipReplication = true
			if ut == generic.UnblockError {
				finishBlocked(true)
				return types.NewError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
			}
			finishBlocked(false)
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return types.RespNil
		}
	}
}
