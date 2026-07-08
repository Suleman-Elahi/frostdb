package listcmd

import (
	"strconv"
	"time"

	"frostdb/internal/blocking"
	"frostdb/internal/command/generic"
	"frostdb/internal/engine/object"
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

	timeoutSecs, err := strconv.ParseFloat(string(args[len(args)-1].Str), 64)
	if err != nil || timeoutSecs < 0 {
		return types.NewError("ERR timeout is not a float or out of range")
	}

	if ctx.ExecutingMulti || ctx.InScript {
		val := c.tryPop(ctx, keys, false)
		if val.IsNull {
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return types.RespNil
		}
		for _, k := range keys {
			if obj, ok := ctx.Engine.GetCheckExpired(k); ok && obj.Type == object.TypeList {
				if obj.Value.(*object.List).LLen() > 0 {
					ctx.DeferredSignals = append(ctx.DeferredSignals, k)
				}
			}
		}
		return val
	}

	defer func() {
		for _, k := range keys {
			if obj, ok := ctx.Engine.GetCheckExpired(k); ok && obj.Type == object.TypeList {
				if obj.Value.(*object.List).LLen() > 0 {
					ctx.Engine.SignalKey(k)
				}
			}
		}
	}()

	// Try popping first (strict: wrong type should error immediately).
	if val := c.tryPop(ctx, keys, false); !val.IsNull {
		return val
	}

	// Subscribe
	waitCh, cleanup := ctx.Engine.Subscribe(keys)
	defer cleanup()
	blocking.Inc()
	defer blocking.Dec()
	generic.UpdateClientBlocked(ctx, true)
	defer generic.UpdateClientBlocked(ctx, false)

	unblockCh := make(chan generic.UnblockType, 1)
	generic.RegisterClientUnblockCh(ctx.ClientID, unblockCh)
	defer generic.UnregisterClientUnblockCh(ctx.ClientID)

	var timeoutCh <-chan time.Time
	if timeoutSecs > 0 {
		timeoutCh = time.After(time.Duration(timeoutSecs * float64(time.Second)))
	}

	// Wait loop
	for {
		// Double-check just in case something was pushed before we subscribed
		if val := c.tryPop(ctx, keys, true); !val.IsNull {
			return val
		}

		select {
		case <-waitCh:
			if val := c.tryPop(ctx, keys, true); !val.IsNull {
				return val
			}
			// Spurious wakeup or someone else grabbed it, loop back and wait
		case <-timeoutCh:
			return types.RespNil
		case <-ctx.Done:
			return types.RespNil
		case <-blocking.StopSignal():
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return types.RespNil
		case ut := <-unblockCh:
			switch ut {
			case generic.UnblockError:
				return types.NewError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
			default:
				return types.RespNil
			}
		}
	}
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
			}
			return types.NewArray([]types.Value{
				types.NewString(key),
				types.NewString(string(val)),
			})
		}
	}
	return types.RespNil
}
