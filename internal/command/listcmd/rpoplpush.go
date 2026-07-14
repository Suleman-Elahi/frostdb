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

// RPOPLPUSH source destination
type RPopLPushCommand struct{}

func (c RPopLPushCommand) Name() string { return "RPOPLPUSH" }

func (c RPopLPushCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'rpoplpush' command")
	}

	source := string(args[0].Str)
	dest := string(args[1].Str)
	v, sig := popRightPushLeft(ctx, source, dest, false)
	if sig {
		if ctx.ExecutingMulti {
			ctx.DeferredSignals = append(ctx.DeferredSignals, source)
		} else {
			ctx.Engine.SignalKey(source)
		}
	}
	return v
}

// BRPOPLPUSH source destination timeout
type BRPopLPushCommand struct{}

func (c BRPopLPushCommand) Name() string { return "BRPOPLPUSH" }

func (c BRPopLPushCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'brpoplpush' command")
	}

	source := string(args[0].Str)
	dest := string(args[1].Str)
	timeoutSecs, err := strconv.ParseFloat(string(args[2].Str), 64)
	if err != nil || timeoutSecs < 0 {
		return types.NewError("ERR timeout is not a float or out of range")
	}

	if ctx.ExecutingMulti || ctx.InScript {
		v, sig := popRightPushLeft(ctx, source, dest, false)
		if sig {
			ctx.DeferredSignals = append(ctx.DeferredSignals, source)
		}
		return v
	}

	var signalSource bool
	defer func() {
		if signalSource {
			ctx.Engine.SignalKey(source)
		}
	}()

	if v, sig := popRightPushLeft(ctx, source, dest, true); !v.IsNull {
		signalSource = sig
		return v
	}

	waitCh, cleanup := ctx.Engine.Subscribe([]string{source})
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
		if v, sig := popRightPushLeft(ctx, source, dest, true); !v.IsNull {
			signalSource = sig
			return v
		}

		select {
		case <-waitCh:
			if v, sig := popRightPushLeft(ctx, source, dest, true); !v.IsNull {
				signalSource = sig
				return v
			}
		case <-timeoutCh:
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
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

// popRightPushLeft pops one element from source tail and pushes to destination head.
// If returnNullSentinel is true and source has no poppable value, it returns a null sentinel value
// for caller flow control.
func popRightPushLeft(ctx *context.Context, source, dest string, returnNullSentinel bool) (types.Value, bool) {
	sourceObj, ok := ctx.Engine.GetCheckExpired(source)
	if !ok {
		if returnNullSentinel {
			return types.NewNull(), false
		}
		if ctx.Protocol >= 3 {
			return types.NewNull(), false
		}
		return types.RespNil, false
	}
	if sourceObj.Type != object.TypeList {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
	}

	sourceList := sourceObj.Value.(*object.List)
	if sourceList.LLen() == 0 {
		if returnNullSentinel {
			return types.NewNull(), false
		}
		if ctx.Protocol >= 3 {
			return types.NewNull(), false
		}
		return types.RespNil, false
	}

	val, popped := sourceList.PopBack()
	if !popped {
		if returnNullSentinel {
			return types.NewNull(), false
		}
		if ctx.Protocol >= 3 {
			return types.NewNull(), false
		}
		return types.RespNil, false
	}

	destObj, ok := ctx.Engine.GetCheckExpired(dest)
	var destList *object.List
	if ok {
		if destObj.Type != object.TypeList {
			// Restore val to source list to maintain order of elements after failure
			sourceList.PushBack(val)
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value"), true
		}
		destList = destObj.Value.(*object.List)
	} else {
		destList = object.NewList()
		ctx.Engine.Set(dest, &object.Object{
			Type:     object.TypeList,
			Encoding: object.EncodingRaw,
			Value:    destList,
		})
	}

	var sig bool
	if sourceList.LLen() == 0 {
		if source != dest {
			ctx.Engine.Del(source)
		}
	} else {
		sig = true
	}

	destList.PushFront(val)
	if ctx.ExecutingMulti {
		ctx.DeferredSignals = append(ctx.DeferredSignals, dest)
	} else {
		ctx.Engine.SignalKey(dest)
	}
	return types.NewString(string(val)), sig
}
