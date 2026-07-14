package listcmd

import (
	"strconv"
	"strings"
	"time"

	"frostdb/internal/blocking"
	"frostdb/internal/command/generic"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// BLMOVE source destination <LEFT|RIGHT> <LEFT|RIGHT> timeout
type BLMoveCommand struct{}

func (c BLMoveCommand) Name() string { return "BLMOVE" }

func (c BLMoveCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 5 {
		return types.NewError("ERR wrong number of arguments for 'blmove' command")
	}

	sourceKey := string(args[0].Str)
	destKey := string(args[1].Str)
	whereFrom := strings.ToUpper(string(args[2].Str))
	whereTo := strings.ToUpper(string(args[3].Str))

	if (whereFrom != "LEFT" && whereFrom != "RIGHT") || (whereTo != "LEFT" && whereTo != "RIGHT") {
		return types.NewError("ERR syntax error")
	}

	timeoutSecs, err := strconv.ParseFloat(string(args[4].Str), 64)
	if err != nil || timeoutSecs < 0 {
		return types.NewError("ERR timeout is not a float or out of range")
	}

	if ctx.ExecutingMulti || ctx.InScript {
		v, sig := moveOnce(ctx, sourceKey, destKey, whereFrom, whereTo)
		if sig {
			if ctx.ExecutingMulti {
				ctx.DeferredSignals = append(ctx.DeferredSignals, sourceKey)
			} else {
				ctx.Engine.SignalKey(sourceKey)
			}
		}
		return v
	}

	var signalSource bool
	defer func() {
		if signalSource {
			ctx.Engine.SignalKey(sourceKey)
		}
	}()

	if v, sig := moveOnce(ctx, sourceKey, destKey, whereFrom, whereTo); !v.IsNull {
		signalSource = sig
		return v
	}

	waitCh, cleanup := ctx.Engine.Subscribe([]string{sourceKey})
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
		if v, sig := moveOnce(ctx, sourceKey, destKey, whereFrom, whereTo); !v.IsNull {
			signalSource = sig
			return v
		}

		select {
		case <-waitCh:
			if v, sig := moveOnce(ctx, sourceKey, destKey, whereFrom, whereTo); !v.IsNull {
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

func moveOnce(ctx *context.Context, sourceKey, destKey, whereFrom, whereTo string) (types.Value, bool) {
	sourceObj, ok := ctx.Engine.GetCheckExpired(sourceKey)
	if !ok {
		return types.NewNull(), false
	}
	if sourceObj.Type != object.TypeList {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
	}

	sourceList := sourceObj.Value.(*object.List)
	if sourceList.LLen() == 0 {
		return types.NewNull(), false
	}
	var (
		val    []byte
		popped bool
	)
	if whereFrom == "LEFT" {
		val, popped = sourceList.PopFront()
	} else {
		val, popped = sourceList.PopBack()
	}
	if !popped {
		return types.NewNull(), false
	}

	destObj, ok := ctx.Engine.GetCheckExpired(destKey)
	var destList *object.List
	if ok {
		if destObj.Type != object.TypeList {
			// Restore val to source list
			if whereFrom == "LEFT" {
				sourceList.PushFront(val)
			} else {
				sourceList.PushBack(val)
			}
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value"), true
		}
		destList = destObj.Value.(*object.List)
	} else {
		destList = object.NewList()
		destObj = &object.Object{
			Type:     object.TypeList,
			Encoding: object.EncodingRaw,
			Value:    destList,
		}
		ctx.Engine.Set(destKey, destObj)
	}

	var sig bool
	if sourceList.LLen() == 0 {
		if sourceKey != destKey {
			ctx.Engine.Del(sourceKey)
		}
	} else {
		sig = true
	}

	if whereTo == "LEFT" {
		destList.PushFront(val)
	} else {
		destList.PushBack(val)
	}
	if ctx.ExecutingMulti {
		ctx.DeferredSignals = append(ctx.DeferredSignals, destKey)
	} else {
		ctx.Engine.SignalKey(destKey)
	}

	return types.NewString(string(val)), sig
}
