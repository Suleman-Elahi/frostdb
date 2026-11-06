package listcmd

import (
	"strings"

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

	timeoutSecs, errVal := generic.ParseTimeout(args[4])
	if errVal.Type == types.TypeError {
		return errVal
	}

	tryMove := func() (types.Value, bool) {
		v, sig := moveOnce(ctx, sourceKey, destKey, whereFrom, whereTo)
		if sig {
			ctx.SignalKey(sourceKey)
		}
		if v.IsNull {
			return v, false
		}
		if v.Type != types.TypeError {
			ctx.ReplicateAs = [][]byte{[]byte("LMOVE"), []byte(sourceKey), []byte(destKey), []byte(strings.ToLower(whereFrom)), []byte(strings.ToLower(whereTo))}
		} else {
			ctx.SkipReplication = true
		}
		return v, true
	}

	if ctx.ExecutingMulti || ctx.InScript {
		v, ok := tryMove()
		if !ok {
			ctx.SkipReplication = true
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return types.RespNil
		}
		return v
	}

	if v, ok := tryMove(); ok {
		return v
	}

	return blockOnListKeys(ctx, []string{sourceKey}, timeoutSecs, tryMove)
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
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
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
		// Redis only counts dirty for source key changes, not dest creation.
		// Set adds +1; decrement it so BLMOVE totals +1 (source deletion only).
		ctx.Engine.AddDirty(-1)
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
	ctx.SignalKey(destKey)

	return types.NewString(string(val)), sig
}
