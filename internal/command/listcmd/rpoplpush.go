package listcmd

import (
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
		ctx.SignalKey(source)
	}
	if v.IsNull {
		ctx.SkipReplication = true
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
	timeoutSecs, errVal := generic.ParseTimeout(args[2])
	if errVal.Type == types.TypeError {
		return errVal
	}

	if ctx.ExecutingMulti || ctx.InScript {
		v, sig := popRightPushLeft(ctx, source, dest, false)
		if sig {
			ctx.SignalKey(source)
		}
		if v.IsNull {
			ctx.SkipReplication = true
		} else if v.Type != types.TypeError {
			ctx.ReplicateAs = [][]byte{[]byte("RPOPLPUSH"), []byte(source), []byte(dest)}
		}
		return v
	}

	tryPopPush := func() (types.Value, bool) {
		v, sig := popRightPushLeft(ctx, source, dest, true)
		if sig {
			ctx.SignalKey(source)
		}
		if v.IsNull {
			return v, false
		}
		if v.Type != types.TypeError {
			ctx.ReplicateAs = [][]byte{[]byte("RPOPLPUSH"), []byte(source), []byte(dest)}
		} else {
			ctx.SkipReplication = true
		}
		return v, true
	}

	if v, ok := tryPopPush(); ok {
		return v
	}

	return blockOnListKeys(ctx, []string{source}, timeoutSecs, tryPopPush)
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
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value"), false
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
	ctx.SignalKey(dest)
	return types.NewString(string(val)), sig
}
