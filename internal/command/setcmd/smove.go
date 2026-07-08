package setcmd

import (
	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// SMoveCommand implements SMOVE source destination member.
type SMoveCommand struct{}

func (c SMoveCommand) Name() string { return "SMOVE" }

func (c SMoveCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'smove' command")
	}

	srcKey := string(args[0].Str)
	dstKey := string(args[1].Str)
	member := string(args[2].Str)

	srcObj, srcOk := ctx.Engine.Get(srcKey)
	dstObj, dstOk := ctx.Engine.Get(dstKey)

	if srcOk && (srcObj == nil || srcObj.Type != object.TypeSet) {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if dstOk && (dstObj == nil || dstObj.Type != object.TypeSet) {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if !srcOk {
		return types.RespZero
	}

	src, ok := srcObj.Value.(*object.Set)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if !src.Contains(member) {
		return types.RespZero
	}

	var dst *object.Set
	if dstOk {
		dst = dstObj.Value.(*object.Set)
	} else {
		dst = object.NewSet()
		ctx.Engine.Set(dstKey, &object.Object{Type: object.TypeSet, Encoding: object.EncodingRaw, Value: dst})
	}

	src.Remove(member)
	ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagSet), "srem", srcKey)
	
	added := dst.Add(member)
	if added > 0 {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagSet), "sadd", dstKey)
	}

	if src.Len() == 0 {
		ctx.Engine.Del(srcKey)
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", srcKey)
	}
	return types.RespOne
}
