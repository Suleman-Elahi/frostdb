package hashcmd

import (
	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type HDelCommand struct{}

func (c HDelCommand) Name() string { return "HDEL" }

func (c HDelCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'hdel' command")
	}

	key := string(args[0].Str)
	
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespZero
	}
	
	if obj.Type != object.TypeHash {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	
	h := obj.Value.(*object.Hash)
	
	fields := make([]string, 0, len(args)-1)
	for _, arg := range args[1:] {
		fields = append(fields, string(arg.Str))
	}
	
	ExpireIfNeeded(ctx, key, h, fields, ctx.NowMs(), false, true)
	
	deleted := h.Del(fields...)
	
	// Send notifications in correct order: hdel first, then del if empty.
	if deleted > 0 {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagHash), "hdel", key)
		ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hdel", key, fields)
	}
	if h.Len() == 0 {
		ctx.Engine.Del(key)
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
		ctx.Engine.LogCommand([][]byte{[]byte("DEL"), []byte(key)})
	}
	
	if deleted > 0 {
		rawArgs := make([][]byte, 0, 1+len(args))
		rawArgs = append(rawArgs, []byte("HDEL"))
		for _, arg := range args {
			rawArgs = append(rawArgs, arg.Str)
		}
		ctx.Engine.LogCommand(rawArgs) //nolint:errcheck // error logged internally
		ctx.SkipReplication = true
	}

	return types.NewInt(int64(deleted))
}
