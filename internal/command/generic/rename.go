package generic

import (
	"frostdb/internal/engine/keyevent"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// RenameCommand implements RENAME key newkey.
type RenameCommand struct{}

func (c RenameCommand) Name() string { return "RENAME" }

func (c RenameCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[0].Str)
	dst := string(args[1].Str)

	obj, ok := ctx.Engine.Get(src)
	if !ok {
		return types.NewError("ERR no such key")
	}

	if oldDst, existed := ctx.Engine.Get(dst); existed {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagOverwrite), "overwritten", dst)
		if oldDst.Type != obj.Type {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagTypeChanged), "type_changed", dst)
		}
	}
	ctx.Engine.Del(src)
	ctx.Engine.Set(dst, obj)
	ctx.Engine.SignalKey(src)
	ctx.Engine.SignalKey(dst)
	return types.RespOK
}

// RenameNXCommand implements RENAMENX key newkey.
type RenameNXCommand struct{}

func (c RenameNXCommand) Name() string { return "RENAMENX" }

func (c RenameNXCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'renamenx' command")
	}
	src := string(args[0].Str)
	dst := string(args[1].Str)

	obj, ok := ctx.Engine.Get(src)
	if !ok {
		return types.NewError("ERR no such key")
	}

	if _, exists := ctx.Engine.Get(dst); exists {
		return types.RespZero
	}

	ctx.Engine.Del(src)
	ctx.Engine.Set(dst, obj)
	ctx.Engine.SignalKey(src)
	ctx.Engine.SignalKey(dst)
	return types.RespOne
}
