package generic

import (
	"strings"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// CopyCommand implements COPY source destination [DB db] [REPLACE].
type CopyCommand struct{}

func (c CopyCommand) Name() string { return "COPY" }

func (c CopyCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'copy' command")
	}

	src := string(args[0].Str)
	dst := string(args[1].Str)
	replace := false

	// Parse optional args
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i].Str)) {
		case "DB":
			i++ // skip DB number — single-db, ignore
		case "REPLACE":
			replace = true
		}
	}

	srcObj, ok := ctx.Engine.Get(src)
	if !ok {
		return types.NewInt(0)
	}

	// Check if destination exists
	if _, exists := ctx.Engine.Get(dst); exists && !replace {
		return types.NewInt(0)
	}

	if replace {
		if oldDst, existed := ctx.Engine.Get(dst); existed {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagOverwrite), "overwritten", dst)
			if oldDst.Type != srcObj.Type {
				ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagTypeChanged), "type_changed", dst)
			}
		}
		ctx.Engine.Del(dst)
	}

	// Deep copy the object
	ctx.Engine.Set(dst, srcObj.DeepCopy())
	return types.NewInt(1)
}
