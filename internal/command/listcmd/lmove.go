package listcmd

import (
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type LMoveCommand struct{}

func (c LMoveCommand) Name() string { return "LMOVE" }

func (c LMoveCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 4 {
		return types.NewError("ERR wrong number of arguments for 'lmove' command")
	}

	sourceKey := string(args[0].Str)
	destKey := string(args[1].Str)
	whereFrom := strings.ToUpper(string(args[2].Str))
	whereTo := strings.ToUpper(string(args[3].Str))

	if (whereFrom != "LEFT" && whereFrom != "RIGHT") || (whereTo != "LEFT" && whereTo != "RIGHT") {
		return types.NewError("ERR syntax error")
	}

	v, sig := moveOnce(ctx, sourceKey, destKey, whereFrom, whereTo)
	if sig {
		ctx.SignalKey(sourceKey)
	}
	if v.IsNull {
		ctx.SkipReplication = true
		if ctx.Protocol >= 3 {
			return types.NewNull()
		}
		return types.RespNil
	}
	return v
}
