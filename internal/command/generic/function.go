package generic

import (
	"runtime"
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
	"frostdb/internal/scripting"
)

type FunctionCommand struct{}

func (c FunctionCommand) Name() string { return "FUNCTION" }

func (c FunctionCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewError("ERR wrong number of arguments for 'function' command")
	}

	sub := strings.ToUpper(string(args[0].Str))
	switch sub {
	case "FLUSH":
		return functionFlush(args[1:])
	case "LOAD":
		return functionLoad(args[1:])
	case "KILL":
		return functionKill(args[1:])
	default:
		return types.NewError("ERR unknown subcommand '" + sub + "'. Try FUNCTION HELP.")
	}
}

func functionFlush(args []types.Value) types.Value {
	if len(args) > 1 {
		return types.NewError("ERR wrong number of arguments for 'function|flush' command")
	}
	if len(args) == 1 {
		mode := strings.ToUpper(string(args[0].Str))
		if mode != "ASYNC" && mode != "SYNC" {
			return types.NewError("ERR syntax error")
		}
	}

	scripting.Libraries().Flush()
	scripting.Global().Flush()
	runtime.GC()
	return types.RespOK
}

func functionLoad(args []types.Value) types.Value {
	replace := false
	codeIdx := 0

	if len(args) == 0 {
		return types.NewError("ERR wrong number of arguments for 'function|load' command")
	}

	if strings.ToUpper(string(args[0].Str)) == "REPLACE" {
		replace = true
		codeIdx = 1
		if len(args) < 2 {
			return types.NewError("ERR wrong number of arguments for 'function|load' command")
		}
	}

	code := string(args[codeIdx].Str)
	name, err := scripting.Libraries().Load(code, replace)
	if err != nil {
		return types.NewError(err.Error())
	}

	return types.NewString(name)
}

func functionKill(args []types.Value) types.Value {
	return types.RespOK
}
