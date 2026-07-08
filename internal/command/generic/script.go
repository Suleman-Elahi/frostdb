package generic

import (
	"runtime"
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
	"frostdb/internal/scripting"
)

// ScriptCommand implements SCRIPT LOAD|EXISTS|FLUSH.
type ScriptCommand struct{}

func (c ScriptCommand) Name() string { return "SCRIPT" }

func (c ScriptCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewError("ERR wrong number of arguments for 'script' command")
	}

	sub := strings.ToUpper(string(args[0].Str))

	switch sub {
	case "LOAD":
		return scriptLoad(args[1:])
	case "EXISTS":
		return scriptExists(args[1:])
	case "FLUSH":
		return scriptFlush()
	default:
		return types.NewError("ERR unknown subcommand '" + sub + "'. Try SCRIPT LOAD, SCRIPT EXISTS, SCRIPT FLUSH.")
	}
}

func scriptLoad(args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'script|load' command")
	}
	script := string(args[0].Str)
	sha := scripting.Global().Store(script)
	return types.NewString(sha)
}

func scriptExists(args []types.Value) types.Value {
	shas := make([]string, len(args))
	for i, a := range args {
		shas[i] = strings.ToLower(string(a.Str))
	}

	exists := scripting.Global().Exists(shas)
	arr := make([]types.Value, len(exists))
	for i, e := range exists {
		if e {
			arr[i] = types.RespOne
		} else {
			arr[i] = types.RespZero
		}
	}
	return types.NewArray(arr)
}

func scriptFlush() types.Value {
	scripting.Global().Flush()
	runtime.GC()
	return types.RespOK
}
