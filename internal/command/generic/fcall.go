package generic

import (
	"strconv"
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
	"frostdb/internal/scripting"
)

type FcallCommand struct{}

func (c FcallCommand) Name() string { return "FCALL" }

func (c FcallCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return fcallExecute(ctx, args, false)
}

type FcallRoCommand struct{}

func (c FcallRoCommand) Name() string { return "FCALL_RO" }

func (c FcallRoCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return fcallExecute(ctx, args, true)
}

func fcallExecute(ctx *context.Context, args []types.Value, readOnly bool) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'fcall' command")
	}

	funcName := strings.ToLower(string(args[0].Str))
	numkeys, err := strconv.Atoi(string(args[1].Str))
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}
	if numkeys < 0 {
		return types.NewError("ERR Number of keys can't be negative")
	}
	if numkeys > len(args)-2 {
		return types.NewError("ERR Number of keys can't be greater than number of args")
	}

	keys, argv := extractKeysArgv(args[2:], numkeys)

	lib, ok := scripting.Libraries().FindLibrary(funcName)
	if !ok {
		return types.NewError("ERR Function not found")
	}

	if readOnly {
		return scripting.ExecuteFCALLRO(ctx, lib, funcName, keys, argv)
	}
	return scripting.ExecuteFCALL(ctx, lib, funcName, keys, argv)
}
