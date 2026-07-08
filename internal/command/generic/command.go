package generic

import (
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// CommandCommand implements COMMAND, COMMAND COUNT, COMMAND DOCS, COMMAND INFO, COMMAND GETKEYS.
type CommandCommand struct{}

func (c CommandCommand) Name() string { return "COMMAND" }

func (c CommandCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) == 0 {
		return types.RespEmptyArr
	}

	sub := strings.ToUpper(string(args[0].Str))
	switch sub {
	case "COUNT":
		return types.NewInt(200)
	case "DOCS":
		return types.RespEmptyArr
	case "INFO":
		return types.RespEmptyArr
	case "LIST":
		return types.RespEmptyArr
	case "GETKEYS":
		return commandGetKeys(args[1:])
	case "GETKEYSANDFLAGS":
		return commandGetKeys(args[1:])
	default:
		return types.RespEmptyArr
	}
}

func commandGetKeys(args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewError("ERR Invalid arguments specified for command")
	}

	cmd := strings.ToUpper(string(args[0].Str))
	cmdArgs := args[1:]

	switch cmd {
	case "SORT":
		return sortGetKeys(cmdArgs)
	case "SORT_RO":
		return sortROGetKeys(cmdArgs)
	default:
		// For other commands, return the first argument as the key
		if len(cmdArgs) > 0 {
			return types.NewArray([]types.Value{types.NewString(string(cmdArgs[0].Str))})
		}
		return types.RespEmptyArr
	}
}

func sortGetKeys(args []types.Value) types.Value {
	keys := make([]types.Value, 0, 2)
	if len(args) > 0 {
		keys = append(keys, types.NewString(string(args[0].Str)))
	}
	// Find the last STORE destination
	for i := 0; i < len(args)-1; i++ {
		if strings.ToUpper(string(args[i].Str)) == "STORE" {
			keys = append(keys, types.NewString(string(args[i+1].Str)))
		}
	}
	// SORT only returns the last STORE key + source key
	res := make([]types.Value, 0, 2)
	res = append(res, keys[0])
	if len(keys) > 1 {
		res = append(res, keys[len(keys)-1])
	}
	return types.NewArray(res)
}

func sortROGetKeys(args []types.Value) types.Value {
	if len(args) > 0 {
		return types.NewArray([]types.Value{types.NewString(string(args[0].Str))})
	}
	return types.RespEmptyArr
}
