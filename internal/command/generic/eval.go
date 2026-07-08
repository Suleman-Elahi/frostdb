package generic

import (
	"strconv"
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
	"frostdb/internal/scripting"
)

// EvalCommand implements EVAL script numkeys key... arg...
type EvalCommand struct{}

func (c EvalCommand) Name() string { return "EVAL" }

func (c EvalCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'eval' command")
	}

	script := string(args[0].Str)
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

	// Cache the script
	scripting.Global().Store(script)

	// Extract KEYS and ARGV
	keys, argv := extractKeysArgv(args[2:], numkeys)

	return scripting.Execute(ctx, script, keys, argv)
}

// EvalShaCommand implements EVALSHA sha1 numkeys key... arg...
type EvalShaCommand struct{}

func (c EvalShaCommand) Name() string { return "EVALSHA" }

func (c EvalShaCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'evalsha' command")
	}

	sha := strings.ToLower(string(args[0].Str))
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

	script, ok := scripting.Global().Get(sha)
	if !ok {
		return types.NewError("NOSCRIPT No matching script. Please use EVAL.")
	}

	keys, argv := extractKeysArgv(args[2:], numkeys)

	return scripting.Execute(ctx, script, keys, argv)
}

// EvalRoCommand implements EVAL_RO script numkeys key... arg...
type EvalRoCommand struct{}

func (c EvalRoCommand) Name() string { return "EVAL_RO" }

func (c EvalRoCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'eval_ro' command")
	}

	script := string(args[0].Str)
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

	scripting.Global().Store(script)
	keys, argv := extractKeysArgv(args[2:], numkeys)
	return scripting.ExecuteRO(ctx, script, keys, argv)
}

// EvalShaRoCommand implements EVALSHA_RO sha1 numkeys key... arg...
type EvalShaRoCommand struct{}

func (c EvalShaRoCommand) Name() string { return "EVALSHA_RO" }

func (c EvalShaRoCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'evalsha_ro' command")
	}
	sha := strings.ToLower(string(args[0].Str))
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
	script, ok := scripting.Global().Get(sha)
	if !ok {
		return types.NewError("NOSCRIPT No matching script. Please use EVAL.")
	}
	keys, argv := extractKeysArgv(args[2:], numkeys)
	return scripting.ExecuteRO(ctx, script, keys, argv)
}

// extractKeysArgv splits the remaining args into KEYS and ARGV slices.
func extractKeysArgv(args []types.Value, numkeys int) ([]string, []string) {
	keys := make([]string, numkeys)
	for i := 0; i < numkeys; i++ {
		keys[i] = string(args[i].Str)
	}

	argv := make([]string, len(args)-numkeys)
	for i := numkeys; i < len(args); i++ {
		argv[i-numkeys] = string(args[i].Str)
	}

	return keys, argv
}
