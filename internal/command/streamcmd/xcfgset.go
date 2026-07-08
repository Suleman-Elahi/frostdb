package streamcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type XCfgSetCommand struct{}

func (c XCfgSetCommand) Name() string { return "XCFGSET" }

func parseStrictPositiveInt(arg string) (int64, bool) {
	if arg == "" {
		return 0, false
	}
	if arg[0] == '+' || arg[0] == '-' {
		n, err := strconv.ParseInt(arg, 10, 64)
		return n, err == nil
	}
	if len(arg) > 1 && arg[0] == '0' {
		return 0, false
	}
	for i := 0; i < len(arg); i++ {
		if arg[i] < '0' || arg[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(arg, 10, 64)
	return n, err == nil
}

func (c XCfgSetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'xcfgset' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewError("ERR no such key")
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if len(args) == 1 {
		return types.NewError("ERR At least one parameter must be specified")
	}

	var duration int64
	var maxSize int64
	hasDuration := false
	hasMaxSize := false

	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return types.NewError("ERR syntax error")
		}
		token := strings.ToUpper(string(args[i].Str))
		rawVal := string(args[i+1].Str)
		n, ok := parseStrictPositiveInt(rawVal)
		if !ok {
			return types.NewError("ERR value is not an integer or out of range")
		}
		switch token {
		case "IDMP-DURATION":
			if n < 1 || n > 86400 {
				return types.NewError("ERR IDMP-DURATION must be between 1 and 86400")
			}
			duration = n
			hasDuration = true
		case "IDMP-MAXSIZE":
			if n < 1 || n > 10000 {
				return types.NewError("ERR IDMP-MAXSIZE must be between 1 and 10000")
			}
			maxSize = n
			hasMaxSize = true
		default:
			return types.NewError("ERR syntax error")
		}
	}

	stream := obj.Value.(*object.Stream)
	stream.SetIDMPConfig(duration, maxSize, hasDuration, hasMaxSize)
	return types.RespOK
}
