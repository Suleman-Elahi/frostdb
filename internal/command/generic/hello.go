package generic

import (
	"strconv"
	"strings"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type HelloCommand struct{}

func (c HelloCommand) Name() string { return "HELLO" }

func (c HelloCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	proto := 2
	if len(args) > 0 {
		p, err := strconv.Atoi(string(args[0].Str))
		if err != nil || (p != 2 && p != 3) {
			return types.NewError("NOPROTO unsupported protocol version")
		}
		proto = p
	}

	// Parse optional AUTH and SETNAME (ignore for now)
	for i := 1; i < len(args); i++ {
		token := strings.ToUpper(string(args[i].Str))
		switch token {
		case "AUTH":
			i += 2 // skip username and password
		case "SETNAME":
			i++ // skip name
		}
	}

	ctx.Protocol = proto

	// Return server info as key-value pairs
	if proto >= 3 {
		return types.Value{
			Type: types.TypeMap,
			Array: []types.Value{
				types.NewString("server"), types.NewString("frostdb"),
				types.NewString("version"), types.NewString("0.1.0"),
				types.NewString("proto"), types.NewInt(int64(proto)),
				types.NewString("id"), types.NewInt(1),
				types.NewString("mode"), types.NewString("standalone"),
				types.NewString("role"), types.NewString("master"),
				types.NewString("modules"), types.NewArray([]types.Value{}),
			},
		}
	}
	return types.NewArray([]types.Value{
		types.NewString("server"),
		types.NewString("frostdb"),
		types.NewString("version"),
		types.NewString("0.1.0"),
		types.NewString("proto"),
		types.NewInt(int64(proto)),
		types.NewString("id"),
		types.NewInt(1),
		types.NewString("mode"),
		types.NewString("standalone"),
		types.NewString("role"),
		types.NewString("master"),
		types.NewString("modules"),
		types.NewArray([]types.Value{}),
	})
}
