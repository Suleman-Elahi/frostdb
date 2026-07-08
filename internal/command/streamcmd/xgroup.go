package streamcmd

import (
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type XGroupCommand struct{}

func (c XGroupCommand) Name() string { return "XGROUP" }

func (c XGroupCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'xgroup' command")
	}

	sub := strings.ToUpper(string(args[0].Str))
	switch sub {
	case "CREATE":
		return c.executeCreate(ctx, args[1:])
	case "CREATECONSUMER":
		return types.NewInt(1)
	case "DELCONSUMER":
		return types.NewInt(1)
	case "HELP":
		if len(args[1:]) > 0 {
			return types.NewError("ERR wrong number of arguments for 'xgroup|help' command")
		}
		return xgroupHelp()
	default:
		return types.NewError("ERR unknown subcommand '" + strings.ToLower(sub) + "'")
	}
}

func (c XGroupCommand) executeCreate(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'xgroup|create' command")
	}

	key := string(args[0].Str)
	groupName := string(args[1].Str)
	startID := string(args[2].Str)
	mkstream := false
	if len(args) > 3 {
		for _, arg := range args[3:] {
			if strings.ToUpper(string(arg.Str)) == "MKSTREAM" {
				mkstream = true
			}
		}
	}

	obj, ok := ctx.Engine.Get(key)
	var stream *object.Stream
	if ok {
		if obj.Type != object.TypeStream {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		stream = obj.Value.(*object.Stream)
	} else {
		if !mkstream {
			return types.NewError("ERR The XGROUP subcommand requires the key to exist")
		}
		stream = object.NewStream()
		obj = &object.Object{
			Type:     object.TypeStream,
			Encoding: object.EncodingRaw,
			Value:    stream,
		}
		ctx.Engine.Set(key, obj)
	}

	if err := stream.CreateGroup(groupName, startID); err != nil {
		return types.NewError(err.Error())
	}

	return types.NewSimpleString("OK")
}

func xgroupHelp() types.Value {
	return types.NewArray([]types.Value{
		types.NewString("XGROUP <subcommand> arg arg ... arg. Subcommands are:"),
		types.NewString("CREATE <key> <groupname> <id or $> [MKSTREAM] [ENTRIESREAD entries_read]"),
		types.NewString("  Create a new consumer group. If MKSTREAM is given, creates the stream if it does not exist."),
		types.NewString("CREATECONSUMER <key> <groupname> <consumername>"),
		types.NewString("  Create a new consumer in the specified consumer group."),
		types.NewString("DELCONSUMER <key> <groupname> <consumername>"),
		types.NewString("  Remove the specified consumer from the consumer group."),
		types.NewString("DESTROY <key> <groupname>"),
		types.NewString("  Remove the specified consumer group."),
		types.NewString("SETID <key> <groupname> <id or $> [ENTRIESREAD entries_read]"),
		types.NewString("  Set the last delivered ID for a consumer group."),
		types.NewString("HELP"),
		types.NewString("  Prints this help."),
	})
}
