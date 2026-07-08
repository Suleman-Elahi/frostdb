package setcmd

import (
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type SAddCommand struct{}

func (c SAddCommand) Name() string { return "SADD" }

func (c SAddCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'sadd' command")
	}

	key := string(args[0].Str)

	obj, ok := ctx.Engine.Get(key)
	var s *object.Set

	if ok {
		if obj.Type != object.TypeSet {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		s = obj.Value.(*object.Set)
	} else {
		s = object.NewSet()
		obj = &object.Object{
			Type:     object.TypeSet,
			Encoding: object.EncodingRaw,
			Value:    s,
		}
		ctx.Engine.Set(key, obj)
	}

	members := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		members = append(members, string(args[i].Str))
	}

	added := s.Add(members...)

	if obj.Encoding != object.EncodingListpack {
		for _, m := range members {
			if _, err := strconv.ParseInt(m, 10, 64); err != nil {
				obj.Encoding = object.EncodingListpack
				break
			}
		}
	}

	rawArgs := make([][]byte, 0, 1+len(args))
	rawArgs = append(rawArgs, []byte("SADD"))
	for _, arg := range args {
		rawArgs = append(rawArgs, arg.Str)
	}
	ctx.Engine.LogCommand(rawArgs) //nolint:errcheck // error logged internally
	ctx.SkipReplication = true

	return types.NewInt(int64(added))
}
