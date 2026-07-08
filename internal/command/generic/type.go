package generic

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// TypeCommand implements TYPE — returns the type of a key.
type TypeCommand struct{}

func (c TypeCommand) Name() string { return "TYPE" }

func (c TypeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'type' command")
	}
	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewSimpleString("none")
	}
	return types.NewSimpleString(typeName(obj.Type))
}

func typeName(t object.Type) string {
	switch t {
	case object.TypeString:
		return "string"
	case object.TypeList:
		return "list"
	case object.TypeHash:
		return "hash"
	case object.TypeSet:
		return "set"
	case object.TypeZSet:
		return "zset"
	case object.TypeStream:
		return "stream"
	case object.TypeArray:
		return "array"
	default:
		return "none"
	}
}
