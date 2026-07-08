package generic

import (
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// MemoryCommand implements MEMORY USAGE|HELP
type MemoryCommand struct{}

func (c MemoryCommand) Name() string { return "MEMORY" }

func (c MemoryCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'memory' command")
	}

	sub := strings.ToUpper(string(args[0].Str))
	switch sub {
	case "USAGE":
		return memoryUsage(ctx, args[1:])
	case "HELP":
		return memoryHelp()
	default:
		return types.NewError("ERR unknown subcommand '" + sub + "'")
	}
}

func memoryUsage(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'memory|usage' command")
	}
	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespNil
	}
	size := estimateObjectSize(obj) + int64(len(key))
	return types.NewInt(size)
}

// estimateObjectSize returns an approximate byte count for the object,
// matching Redis's MEMORY USAGE semantics (includes overhead).
func estimateObjectSize(obj *object.Object) int64 {
	// Base overhead: Object struct + map entry + key string header
	const overhead = 32

	switch obj.Type {
	case object.TypeString:
		if s, ok := obj.Value.(string); ok {
			return overhead + int64(len(s))
		}
		if _, ok := obj.Value.(int64); ok {
			return overhead + 8
		}
		if b, ok := obj.Value.([]byte); ok {
			return overhead + int64(len(b))
		}
		return overhead

	case object.TypeList:
		if l, ok := obj.Value.(*object.List); ok {
			// Each node: prev/next pointers (16) + value slice header (24) + value bytes
			size := int64(0)
			for _, val := range l.Snapshot() {
				size += 40 + int64(len(val))
			}
			return overhead + size
		}
		return overhead

	case object.TypeHash:
		if h, ok := obj.Value.(*object.Hash); ok {
			size := int64(0)
			for k, v := range h.GetAll() {
				// map bucket overhead + key + value
				size += 64 + int64(len(k)) + int64(len(v))
			}
			return overhead + size
		}
		return overhead

	case object.TypeSet:
		if s, ok := obj.Value.(*object.Set); ok {
			members := s.Members()
			size := int64(0)
			for _, m := range members {
				size += 48 + int64(len(m))
			}
			return overhead + size
		}
		return overhead

	case object.TypeZSet:
		if z, ok := obj.Value.(*object.ZSet); ok {
			// Dict entry + skiplist node per member
			size := int64(0)
			for member := range z.Dict {
				// dict entry (64) + skiplist node (~96) + member string
				size += 160 + int64(len(member))
			}
			return overhead + size
		}
		return overhead

	case object.TypeStream:
		// Streams are complex; return a rough estimate based on block data
		return overhead + 1024

	default:
		return overhead
	}
}

func memoryHelp() types.Value {
	lines := []types.Value{
		types.NewString("MEMORY USAGE <key> [SAMPLES <count>] - Return memory usage in bytes"),
		types.NewString("MEMORY HELP - Return this help message"),
	}
	return types.NewArray(lines)
}
