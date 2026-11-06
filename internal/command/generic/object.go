package generic

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/eviction"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ObjectCommand implements OBJECT IDLETIME|FREQ|ENCODING|HELP
type ObjectCommand struct{}

func (c ObjectCommand) Name() string { return "OBJECT" }

func (c ObjectCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'object' command")
	}

	sub := strings.ToUpper(string(args[0].Str))
	switch sub {
	case "IDLETIME":
		return objectIdleTime(ctx, args[1:])
	case "FREQ":
		return objectFreq(ctx, args[1:])
	case "ENCODING":
		return objectEncoding(ctx, args[1:])
	case "REFCOUNT":
		return objectRefCount(ctx, args[1:])
	case "HELP":
		return objectHelp()
	default:
		return types.NewError("ERR unknown subcommand '" + sub + "'")
	}
}

func objectIdleTime(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'object|idletime' command")
	}
	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespNil
	}
	idle := eviction.LRUIdleTime(obj.LastAccess & 0xFFFFFF)
	return types.NewInt(int64(idle))
}

func objectFreq(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'object|freq' command")
	}
	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespNil
	}
	counter := eviction.LFUGetCounter(obj.LastAccess)
	return types.NewInt(int64(counter))
}

func objectEncoding(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'object|encoding' command")
	}
	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespNil
	}
	return types.NewString(encodingName(obj))
}

func encodingName(obj *object.Object) string {
	switch obj.Type {
	case object.TypeString:
		switch obj.Encoding {
		case object.EncodingInt:
			return "int"
		case object.EncodingRawForce:
			return "raw"
		default:
			// Redis uses "embstr" for strings ≤44 bytes, "raw" for longer.
			if s, ok := obj.Value.([]byte); ok {
				if len(s) <= 44 {
					return "embstr"
				}
				return "raw"
			}
			return "embstr"
		}
	case object.TypeList:
		return listEncodingName(obj)
	case object.TypeHash:
		return hashEncodingName(obj)
	case object.TypeSet:
		return setEncodingName(obj)
	case object.TypeZSet:
		return zsetEncodingName(obj)
	case object.TypeStream:
		return "stream"
	case object.TypeArray:
		return "sliced-array"
	default:
		return "raw"
	}
}

func listEncodingName(obj *object.Object) string {
	l, ok := obj.Value.(*object.List)
	if !ok {
		return "quicklist"
	}
	if l.IsQuicklist() {
		return "quicklist"
	}
	return "listpack"
}



func isIntsetSet(s *object.Set) bool {
	members := s.Members()
	for _, m := range members {
		if _, err := strconv.ParseInt(m, 10, 64); err != nil {
			return false
		}
	}
	return true
}

func setEncodingName(obj *object.Object) string {
	s, ok := obj.Value.(*object.Set)
	if !ok {
		return "hashtable"
	}
	size := s.Len()
	
	maxIntset := setMaxIntsetEntries.Load()
	if int64(size) <= maxIntset && obj.Encoding != object.EncodingListpack && isIntsetSet(s) {
		return "intset"
	}
	
	maxListpackEntries := setMaxListpackEntries.Load()
	maxListpackValue := setMaxListpackValue.Load()
	
	if int64(size) <= maxListpackEntries {
		members := s.Members()
		for _, m := range members {
			if int64(len(m)) > maxListpackValue {
				return "hashtable"
			}
		}
		return "listpack"
	}
	return "hashtable"
}

func hashEncodingName(obj *object.Object) string {
	h, ok := obj.Value.(*object.Hash)
	if !ok {
		return "hashtable"
	}
	size := h.Len()
	maxEntries := hashMaxListpackEntries.Load()
	maxValue := hashMaxListpackValue.Load()
	if int64(size) <= maxEntries {
		hasTTL := false
		for f, v := range h.GetAll() {
			if int64(len(f)) > maxValue || int64(len(v)) > maxValue {
				return "hashtable"
			}
			if !hasTTL && h.GetTTL(f) > 0 {
				hasTTL = true
			}
		}
		if hasTTL {
			return "listpackex"
		}
		return "listpack"
	}
	return "hashtable"
}

func zsetEncodingName(obj *object.Object) string {
	z, ok := obj.Value.(*object.ZSet)
	if !ok {
		return "skiplist"
	}
	size := z.Len()
	
	maxListpackEntries := zsetMaxListpackEntries.Load()
	maxListpackValue := zsetMaxListpackValue.Load()

	if int64(size) <= maxListpackEntries {
		node := z.Skiplist.Head.Level[0].Forward
		for node != nil {
			if int64(len(node.Member)) > maxListpackValue {
				return "skiplist"
			}
			node = node.Level[0].Forward
		}
		return "listpack"
	}
	return "skiplist"
}

func objectHelp() types.Value {
	lines := []types.Value{
		types.NewString("OBJECT IDLETIME <key> - Return the idle time of the key (LRU seconds)"),
		types.NewString("OBJECT FREQ <key> - Return the access frequency of the key (LFU counter)"),
		types.NewString("OBJECT ENCODING <key> - Return the internal encoding of the key"),
		types.NewString("OBJECT REFCOUNT <key> - Return the reference count of the key"),
		types.NewString("OBJECT HELP - Return this help message"),
	}
	return types.NewArray(lines)
}

func objectRefCount(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'object|refcount' command")
	}
	key := string(args[0].Str)
	if _, ok := ctx.Engine.Get(key); !ok {
		return types.RespNil
	}
	// FrostDB doesn't use reference counting — always 1
	return types.NewInt(1)
}
