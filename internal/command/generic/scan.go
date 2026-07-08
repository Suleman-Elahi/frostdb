package generic

import (
	"sort"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ScanCommand struct{}

func (c ScanCommand) Name() string {
	return "SCAN"
}

func (c ScanCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'scan' command")
	}

	// Parse options: MATCH pattern, COUNT n, TYPE typename
	pattern := "*"
	count := 10
	var filterType string

	for i := 1; i < len(args)-1; i += 2 {
		opt := strings.ToUpper(string(args[i].Str))
		switch opt {
		case "MATCH":
			pattern = string(args[i+1].Str)
		case "COUNT":
			if n, err := strconv.Atoi(string(args[i+1].Str)); err == nil && n > 0 {
				count = n
			}
		case "TYPE":
			filterType = strings.ToLower(string(args[i+1].Str))
		}
	}

	// Get all keys (including expired) from engine
	allKeys := ctx.Engine.KeysAll(pattern)
	sort.Strings(allKeys)

	// Filter: delete expired keys that match the pattern, apply TYPE filter
	if filterType != "" {
		filtered := make([]string, 0, len(allKeys))
		for _, k := range allKeys {
			obj, ok := ctx.Engine.Get(k)
			if !ok {
				// Key expired or missing — delete it from storage
				ctx.Engine.Del(k)
				continue
			}
			if matchesType(obj, filterType) {
				filtered = append(filtered, k)
			}
		}
		allKeys = filtered
	} else {
		// No TYPE filter: still need to delete expired keys that match the pattern
		valid := make([]string, 0, len(allKeys))
		for _, k := range allKeys {
			_, ok := ctx.Engine.Get(k)
			if !ok {
				// Key expired — delete it from storage
				ctx.Engine.Del(k)
				continue
			}
			valid = append(valid, k)
		}
		allKeys = valid
	}

	// Parse cursor — we do a simple single-pass implementation:
	// cursor is an index into the key list.
	cursor, _ := strconv.Atoi(string(args[0].Str))

	// Determine the slice to return
	var resultKeys []string
	var nextCursor int

	if cursor >= len(allKeys) {
		// Past the end or empty — return cursor 0 (done)
		resultKeys = nil
		nextCursor = 0
	} else if cursor+count >= len(allKeys) {
		// Return remaining keys, cursor 0 (done)
		resultKeys = allKeys[cursor:]
		nextCursor = 0
	} else {
		// Return a batch, advance cursor
		resultKeys = allKeys[cursor : cursor+count]
		nextCursor = cursor + count
	}

	// Build response
	keyVals := make([]types.Value, len(resultKeys))
	for i, k := range resultKeys {
		keyVals[i] = types.NewString(k)
	}

	return types.NewArray([]types.Value{
		types.NewString(strconv.Itoa(nextCursor)),
		types.NewArray(keyVals),
	})
}

// matchesType checks if an object matches the given Redis type name.
func matchesType(obj *object.Object, typeName string) bool {
	switch typeName {
	case "string":
		return obj.Type == object.TypeString
	case "list":
		return obj.Type == object.TypeList
	case "set":
		return obj.Type == object.TypeSet
	case "zset":
		return obj.Type == object.TypeZSet
	case "hash":
		return obj.Type == object.TypeHash
	case "stream":
		return obj.Type == object.TypeStream
	default:
		return false
	}
}
