package setcmd

import (
	"path/filepath"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type SScanCommand struct{}

func (c SScanCommand) Name() string { return "SSCAN" }

func (c SScanCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'sscan' command")
	}

	key := string(args[0].Str)
	cursor, err := strconv.ParseUint(string(args[1].Str), 10, 64)
	if err != nil {
		return types.NewError("ERR invalid cursor")
	}

	pattern := "*"
	count := 10
	for i := 2; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return types.NewError("ERR syntax error")
		}
		switch strings.ToUpper(string(args[i].Str)) {
		case "MATCH":
			pattern = string(args[i+1].Str)
		case "COUNT":
			n, err := strconv.Atoi(string(args[i+1].Str))
			if err == nil && n > 0 {
				count = n
			}
		default:
			return types.NewError("ERR syntax error")
		}
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewArray([]types.Value{types.NewString("0"), types.RespEmptyArr})
	}
	if obj.Type != object.TypeSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	s := obj.Value.(*object.Set)
	hasPattern := pattern != "*" && pattern != ""

	// Set.Scan uses a value-based (hash) cursor over a cached sorted snapshot,
	// which is efficient (O(n log n) for a full scan) and preserves the SCAN
	// guarantee under concurrent modification.
	if !hasPattern {
		members, nextCursor := s.Scan(cursor, count)
		out := make([]types.Value, len(members))
		for i, m := range members {
			out[i] = types.NewString(m)
		}
		return types.NewArray([]types.Value{
			types.NewString(strconv.FormatUint(nextCursor, 10)),
			types.NewArray(out),
		})
	}

	// With a MATCH pattern, keep scanning larger internal batches until we
	// collect enough matches or reach the end.
	out := make([]types.Value, 0, count)
	pos := cursor
	for {
		batchSize := count * 5
		if batchSize < 100 {
			batchSize = 100
		}
		members, nextCursor := s.Scan(pos, batchSize)
		for _, m := range members {
			if ok, _ := filepath.Match(pattern, m); ok {
				out = append(out, types.NewString(m))
			}
		}
		pos = nextCursor
		if nextCursor == 0 || len(out) >= count {
			break
		}
	}

	return types.NewArray([]types.Value{
		types.NewString(strconv.FormatUint(pos, 10)),
		types.NewArray(out),
	})
}
