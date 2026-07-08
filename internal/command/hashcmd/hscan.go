package hashcmd

import (
	"path/filepath"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type HScanCommand struct{}

func (c HScanCommand) Name() string { return "HSCAN" }

func (c HScanCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'hscan' command")
	}

	key := string(args[0].Str)
	cursor, err := strconv.ParseUint(string(args[1].Str), 10, 64)
	if err != nil {
		return types.NewError("ERR invalid cursor")
	}

	pattern := "*"
	count := 10
	novalues := false
	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i].Str))
		switch opt {
		case "NOVALUES":
			novalues = true
		case "MATCH":
			i++
			if i >= len(args) {
				return types.NewError("ERR syntax error")
			}
			pattern = string(args[i].Str)
		case "COUNT":
			i++
			if i >= len(args) {
				return types.NewError("ERR syntax error")
			}
			n, err := strconv.Atoi(string(args[i].Str))
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
	if obj.Type != object.TypeHash {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	h := obj.Value.(*object.Hash)
	ExpireIfNeeded(ctx, key, h, nil, ctx.NowMs(), false, true)

	hasPattern := pattern != "*" && pattern != ""

	// Use the Hash's insertion-order Scan for efficient cursor iteration.
	// The cursor is an index into the field order; a full scan is O(n) total
	// rather than O(n log n) per call.
	appendField := func(pairs []types.Value, f string) []types.Value {
		if novalues {
			return append(pairs, types.NewString(f))
		}
		v, _ := h.Get(f)
		return append(pairs, types.NewString(f), types.NewString(v))
	}

	if !hasPattern {
		fields, next := h.Scan(cursor, count)
		pairs := make([]types.Value, 0, len(fields)*2)
		for _, f := range fields {
			pairs = appendField(pairs, f)
		}
		return types.NewArray([]types.Value{
			types.NewString(strconv.FormatUint(next, 10)),
			types.NewArray(pairs),
		})
	}

	// Pattern path: scan in larger chunks until we collect `count` matches.
	pairs := make([]types.Value, 0, count*2)
	matches := 0
	pos := cursor
	for {
		batchSize := count * 5
		if batchSize < 100 {
			batchSize = 100
		}
		fields, next := h.Scan(pos, batchSize)
		for _, f := range fields {
			if m, _ := filepath.Match(pattern, f); !m {
				continue
			}
			pairs = appendField(pairs, f)
			matches++
		}
		pos = next
		if next == 0 || matches >= count {
			break
		}
	}

	return types.NewArray([]types.Value{
		types.NewString(strconv.FormatUint(pos, 10)),
		types.NewArray(pairs),
	})
}
