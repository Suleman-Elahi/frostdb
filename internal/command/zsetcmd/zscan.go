package zsetcmd

import (
	"path/filepath"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ZScanCommand struct{}

func (c ZScanCommand) Name() string { return "ZSCAN" }

func (c ZScanCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'zscan' command")
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
	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	z := obj.Value.(*object.ZSet)
	hasPattern := pattern != "*" && pattern != ""

	fmtScore := func(m string) string {
		return formatScore(z.Dict[m])
	}

	if !hasPattern {
		members, next := z.Scan(cursor, count)
		pairs := make([]types.Value, 0, len(members)*2)
		for _, m := range members {
			pairs = append(pairs, types.NewString(m), types.NewString(fmtScore(m)))
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
		members, next := z.Scan(pos, batchSize)
		for _, m := range members {
			if ok, _ := filepath.Match(pattern, m); !ok {
				continue
			}
			pairs = append(pairs, types.NewString(m), types.NewString(fmtScore(m)))
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
