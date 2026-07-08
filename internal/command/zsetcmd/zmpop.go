package zsetcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ZMPopCommand implements the ZMPOP command.
// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
type ZMPopCommand struct{}

func (c ZMPopCommand) Name() string { return "ZMPOP" }

func (c ZMPopCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'zmpop' command")
	}

	numKeys, err := strconv.Atoi(string(args[0].Str))
	if err != nil || numKeys < 1 {
		return types.NewError("ERR numkeys can't be non-positive value")
	}

	if len(args) < 1+numKeys+1 {
		return types.NewError("ERR wrong number of arguments for 'zmpop' command")
	}

	keys := args[1 : 1+numKeys]
	idx := 1 + numKeys

	where := strings.ToUpper(string(args[idx].Str))
	if where != "MIN" && where != "MAX" {
		return types.NewError("ERR syntax error")
	}
	idx++

	count := 1
	countSet := false
	for idx < len(args) {
		opt := strings.ToUpper(string(args[idx].Str))
		switch opt {
		case "COUNT":
			if countSet {
				return types.NewError("ERR syntax error")
			}
			if idx+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			n, err := strconv.Atoi(string(args[idx+1].Str))
			if err != nil || n < 1 {
				return types.NewError("ERR count can't be a non-positive value")
			}
			count = n
			countSet = true
			idx += 2
		default:
			return types.NewError("ERR syntax error")
		}
	}

	// Find the first non-empty sorted set
	for _, kv := range keys {
		key := string(kv.Str)
		obj, ok := ctx.Engine.Get(key)
		if !ok {
			continue
		}
		if obj.Type != object.TypeZSet {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		zset := obj.Value.(*object.ZSet)
		if zset.Len() == 0 {
			continue
		}

		var entries []object.ZSetEntry
		if where == "MIN" {
			entries = zset.PopMin(count)
		} else {
			entries = zset.PopMax(count)
		}

		if zset.Len() == 0 {
			ctx.Engine.Del(key)
		}

		elements := make([]types.Value, 0, len(entries))
		for _, e := range entries {
			scoreVal := types.NewString(formatScore(e.Score))
			if ctx.Protocol >= 3 {
				scoreVal = newDouble(e.Score)
			}
			elements = append(elements, types.NewArray([]types.Value{
				types.NewString(e.Member),
				scoreVal,
			}))
		}

		// Rewrite to ZPOPMIN/ZPOPMAX for replication
		replCmd := "ZPOPMIN"
		if where == "MAX" {
			replCmd = "ZPOPMAX"
		}
		ctx.ReplicateAs = [][]byte{[]byte(replCmd), []byte(key), []byte(strconv.Itoa(len(entries)))}

		return types.NewArray([]types.Value{
			types.NewString(key),
			types.NewArray(elements),
		})
	}

	// No non-empty sorted sets found
	ctx.SkipReplication = true
	if ctx.Protocol >= 3 {
		return types.NewNull()
	}
	return types.RespNilArr
}
