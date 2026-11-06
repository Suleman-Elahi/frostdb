package zsetcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/command/generic"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// BZMPopCommand implements BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count].
type BZMPopCommand struct{}

func (c BZMPopCommand) Name() string { return "BZMPOP" }

func (c BZMPopCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 4 {
		return types.NewError("ERR wrong number of arguments for 'bzmpop' command")
	}

	timeoutSecs, errVal := generic.ParseTimeout(args[0])
	if errVal.Type == types.TypeError {
		return errVal
	}

	numKeys, err := strconv.Atoi(string(args[1].Str))
	if err != nil || numKeys < 1 {
		return types.NewError("ERR numkeys can't be non-positive value")
	}

	if len(args) < 2+numKeys+1 {
		return types.NewError("ERR wrong number of arguments for 'bzmpop' command")
	}

	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i].Str)
	}
	idx := 2 + numKeys

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

	// Initial type check
	for _, key := range keys {
		obj, ok := ctx.Engine.Get(key)
		if ok && obj.Type != object.TypeZSet {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	tryOnce := func() (types.Value, bool, bool) {
		for _, key := range keys {
			obj, ok := ctx.Engine.Get(key)
			if !ok {
				continue
			}
			if obj.Type != object.TypeZSet {
				return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value"), true, false
			}
		}
		val := tryBZMPop(ctx, keys, where, count)
		if !val.IsNull {
			return val, true, true
		}
		return val, false, false
	}

	return blockOnZSetKeys(ctx, keys, timeoutSecs, tryOnce)
}

func tryBZMPop(ctx *context.Context, keys []string, where string, count int) types.Value {
	for _, key := range keys {
		obj, ok := ctx.Engine.Get(key)
		if !ok || obj.Type != object.TypeZSet {
			continue
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
		ctx.SkipReplication = false

		return types.NewArray([]types.Value{
			types.NewString(key),
			types.NewArray(elements),
		})
	}

	ctx.SkipReplication = true
	if ctx.Protocol >= 3 {
		return types.NewNull()
	}
	return types.RespNilArr
}

