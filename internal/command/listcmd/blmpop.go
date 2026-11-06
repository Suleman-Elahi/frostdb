package listcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/command/generic"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// BLMPOP timeout numkeys key [key ...] <LEFT|RIGHT> [COUNT count]
type BLMPOPCommand struct{}

func (c BLMPOPCommand) Name() string { return "BLMPOP" }

func (c BLMPOPCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 4 {
		return types.NewError("ERR wrong number of arguments for 'blmpop' command")
	}

	timeoutSecs, errVal := generic.ParseTimeout(args[0])
	if errVal.Type == types.TypeError {
		return errVal
	}

	numKeys, err := strconv.Atoi(string(args[1].Str))
	if err != nil || numKeys <= 0 {
		return types.NewError("ERR numkeys should be greater than 0")
	}

	minArgs := 2 + numKeys + 1
	if len(args) < minArgs {
		return types.NewError("ERR syntax error")
	}

	keys := make([]string, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		keys = append(keys, string(args[2+i].Str))
	}

	dir := strings.ToUpper(string(args[2+numKeys].Str))
	popLeft := true
	switch dir {
	case "LEFT":
		popLeft = true
	case "RIGHT":
		popLeft = false
	default:
		return types.NewError("ERR syntax error")
	}

	count := 1
	if len(args) > minArgs {
		if len(args) != minArgs+2 || strings.ToUpper(string(args[minArgs].Str)) != "COUNT" {
			return types.NewError("ERR syntax error")
		}
		n, err := strconv.Atoi(string(args[minArgs+1].Str))
		if err != nil || n <= 0 {
			return types.NewError("ERR count should be greater than 0")
		}
		count = n
	}

	if ctx.ExecutingMulti || ctx.InScript {
		val := tryBLMPop(ctx, keys, popLeft, count, false)
		if val.IsNull {
			ctx.SkipReplication = true
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return types.RespNil
		}
		return val
	}

	// Initial attempt is strict (Redis returns WRONGTYPE before blocking).
	if val := tryBLMPop(ctx, keys, popLeft, count, false); !val.IsNull {
		return val
	}

	return blockOnListKeys(ctx, keys, timeoutSecs, func() (types.Value, bool) {
		val := tryBLMPop(ctx, keys, popLeft, count, true)
		return val, !val.IsNull
	})
}

func tryBLMPop(ctx *context.Context, keys []string, popLeft bool, count int, ignoreWrongType bool) types.Value {
	for _, key := range keys {
		obj, ok := ctx.Engine.GetCheckExpired(key)
		if !ok {
			continue
		}
		if obj.Type != object.TypeList {
			if ignoreWrongType {
				continue
			}
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		list := obj.Value.(*object.List)
		items := make([]types.Value, 0, count)
		for i := 0; i < count; i++ {
			var (
				v      []byte
				popped bool
			)
			if popLeft {
				v, popped = list.PopFront()
			} else {
				v, popped = list.PopBack()
			}
			if !popped {
				break
			}
			items = append(items, types.NewString(string(v)))
		}

		if len(items) == 0 {
			continue
		}

		if list.LLen() == 0 {
			ctx.Engine.Del(key)
		} else {
			ctx.SignalKey(key)
		}

		popCmd := "LPOP"
		if !popLeft {
			popCmd = "RPOP"
		}
		ctx.ReplicateAs = [][]byte{[]byte(popCmd), []byte(key), []byte(strconv.Itoa(len(items)))}

		return types.NewArray([]types.Value{
			types.NewString(key),
			types.NewArray(items),
		})
	}

	// Sentinel "not found" value for internal flow.
	return types.Value{Type: types.TypeArray, IsNull: true}
}
