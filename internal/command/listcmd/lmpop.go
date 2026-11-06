package listcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// LMPOP numkeys key [key ...] <LEFT|RIGHT> [COUNT count]
type LMPOPCommand struct{}

func (c LMPOPCommand) Name() string { return "LMPOP" }

func (c LMPOPCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'lmpop' command")
	}

	numKeys, err := strconv.Atoi(string(args[0].Str))
	if err != nil || numKeys <= 0 {
		return types.NewError("ERR numkeys should be greater than 0")
	}

	minArgs := 1 + numKeys + 1
	if len(args) < minArgs {
		return types.NewError("ERR syntax error")
	}

	keys := make([]string, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		keys = append(keys, string(args[1+i].Str))
	}

	dir := strings.ToUpper(string(args[1+numKeys].Str))
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

	for _, key := range keys {
		obj, ok := ctx.Engine.Get(key)
		if !ok {
			continue
		}
		if obj.Type != object.TypeList {
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

	ctx.SkipReplication = true
	if ctx.Protocol >= 3 {
		return types.NewNull()
	}
	return types.RespNil
}
