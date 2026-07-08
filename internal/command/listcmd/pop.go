package listcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type PopCommand struct {
	IsLeft bool
}

func (c PopCommand) Name() string {
	if c.IsLeft {
		return "LPOP"
	}
	return "RPOP"
}

func (c PopCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 && len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for '" + strings.ToLower(c.Name()) + "' command")
	}

	key := string(args[0].Str)

	withCount := len(args) == 2
	count := 1
	if withCount {
		n, err := strconv.Atoi(string(args[1].Str))
		if err != nil || n < 0 {
			return types.NewError("ERR value is out of range, must be positive")
		}
		count = n
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		if ctx.Protocol >= 3 {
			return types.NewNull()
		}
		if withCount {
			return types.Value{Type: types.TypeArray, IsNull: true}
		}
		return types.RespNil
	}

	if obj.Type != object.TypeList {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := obj.Value.(*object.List)
	if withCount {
		if count == 0 {
			return types.RespEmptyArr
		}
		out := make([]types.Value, 0, count)
		for i := 0; i < count; i++ {
			var (
				val    []byte
				popped bool
			)
			if c.IsLeft {
				val, popped = list.PopFront()
			} else {
				val, popped = list.PopBack()
			}
			if !popped {
				break
			}
			out = append(out, types.NewString(string(val)))
		}
		if list.LLen() == 0 {
			ctx.Engine.Del(key)
		}
		return types.NewArray(out)
	}

	var (
		val    []byte
		popped bool
	)
	if c.IsLeft {
		val, popped = list.PopFront()
	} else {
		val, popped = list.PopBack()
	}
	if !popped {
		return types.RespNil
	}

	if list.LLen() == 0 {
		ctx.Engine.Del(key)
	}

	return types.NewString(string(val))
}
