package listcmd

import (
	"math"
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// LPosCommand implements LPOS key element [RANK rank] [COUNT num] [MAXLEN len].
type LPosCommand struct{}

func (c LPosCommand) Name() string { return "LPOS" }

func (c LPosCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'lpos' command")
	}

	key := string(args[0].Str)
	element := string(args[1].Str)

	rank := 1
	count := -1 // -1 means no COUNT specified
	maxlen := 0

	// Parse options
	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i].Str))
		switch opt {
		case "RANK":
			if i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			i++
			v64, err := strconv.ParseInt(string(args[i].Str), 10, 64)
			if err != nil || v64 == math.MinInt64 {
				return types.NewError("ERR value is out of range")
			}
			if v64 == 0 {
				return types.NewError("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list")
			}
			rank = int(v64)
		case "COUNT":
			if i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			i++
			v, err := strconv.Atoi(string(args[i].Str))
			if err != nil || v < 0 {
				return types.NewError("ERR COUNT can't be negative")
			}
			count = v
		case "MAXLEN":
			if i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			i++
			v, err := strconv.Atoi(string(args[i].Str))
			if err != nil || v < 0 {
				return types.NewError("ERR MAXLEN can't be negative")
			}
			maxlen = v
		default:
			return types.NewError("ERR syntax error")
		}
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		if count >= 0 {
			return types.RespEmptyArr
		}
		return types.RespNil
	}
	if obj.Type != object.TypeList {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	l := obj.Value.(*object.List)
	matches := l.FindPositions(element, rank, count, maxlen)

	if count >= 0 {
		result := make([]types.Value, len(matches))
		for i, pos := range matches {
			result[i] = types.NewInt(int64(pos))
		}
		return types.NewArray(result)
	}

	// No COUNT: return first match or nil
	if len(matches) == 0 {
		return types.RespNil
	}
	return types.NewInt(int64(matches[0]))
}
