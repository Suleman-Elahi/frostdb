package zsetcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ZRangeStoreCommand implements ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
type ZRangeStoreCommand struct{}

func (c ZRangeStoreCommand) Name() string { return "ZRANGESTORE" }

func (c ZRangeStoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 4 {
		return types.NewError("ERR wrong number of arguments for 'zrangestore' command")
	}

	dstKey := string(args[0].Str)
	srcKey := string(args[1].Str)
	startStr := string(args[2].Str)
	stopStr := string(args[3].Str)

	byScore := false
	byLex := false
	rev := false
	limitOffset := 0
	limitCount := -1
	hasLimit := false

	for i := 4; i < len(args); i++ {
		flag := strings.ToUpper(string(args[i].Str))
		switch flag {
		case "BYSCORE":
			byScore = true
		case "BYLEX":
			byLex = true
		case "REV":
			rev = true
		case "LIMIT":
			if !byScore && !byLex {
				return types.NewError("ERR syntax error")
			}
			if i+2 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			var err error
			limitOffset, err = strconv.Atoi(string(args[i+1].Str))
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			limitCount, err = strconv.Atoi(string(args[i+2].Str))
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			hasLimit = true
			i += 2
		case "WITHSCORES":
			return types.NewError("ERR syntax error")
		default:
			return types.NewError("ERR syntax error")
		}
	}

	if hasLimit && !byScore && !byLex {
		return types.NewError("ERR syntax error")
	}

	obj, ok := ctx.Engine.Get(srcKey)
	if !ok {
		ctx.Engine.Del(dstKey)
		return types.NewInt(0)
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	var entries []object.ZSetEntry

	if byLex {
		if rev {
			// REV with BYLEX: first bound is max, second is min
			max, maxExcl, err := object.ParseLexBound(startStr)
			if err != nil {
				return types.NewError("ERR min or max not valid string range item")
			}
			min, minExcl, err := object.ParseLexBound(stopStr)
			if err != nil {
				return types.NewError("ERR min or max not valid string range item")
			}
			entries = zset.RangeByLex(min, max, minExcl, maxExcl)
			reverseEntries(entries)
		} else {
			min, minExcl, err := object.ParseLexBound(startStr)
			if err != nil {
				return types.NewError("ERR min or max not valid string range item")
			}
			max, maxExcl, err := object.ParseLexBound(stopStr)
			if err != nil {
				return types.NewError("ERR min or max not valid string range item")
			}
			entries = zset.RangeByLex(min, max, minExcl, maxExcl)
		}
	} else if byScore {
		if rev {
			// REV with BYSCORE: first bound is max, second is min
			max, maxExcl := parseScoreBound(startStr, true)
			min, minExcl := parseScoreBound(stopStr, false)
			entries = zset.RangeByScore(min, max, minExcl, maxExcl)
			reverseEntries(entries)
		} else {
			min, minExcl := parseScoreBound(startStr, false)
			max, maxExcl := parseScoreBound(stopStr, true)
			entries = zset.RangeByScore(min, max, minExcl, maxExcl)
		}
	} else {
		start, err1 := strconv.Atoi(startStr)
		stop, err2 := strconv.Atoi(stopStr)
		if err1 != nil || err2 != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}

		if rev {
			all := zset.RangeByIndex(0, -1)
			reverseEntries(all)
			length := len(all)
			if start < 0 {
				start = length + start
			}
			if stop < 0 {
				stop = length + stop
			}
			if start < 0 {
				start = 0
			}
			if stop >= length {
				stop = length - 1
			}
			if start > stop || start >= length {
				entries = nil
			} else {
				entries = all[start : stop+1]
			}
		} else {
			entries = zset.RangeByIndex(start, stop)
		}
	}

	// Apply LIMIT
	if hasLimit {
		if limitOffset < 0 || limitCount < 0 {
			entries = nil
		} else {
			if limitOffset > 0 {
				if limitOffset >= len(entries) {
					entries = nil
				} else {
					entries = entries[limitOffset:]
				}
			}
			if limitCount >= 0 && limitCount < len(entries) {
				entries = entries[:limitCount]
			}
		}
	}

	if len(entries) == 0 {
		ctx.Engine.Del(dstKey)
		return types.NewInt(0)
	}

	resZSet := object.NewZSet()
	for _, e := range entries {
		resZSet.Add(e.Score, e.Member)
	}
	oldObj, existed := ctx.Engine.Get(dstKey)
	ctx.Engine.Set(dstKey, &object.Object{
		Type:     object.TypeZSet,
		Encoding: object.EncodingRaw,
		Value:    resZSet,
	})
	if existed && oldObj != nil {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagOverwrite), "overwritten", dstKey)
		if oldObj.Type != object.TypeZSet {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagTypeChanged), "type_changed", dstKey)
		}
	}
	ctx.SignalKey(dstKey)

	return types.NewInt(int64(len(entries)))
}
