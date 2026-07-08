package hashcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type HGetExCommand struct{}

func (c HGetExCommand) Name() string { return "HGETEX" }

func (c HGetExCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 4 {
		return types.NewError("ERR wrong number of arguments for 'hgetex' command")
	}

	nowMs := ctx.NowMs()
	key := string(args[0].Str)
	idx := 1
	var expireTime int64
	var hasExpire bool
	var isPersist bool
	var isRelative bool

	opt := strings.ToUpper(string(args[idx].Str))
	if opt == "EX" {
		idx++
		if idx >= len(args) { return types.NewError("ERR syntax error") }
		val, err := strconv.ParseInt(string(args[idx].Str), 10, 64)
		if err != nil { return types.NewError("ERR value is not an integer or out of range") }
		idx++
		expireTime = nowMs + (val * 1000)
		hasExpire = true
		isRelative = true
	} else if opt == "PX" {
		idx++
		if idx >= len(args) { return types.NewError("ERR syntax error") }
		val, err := strconv.ParseInt(string(args[idx].Str), 10, 64)
		if err != nil { return types.NewError("ERR value is not an integer or out of range") }
		idx++
		expireTime = nowMs + val
		hasExpire = true
		isRelative = true
	} else if opt == "EXAT" {
		idx++
		if idx >= len(args) { return types.NewError("ERR syntax error") }
		val, err := strconv.ParseInt(string(args[idx].Str), 10, 64)
		if err != nil { return types.NewError("ERR value is not an integer or out of range") }
		idx++
		expireTime = val * 1000
		hasExpire = true
	} else if opt == "PXAT" {
		idx++
		if idx >= len(args) { return types.NewError("ERR syntax error") }
		val, err := strconv.ParseInt(string(args[idx].Str), 10, 64)
		if err != nil { return types.NewError("ERR value is not an integer or out of range") }
		idx++
		expireTime = val
		hasExpire = true
	} else if opt == "PERSIST" {
		idx++
		isPersist = true
		hasExpire = true
	}

	if idx >= len(args) || strings.ToUpper(string(args[idx].Str)) != "FIELDS" {
		return types.NewError("ERR syntax error")
	}
	idx++

	if idx >= len(args) {
		return types.NewError("ERR syntax error")
	}
	numfields, err := strconv.Atoi(string(args[idx].Str))
	if err != nil || numfields <= 0 {
		return types.NewError("ERR value is not an integer or out of range")
	}
	idx++

	if len(args)-idx != numfields {
		return types.NewError("ERR syntax error")
	}

	fields := make([]string, numfields)
	for i := 0; i < numfields; i++ {
		fields[i] = string(args[idx+i].Str)
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		res := make([]types.Value, numfields)
		for i := 0; i < numfields; i++ {
			res[i] = types.RespNil
		}
		return types.NewArray(res)
	}

	if obj.Type != object.TypeHash {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	h := obj.Value.(*object.Hash)
	
	ExpireIfNeeded(ctx, key, h, fields, nowMs, true, true)

	results := make([]types.Value, numfields)
	anyModified := false
	ttlSetCount := 0
	ttlDeletedCount := 0
	ttlPersistCount := 0

	for i, field := range fields {
		val, exists := h.Get(field)
		if !exists {
			results[i] = types.RespNil
			continue
		}

		results[i] = types.NewString(val)

		if hasExpire {
			if isPersist {
				if h.GetTTL(field) > 0 {
					h.DelTTL(field)
					anyModified = true
					ttlPersistCount++
				}
			} else {
				if isRelative && expireTime <= nowMs {
					h.Del(field)
					results[i] = types.RespNil
					anyModified = true
					ttlDeletedCount++
				} else if expireTime <= nowMs {
					h.Del(field)
					results[i] = types.RespNil
					anyModified = true
					ttlDeletedCount++
				} else {
					h.SetTTL(field, expireTime)
					anyModified = true
					ttlSetCount++
				}
			}
		}
	}

	if anyModified {
		if ttlSetCount > 0 {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagHash), "hexpire", key)
			ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hexpire", key, fields)
		}
		if ttlDeletedCount > 0 {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagHash), "hdel", key)
			ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hdel", key, fields)
		}
		if ttlPersistCount > 0 {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagHash), "hpersist", key)
			ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hpersist", key, fields)
		}
		if h.Len() == 0 {
			ctx.Engine.Del(key)
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
			ctx.Engine.LogCommand([][]byte{[]byte("DEL"), []byte(key)})
		}

		rawArgs := make([][]byte, 0, len(args)+1)
		rawArgs = append(rawArgs, []byte("HGETEX"))
		for _, a := range args {
			rawArgs = append(rawArgs, a.Str)
		}
		ctx.Engine.LogCommand(rawArgs)
		ctx.SkipReplication = true
	}

	return types.NewArray(results)
}
