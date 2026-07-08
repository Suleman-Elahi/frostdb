package hashcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type HSetExCommand struct{}

func (c HSetExCommand) Name() string { return "HSETEX" }

func (c HSetExCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 4 {
		return types.NewError("ERR wrong number of arguments for 'hsetex' command")
	}

	nowMs := ctx.NowMs()
	key := string(args[0].Str)
	idx := 1

	condition := 0 // 0=none, 1=FNX, 2=FXX
	opt := strings.ToUpper(string(args[idx].Str))
	if opt == "FNX" {
		condition = 1
		idx++
	} else if opt == "FXX" {
		condition = 2
		idx++
	}

	var expireTime int64
	var hasExpire bool
	var keepTTL bool
	var isRelative bool

	if idx < len(args) {
		opt = strings.ToUpper(string(args[idx].Str))
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
		} else if opt == "KEEPTTL" {
			idx++
			keepTTL = true
		}
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

	if len(args)-idx != numfields*2 {
		return types.NewError("ERR syntax error")
	}

	fields := make([]string, numfields)
	values := make([]string, numfields)
	for i := 0; i < numfields; i++ {
		fields[i] = string(args[idx+i*2].Str)
		values[i] = string(args[idx+i*2+1].Str)
	}

	obj, ok := ctx.Engine.Get(key)
	var h *object.Hash
	
	if ok {
		if obj.Type != object.TypeHash {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		h = obj.Value.(*object.Hash)
		expiredCount := ExpireIfNeeded(ctx, key, h, fields, nowMs, true, false)
		if condition == 2 && expiredCount > 0 {
			if h.Len() == 0 {
				ctx.Engine.Del(key)
				ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
				ctx.Engine.LogCommand([][]byte{[]byte("DEL"), []byte(key)})
			}
			return types.NewInt(0)
		}
	} else {
		if condition == 2 { // FXX
			return types.NewInt(0)
		}
		h = object.NewHash()
		ctx.Engine.Set(key, &object.Object{Type: object.TypeHash, Value: h})
	}
	hashBecameEmptyAfterLazyExpire := h.Len() == 0

	// For FNX and FXX: check all fields first
	if condition == 1 { // FNX
		for _, f := range fields {
			if _, exists := h.Get(f); exists {
				if hashBecameEmptyAfterLazyExpire {
					ctx.Engine.Del(key)
					ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
					ctx.Engine.LogCommand([][]byte{[]byte("DEL"), []byte(key)})
				}
				return types.NewInt(0) // If any exists, do not set anything
			}
		}
	} else if condition == 2 { // FXX
		for _, f := range fields {
			if _, exists := h.Get(f); !exists {
				if hashBecameEmptyAfterLazyExpire {
					ctx.Engine.Del(key)
					ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
					ctx.Engine.LogCommand([][]byte{[]byte("DEL"), []byte(key)})
				}
				return types.NewInt(0) // If any does not exist, do not set anything
			}
		}
	}

	// Now apply the updates
	ttlSetCount := 0
	ttlDeletedCount := 0
	for i, f := range fields {
		v := values[i]
		
		oldTTL := h.GetTTL(f)
		
		h.Set(f, v) // this clears the TTL inside object.Hash
		
		if keepTTL {
			if oldTTL > 0 {
				h.SetTTL(f, oldTTL)
			}
		} else if hasExpire {
			if isRelative && expireTime <= nowMs {
				h.Del(f)
				ttlDeletedCount++
			} else if expireTime <= nowMs {
				h.Del(f)
				ttlDeletedCount++
			} else {
				h.SetTTL(f, expireTime)
				ttlSetCount++
			}
		}
	}

	// Send keyspace notifications
	ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagHash), "hset", key)
	ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hset", key, fields)
	if ttlSetCount > 0 {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagHash), "hexpire", key)
		ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hexpire", key, fields)
	}
	if ttlDeletedCount > 0 {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagHash), "hdel", key)
		ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hdel", key, fields)
	}
	if h.Len() == 0 {
		ctx.Engine.Del(key)
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
		ctx.Engine.LogCommand([][]byte{[]byte("DEL"), []byte(key)})
	}

	rawArgs := make([][]byte, 0, len(args)+1)
	rawArgs = append(rawArgs, []byte("HSETEX"))
	for _, a := range args {
		rawArgs = append(rawArgs, a.Str)
	}
	ctx.Engine.LogCommand(rawArgs)
	ctx.SkipReplication = true

	// If FNX/FXX condition passes, all fields are set.
	return types.NewInt(1)
}
