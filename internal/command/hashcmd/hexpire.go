package hashcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type hexpireBase struct {
	name      string
	timeUnit  int64 // 1000 for seconds, 1 for ms
	isAt      bool  // true for EXPIREAT, false for EXPIRE
	isPersist bool
}

func (c hexpireBase) Name() string { return c.name }

func (c hexpireBase) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for '" + strings.ToLower(c.name) + "' command")
	}

	nowMs := ctx.NowMs()
	key := string(args[0].Str)
	idx := 1
	var expireTime int64
	var err error

	if !c.isPersist {
		val, err := strconv.ParseInt(string(args[idx].Str), 10, 64)
		if err != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}
		idx++
		if c.isAt {
			expireTime = val * c.timeUnit
		} else {
			expireTime = nowMs + (val * c.timeUnit)
		}
	} else {
		expireTime = 0
	}

	condition := 0 // 0=none, 1=NX, 2=XX, 3=GT, 4=LT
	if idx < len(args) {
		opt := strings.ToUpper(string(args[idx].Str))
		if opt == "NX" {
			condition = 1; idx++
		} else if opt == "XX" {
			condition = 2; idx++
		} else if opt == "GT" {
			condition = 3; idx++
		} else if opt == "LT" {
			condition = 4; idx++
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

	if len(args)-idx != numfields {
		return types.NewError("ERR syntax error")
	}

	fields := make([]string, numfields)
	for i := 0; i < numfields; i++ {
		fields[i] = string(args[idx+i].Str)
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		// Key does not exist: return -2 for all fields
		res := make([]types.Value, numfields)
		for i := 0; i < numfields; i++ {
			res[i] = types.NewInt(-2)
		}
		return types.NewArray(res)
	}

	if obj.Type != object.TypeHash {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	h := obj.Value.(*object.Hash)
	
	// Lazy expire before checking
	ExpireIfNeeded(ctx, key, h, fields, nowMs, true, true)
	
	// If hash is deleted by lazy expire
	if h.Len() == 0 {
		res := make([]types.Value, numfields)
		for i := 0; i < numfields; i++ {
			res[i] = types.NewInt(-2)
		}
		return types.NewArray(res)
	}

	results := make([]types.Value, numfields)
	anyModified := false
	ttlSetCount := 0
	ttlDeletedCount := 0
	ttlPersistCount := 0

	for i, field := range fields {
		if _, exists := h.Get(field); !exists {
			results[i] = types.NewInt(-2)
			continue
		}

		currentTTL := h.GetTTL(field)
		hasTTL := currentTTL > 0

		if !c.isPersist && expireTime <= nowMs {
			// Zero TTL or past -> delete field
			// Wait, redis returns 2 when field is deleted by HEXPIRE with past/0 ttl!
			if condition == 1 && hasTTL { results[i] = types.NewInt(0); continue }
			if condition == 2 && !hasTTL { results[i] = types.NewInt(0); continue }
			if condition == 3 && hasTTL && expireTime <= currentTTL { results[i] = types.NewInt(0); continue }
			if condition == 4 && (!hasTTL || expireTime >= currentTTL) { results[i] = types.NewInt(0); continue }
			
			h.Del(field)
			results[i] = types.NewInt(2)
			anyModified = true
			ttlDeletedCount++
			continue
		}

		if condition == 1 && hasTTL {
			results[i] = types.NewInt(0)
			continue
		}
		if condition == 2 && !hasTTL {
			results[i] = types.NewInt(0)
			continue
		}
		if condition == 3 {
			// GT: new expiration is greater than current one. 
			// Non-volatile field is treated as infinite TTL (so GT always fails).
			if !hasTTL {
				results[i] = types.NewInt(0)
				continue
			}
			if expireTime <= currentTTL {
				results[i] = types.NewInt(0)
				continue
			}
		}
		if condition == 4 {
			// LT: new expiration is less than current one.
			if hasTTL && expireTime >= currentTTL {
				results[i] = types.NewInt(0)
				continue
			}
		}

		if c.isPersist {
			if hasTTL {
				h.DelTTL(field)
				results[i] = types.NewInt(1)
				anyModified = true
				ttlPersistCount++
			} else {
				results[i] = types.NewInt(-1)
			}
		} else {
			h.SetTTL(field, expireTime)
			results[i] = types.NewInt(1)
			anyModified = true
			ttlSetCount++
		}
	}

	if anyModified {
		// Send keyspace notifications based on the actual operations performed.
		if ttlDeletedCount > 0 {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagHash), "hdel", key)
			ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hdel", key, fields)
		}
		if ttlSetCount > 0 {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagHash), "hexpire", key)
			ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hexpire", key, fields)
		}
		if ttlPersistCount > 0 {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagHash), "hpersist", key)
			ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hpersist", key, fields)
		}

		// If hash became empty after deletions, clean up
		if h.Len() == 0 {
			ctx.Engine.Del(key)
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
			ctx.Engine.LogCommand([][]byte{[]byte("DEL"), []byte(key)})
		}

		rawArgs := make([][]byte, 0, len(args)+1)
		rawArgs = append(rawArgs, []byte(c.name))
		for _, a := range args {
			rawArgs = append(rawArgs, a.Str)
		}
		ctx.Engine.LogCommand(rawArgs)
		ctx.SkipReplication = true
	}

	return types.NewArray(results)
}

type HExpireCommand struct{ hexpireBase }
func NewHExpireCommand() HExpireCommand { return HExpireCommand{hexpireBase{"HEXPIRE", 1000, false, false}} }

type HExpireAtCommand struct{ hexpireBase }
func NewHExpireAtCommand() HExpireAtCommand { return HExpireAtCommand{hexpireBase{"HEXPIREAT", 1000, true, false}} }

type HPExpireCommand struct{ hexpireBase }
func NewHPExpireCommand() HPExpireCommand { return HPExpireCommand{hexpireBase{"HPEXPIRE", 1, false, false}} }

type HPExpireAtCommand struct{ hexpireBase }
func NewHPExpireAtCommand() HPExpireAtCommand { return HPExpireAtCommand{hexpireBase{"HPEXPIREAT", 1, true, false}} }

type HPersistCommand struct{ hexpireBase }
func NewHPersistCommand() HPersistCommand { return HPersistCommand{hexpireBase{"HPERSIST", 1, false, true}} }
