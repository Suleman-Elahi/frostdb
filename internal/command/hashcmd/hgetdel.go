package hashcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// HGetDelCommand implements HGETDEL key FIELDS numfields field [field ...]
type HGetDelCommand struct{}

func (c HGetDelCommand) Name() string { return "HGETDEL" }

func (c HGetDelCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	// Minimum: key FIELDS numfields field → 4 args
	if len(args) < 4 {
		return types.NewError("ERR wrong number of arguments for 'hgetdel' command")
	}

	keyword := strings.ToUpper(string(args[1].Str))
	if keyword != "FIELDS" {
		return types.NewError("ERR mandatory argument FIELDS is missing or not in the right position")
	}

	// Parse numfields
	numfields, err := strconv.ParseInt(string(args[2].Str), 10, 64)
	if err != nil || numfields <= 0 {
		return types.NewError("ERR Number of fields must be a positive integer")
	}

	// Validate that numfields matches the actual number of field arguments
	actualFields := len(args) - 3
	if int64(actualFields) != numfields {
		return types.NewError("ERR The `numfields` parameter must match the number of arguments")
	}

	// Extract field names
	fields := make([]string, actualFields)
	for i := 0; i < actualFields; i++ {
		fields[i] = string(args[3+i].Str)
	}

	// Look up the key
	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		// Key doesn't exist — return nil for each field
		result := make([]types.Value, len(fields))
		for i := range result {
			result[i] = types.RespNil
		}
		return types.NewArray(result)
	}

	if obj.Type != object.TypeHash {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	h := obj.Value.(*object.Hash)
	
	ExpireIfNeeded(ctx, key, h, fields, ctx.NowMs(), true, true)

	// Get values and delete existing fields
	result := make([]types.Value, len(fields))
	var deletedFields []string
	for i, field := range fields {
		val, exists := h.Get(field)
		if exists {
			result[i] = types.NewString(val)
			h.Del(field)
			deletedFields = append(deletedFields, field)
		} else {
			result[i] = types.RespNil
		}
	}

	// Clean up empty hash and send notifications
	hashBecameEmpty := h.Len() == 0
	if hashBecameEmpty {
		ctx.Engine.Del(key)
	}

	// WAL: replicate as HDEL with all requested fields when at least one was deleted.
	// Redis includes non-existing fields in the replication stream.
	if len(deletedFields) > 0 {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagHash), "hdel", key)
		ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hdel", key, fields)
		if hashBecameEmpty {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagGeneric), "del", key)
		}
		replArgs := make([][]byte, 0, 2+len(fields))
		replArgs = append(replArgs, []byte("HDEL"), args[0].Str)
		for _, f := range fields {
			replArgs = append(replArgs, []byte(f))
		}
		ctx.Engine.LogCommand(replArgs)
	}
	ctx.SkipReplication = true

	return types.NewArray(result)
}
