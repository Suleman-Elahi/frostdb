package hashcmd

import (
	"math"
	"strconv"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type HIncrByCommand struct{}

func (c HIncrByCommand) Name() string { return "HINCRBY" }

func (c HIncrByCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'hincrby' command")
	}

	key := string(args[0].Str)
	field := string(args[1].Str)
	incrStr := string(args[2].Str)

	incr, err := strconv.ParseInt(incrStr, 10, 64)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	obj, ok := ctx.Engine.Get(key)
	var hash *object.Hash
	
	if ok {
		if obj.Type != object.TypeHash {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		hash = obj.Value.(*object.Hash)
	} else {
		hash = object.NewHash()
		obj = &object.Object{
			Type:     object.TypeHash,
			Encoding: object.EncodingRaw,
			Value:    hash,
		}
		ctx.Engine.Set(key, obj)
	}

	ExpireIfNeeded(ctx, key, hash, []string{field}, ctx.NowMs(), false, true)

	valStr, exists := hash.Get(field)
	var val int64
	if exists {
		val, err = strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return types.NewError("ERR hash value is not an integer")
		}
	}

	// Overflow detection: check if val + incr would overflow int64
	if (incr > 0 && val > math.MaxInt64-incr) || (incr < 0 && val < math.MinInt64-incr) {
		return types.NewError("ERR increment or decrement would overflow")
	}
	
	newVal := val + incr
	hash.Set(field, strconv.FormatInt(newVal, 10))
	ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hincrby", key, []string{field})

	// WAL logging
	ctx.Engine.LogCommand([][]byte{
		[]byte("HINCRBY"),
		args[0].Str,
		args[1].Str,
		args[2].Str,
	}) //nolint:errcheck // error logged internally
	ctx.SkipReplication = true

	return types.NewInt(newVal)
}
