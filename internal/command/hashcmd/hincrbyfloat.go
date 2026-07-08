package hashcmd

import (
	"math"
	"strconv"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type HIncrByFloatCommand struct{}

func (c HIncrByFloatCommand) Name() string { return "HINCRBYFLOAT" }

func (c HIncrByFloatCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'hincrbyfloat' command")
	}

	key := string(args[0].Str)
	field := string(args[1].Str)
	incrStr := string(args[2].Str)

	incr, err := strconv.ParseFloat(incrStr, 64)
	if err != nil {
		return types.NewError("ERR value is not a valid float")
	}

	// Reject NaN and Infinity as increment values
	if math.IsNaN(incr) || math.IsInf(incr, 0) {
		return types.NewError("ERR value is NaN or Infinity")
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
	var val float64
	if exists {
		val, err = strconv.ParseFloat(valStr, 64)
		if err != nil {
			return types.NewError("ERR hash value is not a valid float")
		}
	}
	
	newVal := val + incr

	// Reject results that are NaN or Infinity
	if math.IsNaN(newVal) || math.IsInf(newVal, 0) {
		return types.NewError("ERR value is NaN or Infinity")
	}

	newValStr := strconv.FormatFloat(newVal, 'f', -1, 64)
	hash.Set(field, newValStr)
	ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hincrbyfloat", key, []string{field})

	// WAL logging
	ctx.Engine.LogCommand([][]byte{
		[]byte("HINCRBYFLOAT"),
		args[0].Str,
		args[1].Str,
		args[2].Str,
	}) //nolint:errcheck // error logged internally
	ctx.SkipReplication = true

	return types.NewString(newValStr)
}
