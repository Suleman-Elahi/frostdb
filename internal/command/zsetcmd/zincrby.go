package zsetcmd

import (
	"math"
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type ZIncrByCommand struct{}

func (c ZIncrByCommand) Name() string { return "ZINCRBY" }

func (c ZIncrByCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'zincrby' command")
	}

	key := string(args[0].Str)
	incrStr := string(args[1].Str)
	member := string(args[2].Str)

	increment, err := strconv.ParseFloat(incrStr, 64)
	if err != nil || math.IsNaN(increment) {
		return types.NewError("ERR value is not a valid float")
	}

	obj, ok := ctx.Engine.Get(key)
	var zset *object.ZSet

	if ok {
		if obj.Type != object.TypeZSet {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		zset = obj.Value.(*object.ZSet)
	} else {
		zset = object.NewZSet()
		obj = &object.Object{
			Type:     object.TypeZSet,
			Encoding: object.EncodingRaw,
			Value:    zset,
		}
		ctx.Engine.Set(key, obj)
	}

	oldScore, _ := zset.Score(member)
	newScore := oldScore + increment
	if math.IsNaN(newScore) {
		return types.NewError("ERR resulting score is not a number (NaN)")
	}

	zset.Add(newScore, member)

	return types.NewString(formatScore(newScore))
}
