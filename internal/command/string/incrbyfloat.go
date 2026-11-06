package str

import (
	"math"
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// IncrByFloatCommand implements INCRBYFLOAT key increment
type IncrByFloatCommand struct{}

func (c IncrByFloatCommand) Name() string { return "INCRBYFLOAT" }

func (c IncrByFloatCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'incrbyfloat' command")
	}

	key := string(args[0].Str)
	incr, err := strconv.ParseFloat(string(args[1].Str), 64)
	if err != nil {
		return types.NewError("ERR value is not a valid float")
	}

	if isInfOrNaN(incr) {
		return types.NewError("ERR increment would produce NaN or Infinity")
	}

	var current float64
	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if ok {
		strVal, isStr := obj.Value.([]byte)
		if !isStr {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		current, err = strconv.ParseFloat(string(strVal), 64)
		if err != nil {
			return types.NewError("ERR value is not a valid float")
		}
		if isInfOrNaN(current) {
			return types.NewError("ERR increment would produce NaN or Infinity")
		}
	}

	current += incr

	if isInfOrNaN(current) {
		return types.NewError("ERR increment would produce NaN or Infinity")
	}

	var result string
	if current == 0 {
		result = "0"
	} else {
		result = strconv.FormatFloat(current, 'f', -1, 64)
	}

	newObj := object.AcquireStringObject(result)
	if ok {
		newObj.ExpireAt = obj.ExpireAt
	}
	ctx.Engine.Set(key, newObj)

	ctx.ReplicateAs = [][]byte{
		[]byte("SET"),
		[]byte(key),
		[]byte(result),
		[]byte("KEEPTTL"),
	}

	return types.NewString(result)
}

func isInfOrNaN(f float64) bool {
	return math.IsInf(f, 0) || math.IsNaN(f)
}
