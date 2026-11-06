package streamcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type XRevRangeCommand struct{}

func (c XRevRangeCommand) Name() string { return "XREVRANGE" }

func (c XRevRangeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'xrevrange' command")
	}

	key := string(args[0].Str)
	endStr := string(args[1].Str)   // 'end' is the first id conceptually because it's reversed (highest ID first)
	startStr := string(args[2].Str) // 'start' is the lower ID

	if err := validateEndRangeID(endStr); err != nil {
		return types.NewError(err.Error())
	}
	if err := validateStartRangeID(startStr); err != nil {
		return types.NewError(err.Error())
	}

	count := 0
	hasCount := false
	for i := 3; i < len(args); i++ {
		if strings.ToUpper(string(args[i].Str)) == "COUNT" && i+1 < len(args) {
			parsed, err := strconv.Atoi(string(args[i+1].Str))
			if err != nil || parsed < 0 {
				return types.NewError("ERR value is not an integer or out of range")
			}
			count = parsed
			hasCount = true
			i++
		}
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewArray([]types.Value{})
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if hasCount && count == 0 {
		return types.NewArray([]types.Value{})
	}

	stream := obj.Value.(*object.Stream)
	entries := stream.RevRange(endStr, startStr, count)

	result := make([]types.Value, 0, len(entries))
	for _, entry := range entries {
		result = append(result, streamEntryValue(entry))
	}

	return types.NewArray(result)
}
