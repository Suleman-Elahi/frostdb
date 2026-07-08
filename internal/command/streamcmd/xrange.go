package streamcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// XRangeCommand implements XRANGE key start end [COUNT count].
type XRangeCommand struct{}

func (c XRangeCommand) Name() string { return "XRANGE" }

func (c XRangeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'xrange' command")
	}

	key := string(args[0].Str)
	startStr := string(args[1].Str)
	endStr := string(args[2].Str)
	count := 0

	// Validate range IDs with context-awareness
	if err := validateStartRangeID(startStr); err != nil {
		return types.NewError(err.Error())
	}
	if err := validateEndRangeID(endStr); err != nil {
		return types.NewError(err.Error())
	}

	// Parse COUNT
	for i := 3; i < len(args); i++ {
		if strings.ToUpper(string(args[i].Str)) == "COUNT" && i+1 < len(args) {
			v, err := strconv.Atoi(string(args[i+1].Str))
			if err != nil || v < 0 {
				return types.NewError("ERR value is not an integer or out of range")
			}
			count = v
			break
		}
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.RespEmptyArr
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)
	entries := stream.Range(startStr, endStr, count)

	result := make([]types.Value, len(entries))
	for i, entry := range entries {
		result[i] = streamEntryValue(entry)
	}
	return types.NewArray(result)
}
