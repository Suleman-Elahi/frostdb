package str

import (
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// GetRangeCommand implements GETRANGE key start end.
type GetRangeCommand struct{}

func (c GetRangeCommand) Name() string { return "GETRANGE" }

func (c GetRangeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'getrange' command")
	}

	key := string(args[0].Str)
	start, err1 := strconv.Atoi(string(args[1].Str))
	end, err2 := strconv.Atoi(string(args[2].Str))
	if err1 != nil || err2 != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewString("")
	}
	if obj.Type != object.TypeString {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	s, _ := obj.Value.([]byte)
	length := len(s)

	// Step 1: If both indices are negative AND start > end, return empty
	// (mirrors Redis: if (start < 0 && end < 0 && start > end) return empty)
	if start < 0 && end < 0 && start > end {
		return types.NewString("")
	}

	// Step 2: Normalize negative indices
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	// Step 3: Clamp both to 0 if still negative after normalization
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}

	// Step 4: Clamp end to last valid index
	if end >= length {
		end = length - 1
	}

	// Step 5: Final range validity check
	if start > end || length == 0 {
		return types.NewString("")
	}

	return types.NewBytes(s[start : end+1])
}

// SubstrCommand is an alias for GETRANGE (deprecated since Redis 2.0).
type SubstrCommand struct{}

func (c SubstrCommand) Name() string { return "SUBSTR" }

func (c SubstrCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return (GetRangeCommand{}).Execute(ctx, args)
}
