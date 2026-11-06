package str

import (
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// AppendCommand implements APPEND key value
type AppendCommand struct{}

func (c AppendCommand) Name() string { return "APPEND" }

func (c AppendCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'append' command")
	}

	key := string(args[0].Str)
	appendVal := args[1].Str

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		// Brand-new key: encoding follows normal rules (int if numeric).
		// appendVal is safe to store directly: the RESP parser hands back a
		// freshly-owned []byte per bulk argument that is never reused or
		// mutated afterward (see parser.go's bulkPool usage), so the Object
		// can take ownership of it with no copy.
		ctx.Engine.Set(key, object.AcquireStringObject(appendVal))
		return types.NewInt(int64(len(appendVal)))
	}

	strVal, isStr := obj.Value.([]byte)
	if !isStr {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	newVal := make([]byte, 0, len(strVal)+len(appendVal))
	newVal = append(newVal, strVal...)
	newVal = append(newVal, appendVal...)
	// Redis forces EncodingRaw after APPEND — even if the result looks like an
	// integer, the encoding stays raw (SDS-backed string).
	newObj := object.AcquireRawStringObject(newVal)
	newObj.ExpireAt = obj.ExpireAt
	ctx.Engine.Set(key, newObj)
	return types.NewInt(int64(len(newVal)))
}

// StrlenCommand implements STRLEN key
type StrlenCommand struct{}

func (c StrlenCommand) Name() string { return "STRLEN" }

func (c StrlenCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewError("ERR wrong number of arguments for 'strlen' command")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		return types.RespZero
	}

	strVal, isStr := obj.Value.([]byte)
	if !isStr {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return types.NewInt(int64(len(strVal)))
}
