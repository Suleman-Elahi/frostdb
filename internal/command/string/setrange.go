package str

import (
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// maxStringSize is Redis's 512 MiB limit for string values.
const maxStringSize = 512 * 1024 * 1024 // 512 MiB

// SetRangeCommand implements SETRANGE key offset value.
type SetRangeCommand struct{}

func (c SetRangeCommand) Name() string { return "SETRANGE" }

func (c SetRangeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'setrange' command")
	}

	key := string(args[0].Str)

	// Use ParseInt with int64 to catch values that overflow int on 32-bit and
	// also catch the max-int64 edge case before we do any arithmetic.
	offset64, err := parseInt(args[1].Str)
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}
	if offset64 < 0 {
		return types.NewError("ERR offset is out of range")
	}
	// Guard: offset alone already exceeds the 512 MiB limit.
	if offset64 >= maxStringSize {
		return types.NewError("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
	}
	offset := int(offset64)

	value := string(args[2].Str)

	var current string
	obj, ok := ctx.Engine.Get(key)
	if ok {
		if obj.Type != object.TypeString {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		current, _ = obj.Value.(string)
	}

	// If key doesn't exist and value is empty, return 0 without creating key.
	if !ok && len(value) == 0 {
		return types.RespZero
	}

	// Check resulting string length against the 512 MiB limit.
	needed := offset + len(value)
	if needed > maxStringSize {
		return types.NewError("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
	}

	// Extend with zero bytes if needed.
	if needed > len(current) {
		var b strings.Builder
		b.Grow(needed)
		b.WriteString(current)
		for i := len(current); i < needed; i++ {
			b.WriteByte(0)
		}
		current = b.String()
	}

	// Overwrite at offset.
	result := current[:offset] + value + current[offset+len(value):]

	var newObj *object.Object
	if ok {
		if len(value) > 0 {
			// Actual content change: Redis always produces raw encoding
			// (assert_encoding raw checks after SETRANGE on int-encoded key).
			newObj = object.AcquireRawStringObject(result)
		} else {
			// Empty value: no content change; preserve existing encoding.
			newObj = object.AcquireStringObject(result)
		}
		newObj.ExpireAt = obj.ExpireAt // preserve TTL
	} else {
		newObj = object.AcquireStringObject(result)
	}
	ctx.Engine.Set(key, newObj)

	return types.NewInt(int64(len(result)))
}
