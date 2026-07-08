package str

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// BitFieldCommand implements BITFIELD.
type BitFieldCommand struct{}

func (c BitFieldCommand) Name() string { return "BITFIELD" }

func (c BitFieldCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return executeBitfield(ctx, args, false)
}

// BitFieldROCommand implements BITFIELD_RO (read-only BITFIELD).
type BitFieldROCommand struct{}

func (c BitFieldROCommand) Name() string { return "BITFIELD_RO" }

func (c BitFieldROCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	return executeBitfield(ctx, args, true)
}

func executeBitfield(ctx *context.Context, args []types.Value, readOnly bool) types.Value {
	if len(args) < 1 {
		if readOnly {
			return types.NewError("ERR wrong number of arguments for 'bitfield_ro' command")
		}
		return types.NewError("ERR wrong number of arguments for 'bitfield' command")
	}

	key := string(args[0].Str)
	var data []byte
	obj, exists := ctx.Engine.Get(key)
	if exists {
		if obj.Type != object.TypeString {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		s, ok := obj.Value.(string)
		if !ok {
			return types.NewError("ERR internal string encoding")
		}
		data = []byte(s)
	}

	mode := overflowWrap
	out := make([]types.Value, 0, 4)
	dirty := false

	i := 1
	for i < len(args) {
		token := strings.ToUpper(string(args[i].Str))
		switch token {
		case "OVERFLOW":
			if readOnly {
				return types.NewError("ERR BITFIELD_RO only supports the GET subcommand")
			}
			if i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			m, ok := parseOverflowMode(string(args[i+1].Str))
			if !ok {
				return types.NewError("ERR syntax error")
			}
			mode = m
			i += 2
		case "GET":
			if i+2 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			enc, ok := parseBitfieldEncoding(string(args[i+1].Str))
			if !ok {
				return types.NewError("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
			}
			offset, ok := parseBitfieldOffset(string(args[i+2].Str), enc.width)
			if !ok || offset > maxBitOffset || offset+int64(enc.width)-1 > maxBitOffset {
				return types.NewError("ERR bit offset is not an integer or out of range")
			}

			raw := readUnsignedBits(data, offset, enc.width)
			if enc.signed {
				out = append(out, types.NewInt(decodeSigned(raw, enc.width)))
			} else {
				out = append(out, types.NewInt(int64(raw)))
			}
			i += 3
		case "SET":
			if readOnly {
				return types.NewError("ERR BITFIELD_RO only supports the GET subcommand")
			}
			if i+3 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			enc, ok := parseBitfieldEncoding(string(args[i+1].Str))
			if !ok {
				return types.NewError("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
			}
			offset, ok := parseBitfieldOffset(string(args[i+2].Str), enc.width)
			if !ok || offset > maxBitOffset || offset+int64(enc.width)-1 > maxBitOffset {
				return types.NewError("ERR bit offset is not an integer or out of range")
			}
			input, err := strconv.ParseInt(string(args[i+3].Str), 10, 64)
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}

			oldRaw := readUnsignedBits(data, offset, enc.width)
			oldVal := int64(oldRaw)
			if enc.signed {
				oldVal = decodeSigned(oldRaw, enc.width)
			}

			newVal, apply := applyModeToSet(enc, mode, input)
			if !apply {
				out = append(out, types.RespNil)
				i += 4
				continue
			}

			targetByteLen := int((offset+int64(enc.width)-1)/8 + 1)
			resized := len(data) < targetByteLen
			data, ok = ensureByteLen(data, targetByteLen)
			if !ok {
				return types.NewError("ERR string exceeds maximum allowed size (512MB)")
			}
			writeUnsignedBits(data, offset, enc.width, encodeUnsignedFromSigned(newVal, enc.width))
			if !exists || resized || newVal != oldVal {
				dirty = true
			}
			out = append(out, types.NewInt(oldVal))
			i += 4
		case "INCRBY":
			if readOnly {
				return types.NewError("ERR BITFIELD_RO only supports the GET subcommand")
			}
			if i+3 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			enc, ok := parseBitfieldEncoding(string(args[i+1].Str))
			if !ok {
				return types.NewError("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
			}
			offset, ok := parseBitfieldOffset(string(args[i+2].Str), enc.width)
			if !ok || offset > maxBitOffset || offset+int64(enc.width)-1 > maxBitOffset {
				return types.NewError("ERR bit offset is not an integer or out of range")
			}
			incr, err := strconv.ParseInt(string(args[i+3].Str), 10, 64)
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}

			oldRaw := readUnsignedBits(data, offset, enc.width)
			oldVal := int64(oldRaw)
			if enc.signed {
				oldVal = decodeSigned(oldRaw, enc.width)
			}

			newVal, apply := applyModeToIncr(enc, mode, oldVal, incr)
			if !apply {
				out = append(out, types.RespNil)
				i += 4
				continue
			}

			targetByteLen := int((offset+int64(enc.width)-1)/8 + 1)
			var resized bool
			data, resized = ensureByteLen(data, targetByteLen)
			if !resized {
				return types.NewError("ERR string exceeds maximum allowed size (512MB)")
			}
			writeUnsignedBits(data, offset, enc.width, encodeUnsignedFromSigned(newVal, enc.width))
			dirty = true
			out = append(out, types.NewInt(newVal))
			i += 4
		default:
			return types.NewError("ERR syntax error")
		}
	}

	if dirty {
		ctx.Engine.Set(key, object.AcquireStringObject(string(data)))
	}

	return types.NewArray(out)
}

func signedRange(width int) (int64, int64) {
	if width == 64 {
		return -(1 << 63), (1 << 63) - 1
	}
	max := int64(1<<(width-1)) - 1
	min := -int64(1 << (width - 1))
	return min, max
}

func unsignedRange(width int) (int64, int64) {
	max := int64((uint64(1) << width) - 1)
	return 0, max
}

func applyModeToSet(enc bitfieldEncoding, mode overflowMode, input int64) (int64, bool) {
	min, max := unsignedRange(enc.width)
	if enc.signed {
		min, max = signedRange(enc.width)
	}
	if input >= min && input <= max {
		return input, true
	}

	switch mode {
	case overflowFail:
		return 0, false
	case overflowSat:
		if input < min {
			return min, true
		}
		return max, true
	default:
		return wrapToWidth(enc, input), true
	}
}

func applyModeToIncr(enc bitfieldEncoding, mode overflowMode, oldVal, incr int64) (int64, bool) {
	min, max := unsignedRange(enc.width)
	if enc.signed {
		min, max = signedRange(enc.width)
	}

	sum, overflowed := addInt64(oldVal, incr)
	if !overflowed && sum >= min && sum <= max {
		return sum, true
	}

	switch mode {
	case overflowFail:
		return 0, false
	case overflowSat:
		if overflowed {
			if incr >= 0 {
				return max, true
			}
			return min, true
		}
		if sum < min {
			return min, true
		}
		return max, true
	default:
		return wrapToWidth(enc, oldVal+incr), true
	}
}

func wrapToWidth(enc bitfieldEncoding, v int64) int64 {
	if enc.signed {
		return decodeSigned(encodeUnsignedFromSigned(v, enc.width), enc.width)
	}
	return int64(encodeUnsignedFromSigned(v, enc.width))
}

func addInt64(a, b int64) (int64, bool) {
	c := a + b
	if (b > 0 && c < a) || (b < 0 && c > a) {
		return 0, true
	}
	return c, false
}
