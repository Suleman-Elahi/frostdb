package str

import (
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// SetBitCommand implements SETBIT key offset value
type SetBitCommand struct{}

func (c SetBitCommand) Name() string { return "SETBIT" }

func (c SetBitCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'setbit' command")
	}

	offset, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil || offset < 0 {
		return types.NewError("ERR bit offset is not an integer or out of range")
	}
	// Max offset is 2^32 - 1 (512MB bitmap)
	if offset > 4294967295 {
		return types.NewError("ERR bit offset is not an integer or out of range")
	}

	value, err := strconv.ParseInt(string(args[2].Str), 10, 64)
	if err != nil || (value != 0 && value != 1) {
		return types.NewError("ERR bit is not an integer or out of range")
	}

	key := string(args[0].Str)
	byteIndex := int(offset / 8)
	bitIndex := uint(7 - (offset % 8)) // MSB first

	// Get or create string
	var data []byte
	obj, exists := ctx.Engine.Get(key)
	if exists {
		strVal, ok := obj.Value.(string)
		if !ok {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		data = []byte(strVal)
	}

	// Extend string if needed
	origLen := len(data)
	if byteIndex >= len(data) {
		newData := make([]byte, byteIndex+1)
		copy(newData, data)
		data = newData
	}

	// Get old bit value
	oldBit := (data[byteIndex] >> bitIndex) & 1

	// Set new bit value
	if value == 1 {
		data[byteIndex] |= (1 << bitIndex)
	} else {
		data[byteIndex] &^= (1 << bitIndex)
	}

	// Only write to engine if the value actually changed
	if oldBit != byte(value) || !exists || byteIndex >= origLen {
		newObj := object.AcquireRawStringObject(string(data))
		if exists && obj.ExpireAt > 0 {
			newObj.ExpireAt = obj.ExpireAt
		}
		ctx.Engine.Set(key, newObj)
	}

	return types.NewInt(int64(oldBit))
}

// GetBitCommand implements GETBIT key offset
type GetBitCommand struct{}

func (c GetBitCommand) Name() string { return "GETBIT" }

func (c GetBitCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'getbit' command")
	}

	offset, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil || offset < 0 {
		return types.NewError("ERR bit offset is not an integer or out of range")
	}

	key := string(args[0].Str)
	obj, exists := ctx.Engine.Get(key)
	if !exists {
		return types.RespZero
	}

	strVal, ok := obj.Value.(string)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	byteIndex := int(offset / 8)
	if byteIndex >= len(strVal) {
		return types.RespZero
	}

	bitIndex := uint(7 - (offset % 8)) // MSB first
	bit := (strVal[byteIndex] >> bitIndex) & 1

	return types.NewInt(int64(bit))
}

// BitCountCommand implements BITCOUNT key [start end [BYTE|BIT]]
type BitCountCommand struct{}

func (c BitCountCommand) Name() string { return "BITCOUNT" }

func (c BitCountCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	// BITCOUNT key [start end [BYTE|BIT]] — valid arg counts: 1, 3, 4
	if len(args) != 1 && len(args) != 3 && len(args) != 4 {
		return types.NewError("ERR syntax error")
	}

	var start, end int64
	byteMode := true

	if len(args) >= 3 {
		var err error
		start, err = strconv.ParseInt(string(args[1].Str), 10, 64)
		if err != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}
		end, err = strconv.ParseInt(string(args[2].Str), 10, 64)
		if err != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}
		if len(args) >= 4 {
			mode := string(args[3].Str)
			if mode == "BIT" || mode == "bit" {
				byteMode = false
			} else if mode != "BYTE" && mode != "byte" {
				return types.NewError("ERR syntax error")
			}
		}
	}

	key := string(args[0].Str)
	obj, exists := ctx.Engine.Get(key)
	if !exists {
		return types.RespZero
	}

	strVal, ok := obj.Value.(string)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	data := []byte(strVal)
	length := len(data)

	if len(args) == 1 {
		count := 0
		for i := 0; i < length; i++ {
			count += popcount(data[i])
		}
		return types.NewInt(int64(count))
	}

	s := int(start)
	e := int(end)

	if byteMode {
		if s < 0 {
			s = length + s
		}
		if e < 0 {
			e = length + e
		}
		if s < 0 {
			s = 0
		}
		if e >= length {
			e = length - 1
		}
		if s > e || s >= length {
			return types.RespZero
		}

		count := 0
		for i := s; i <= e; i++ {
			count += popcount(data[i])
		}
		return types.NewInt(int64(count))
	}

	// BIT mode
	bitLen := length * 8
	if s < 0 {
		s = bitLen + s
	}
	if e < 0 {
		e = bitLen + e
	}
	if s < 0 {
		s = 0
	}
	if e >= bitLen {
		e = bitLen - 1
	}
	if s > e || s >= bitLen {
		return types.RespZero
	}

	count := 0
	for i := s; i <= e; i++ {
		byteIdx := i / 8
		bitIdx := uint(7 - (i % 8))
		if (data[byteIdx]>>bitIdx)&1 == 1 {
			count++
		}
	}
	return types.NewInt(int64(count))
}

// popcount returns the number of set bits in a byte
func popcount(b byte) int {
	count := 0
	for b != 0 {
		count += int(b & 1)
		b >>= 1
	}
	return count
}

// BitPosCommand implements BITPOS key bit [start [end [BYTE|BIT]]]
type BitPosCommand struct{}

func (c BitPosCommand) Name() string { return "BITPOS" }

func (c BitPosCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	// BITPOS key bit [start [end [BYTE|BIT]]]
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'bitpos' command")
	}

	bit, err := strconv.ParseInt(string(args[1].Str), 10, 64)
	if err != nil || (bit != 0 && bit != 1) {
		return types.NewError("ERR value is not an integer or out of range")
	}

	var start, end int64
	byteMode := true
	hasEnd := false

	switch len(args) {
	case 2:
		// key bit — no range
	case 3:
		// key bit start
		start, err = strconv.ParseInt(string(args[2].Str), 10, 64)
		if err != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}
	case 4:
		// key bit start [end|mode] — ambiguous: last arg could be end or BIT/BYTE
		modeOrEnd := string(args[3].Str)
		if modeOrEnd == "BIT" || modeOrEnd == "bit" || modeOrEnd == "BYTE" || modeOrEnd == "byte" {
			if modeOrEnd == "BIT" || modeOrEnd == "bit" {
				byteMode = false
			}
			start, err = strconv.ParseInt(string(args[2].Str), 10, 64)
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
		} else {
			start, err = strconv.ParseInt(string(args[2].Str), 10, 64)
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			end, err = strconv.ParseInt(string(args[3].Str), 10, 64)
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			hasEnd = true
		}
	case 5:
		// key bit start end mode — last arg must be BIT/BYTE
		mode := string(args[4].Str)
		if mode == "BIT" || mode == "bit" {
			byteMode = false
		} else if mode != "BYTE" && mode != "byte" {
			return types.NewError("ERR syntax error")
		}
		start, err = strconv.ParseInt(string(args[2].Str), 10, 64)
		if err != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}
		end, err = strconv.ParseInt(string(args[3].Str), 10, 64)
		if err != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}
		hasEnd = true
	default:
		return types.NewError("ERR syntax error")
	}

	key := string(args[0].Str)
	obj, exists := ctx.Engine.Get(key)
	if !exists {
		if bit == 0 {
			return types.RespZero
		}
		return types.NewInt(-1)
	}

	strVal, ok := obj.Value.(string)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	data := []byte(strVal)
	length := len(data)

	s := int(start)
	e := int(end)

	if byteMode {
		// Default end when not specified: end of string (byte index)
		if len(args) <= 3 {
			e = length - 1
		}
		if s < 0 {
			s = length + s
		}
		if e < 0 {
			e = length + e
		}
		if s < 0 {
			s = 0
		}
		if e >= length {
			e = length - 1
		}
		if s > e || s >= length {
			return types.NewInt(-1)
		}

		for i := s; i <= e; i++ {
			for j := 7; j >= 0; j-- {
				b := (data[i] >> uint(j)) & 1
				if int64(b) == bit {
					return types.NewInt(int64(i*8 + (7 - j)))
				}
			}
		}

		if bit == 0 && !hasEnd {
			return types.NewInt(int64(length * 8))
		}
		return types.NewInt(-1)
	}

	// BIT mode
	bitLen := length * 8
	// Default end when not specified: end of bit string
	if len(args) <= 3 {
		e = bitLen - 1
	}
	if s < 0 {
		s = bitLen + s
	}
	if e < 0 {
		e = bitLen + e
	}
	if s < 0 {
		s = 0
	}
	if e >= bitLen {
		e = bitLen - 1
	}
	if s > e || s >= bitLen {
		return types.NewInt(-1)
	}

	for i := s; i <= e; i++ {
		byteIdx := i / 8
		bitIdx := uint(7 - (i % 8))
		b := (data[byteIdx] >> bitIdx) & 1
		if int64(b) == bit {
			return types.NewInt(int64(i))
		}
	}

	return types.NewInt(-1)
}
