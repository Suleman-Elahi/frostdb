package str

import (
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// BitOpCommand implements BITOP <AND|OR|XOR|NOT|DIFF|DIFF1|ANDOR|ONE> destkey key [key ...]
type BitOpCommand struct{}

func (c BitOpCommand) Name() string { return "BITOP" }

func (c BitOpCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'bitop' command")
	}

	op := strings.ToUpper(string(args[0].Str))
	destKey := string(args[1].Str)

	srcKeys := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		srcKeys[i-2] = string(args[i].Str)
	}

	switch op {
	case "NOT":
		if len(srcKeys) != 1 {
			return types.NewError("ERR BITOP NOT must be called with a single source key.")
		}
	case "AND", "OR", "XOR", "ONE":
		if len(srcKeys) < 1 {
			return types.NewError("ERR wrong number of arguments for 'bitop' command")
		}
	case "DIFF", "DIFF1", "ANDOR":
		if len(srcKeys) < 2 {
			return types.NewError("ERR BITOP " + op + " must be called with at least two source keys.")
		}
	default:
		return types.NewError("ERR unknown operation for 'bitop' command")
	}

	srcVals := make([][]byte, len(srcKeys))
	var maxLen int
	for i, key := range srcKeys {
		obj, exists := ctx.Engine.Get(key)
		if !exists {
			continue
		}
		strVal, ok := obj.Value.(string)
		if !ok {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		b := []byte(strVal)
		if len(b) > maxLen {
			maxLen = len(b)
		}
		srcVals[i] = b
	}

	result := make([]byte, maxLen)

	if maxLen > 0 {
		switch op {
		case "NOT":
			for i := 0; i < maxLen; i++ {
				result[i] = ^getByte(srcVals[0], i)
			}
		case "AND":
			for i := 0; i < maxLen; i++ {
				b := byte(0xFF)
				for j := 0; j < len(srcVals); j++ {
					b &= getByte(srcVals[j], i)
				}
				result[i] = b
			}
		case "OR":
			for i := 0; i < maxLen; i++ {
				var b byte
				for j := 0; j < len(srcVals); j++ {
					b |= getByte(srcVals[j], i)
				}
				result[i] = b
			}
		case "XOR":
			for i := 0; i < maxLen; i++ {
				var b byte
				for j := 0; j < len(srcVals); j++ {
					b ^= getByte(srcVals[j], i)
				}
				result[i] = b
			}
		case "DIFF":
			for i := 0; i < maxLen; i++ {
				first := getByte(srcVals[0], i)
				var others byte
				for j := 1; j < len(srcVals); j++ {
					others |= getByte(srcVals[j], i)
				}
				result[i] = first & ^others
			}
		case "DIFF1":
			for i := 0; i < maxLen; i++ {
				first := getByte(srcVals[0], i)
				var others byte
				for j := 1; j < len(srcVals); j++ {
					others |= getByte(srcVals[j], i)
				}
				result[i] = ^first & others
			}
		case "ANDOR":
			for i := 0; i < maxLen; i++ {
				first := getByte(srcVals[0], i)
				var others byte
				for j := 1; j < len(srcVals); j++ {
					others |= getByte(srcVals[j], i)
				}
				result[i] = first & others
			}
		case "ONE":
			for i := 0; i < maxLen; i++ {
				var res byte
				for bit := uint(0); bit < 8; bit++ {
					mask := byte(1 << bit)
					count := 0
					for j := 0; j < len(srcVals); j++ {
						if getByte(srcVals[j], i)&mask != 0 {
							count++
						}
					}
					if count == 1 {
						res |= mask
					}
				}
				result[i] = res
			}
		}
	}

	newObj := object.AcquireRawStringObject(string(result))
	ctx.Engine.Set(destKey, newObj)

	return types.NewInt(int64(maxLen))
}

func getByte(data []byte, i int) byte {
	if data == nil || i >= len(data) {
		return 0
	}
	return data[i]
}
