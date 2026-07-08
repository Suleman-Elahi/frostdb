package str

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// MSetEXCommand implements MSETEX numkeys key value [key value ...] [NX|XX] [EX|PX|EXAT|PXAT|KEEPTTL]
type MSetEXCommand struct{}

func (c MSetEXCommand) Name() string { return "MSETEX" }

func (c MSetEXCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'msetex' command")
	}

	// Parse numkeys - check for very large values that could overflow
	numKeysStr := string(args[0].Str)
	numKeys64, err := strconv.ParseInt(numKeysStr, 10, 64)
	if err != nil || numKeys64 <= 0 || numKeys64 > 2147483647 {
		return types.NewError("ERR invalid numkeys value")
	}
	numKeys := int(numKeys64)

	// Calculate expected length: 1(numkeys) + numKeys*2(key-value pairs)
	// If we can't even have the minimum pairs, return invalid numkeys
	expectedMinLen := 1 + numKeys*2
	if len(args) < expectedMinLen {
		return types.NewError("ERR wrong number of key-value pairs")
	}

	// Extract key-value pairs
	kvPairs := args[1 : 1+numKeys*2]
	optArgs := args[1+numKeys*2:]

	// Parse options - track which expiration option is set
	var nx, xx, keepttl bool
	var expireAt int64
	var hasExpiration bool

	for i := 0; i < len(optArgs); i++ {
		opt := strings.ToUpper(string(optArgs[i].Str))
		switch opt {
		case "NX":
			if nx {
				return types.NewError("ERR syntax error")
			}
			nx = true
		case "XX":
			if xx {
				return types.NewError("ERR syntax error")
			}
			xx = true
		case "KEEPTTL":
			if keepttl || hasExpiration {
				return types.NewError("ERR syntax error")
			}
			keepttl = true
		case "EX":
			if hasExpiration || keepttl {
				return types.NewError("ERR syntax error")
			}
			if i+1 >= len(optArgs) {
				return types.NewError("ERR syntax error")
			}
			i++
			secs, err := strconv.ParseInt(string(optArgs[i].Str), 10, 64)
			if err != nil || secs <= 0 {
				return types.NewError("ERR invalid expire time in 'msetex' command")
			}
			expireAt = ctx.NowMs() + secs*1000
			hasExpiration = true
		case "PX":
			if hasExpiration || keepttl {
				return types.NewError("ERR syntax error")
			}
			if i+1 >= len(optArgs) {
				return types.NewError("ERR syntax error")
			}
			i++
			ms, err := strconv.ParseInt(string(optArgs[i].Str), 10, 64)
			if err != nil || ms <= 0 {
				return types.NewError("ERR invalid expire time in 'msetex' command")
			}
			expireAt = ctx.NowMs() + ms
			hasExpiration = true
		case "EXAT":
			if hasExpiration || keepttl {
				return types.NewError("ERR syntax error")
			}
			if i+1 >= len(optArgs) {
				return types.NewError("ERR syntax error")
			}
			i++
			secs, err := strconv.ParseInt(string(optArgs[i].Str), 10, 64)
			if err != nil || secs <= 0 {
				return types.NewError("ERR invalid expire time in 'msetex' command")
			}
			expireAt = secs * 1000
			hasExpiration = true
		case "PXAT":
			if hasExpiration || keepttl {
				return types.NewError("ERR syntax error")
			}
			if i+1 >= len(optArgs) {
				return types.NewError("ERR syntax error")
			}
			i++
			ms, err := strconv.ParseInt(string(optArgs[i].Str), 10, 64)
			if err != nil || ms <= 0 {
				return types.NewError("ERR invalid expire time in 'msetex' command")
			}
			expireAt = ms
			hasExpiration = true
		default:
			return types.NewError("ERR syntax error")
		}
	}

	// Validate conflicting options
	if nx && xx {
		return types.NewError("ERR syntax error")
	}

	// Check NX/XX conditions
	if nx {
		for i := 0; i < len(kvPairs); i += 2 {
			if _, ok := ctx.Engine.GetBytes(kvPairs[i].Str); ok {
				return types.RespZero
			}
		}
	}
	if xx {
		for i := 0; i < len(kvPairs); i += 2 {
			if _, ok := ctx.Engine.GetBytes(kvPairs[i].Str); !ok {
				return types.RespZero
			}
		}
	}

	// Set all keys
	for i := 0; i < len(kvPairs); i += 2 {
		key := string(kvPairs[i].Str)
		val := string(kvPairs[i+1].Str)
		obj := object.AcquireStringObject(val)

		if keepttl {
			// Preserve existing TTL if key exists
			if existing, ok := ctx.Engine.Get(key); ok {
				obj.ExpireAt = existing.ExpireAt
			}
		} else if expireAt > 0 {
			obj.ExpireAt = expireAt
		}

		ctx.Engine.Set(key, obj)
	}

	return types.RespOne
}
