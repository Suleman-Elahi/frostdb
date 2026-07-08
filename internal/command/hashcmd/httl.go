package hashcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type httlBase struct {
	name      string
	isMillis  bool
	isAbsolute bool
}

func (c httlBase) Name() string { return c.name }

func (c httlBase) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for '" + strings.ToLower(c.name) + "' command")
	}

	key := string(args[0].Str)
	
	if strings.ToUpper(string(args[1].Str)) != "FIELDS" {
		return types.NewError("ERR syntax error")
	}

	numfields, err := strconv.Atoi(string(args[2].Str))
	if err != nil || numfields <= 0 {
		return types.NewError("ERR value is not an integer or out of range")
	}

	if len(args)-3 != numfields {
		return types.NewError("ERR syntax error")
	}

	fields := make([]string, numfields)
	for i := 0; i < numfields; i++ {
		fields[i] = string(args[3+i].Str)
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		// Key does not exist: return -2 for all fields
		res := make([]types.Value, numfields)
		for i := 0; i < numfields; i++ {
			res[i] = types.NewInt(-2)
		}
		return types.NewArray(res)
	}

	if obj.Type != object.TypeHash {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	h := obj.Value.(*object.Hash)
	
	// Lazy expire before checking
	ExpireIfNeeded(ctx, key, h, fields, ctx.NowMs(), false, true)
	
	if h.Len() == 0 {
		res := make([]types.Value, numfields)
		for i := 0; i < numfields; i++ {
			res[i] = types.NewInt(-2)
		}
		return types.NewArray(res)
	}

	results := make([]types.Value, numfields)
	nowMs := ctx.NowMs()

	for i, field := range fields {
		if _, exists := h.Get(field); !exists {
			results[i] = types.NewInt(-2)
			continue
		}

		ttl := h.GetTTL(field)
		if ttl == 0 {
			results[i] = types.NewInt(-1)
		} else {
			if c.isAbsolute {
				if c.isMillis {
					results[i] = types.NewInt(ttl)
				} else {
					// Round up/down? Redis rounds down for EXPIRETIME? Actually UnixTime / 1000.
					results[i] = types.NewInt(ttl / 1000)
				}
			} else {
				remainMs := ttl - nowMs
				if remainMs < 0 {
					remainMs = 0
				}
				if c.isMillis {
					results[i] = types.NewInt(remainMs)
				} else {
					results[i] = types.NewInt(remainMs / 1000)
				}
			}
		}
	}

	return types.NewArray(results)
}

type HTtlCommand struct{ httlBase }
func NewHTtlCommand() HTtlCommand { return HTtlCommand{httlBase{"HTTL", false, false}} }

type HPTtlCommand struct{ httlBase }
func NewHPTtlCommand() HPTtlCommand { return HPTtlCommand{httlBase{"HPTTL", true, false}} }

type HExpireTimeCommand struct{ httlBase }
func NewHExpireTimeCommand() HExpireTimeCommand { return HExpireTimeCommand{httlBase{"HEXPIRETIME", false, true}} }

type HPExpireTimeCommand struct{ httlBase }
func NewHPExpireTimeCommand() HPExpireTimeCommand { return HPExpireTimeCommand{httlBase{"HPEXPIRETIME", true, true}} }
