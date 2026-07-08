package streamcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// XTrimCommand implements XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count] [ACKED|DELREF].
type XTrimCommand struct{}

func (c XTrimCommand) Name() string { return "XTRIM" }

func (c XTrimCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'xtrim' command")
	}

	key := string(args[0].Str)
	strategy := strings.ToUpper(string(args[1].Str))

	if strategy != "MAXLEN" && strategy != "MINID" {
		return types.NewError("ERR syntax error")
	}

	idx := 2
	approx := false
	
	// Check for optional = or ~
	if idx < len(args) {
		s := string(args[idx].Str)
		switch s {
		case "=":
			idx++
		case "~":
			approx = true
			idx++
		}
	}
	
	if idx >= len(args) {
		return types.NewError("ERR syntax error")
	}

	threshold := string(args[idx].Str)
	idx++

	// Parse optional LIMIT and ACKED/DELREF
	limit := -1
	refPolicy := object.StreamTrimKeepRef
	
	for idx < len(args) {
		opt := strings.ToUpper(string(args[idx].Str))
		switch opt {
		case "LIMIT":
			if !approx {
				// LIMIT without ~ is an error
				return types.NewError("ERR syntax error, LIMIT cannot be used without the special ~ option")
			}
			if idx+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			v, err := strconv.Atoi(string(args[idx+1].Str))
			if err != nil || v < 0 {
				return types.NewError("ERR value is not an integer or out of range")
			}
			limit = v
			idx += 2
		case "ACKED":
			refPolicy = object.StreamTrimAcked
			idx++
		case "DELREF":
			refPolicy = object.StreamTrimDelRef
			idx++
		default:
			idx++
		}
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewInt(0)
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)

	var trimmed int
	switch strategy {
	case "MAXLEN":
		maxlen, err := strconv.Atoi(threshold)
		if err != nil || maxlen < 0 {
			return types.NewError("ERR value is not an integer or out of range")
		}
		if refPolicy != object.StreamTrimKeepRef {
			// Use policy-aware trimming
			trimmed = stream.TrimMaxLenWithPolicy(maxlen, approx, refPolicy)
		} else if approx {
			trimmed = stream.TrimMaxLenApprox(maxlen, limit)
		} else {
			trimmed = stream.TrimMaxLen(maxlen)
		}
	case "MINID":
		if refPolicy != object.StreamTrimKeepRef {
			// Use policy-aware trimming
			trimmed = stream.TrimMinIDWithPolicy(threshold, approx, refPolicy)
		} else if approx {
			trimmed = stream.TrimMinIDApprox(threshold, limit)
		} else {
			trimmed = stream.TrimMinID(threshold)
		}
	}

	return types.NewInt(int64(trimmed))
}
