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

	var maxlen int
	if strategy == "MAXLEN" {
		v, err := strconv.Atoi(threshold)
		if err != nil {
			return types.NewError("ERR value is not an integer or out of range")
		}
		if v < 0 {
			return types.NewError("ERR The MAXLEN argument must be >= 0.")
		}
		maxlen = v
	} else {
		if _, err := object.ParseStreamID(threshold); err != nil {
			return types.NewError("ERR Invalid stream ID specified as stream command argument")
		}
	}

	// Parse optional LIMIT and KEEPREF/ACKED/DELREF
	limit := -1
	refPolicy := object.StreamTrimKeepRef
	hasPolicy := false

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
		case "KEEPREF":
			if hasPolicy {
				return types.NewError("ERR syntax error, only one of KEEPREF, DELREF, or ACKED can be specified")
			}
			refPolicy = object.StreamTrimKeepRef
			hasPolicy = true
			idx++
		case "ACKED":
			if hasPolicy {
				return types.NewError("ERR syntax error, only one of KEEPREF, DELREF, or ACKED can be specified")
			}
			refPolicy = object.StreamTrimAcked
			hasPolicy = true
			idx++
		case "DELREF":
			if hasPolicy {
				return types.NewError("ERR syntax error, only one of KEEPREF, DELREF, or ACKED can be specified")
			}
			refPolicy = object.StreamTrimDelRef
			hasPolicy = true
			idx++
		default:
			return types.NewError("ERR syntax error")
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
		trimmed = stream.TrimMaxLenFull(maxlen, approx, limit, refPolicy)
	case "MINID":
		trimmed = stream.TrimMinIDFull(threshold, approx, limit, refPolicy)
	}

	if approx && refPolicy == object.StreamTrimKeepRef {
		ctx.ReplicateAs = [][]byte{
			[]byte("XTRIM"),
			[]byte(key),
			[]byte("MAXLEN"),
			[]byte("="),
			[]byte(strconv.Itoa(stream.Len())),
		}
	}

	return types.NewInt(int64(trimmed))
}
