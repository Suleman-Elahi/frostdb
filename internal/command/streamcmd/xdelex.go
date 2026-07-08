package streamcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// XDelexCommand implements XDELEX key [KEEPREF | DELREF | ACKED] IDS numids id [id ...].
type XDelexCommand struct{}

func (c XDelexCommand) Name() string { return "XDELEX" }

func (c XDelexCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'xdelex' command")
	}

	key := string(args[0].Str)
	policy := object.StreamTrimKeepRef
	policySet := false
	idsIdx := -1
	numids := 0

	for i := 1; i < len(args); {
		token := strings.ToUpper(string(args[i].Str))
		switch token {
		case "KEEPREF":
			if policySet {
				return types.NewError("ERR syntax error")
			}
			policy = object.StreamTrimKeepRef
			policySet = true
			i++
		case "DELREF":
			if policySet {
				return types.NewError("ERR syntax error")
			}
			policy = object.StreamTrimDelRef
			policySet = true
			i++
		case "ACKED":
			if policySet {
				return types.NewError("ERR syntax error")
			}
			policy = object.StreamTrimAcked
			policySet = true
			i++
		case "IDS":
			if idsIdx != -1 || i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			n, err := strconv.Atoi(string(args[i+1].Str))
			if err != nil || n <= 0 {
				return types.NewError("ERR Number of IDs must be a positive integer")
			}
			if n > len(args)-(i+2) {
				return types.NewError("ERR The `numids` parameter must match the number of arguments")
			}
			idsIdx = i + 2
			numids = n
			i = idsIdx + numids
		default:
			return types.NewError("ERR syntax error")
		}
	}

	if idsIdx == -1 {
		return types.NewError("ERR syntax error")
	}

	ids := make([]object.StreamID, numids)
	for i := 0; i < numids; i++ {
		id, err := object.ParseStreamID(string(args[idsIdx+i].Str))
		if err != nil {
			return types.NewError("ERR Invalid stream ID specified as stream command argument")
		}
		ids[i] = id
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		result := make([]types.Value, numids)
		for i := range result {
			result[i] = types.NewInt(-1)
		}
		return types.NewArray(result)
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)
	result := make([]types.Value, 0, numids)

	for _, id := range ids {
		result = append(result, types.NewInt(stream.DeleteWithPolicy(id, policy)))
	}

	return types.NewArray(result)
}
