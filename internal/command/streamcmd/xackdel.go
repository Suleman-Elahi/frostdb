package streamcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// XAckDelCommand implements XACKDEL key group [KEEPREF | DELREF | ACKED] IDS numids id [id ...].
type XAckDelCommand struct{}

func (c XAckDelCommand) Name() string { return "XACKDEL" }

func (c XAckDelCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 4 {
		return types.NewError("ERR wrong number of arguments for 'xackdel' command")
	}

	key := string(args[0].Str)
	groupName := string(args[1].Str)
	policy := object.StreamTrimKeepRef
	policySet := false
	idsIdx := -1
	numids := 0

	for i := 2; i < len(args); {
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
				return types.NewError("ERR number of IDs doesn't match numids")
			}
			idsIdx = i + 2
			numids = n
			i = idsIdx + numids
		default:
			return types.NewError("ERR syntax error")
		}
	}

	if idsIdx == -1 {
		return types.NewError("ERR syntax error, expected IDS keyword")
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
	if _, exists := stream.Groups[groupName]; !exists {
		result := make([]types.Value, numids)
		for i := range result {
			result[i] = types.NewInt(-1)
		}
		return types.NewArray(result)
	}

	result := make([]types.Value, 0, numids)
	for _, id := range ids {
		result = append(result, types.NewInt(stream.AckDelete(groupName, id, policy)))
	}

	return types.NewArray(result)
}
