package streamcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type XAutoClaimCommand struct{}

func (c XAutoClaimCommand) Name() string { return "XAUTOCLAIM" }

func (c XAutoClaimCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 5 {
		return types.NewError("ERR wrong number of arguments for 'xautoclaim' command")
	}

	key := string(args[0].Str)
	groupName := string(args[1].Str)
	consumer := string(args[2].Str)

	minIdleMs, err := strconv.ParseInt(string(args[3].Str), 10, 64)
	if err != nil || minIdleMs < 0 {
		return types.NewError("ERR value is not an integer or out of range")
	}
	start := string(args[4].Str)

	count := 100
	justID := false
	for i := 5; i < len(args); i++ {
		token := strings.ToUpper(string(args[i].Str))
		switch token {
		case "COUNT":
			if i+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			n, err := strconv.Atoi(string(args[i+1].Str))
			if err != nil || n <= 0 {
				return types.NewError("ERR value is not an integer or out of range")
			}
			count = n
			i++
		case "JUSTID":
			justID = true
		default:
			return types.NewError("ERR syntax error")
		}
	}

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewArray([]types.Value{
			types.NewString("0-0"),
			types.NewArray([]types.Value{}),
			types.NewArray([]types.Value{}),
		})
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)
	next, claimed, deleted := stream.AutoClaim(groupName, consumer, minIdleMs, start, count, justID)

	claimedValues := make([]types.Value, 0, len(claimed))
	if justID {
		for _, entry := range claimed {
			claimedValues = append(claimedValues, types.NewString(entry.Entry.ID.String()))
		}
	} else {
		for _, entry := range claimed {
			claimedValues = append(claimedValues, streamEntryValue(entry.Entry))
		}
	}

	deletedValues := make([]types.Value, 0, len(deleted))
	for _, id := range deleted {
		deletedValues = append(deletedValues, types.NewString(id))
	}

	return types.NewArray([]types.Value{
		types.NewString(next),
		types.NewArray(claimedValues),
		types.NewArray(deletedValues),
	})
}

