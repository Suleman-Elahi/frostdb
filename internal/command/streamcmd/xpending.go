package streamcmd

import (
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type XPendingCommand struct{}

func (c XPendingCommand) Name() string { return "XPENDING" }

func (c XPendingCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'xpending' command")
	}

	key := string(args[0].Str)
	groupName := string(args[1].Str)

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewArray([]types.Value{
			types.NewInt(0),
			bulkNil(),
			bulkNil(),
			types.NewArray([]types.Value{}),
		})
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)

	if len(args) == 2 {
		count, smallest, largest, consumers := stream.PendingSummary(groupName)
		consumerValues := make([]types.Value, 0, len(consumers))
		for _, c := range consumers {
			consumerValues = append(consumerValues, pendingConsumerValue(c.Name, c.Pending))
		}

		return types.NewArray([]types.Value{
			types.NewInt(count),
			stringOrNil(smallest),
			stringOrNil(largest),
			types.NewArray(consumerValues),
		})
	}

	if len(args) < 5 {
		return types.NewError("ERR syntax error")
	}

	start := string(args[2].Str)
	end := string(args[3].Str)
	count, err := strconv.Atoi(string(args[4].Str))
	if err != nil || count <= 0 {
		return types.NewError("ERR value is not an integer or out of range")
	}
	consumer := ""
	if len(args) > 5 {
		consumer = string(args[5].Str)
	}

	pending := stream.PendingRange(groupName, start, end, count, consumer)
	values := make([]types.Value, 0, len(pending))
	for _, entry := range pending {
		values = append(values, streamPendingValue(entry))
	}
	return types.NewArray(values)
}

func stringOrNil(s string) types.Value {
	if s == "" {
		return bulkNil()
	}
	return types.NewString(s)
}
