package streamcmd

import (
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// XInfoCommand implements XINFO STREAM|GROUPS|CONSUMERS|HELP.
type XInfoCommand struct{}

func (c XInfoCommand) Name() string { return "XINFO" }

func (c XInfoCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'xinfo' command")
	}

	sub := strings.ToUpper(string(args[0].Str))
	switch sub {
	case "STREAM":
		return xinfoStream(ctx, args[1:])
	case "GROUPS":
		return xinfoGroups(ctx, args[1:])
	case "CONSUMERS":
		return xinfoConsumers(ctx, args[1:])
	case "HELP":
		if len(args[1:]) > 0 {
			return types.NewError("ERR wrong number of arguments for 'xinfo|help' command")
		}
		return xinfoHelp()
	default:
		return types.NewError("ERR unknown subcommand '" + sub + "'")
	}
}

func xinfoStream(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'xinfo|stream' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewError("ERR no such key")
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)
	length := stream.Len()
	groups := len(stream.Groups)
	idmpDuration, idmpMaxSize, pidsTracked, iidsTracked, iidsAdded, iidsDuplicates := stream.IDMPStats()

	firstEntryArr := types.NewNull()
	lastEntryArr := types.NewNull()
	firstEntries := stream.Range("-", "+", 1)
	if len(firstEntries) > 0 {
		firstEntryArr = streamEntryValue(firstEntries[0])
	}
	lastEntries := stream.RevRange("+", "-", 1)
	if len(lastEntries) > 0 {
		lastEntryArr = streamEntryValue(lastEntries[0])
	}

	// Get stream metadata
	entriesAdded := stream.EntriesAdded()
	recordedFirstEntryID := stream.RecordedFirstEntryID()
	maxDeletedEntryID := stream.MaxDeletedEntryID()

	return types.NewArray([]types.Value{
		types.NewString("length"),
		types.NewInt(int64(length)),
		types.NewString("radix-tree-keys"),
		types.NewInt(int64(stream.BlockCount())),
		types.NewString("radix-tree-nodes"),
		types.NewInt(int64(stream.BlockCount())),
		types.NewString("last-generated-id"),
		types.NewString(stream.LastGeneratedID()),
		types.NewString("max-deleted-entry-id"),
		types.NewString(maxDeletedEntryID.String()),
		types.NewString("recorded-first-entry-id"),
		types.NewString(recordedFirstEntryID.String()),
		types.NewString("entries-added"),
		types.NewInt(int64(entriesAdded)),
		types.NewString("idmp-duration"),
		types.NewInt(idmpDuration),
		types.NewString("idmp-maxsize"),
		types.NewInt(idmpMaxSize),
		types.NewString("pids-tracked"),
		types.NewInt(pidsTracked),
		types.NewString("iids-tracked"),
		types.NewInt(iidsTracked),
		types.NewString("iids-added"),
		types.NewInt(iidsAdded),
		types.NewString("iids-duplicates"),
		types.NewInt(iidsDuplicates),
		types.NewString("groups"),
		types.NewInt(int64(groups)),
		types.NewString("first-entry"),
		firstEntryArr,
		types.NewString("last-entry"),
		lastEntryArr,
	})
}

func xinfoGroups(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'xinfo|groups' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewError("ERR no such key")
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)
	result := make([]types.Value, 0, len(stream.Groups))

	for _, group := range stream.Groups {
		entry := types.NewArray([]types.Value{
			types.NewString("name"),
			types.NewString(group.Name),
			types.NewString("consumers"),
			types.NewInt(int64(len(group.Consumers))),
			types.NewString("pending"),
			types.NewInt(int64(len(group.Pending))),
			types.NewString("last-delivered-id"),
			types.NewString(group.LastDeliveredID.String()),
			types.NewString("entries-read"),
			types.NewInt(0),
			types.NewString("lag"),
			types.NewInt(int64(stream.Len()) - int64(len(group.Pending))),
		})
		result = append(result, entry)
	}
	return types.NewArray(result)
}

func xinfoHelp() types.Value {
	return types.NewArray([]types.Value{
		types.NewString("XINFO STREAM <key> - Show stream info"),
		types.NewString("XINFO GROUPS <key> - Show consumer groups"),
		types.NewString("XINFO CONSUMERS <key> <group> - Show consumers in a group"),
		types.NewString("XINFO HELP - Show this help"),
	})
}

func xinfoConsumers(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'xinfo|consumers' command")
	}

	key := string(args[0].Str)
	groupName := string(args[1].Str)

	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewError("ERR no such key")
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)
	group, exists := stream.Groups[groupName]
	if !exists {
		return types.NewError("NOGROUP No such consumer group '" + groupName + "'")
	}

	// Build per-consumer pending counts
	consumerPending := make(map[string]int64)
	for _, pe := range group.Pending {
		consumerPending[pe.Consumer]++
	}

	result := make([]types.Value, 0, len(group.Consumers))
	for name := range group.Consumers {
		pending := consumerPending[name]
		entry := types.NewArray([]types.Value{
			types.NewString("name"),
			types.NewString(name),
			types.NewString("pending"),
			types.NewInt(pending),
			types.NewString("idle"),
			types.NewInt(0),
		})
		result = append(result, entry)
	}
	return types.NewArray(result)
}
