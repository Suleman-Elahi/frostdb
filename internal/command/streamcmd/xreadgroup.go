package streamcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type XReadGroupCommand struct{}

func (c XReadGroupCommand) Name() string { return "XREADGROUP" }

func (c XReadGroupCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 5 {
		return types.NewError("ERR wrong number of arguments for 'xreadgroup' command")
	}

	idx := 0
	if strings.ToUpper(string(args[idx].Str)) != "GROUP" {
		return types.NewError("ERR syntax error")
	}
	idx++
	if idx+1 >= len(args) {
		return types.NewError("ERR syntax error")
	}

	groupName := string(args[idx].Str)
	consumer := string(args[idx+1].Str)
	idx += 2

	count := 0
	blockMs := int64(-1)
	noAck := false

	for idx < len(args) {
		token := strings.ToUpper(string(args[idx].Str))
		switch token {
		case "COUNT":
			if idx+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			n, err := strconv.Atoi(string(args[idx+1].Str))
			if err != nil || n < 0 {
				return types.NewError("ERR value is not an integer or out of range")
			}
			count = n
			idx += 2
		case "BLOCK":
			if idx+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			n, err := strconv.ParseInt(string(args[idx+1].Str), 10, 64)
			if err != nil {
				return types.NewError("ERR timeout is not an integer or out of range")
			}
			if n < 0 {
				return types.NewError("ERR timeout is negative")
			}
			blockMs = n
			idx += 2
		case "NOACK":
			noAck = true
			idx++
		case "STREAMS":
			idx++
			goto parseStreams
		default:
			return types.NewError("ERR syntax error")
		}
	}

	return types.NewError("ERR syntax error")

parseStreams:
	if idx >= len(args) {
		return types.NewError("ERR syntax error")
	}
	remaining := args[idx:]
	if len(remaining)%2 != 0 {
		return types.NewError("ERR Unbalanced XREADGROUP list of streams: for each stream key an ID or '>' must be specified.")
	}

	pairs := len(remaining) / 2
	streamKeys := make([]string, pairs)
	streamIDs := make([]string, pairs)
	hasNewID := false
	for i := 0; i < pairs; i++ {
		streamKeys[i] = string(remaining[i].Str)
		streamIDs[i] = string(remaining[pairs+i].Str)
		if streamIDs[i] == ">" {
			hasNewID = true
		} else {
			if _, err := object.ParseStreamID(streamIDs[i]); err != nil {
				return types.NewError("ERR Invalid stream ID specified as stream command argument")
			}
		}
	}

	readOnce := func() (types.Value, bool) {
		results := make([]types.Value, 0, len(streamKeys))
		for i, key := range streamKeys {
			obj, ok := ctx.Engine.Get(key)
			if !ok {
				return types.NewError("NOGROUP No such key '" + key + "' or consumer group '" + groupName + "' in XREADGROUP with GROUP option"), true
			}
			if obj.Type != object.TypeStream {
				return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value"), true
			}

			stream := obj.Value.(*object.Stream)
			hadConsumer := stream.HasConsumer(groupName, consumer)
			entries, err := stream.ReadGroup(groupName, consumer, count, streamIDs[i], noAck)
			if err != nil {
				return types.NewError(err.Error()), true
			}
			if !hadConsumer && stream.HasConsumer(groupName, consumer) {
				ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagStream), "xgroup-createconsumer", key)
			}
			if len(entries) == 0 && streamIDs[i] == ">" {
				continue
			}
			results = append(results, streamEntriesWithDeletedValue(key, entries))
		}

		if len(results) == 0 {
			if ctx.Protocol >= 3 {
				return types.NewNull(), false
			}
			return arrayNil(), false
		}
		return types.NewArray(results), true
	}

	result, ok := readOnce()
	if ok || blockMs < 0 || !hasNewID || ctx.ExecutingMulti || ctx.InScript {
		return result
	}

	rep := make([][]byte, len(args)+1)
	rep[0] = []byte("XREADGROUP")
	for i, a := range args {
		rep[i+1] = a.Str
	}
	ctx.ReplicateAs = rep

	return blockOnStreamKeys(ctx, streamKeys, blockMs, arrayNil(), func() (types.Value, bool) {
		return readOnce()
	})
}

func streamEntriesWithDeletedValue(key string, entries []object.StreamEntry) types.Value {
	values := make([]types.Value, 0, len(entries))
	for _, entry := range entries {
		if entry.Fields == nil {
			values = append(values, types.NewArray([]types.Value{
				types.NewString(entry.ID.String()),
				types.Value{Type: types.TypeArray, IsNull: true},
			}))
		} else {
			values = append(values, streamEntryValue(entry))
		}
	}
	return types.NewArray([]types.Value{
		types.NewString(key),
		types.NewArray(values),
	})
}

