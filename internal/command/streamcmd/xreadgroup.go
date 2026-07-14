package streamcmd

import (
	"strconv"
	"strings"
	"time"

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
			if err != nil || n <= 0 {
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
				return types.NewError("ERR value is not an integer or out of range")
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
		return types.NewError("ERR syntax error")
	}

	pairs := len(remaining) / 2
	streamKeys := make([]string, 0, pairs)
	streamIDs := make([]string, 0, pairs)
	for i := 0; i < len(remaining); i += 2 {
		streamKeys = append(streamKeys, string(remaining[i].Str))
		streamIDs = append(streamIDs, string(remaining[i+1].Str))
	}

	read := func() types.Value {
		results := make([]types.Value, 0, len(streamKeys))
		for i, key := range streamKeys {
			obj, ok := ctx.Engine.Get(key)
			if !ok || obj.Type != object.TypeStream {
				continue
			}

			stream := obj.Value.(*object.Stream)
			entries, err := stream.ReadGroup(groupName, consumer, count, streamIDs[i], noAck)
			if err != nil {
				return types.NewError(err.Error())
			}
			if len(entries) == 0 {
				continue
			}
			results = append(results, streamEntriesValue(key, entries))
		}

		if len(results) == 0 {
			return arrayNil()
		}
		return types.NewArray(results)
	}

	result := read()
	if result.Type != types.TypeArray || !result.IsNull || blockMs < 0 || ctx.InScript {
		return result
	}

	ch, cleanup := ctx.Engine.Subscribe(streamKeys)
	defer cleanup()

	// Release the command lock before blocking.
	if ctx.CmdMu != nil {
		ctx.CmdMu.Unlock()
	}
	defer func() {
		if ctx.CmdMu != nil {
			ctx.CmdMu.Lock()
		}
	}()

	if blockMs == 0 {
		<-ch
		return read()
	}

	select {
	case <-ch:
		return read()
	case <-time.After(time.Duration(blockMs) * time.Millisecond):
		return arrayNil()
	}
}
