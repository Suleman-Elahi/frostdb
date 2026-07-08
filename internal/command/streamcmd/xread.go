package streamcmd

import (
	"strconv"
	"strings"
	"time"

	"frostdb/internal/blocking"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// XReadCommand implements XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...].
type XReadCommand struct{}

func (c XReadCommand) Name() string { return "XREAD" }

func (c XReadCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 {
		return types.NewError("ERR wrong number of arguments for 'xread' command")
	}

	count := 0
	blockMs := int64(-1)
	idx := 0

	// Parse options
	for idx < len(args) {
		opt := strings.ToUpper(string(args[idx].Str))
		switch opt {
		case "COUNT":
			if idx+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			idx++
			v, err := strconv.Atoi(string(args[idx].Str))
			if err != nil || v < 0 {
				return types.NewError("ERR value is not an integer or out of range")
			}
			count = v
			idx++
		case "BLOCK":
			if idx+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			v, err := strconv.ParseInt(string(args[idx+1].Str), 10, 64)
			if err != nil || v < 0 {
				return types.NewError("ERR value is not an integer or out of range")
			}
			blockMs = v
			idx += 2
		case "STREAMS":
			idx++
			goto parseStreams
		default:
			idx++
		}
	}
	return types.NewError("ERR syntax error")

parseStreams:
	remaining := args[idx:]
	if len(remaining) == 0 || len(remaining)%2 != 0 {
		return types.NewError("ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified")
	}

	numStreams := len(remaining) / 2
	keys := remaining[:numStreams]
	ids := remaining[numStreams:]

	// Check if any stream uses "+" (read last element)
	usesPlus := make([]bool, numStreams)
	for i := 0; i < numStreams; i++ {
		usesPlus[i] = isSpecialPlusID(string(ids[i].Str))
	}

	// Resolve "$" to actual last IDs at the start
	resolvedIDs := make([]string, numStreams)
	for i := 0; i < numStreams; i++ {
		startID := string(ids[i].Str)
		if startID == "$" {
			// Get the current last ID of the stream
			key := string(keys[i].Str)
			obj, ok := ctx.Engine.Get(key)
			if ok && obj.Type == object.TypeStream {
				stream := obj.Value.(*object.Stream)
				resolvedIDs[i] = stream.LastGeneratedID()
			} else {
				resolvedIDs[i] = "0-0"
			}
		} else if usesPlus[i] {
			// "+" means read last element - we'll handle this specially
			resolvedIDs[i] = "+"
		} else {
			resolvedIDs[i] = startID
		}
	}

	readOnce := func() types.Value {
		results := make([]types.Value, 0, numStreams)
		for i := 0; i < numStreams; i++ {
			key := string(keys[i].Str)
			startID := resolvedIDs[i]

			obj, ok := ctx.Engine.Get(key)
			if !ok || obj.Type != object.TypeStream {
				continue
			}
			stream := obj.Value.(*object.Stream)

			var entries []object.StreamEntry
			if usesPlus[i] {
				// "+" means read the last element only
				lastEntry := getLastStreamEntry(stream)
				if lastEntry != nil {
					entries = []object.StreamEntry{*lastEntry}
				}
			} else {
				entries = stream.Range("("+startID, "+", count)
			}
			if len(entries) == 0 {
				continue
			}

			entryValues := make([]types.Value, len(entries))
			for j, entry := range entries {
				entryValues[j] = streamEntryValue(entry)
			}
			results = append(results, types.NewArray([]types.Value{
				types.NewString(key),
				types.NewArray(entryValues),
			}))
		}
		if len(results) == 0 {
			return types.RespNil
		}
		return types.NewArray(results)
	}

	res := readOnce()
	if !res.IsNull || blockMs < 0 || ctx.InScript {
		return res
	}

	// Track blocking client
	blocking.Inc()
	defer blocking.Dec()

	streamKeys := make([]string, 0, numStreams)
	for i := 0; i < numStreams; i++ {
		streamKeys = append(streamKeys, string(keys[i].Str))
	}
	ch, cleanup := ctx.Engine.Subscribe(streamKeys)
	defer cleanup()

	if blockMs == 0 {
		for {
			<-ch
			if r := readOnce(); !r.IsNull {
				return r
			}
		}
	}

	deadline := time.Now().Add(time.Duration(blockMs) * time.Millisecond)
	for {
		remain := time.Until(deadline)
		if remain <= 0 {
			return types.RespNil
		}
		select {
		case <-ch:
			if r := readOnce(); !r.IsNull {
				return r
			}
		case <-time.After(remain):
			return types.RespNil
		}
	}
}
