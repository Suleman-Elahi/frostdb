package streamcmd

import (
	"strconv"
	"strings"
	"time"

	"frostdb/internal/blocking"
	"frostdb/internal/command/generic"
	"frostdb/internal/engine/api"
	"frostdb/internal/engine/object"
	"frostdb/internal/engine/shard"
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
			if err != nil {
				return types.NewError("ERR timeout is not an integer or out of range")
			}
			if v < 0 {
				return types.NewError("ERR timeout is negative")
			}
			blockMs = v
			idx += 2
		case "STREAMS":
			idx++
			goto parseStreams
		default:
			return types.NewError("ERR syntax error")
		}
	}
	return types.NewError("ERR syntax error")

parseStreams:
	remaining := args[idx:]
	if len(remaining) == 0 || len(remaining)%2 != 0 {
		return types.NewError("ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.")
	}

	numStreams := len(remaining) / 2
	keys := remaining[:numStreams]
	ids := remaining[numStreams:]

	usesPlus := make([]bool, numStreams)
	resolvedIDs := make([]string, numStreams)
	streamKeys := make([]string, numStreams)
	for i := 0; i < numStreams; i++ {
		key := string(keys[i].Str)
		streamKeys[i] = key
		startID := string(ids[i].Str)
		usesPlus[i] = isSpecialPlusID(startID)
		if startID != "$" && !usesPlus[i] {
			if _, err := object.ParseStreamID(startID); err != nil {
				return types.NewError("ERR Invalid stream ID specified as stream command argument")
			}
		}

		obj, ok := ctx.Engine.Get(key)
		if ok && obj.Type != object.TypeStream {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		if startID == "$" {
			if ok {
				stream := obj.Value.(*object.Stream)
				resolvedIDs[i] = stream.LastGeneratedID()
			} else {
				resolvedIDs[i] = "0-0"
			}
		} else if usesPlus[i] {
			resolvedIDs[i] = "+"
		} else {
			resolvedIDs[i] = startID
		}
	}

	readOnce := func(ignoreWrongType bool) (types.Value, bool) {
		results := make([]types.Value, 0, numStreams)
		for i := 0; i < numStreams; i++ {
			key := streamKeys[i]
			startID := resolvedIDs[i]

			obj, ok := ctx.Engine.Get(key)
			if !ok {
				continue
			}
			if obj.Type != object.TypeStream {
				if ignoreWrongType {
					continue
				}
				return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value"), true
			}
			stream := obj.Value.(*object.Stream)

			var entries []object.StreamEntry
			if usesPlus[i] {
				if lastEntry := getLastStreamEntry(stream); lastEntry != nil {
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
			if ctx.Protocol >= 3 {
				return types.NewNull(), false
			}
			return types.RespNil, false
		}
		return types.NewArray(results), true
	}

	res, ok := readOnce(false)
	if ok || blockMs < 0 || ctx.ExecutingMulti || ctx.InScript {
		return res
	}

	// If "+" was specified and the stream was empty/missing at start,
	// convert "+" to waiting for entries after the current last ID.
	for i, key := range streamKeys {
		if usesPlus[i] {
			obj, ok := ctx.Engine.Get(key)
			if ok && obj.Type == object.TypeStream {
				stream := obj.Value.(*object.Stream)
				resolvedIDs[i] = stream.LastGeneratedID()
			} else {
				resolvedIDs[i] = "0-0"
			}
			usesPlus[i] = false
		}
	}

	return blockOnStreamKeys(ctx, streamKeys, blockMs, types.RespNil, func() (types.Value, bool) {
		return readOnce(true)
	})
}

func blockOnStreamKeys(
	ctx *context.Context,
	keys []string,
	blockMs int64,
	nilReply types.Value,
	tryWakeFn func() (types.Value, bool),
) types.Value {
	var waiter *shard.Waiter
	var legacyCh <-chan struct{}
	var cleanup func()
	if be, ok := ctx.Engine.(api.BlockingEngine); ok {
		waiter, cleanup = be.SubscribeWaiter(keys)
	} else {
		legacyCh, cleanup = ctx.Engine.Subscribe(keys)
	}

	blocking.Inc()
	generic.UpdateClientBlocked(ctx, true)
	unblockCh := make(chan generic.UnblockType, 1)
	generic.RegisterClientUnblockCh(ctx.ClientID, unblockCh)

	blockedActive := true
	finishBlocked := func(failed bool) {
		if blockedActive {
			blockedActive = false
			if ctx.RecordStatsNow != nil {
				ctx.RecordStatsNow(failed)
			}
			blocking.Dec()
			generic.UpdateClientBlocked(ctx, false)
			generic.UnregisterClientUnblockCh(ctx.ClientID)
		}
	}
	defer func() {
		finishBlocked(false)
		cleanup()
	}()

	discCh, stopDisc := ctx.WatchDisconnect()
	defer stopDisc()

	var timeoutCh <-chan time.Time
	if blockMs > 0 {
		timeoutCh = time.After(time.Duration(blockMs) * time.Millisecond)
	}

	if ctx.FlushWriter != nil {
		ctx.FlushWriter()
	}

	if ctx.UnlockKeys != nil {
		ctx.UnlockKeys()
	}

	var wakeCh <-chan chan shard.SignalAck
	if waiter != nil {
		wakeCh = waiter.WakeCh
	}

	for {
		select {
		case ackCh := <-wakeCh:
			if ctx.LockKeys != nil {
				ctx.LockKeys()
			}
			ctx.InWakeup = true
			dirtyBefore := ctx.Engine.GetDirty()
			val, ok := tryWakeFn()
			dirtyAfter := ctx.Engine.GetDirty()
			ctx.InWakeup = false
			if ok {
				failed := val.Type == types.TypeError
				if !failed && !ctx.SkipReplication && len(ctx.ReplicateAs) > 0 {
					if dirtyAfter == dirtyBefore {
						ctx.Engine.AddDirty(1)
					}
					_ = ctx.Engine.LogCommand(ctx.ReplicateAs)
					ctx.SkipReplication = true
				} else {
					ctx.SkipReplication = true
				}
				cascaded := ctx.TakeSignals()
				finishBlocked(failed)
				if ctx.UnlockKeys != nil {
					ctx.UnlockKeys()
				}
				ackCh <- shard.SignalAck{
					Consumed: true,
					Cascaded: cascaded,
				}
				return val
			}
			if ctx.UnlockKeys != nil {
				ctx.UnlockKeys()
			}
			ackCh <- shard.SignalAck{Consumed: true}

		case <-legacyCh:
			if ctx.LockKeys != nil {
				ctx.LockKeys()
			}
			val, ok := tryWakeFn()
			if ok {
				failed := val.Type == types.TypeError
				finishBlocked(failed)
				if ctx.UnlockKeys != nil {
					ctx.UnlockKeys()
				}
				return val
			}
			if ctx.UnlockKeys != nil {
				ctx.UnlockKeys()
			}

		case <-timeoutCh:
			ctx.SkipReplication = true
			finishBlocked(false)
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return nilReply

		case <-ctx.Done:
			ctx.SkipReplication = true
			finishBlocked(false)
			return nilReply

		case <-discCh:
			ctx.SkipReplication = true
			finishBlocked(false)
			return nilReply

		case <-blocking.StopSignal():
			ctx.SkipReplication = true
			finishBlocked(false)
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return nilReply

		case ut := <-unblockCh:
			ctx.SkipReplication = true
			if ut == generic.UnblockError {
				finishBlocked(true)
				return types.NewError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
			}
			finishBlocked(false)
			if ctx.Protocol >= 3 {
				return types.NewNull()
			}
			return nilReply
		}
	}
}

