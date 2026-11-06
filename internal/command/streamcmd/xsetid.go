package streamcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// XSetIDCommand implements XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id].
type XSetIDCommand struct{}

func (c XSetIDCommand) Name() string { return "XSETID" }

func (c XSetIDCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'xsetid' command")
	}

	key := string(args[0].Str)
	lastIDStr := string(args[1].Str)

	// Parse the last ID first (syntax validation)
	lastID, err := object.ParseStreamID(lastIDStr)
	if err != nil {
		return types.NewError("ERR Invalid stream ID specified as stream command argument")
	}

	// Parse optional arguments FIRST (syntax validation before key existence check - Redis behavior)
	entriesAdded := int64(-1) // -1 means not specified
	var maxDeletedID *object.StreamID
	hasEntriesAdded := false
	hasMaxDeletedID := false

	idx := 2
	for idx < len(args) {
		opt := strings.ToUpper(string(args[idx].Str))
		switch opt {
		case "ENTRIESADDED":
			if idx+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			val, err := strconv.ParseInt(string(args[idx+1].Str), 10, 64)
			if err != nil {
				return types.NewError("ERR value is not an integer or out of range")
			}
			if val < 0 {
				return types.NewError("ERR Invalid entries-added specified: must be positive")
			}
			entriesAdded = val
			hasEntriesAdded = true
			idx += 2
		case "MAXDELETEDID":
			if idx+1 >= len(args) {
				return types.NewError("ERR syntax error")
			}
			parsedID, err := object.ParseStreamID(string(args[idx+1].Str))
			if err != nil {
				return types.NewError("ERR Invalid stream ID specified as stream command argument")
			}
			maxDeletedID = &parsedID
			hasMaxDeletedID = true
			idx += 2
		default:
			return types.NewError("ERR syntax error")
		}
	}

	// NOW check key existence (after syntax validation)
	obj, ok := ctx.Engine.Get(key)
	if !ok {
		return types.NewError("ERR no such key")
	}
	if obj.Type != object.TypeStream {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	stream := obj.Value.(*object.Stream)

	// Validate MAXDELETEDID must not be greater than the new last-id
	if maxDeletedID != nil {
		if maxDeletedID.Compare(lastID) > 0 {
			return types.NewError("ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id")
		}
	}

	// Validate entriesAdded (offset) must be >= stream length
	if hasEntriesAdded {
		streamLen := stream.Len()
		if entriesAdded < int64(streamLen) {
			return types.NewError("ERR The ENTRIESADDED specified is smaller than the current number of entries in the stream")
		}
	}

	// Validate new last-id must be >= current max deleted entry ID
	currentMaxDeleted := stream.MaxDeletedEntryID()
	if currentMaxDeleted.MS > 0 || currentMaxDeleted.Seq > 0 {
		if lastID.Compare(currentMaxDeleted) < 0 {
			return types.NewError("ERR The ID specified is smaller than the current MAXDELETEDID")
		}
	}

	// Use the extended method if we have options, otherwise simple SetLastID
	if hasEntriesAdded || hasMaxDeletedID {
		if err := stream.SetLastIDWithOptions(lastID, entriesAdded, maxDeletedID); err != nil {
			return types.NewError(err.Error())
		}
	} else {
		if err := stream.SetLastID(lastID); err != nil {
			return types.NewError(err.Error())
		}
	}

	return types.RespOK
}
