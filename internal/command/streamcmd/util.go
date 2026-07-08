package streamcmd

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"frostdb/internal/engine/object"
	"frostdb/internal/resp/types"
)

func bulkNil() types.Value {
	return types.Value{Type: types.TypeBulkString, IsNull: true}
}

func arrayNil() types.Value {
	return types.Value{Type: types.TypeArray, IsNull: true}
}

func streamEntryValue(entry object.StreamEntry) types.Value {
	fields := make([]types.Value, 0, len(entry.Fields)*2)
	for _, field := range entry.Fields {
		fields = append(fields, types.NewString(field.Name))
		fields = append(fields, types.NewString(field.Value))
	}
	return types.NewArray([]types.Value{
		types.NewString(entry.ID.String()),
		types.NewArray(fields),
	})
}

func streamEntriesValue(key string, entries []object.StreamEntry) types.Value {
	values := make([]types.Value, 0, len(entries))
	for _, entry := range entries {
		values = append(values, streamEntryValue(entry))
	}
	return types.NewArray([]types.Value{
		types.NewString(key),
		types.NewArray(values),
	})
}

func streamPendingValue(entry object.StreamPendingEntry) types.Value {
	idle := time.Now().UnixMilli() - entry.LastDeliveryMs
	return types.NewArray([]types.Value{
		types.NewString(entry.Entry.ID.String()),
		types.NewString(entry.Consumer),
		types.NewInt(idle),
		types.NewInt(int64(entry.DeliveryCount)),
	})
}

func pendingConsumerValue(name string, count int64) types.Value {
	return types.NewArray([]types.Value{
		types.NewString(name),
		types.NewInt(count),
	})
}

func streamFieldPairs(args []types.Value) []object.StreamField {
	fields := make([]object.StreamField, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		fields = append(fields, object.StreamField{
			Name:  string(args[i].Str),
			Value: string(args[i+1].Str),
		})
	}
	return fields
}

func parseIntArg(val types.Value) (int, error) {
	return strconv.Atoi(string(val.Str))
}

// validateRangeID validates a stream ID used in XRANGE/XREVRANGE.
// Returns error for invalid exclusive ranges like "(–", "(+".
func validateRangeID(id string) error {
	if id == "-" || id == "+" {
		return nil
	}

	exclusive := false
	if len(id) > 0 && id[0] == '(' {
		exclusive = true
		id = id[1:]
	}

	// Can't have exclusive on special characters
	if exclusive && (id == "-" || id == "+") {
		return fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}

	// Parse the ID to validate format
	_, err := object.ParseStreamID(id)
	if err != nil {
		return fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	}

	return nil
}

// validateStartRangeID validates a start ID for XRANGE.
// (MAX-MAX) is invalid as start since nothing is greater than max.
func validateStartRangeID(id string) error {
	if err := validateRangeID(id); err != nil {
		return err
	}
	
	if len(id) > 0 && id[0] == '(' {
		parsed, _ := object.ParseStreamID(id[1:])
		if parsed.MS == math.MaxUint64 && parsed.Seq == math.MaxUint64 {
			return fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
	}
	return nil
}

// validateEndRangeID validates an end ID for XRANGE.
// (0-0) is invalid as end since nothing is less than 0-0.
func validateEndRangeID(id string) error {
	if err := validateRangeID(id); err != nil {
		return err
	}
	
	if len(id) > 0 && id[0] == '(' {
		parsed, _ := object.ParseStreamID(id[1:])
		if parsed.MS == 0 && parsed.Seq == 0 {
			return fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
	}
	return nil
}

// getLastStreamEntry returns the last entry from a stream, or nil if empty.
func getLastStreamEntry(stream *object.Stream) *object.StreamEntry {
	entries := stream.RevRange("+", "-", 1)
	if len(entries) == 0 {
		return nil
	}
	return &entries[0]
}

// isSpecialPlusID returns true if the ID is "+" (read last element feature).
func isSpecialPlusID(id string) bool {
	return strings.TrimSpace(id) == "+"
}

