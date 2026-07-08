package streamcmd

import (
	"strings"
	"testing"

	"frostdb/internal/engine/core"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func newTestCtx() *context.Context {
	return &context.Context{Engine: core.New()}
}

func setupStream(ctx *context.Context, key string) *object.Stream {
	stream := object.NewStream()
	stream.Add("1-1", []object.StreamField{{Name: "a", Value: "1"}})
	stream.Add("2-1", []object.StreamField{{Name: "b", Value: "2"}})
	stream.Add("3-1", []object.StreamField{{Name: "c", Value: "3"}})
	ctx.Engine.Set(key, &object.Object{Type: object.TypeStream, Value: stream})
	return stream
}

func TestXLen(t *testing.T) {
	ctx := newTestCtx()
	setupStream(ctx, "mystream")

	cmd := XLenCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("mystream")},
	})
	if result.Num != 3 {
		t.Fatalf("expected 3, got %d", result.Num)
	}

	// Non-existent key
	result = cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("nokey")},
	})
	if result.Num != 0 {
		t.Fatalf("expected 0 for non-existent, got %d", result.Num)
	}
}

func TestXRange(t *testing.T) {
	ctx := newTestCtx()
	setupStream(ctx, "mystream")

	cmd := XRangeCommand{}

	// Full range
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("mystream")},
		{Type: types.TypeBulkString, Str: []byte("-")},
		{Type: types.TypeBulkString, Str: []byte("+")},
	})
	if result.Type != types.TypeArray || len(result.Array) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result.Array))
	}

	// With COUNT
	result = cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("mystream")},
		{Type: types.TypeBulkString, Str: []byte("-")},
		{Type: types.TypeBulkString, Str: []byte("+")},
		{Type: types.TypeBulkString, Str: []byte("COUNT")},
		{Type: types.TypeBulkString, Str: []byte("2")},
	})
	if len(result.Array) != 2 {
		t.Fatalf("expected 2 entries with COUNT, got %d", len(result.Array))
	}

	// Specific range
	result = cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("mystream")},
		{Type: types.TypeBulkString, Str: []byte("2-0")},
		{Type: types.TypeBulkString, Str: []byte("3-1")},
	})
	if len(result.Array) != 2 {
		t.Fatalf("expected 2 entries in range, got %d", len(result.Array))
	}
}

func TestXDel(t *testing.T) {
	ctx := newTestCtx()
	setupStream(ctx, "mystream")

	cmd := XDelCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("mystream")},
		{Type: types.TypeBulkString, Str: []byte("2-1")},
	})
	if result.Num != 1 {
		t.Fatalf("expected 1 deleted, got %d", result.Num)
	}

	// Verify length decreased
	lenCmd := XLenCommand{}
	lenResult := lenCmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("mystream")},
	})
	if lenResult.Num != 2 {
		t.Fatalf("expected 2 after delete, got %d", lenResult.Num)
	}

	// Delete non-existent ID
	result = cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("mystream")},
		{Type: types.TypeBulkString, Str: []byte("99-99")},
	})
	if result.Num != 0 {
		t.Fatalf("expected 0 for non-existent ID, got %d", result.Num)
	}
}

func TestXTrim(t *testing.T) {
	ctx := newTestCtx()
	setupStream(ctx, "mystream")

	cmd := XTrimCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("mystream")},
		{Type: types.TypeBulkString, Str: []byte("MAXLEN")},
		{Type: types.TypeBulkString, Str: []byte("2")},
	})
	if result.Num != 1 {
		t.Fatalf("expected 1 trimmed, got %d", result.Num)
	}

	lenCmd := XLenCommand{}
	lenResult := lenCmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("mystream")},
	})
	if lenResult.Num != 2 {
		t.Fatalf("expected 2 after trim, got %d", lenResult.Num)
	}
}

func TestXRead(t *testing.T) {
	ctx := newTestCtx()
	setupStream(ctx, "mystream")

	cmd := XReadCommand{}

	// Read all after 0-0
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("STREAMS")},
		{Type: types.TypeBulkString, Str: []byte("mystream")},
		{Type: types.TypeBulkString, Str: []byte("0-0")},
	})
	if result.Type != types.TypeArray || len(result.Array) != 1 {
		t.Fatalf("expected 1 stream result, got %+v", result)
	}
	streamResult := result.Array[0]
	entries := streamResult.Array[1]
	if len(entries.Array) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries.Array))
	}

	// Read with COUNT
	result = cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("COUNT")},
		{Type: types.TypeBulkString, Str: []byte("1")},
		{Type: types.TypeBulkString, Str: []byte("STREAMS")},
		{Type: types.TypeBulkString, Str: []byte("mystream")},
		{Type: types.TypeBulkString, Str: []byte("0-0")},
	})
	if result.Type != types.TypeArray {
		t.Fatalf("expected array, got %+v", result)
	}
	entries = result.Array[0].Array[1]
	if len(entries.Array) != 1 {
		t.Fatalf("expected 1 entry with COUNT, got %d", len(entries.Array))
	}

	// Read after last ID should return nil
	result = cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("STREAMS")},
		{Type: types.TypeBulkString, Str: []byte("mystream")},
		{Type: types.TypeBulkString, Str: []byte("3-1")},
	})
	if !result.IsNull {
		t.Fatalf("expected nil when no new entries, got %+v", result)
	}
}

func TestXInfoStream(t *testing.T) {
	ctx := newTestCtx()
	stream := setupStream(ctx, "mystream")
	stream.CreateGroup("grp1", "0")

	cmd := XInfoCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("STREAM")},
		{Type: types.TypeBulkString, Str: []byte("mystream")},
	})
	if result.Type != types.TypeArray {
		t.Fatalf("expected array, got %+v", result)
	}
	// Should contain length=3, groups=1
	found := false
	for i := 0; i < len(result.Array)-1; i += 2 {
		if string(result.Array[i].Str) == "length" && result.Array[i+1].Num == 3 {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected length=3 in XINFO STREAM output")
	}
}

func TestXInfoConsumers(t *testing.T) {
	ctx := newTestCtx()
	stream := setupStream(ctx, "mystream")
	stream.CreateGroup("grp1", "0")
	stream.ReadGroup("grp1", "consumer1", 2, ">", false)

	cmd := XInfoCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("CONSUMERS")},
		{Type: types.TypeBulkString, Str: []byte("mystream")},
		{Type: types.TypeBulkString, Str: []byte("grp1")},
	})
	if result.Type != types.TypeArray || len(result.Array) == 0 {
		t.Fatalf("expected non-empty consumers array, got %+v", result)
	}
}

func TestXGroupHelpRejectsExtraArgs(t *testing.T) {
	cmd := XGroupCommand{}
	res := cmd.Execute(newTestCtx(), []types.Value{
		types.NewString("HELP"),
		types.NewString("unexpected"),
	})
	if res.Type != types.TypeError || !strings.Contains(string(res.Str), "wrong number of arguments for 'xgroup|help' command") {
		t.Fatalf("unexpected XGROUP HELP response: %v", res)
	}
}

func TestXInfoHelpRejectsExtraArgs(t *testing.T) {
	cmd := XInfoCommand{}
	res := cmd.Execute(newTestCtx(), []types.Value{
		types.NewString("HELP"),
		types.NewString("unexpected"),
	})
	if res.Type != types.TypeError || !strings.Contains(string(res.Str), "wrong number of arguments for 'xinfo|help' command") {
		t.Fatalf("unexpected XINFO HELP response: %v", res)
	}
}
