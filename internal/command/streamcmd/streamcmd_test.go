package streamcmd

import (
	"testing"

	"frostdb/internal/engine/core"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func TestXAddAndXReadGroup(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	xgroup := XGroupCommand{}
	xadd := XAddCommand{}
	xreadgroup := XReadGroupCommand{}
	xack := XAckCommand{}
	xpending := XPendingCommand{}

	res := xgroup.Execute(ctx, []types.Value{
		types.NewString("CREATE"),
		types.NewString("mystream"),
		types.NewString("g1"),
		types.NewString("0-0"),
		types.NewString("MKSTREAM"),
	})
	if res.Type != types.TypeSimpleString || string(res.Str) != "OK" {
		t.Fatalf("expected OK, got %v", res)
	}

	id1 := xadd.Execute(ctx, []types.Value{
		types.NewString("mystream"),
		types.NewString("1-0"),
		types.NewString("field"),
		types.NewString("one"),
	})
	if id1.Type != types.TypeBulkString || string(id1.Str) != "1-0" {
		t.Fatalf("unexpected xadd result: %v", id1)
	}

	id2 := xadd.Execute(ctx, []types.Value{
		types.NewString("mystream"),
		types.NewString("2-0"),
		types.NewString("field"),
		types.NewString("two"),
	})
	if id2.Type != types.TypeBulkString || string(id2.Str) != "2-0" {
		t.Fatalf("unexpected xadd result: %v", id2)
	}

	read := xreadgroup.Execute(ctx, []types.Value{
		types.NewString("GROUP"),
		types.NewString("g1"),
		types.NewString("c1"),
		types.NewString("COUNT"),
		types.NewString("10"),
		types.NewString("STREAMS"),
		types.NewString("mystream"),
		types.NewString(">"),
	})
	if read.Type != types.TypeArray || read.IsNull {
		t.Fatalf("expected array response, got %v", read)
	}
	if len(read.Array) != 1 {
		t.Fatalf("expected one stream result, got %d", len(read.Array))
	}
	if string(read.Array[0].Array[0].Str) != "mystream" {
		t.Fatalf("unexpected stream key: %v", read.Array[0])
	}
	entries := read.Array[0].Array[1]
	if len(entries.Array) != 2 {
		t.Fatalf("expected two stream entries, got %d", len(entries.Array))
	}

	acked := xack.Execute(ctx, []types.Value{
		types.NewString("mystream"),
		types.NewString("g1"),
		types.NewString("1-0"),
	})
	if acked.Type != types.TypeInteger || acked.Num != 1 {
		t.Fatalf("expected one ack, got %v", acked)
	}

	summary := xpending.Execute(ctx, []types.Value{
		types.NewString("mystream"),
		types.NewString("g1"),
	})
	if summary.Type != types.TypeArray || len(summary.Array) != 4 {
		t.Fatalf("unexpected XPENDING summary: %v", summary)
	}
	if summary.Array[0].Num != 1 {
		t.Fatalf("expected one pending message after one ack, got %d", summary.Array[0].Num)
	}
}

