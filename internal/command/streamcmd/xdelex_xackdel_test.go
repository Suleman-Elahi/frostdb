package streamcmd

import (
	"testing"

	"frostdb/internal/engine/core"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func TestXDelexDelRefRemovesPendingEntries(t *testing.T) {
	ctx := &context.Context{Engine: core.New()}

	group := XGroupCommand{}
	add := XAddCommand{}
	readGroup := XReadGroupCommand{}
	cmd := XDelexCommand{}

	if res := group.Execute(ctx, []types.Value{
		types.NewString("CREATE"),
		types.NewString("mystream"),
		types.NewString("g1"),
		types.NewString("0"),
		types.NewString("MKSTREAM"),
	}); res.Type != types.TypeSimpleString {
		t.Fatalf("create g1 failed: %v", res)
	}
	if res := group.Execute(ctx, []types.Value{
		types.NewString("CREATE"),
		types.NewString("mystream"),
		types.NewString("g2"),
		types.NewString("0"),
	}); res.Type != types.TypeSimpleString {
		t.Fatalf("create g2 failed: %v", res)
	}

	if id := add.Execute(ctx, []types.Value{
		types.NewString("mystream"),
		types.NewString("1-0"),
		types.NewString("field"),
		types.NewString("one"),
	}); id.Type != types.TypeBulkString {
		t.Fatalf("unexpected xadd result: %v", id)
	}
	if id := add.Execute(ctx, []types.Value{
		types.NewString("mystream"),
		types.NewString("2-0"),
		types.NewString("field"),
		types.NewString("two"),
	}); id.Type != types.TypeBulkString {
		t.Fatalf("unexpected xadd result: %v", id)
	}

	readArgs := []types.Value{
		types.NewString("GROUP"),
		types.NewString("g1"),
		types.NewString("c1"),
		types.NewString("STREAMS"),
		types.NewString("mystream"),
		types.NewString(">"),
	}
	if res := readGroup.Execute(ctx, readArgs); res.Type != types.TypeArray {
		t.Fatalf("xreadgroup g1 failed: %v", res)
	}
	readArgs[1] = types.NewString("g2")
	readArgs[2] = types.NewString("c2")
	if res := readGroup.Execute(ctx, readArgs); res.Type != types.TypeArray {
		t.Fatalf("xreadgroup g2 failed: %v", res)
	}

	res := cmd.Execute(ctx, []types.Value{
		types.NewString("mystream"),
		types.NewString("DELREF"),
		types.NewString("IDS"),
		types.NewString("1"),
		types.NewString("1-0"),
	})
	if res.Type != types.TypeArray || len(res.Array) != 1 || res.Array[0].Num != 1 {
		t.Fatalf("unexpected XDELEX result: %v", res)
	}

	obj, ok := ctx.Engine.Get("mystream")
	if !ok {
		t.Fatal("mystream missing after XDELEX")
	}
	stream := obj.Value.(*object.Stream)
	if stream.Len() != 1 {
		t.Fatalf("expected 1 entry remaining, got %d", stream.Len())
	}
	if pending, _, _, _ := stream.PendingSummary("g1"); pending != 1 {
		t.Fatalf("expected 1 pending entry in g1, got %d", pending)
	}
	if pending, _, _, _ := stream.PendingSummary("g2"); pending != 1 {
		t.Fatalf("expected 1 pending entry in g2, got %d", pending)
	}
}

func TestXAckDelAckedKeepsReferencedEntries(t *testing.T) {
	ctx := &context.Context{Engine: core.New()}

	group := XGroupCommand{}
	add := XAddCommand{}
	readGroup := XReadGroupCommand{}
	ackdel := XAckDelCommand{}

	if res := group.Execute(ctx, []types.Value{
		types.NewString("CREATE"),
		types.NewString("mystream"),
		types.NewString("g1"),
		types.NewString("0"),
		types.NewString("MKSTREAM"),
	}); res.Type != types.TypeSimpleString {
		t.Fatalf("create g1 failed: %v", res)
	}
	if res := group.Execute(ctx, []types.Value{
		types.NewString("CREATE"),
		types.NewString("mystream"),
		types.NewString("g2"),
		types.NewString("0"),
	}); res.Type != types.TypeSimpleString {
		t.Fatalf("create g2 failed: %v", res)
	}

	if id := add.Execute(ctx, []types.Value{
		types.NewString("mystream"),
		types.NewString("1-0"),
		types.NewString("field"),
		types.NewString("one"),
	}); id.Type != types.TypeBulkString {
		t.Fatalf("unexpected xadd result: %v", id)
	}

	if res := readGroup.Execute(ctx, []types.Value{
		types.NewString("GROUP"),
		types.NewString("g1"),
		types.NewString("c1"),
		types.NewString("STREAMS"),
		types.NewString("mystream"),
		types.NewString(">"),
	}); res.Type != types.TypeArray {
		t.Fatalf("xreadgroup failed: %v", res)
	}

	res := ackdel.Execute(ctx, []types.Value{
		types.NewString("mystream"),
		types.NewString("g1"),
		types.NewString("ACKED"),
		types.NewString("IDS"),
		types.NewString("1"),
		types.NewString("1-0"),
	})
	if res.Type != types.TypeArray || len(res.Array) != 1 || res.Array[0].Num != 2 {
		t.Fatalf("expected ACKED keep result 2, got %v", res)
	}

	obj, ok := ctx.Engine.Get("mystream")
	if !ok {
		t.Fatal("mystream missing after XACKDEL")
	}
	stream := obj.Value.(*object.Stream)
	if stream.Len() != 1 {
		t.Fatalf("expected stream entry to remain, got %d", stream.Len())
	}
	if pending, _, _, _ := stream.PendingSummary("g1"); pending != 0 {
		t.Fatalf("expected g1 pending to be cleared, got %d", pending)
	}
}
