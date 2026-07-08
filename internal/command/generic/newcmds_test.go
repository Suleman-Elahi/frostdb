package generic

import (
	"testing"

	"frostdb/internal/engine/core"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func newTestCtx() *context.Context {
	return &context.Context{Engine: core.New()}
}

func TestTimeCommand(t *testing.T) {
	ctx := newTestCtx()
	cmd := TimeCommand{}
	result := cmd.Execute(ctx, nil)
	if result.Type != types.TypeArray || len(result.Array) != 2 {
		t.Fatalf("expected 2-element array, got %+v", result)
	}
	// Both elements should be bulk strings representing numbers
	if result.Array[0].Type != types.TypeBulkString {
		t.Fatalf("expected bulk string for seconds")
	}
	if result.Array[1].Type != types.TypeBulkString {
		t.Fatalf("expected bulk string for microseconds")
	}
}

func TestTouchCommand(t *testing.T) {
	ctx := newTestCtx()
	ctx.Engine.Set("a", &object.Object{Type: object.TypeString, Value: "1"})
	ctx.Engine.Set("b", &object.Object{Type: object.TypeString, Value: "2"})

	cmd := TouchCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("a")},
		{Type: types.TypeBulkString, Str: []byte("b")},
		{Type: types.TypeBulkString, Str: []byte("nonexistent")},
	})
	if result.Num != 2 {
		t.Fatalf("expected 2 touched, got %d", result.Num)
	}
}

func TestCopyCommand(t *testing.T) {
	ctx := newTestCtx()
	ctx.Engine.Set("src", &object.Object{Type: object.TypeString, Value: "hello"})

	cmd := CopyCommand{}

	// Copy to new key
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("src")},
		{Type: types.TypeBulkString, Str: []byte("dst")},
	})
	if result.Num != 1 {
		t.Fatalf("expected 1, got %d", result.Num)
	}

	obj, ok := ctx.Engine.Get("dst")
	if !ok || obj.Value.(string) != "hello" {
		t.Fatalf("copy failed")
	}

	// Copy without REPLACE should fail if dest exists
	result = cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("src")},
		{Type: types.TypeBulkString, Str: []byte("dst")},
	})
	if result.Num != 0 {
		t.Fatalf("expected 0 (dest exists), got %d", result.Num)
	}

	// Copy with REPLACE should succeed
	ctx.Engine.Set("src", &object.Object{Type: object.TypeString, Value: "world"})
	result = cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("src")},
		{Type: types.TypeBulkString, Str: []byte("dst")},
		{Type: types.TypeBulkString, Str: []byte("REPLACE")},
	})
	if result.Num != 1 {
		t.Fatalf("expected 1 with REPLACE, got %d", result.Num)
	}
	obj, _ = ctx.Engine.Get("dst")
	if obj.Value.(string) != "world" {
		t.Fatalf("REPLACE copy failed")
	}
}

func TestWaitCommand(t *testing.T) {
	ctx := newTestCtx()
	cmd := WaitCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("1")},
		{Type: types.TypeBulkString, Str: []byte("0")},
	})
	if result.Num != 0 {
		t.Fatalf("expected 0 replicas, got %d", result.Num)
	}
}

func TestObjectRefCount(t *testing.T) {
	ctx := newTestCtx()
	ctx.Engine.Set("mykey", &object.Object{Type: object.TypeString, Value: "val"})

	cmd := ObjectCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("REFCOUNT")},
		{Type: types.TypeBulkString, Str: []byte("mykey")},
	})
	if result.Num != 1 {
		t.Fatalf("expected refcount 1, got %d", result.Num)
	}

	// Non-existent key
	result = cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("REFCOUNT")},
		{Type: types.TypeBulkString, Str: []byte("nokey")},
	})
	if !result.IsNull {
		t.Fatalf("expected nil for non-existent key")
	}
}

func TestResetCommand(t *testing.T) {
	ctx := newTestCtx()
	ctx.ClientName = "myconn"
	ctx.InMulti = true
	ctx.Protocol = 3

	cmd := ResetCommand{}
	result := cmd.Execute(ctx, nil)
	if string(result.Str) != "RESET" {
		t.Fatalf("expected RESET response, got %s", string(result.Str))
	}
	if ctx.ClientName != "" || ctx.InMulti || ctx.Protocol != 2 {
		t.Fatalf("state not reset properly")
	}
}

func TestAclWhoami(t *testing.T) {
	ctx := newTestCtx()
	cmd := AclCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("WHOAMI")},
	})
	if string(result.Str) != "default" {
		t.Fatalf("expected 'default', got %s", string(result.Str))
	}
}

func TestCommandCount(t *testing.T) {
	ctx := newTestCtx()
	cmd := CommandCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("COUNT")},
	})
	if result.Num < 1 {
		t.Fatalf("expected positive count, got %d", result.Num)
	}
}

func TestLatencyLatest(t *testing.T) {
	ctx := newTestCtx()
	cmd := LatencyCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("LATEST")},
	})
	if result.Type != types.TypeArray || len(result.Array) != 0 {
		t.Fatalf("expected empty array, got %+v", result)
	}
}

func TestDebugCommand(t *testing.T) {
	ctx := newTestCtx()
	cmd := DebugCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("SLEEP")},
		{Type: types.TypeBulkString, Str: []byte("0")},
	})
	if string(result.Str) != "OK" {
		t.Fatalf("expected OK, got %s", string(result.Str))
	}
}
