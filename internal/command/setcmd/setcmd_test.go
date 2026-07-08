package setcmd

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

func TestSMisMember(t *testing.T) {
	ctx := newTestCtx()
	s := object.NewSet()
	s.Add("a", "b", "c")
	ctx.Engine.Set("myset", &object.Object{Type: object.TypeSet, Value: s})

	cmd := SMisMemberCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("myset")},
		{Type: types.TypeBulkString, Str: []byte("a")},
		{Type: types.TypeBulkString, Str: []byte("x")},
		{Type: types.TypeBulkString, Str: []byte("b")},
	})

	if result.Type != types.TypeArray || len(result.Array) != 3 {
		t.Fatalf("expected 3-element array, got %+v", result)
	}
	if result.Array[0].Num != 1 {
		t.Errorf("expected 1 for 'a', got %d", result.Array[0].Num)
	}
	if result.Array[1].Num != 0 {
		t.Errorf("expected 0 for 'x', got %d", result.Array[1].Num)
	}
	if result.Array[2].Num != 1 {
		t.Errorf("expected 1 for 'b', got %d", result.Array[2].Num)
	}
}

func TestSMisMemberNonExistent(t *testing.T) {
	ctx := newTestCtx()
	cmd := SMisMemberCommand{}
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("nokey")},
		{Type: types.TypeBulkString, Str: []byte("a")},
		{Type: types.TypeBulkString, Str: []byte("b")},
	})
	if result.Type != types.TypeArray || len(result.Array) != 2 {
		t.Fatalf("expected 2-element array, got %+v", result)
	}
	if result.Array[0].Num != 0 || result.Array[1].Num != 0 {
		t.Errorf("expected all zeros for non-existent key")
	}
}

func TestSInterCard(t *testing.T) {
	ctx := newTestCtx()
	s1 := object.NewSet()
	s1.Add("a", "b", "c", "d")
	s2 := object.NewSet()
	s2.Add("b", "c", "e")
	ctx.Engine.Set("s1", &object.Object{Type: object.TypeSet, Value: s1})
	ctx.Engine.Set("s2", &object.Object{Type: object.TypeSet, Value: s2})

	cmd := SInterCardCommand{}

	// Full intersection
	result := cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("2")},
		{Type: types.TypeBulkString, Str: []byte("s1")},
		{Type: types.TypeBulkString, Str: []byte("s2")},
	})
	if result.Num != 2 {
		t.Fatalf("expected 2, got %d", result.Num)
	}

	// With LIMIT 1
	result = cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("2")},
		{Type: types.TypeBulkString, Str: []byte("s1")},
		{Type: types.TypeBulkString, Str: []byte("s2")},
		{Type: types.TypeBulkString, Str: []byte("LIMIT")},
		{Type: types.TypeBulkString, Str: []byte("1")},
	})
	if result.Num != 1 {
		t.Fatalf("expected 1 with LIMIT, got %d", result.Num)
	}

	// Non-existent key
	result = cmd.Execute(ctx, []types.Value{
		{Type: types.TypeBulkString, Str: []byte("2")},
		{Type: types.TypeBulkString, Str: []byte("s1")},
		{Type: types.TypeBulkString, Str: []byte("nokey")},
	})
	if result.Num != 0 {
		t.Fatalf("expected 0 for non-existent key, got %d", result.Num)
	}
}
