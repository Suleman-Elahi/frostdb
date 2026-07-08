package hashcmd

import (
	"testing"

	"frostdb/internal/engine/core"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func TestHashCommands(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	hset := HSetCommand{}
	hget := HGetCommand{}
	hdel := HDelCommand{}
	hlen := HLenCommand{}
	hgetall := HGetAllCommand{}

	// HSET myhash f1 v1 f2 v2
	res := hset.Execute(ctx, []types.Value{
		types.NewString("myhash"),
		types.NewString("f1"), types.NewString("v1"),
		types.NewString("f2"), types.NewString("v2"),
	})
	if res.Type != types.TypeInteger || res.Num != 2 {
		t.Errorf("Expected 2 added, got %v", res)
	}

	// HSET update existing
	res = hset.Execute(ctx, []types.Value{
		types.NewString("myhash"),
		types.NewString("f1"), types.NewString("v1-new"),
	})
	if res.Type != types.TypeInteger || res.Num != 0 {
		t.Errorf("Expected 0 added (update), got %v", res)
	}

	// HGET f1
	res = hget.Execute(ctx, []types.Value{
		types.NewString("myhash"),
		types.NewString("f1"),
	})
	if res.Type != types.TypeBulkString || string(res.Str) != "v1-new" {
		t.Errorf("Expected v1-new, got %v", res)
	}

	// HGET non-existent field
	res = hget.Execute(ctx, []types.Value{
		types.NewString("myhash"),
		types.NewString("f3"),
	})
	if !res.IsNull {
		t.Errorf("Expected Null, got %v", res)
	}

	// HLEN
	res = hlen.Execute(ctx, []types.Value{
		types.NewString("myhash"),
	})
	if res.Type != types.TypeInteger || res.Num != 2 {
		t.Errorf("Expected length 2, got %v", res)
	}

	// HGETALL
	res = hgetall.Execute(ctx, []types.Value{
		types.NewString("myhash"),
	})
	if res.Type != types.TypeArray || len(res.Array) != 4 {
		t.Errorf("Expected array of length 4, got %v", res)
	}

	// HDEL f1
	res = hdel.Execute(ctx, []types.Value{
		types.NewString("myhash"),
		types.NewString("f1"),
	})
	if res.Type != types.TypeInteger || res.Num != 1 {
		t.Errorf("Expected 1 deleted, got %v", res)
	}

	// HLEN after del
	res = hlen.Execute(ctx, []types.Value{
		types.NewString("myhash"),
	})
	if res.Type != types.TypeInteger || res.Num != 1 {
		t.Errorf("Expected length 1, got %v", res)
	}
}
