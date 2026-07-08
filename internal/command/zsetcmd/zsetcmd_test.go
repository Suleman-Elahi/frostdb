package zsetcmd

import (
	"testing"

	"frostdb/internal/engine/core"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func TestZAddAndZRange(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	zadd := ZAddCommand{}
	zrange := ZRangeCommand{}

	// ZADD myzset 1 "one" 2 "two" 3 "three"
	res := zadd.Execute(ctx, []types.Value{
		types.NewString("myzset"),
		types.NewString("1"), types.NewString("one"),
		types.NewString("2"), types.NewString("two"),
		types.NewString("3"), types.NewString("three"),
	})
	if res.Num != 3 {
		t.Errorf("Expected 3 added, got %d", res.Num)
	}

	// ZRANGE myzset 0 -1
	res = zrange.Execute(ctx, []types.Value{
		types.NewString("myzset"),
		types.NewString("0"),
		types.NewString("-1"),
	})
	if len(res.Array) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(res.Array))
	}
	if string(res.Array[0].Str) != "one" {
		t.Errorf("Expected 'one' first, got '%s'", string(res.Array[0].Str))
	}

	// ZRANGE myzset 0 -1 WITHSCORES
	res = zrange.Execute(ctx, []types.Value{
		types.NewString("myzset"),
		types.NewString("0"),
		types.NewString("-1"),
		types.NewString("WITHSCORES"),
	})
	if len(res.Array) != 6 {
		t.Fatalf("Expected 6 results with scores, got %d", len(res.Array))
	}
	if string(res.Array[1].Str) != "1" {
		t.Errorf("Expected score '1', got '%s'", string(res.Array[1].Str))
	}
}

func TestZAddFlags(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	zadd := ZAddCommand{}

	// Add initial
	zadd.Execute(ctx, []types.Value{
		types.NewString("myzset"),
		types.NewString("1"), types.NewString("one"),
	})

	// ZADD NX - should not update existing
	res := zadd.Execute(ctx, []types.Value{
		types.NewString("myzset"),
		types.NewString("NX"),
		types.NewString("5"), types.NewString("one"),
		types.NewString("2"), types.NewString("two"),
	})
	if res.Num != 1 {
		t.Errorf("Expected 1 new with NX, got %d", res.Num)
	}

	// ZADD XX - should only update existing
	res = zadd.Execute(ctx, []types.Value{
		types.NewString("myzset"),
		types.NewString("XX"),
		types.NewString("10"), types.NewString("one"),
		types.NewString("20"), types.NewString("nonexistent"),
	})
	if res.Num != 0 {
		t.Errorf("Expected 0 new with XX, got %d", res.Num)
	}
}

func TestZPopMin(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	zadd := ZAddCommand{}
	zpopmin := ZPopMinCommand{}

	zadd.Execute(ctx, []types.Value{
		types.NewString("myzset"),
		types.NewString("3"), types.NewString("three"),
		types.NewString("1"), types.NewString("one"),
		types.NewString("2"), types.NewString("two"),
	})

	// ZPOPMIN myzset
	res := zpopmin.Execute(ctx, []types.Value{types.NewString("myzset")})
	if len(res.Array) != 2 {
		t.Fatalf("Expected 2 elements (member+score), got %d", len(res.Array))
	}
	if string(res.Array[0].Str) != "one" {
		t.Errorf("Expected 'one', got '%s'", string(res.Array[0].Str))
	}

	// ZPOPMIN myzset 2
	res = zpopmin.Execute(ctx, []types.Value{types.NewString("myzset"), types.NewString("2")})
	if len(res.Array) != 4 {
		t.Fatalf("Expected 4 elements (2 pairs), got %d", len(res.Array))
	}
}

func TestZRangeByScore(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	zadd := ZAddCommand{}
	zrangebyscore := ZRangeByScoreCommand{}
	zrange := ZRangeCommand{}

	zadd.Execute(ctx, []types.Value{
		types.NewString("myzset"),
		types.NewString("1"), types.NewString("one"),
		types.NewString("2"), types.NewString("two"),
		types.NewString("3"), types.NewString("three"),
	})

	// ZRANGEBYSCORE myzset -inf +inf
	res := zrangebyscore.Execute(ctx, []types.Value{
		types.NewString("myzset"),
		types.NewString("-inf"),
		types.NewString("+inf"),
	})
	if len(res.Array) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(res.Array))
	}

	// ZRANGEBYSCORE myzset 1 2
	res = zrangebyscore.Execute(ctx, []types.Value{
		types.NewString("myzset"),
		types.NewString("1"),
		types.NewString("2"),
	})
	if len(res.Array) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(res.Array))
	}

	// ZRANGEBYSCORE myzset (1 2 — exclusive lower bound
	res = zrangebyscore.Execute(ctx, []types.Value{
		types.NewString("myzset"),
		types.NewString("(1"),
		types.NewString("2"),
	})
	if len(res.Array) != 1 {
		t.Fatalf("Expected 1 result with exclusive min, got %d", len(res.Array))
	}
	if string(res.Array[0].Str) != "two" {
		t.Errorf("Expected 'two', got '%s'", string(res.Array[0].Str))
	}

	// ZRANGE myzset 3 1 BYSCORE REV WITHSCORES
	res = zrange.Execute(ctx, []types.Value{
		types.NewString("myzset"),
		types.NewString("3"),
		types.NewString("1"),
		types.NewString("BYSCORE"),
		types.NewString("REV"),
		types.NewString("WITHSCORES"),
	})
	if len(res.Array) != 6 {
		t.Fatalf("Expected 6 results for reverse byscore with scores, got %d", len(res.Array))
	}
	if string(res.Array[0].Str) != "three" || string(res.Array[2].Str) != "two" || string(res.Array[4].Str) != "one" {
		t.Fatalf("Unexpected reverse byscore order: %q %q %q", string(res.Array[0].Str), string(res.Array[2].Str), string(res.Array[4].Str))
	}
}
