package listcmd

import (
	"testing"
	"time"

	"frostdb/internal/engine/core"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func TestPushPop(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	lpush := PushCommand{IsLeft: true}
	lpop := PopCommand{IsLeft: true}
	rpop := PopCommand{IsLeft: false}

	// LPUSH k1 a b c -> [c, b, a]
	res := lpush.Execute(ctx, []types.Value{types.NewString("k1"), types.NewString("a"), types.NewString("b"), types.NewString("c")})
	if res.Type != types.TypeInteger || res.Num != 3 {
		t.Errorf("Expected 3, got %v", res)
	}

	// RPOP k1 -> a
	res = rpop.Execute(ctx, []types.Value{types.NewString("k1")})
	if res.Type != types.TypeBulkString || string(res.Str) != "a" {
		t.Errorf("Expected 'a', got %v", res)
	}

	// LPOP k1 -> c
	res = lpop.Execute(ctx, []types.Value{types.NewString("k1")})
	if res.Type != types.TypeBulkString || string(res.Str) != "c" {
		t.Errorf("Expected 'c', got %v", res)
	}
}

func TestBPop(t *testing.T) {
	engine := core.New()
	
	blpop := BPopCommand{IsLeft: true}
	rpush := PushCommand{IsLeft: false}

	// Start BLPOP in a separate goroutine
	done := make(chan types.Value)
	go func() {
		ctx := &context.Context{Engine: engine}
		res := blpop.Execute(ctx, []types.Value{types.NewString("k1"), types.NewString("0")})
		done <- res
	}()

	// Wait briefly to ensure BLPOP is blocking
	time.Sleep(50 * time.Millisecond)

	// RPUSH k1 val
	ctx := &context.Context{Engine: engine}
	rpush.Execute(ctx, []types.Value{types.NewString("k1"), types.NewString("val")})

	// Wait for BLPOP to complete
	select {
	case res := <-done:
		if res.Type != types.TypeArray || len(res.Array) != 2 || string(res.Array[1].Str) != "val" {
			t.Errorf("Expected ['k1', 'val'], got %v", res)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("Timeout waiting for BLPOP")
	}
}
