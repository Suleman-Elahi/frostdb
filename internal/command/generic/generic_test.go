package generic

import (
	"testing"
	"time"

	"frostdb/internal/engine/core"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func TestPingCommand(t *testing.T) {
	cmd := PingCommand{}
	ctx := &context.Context{}

	// Test no args
	res := cmd.Execute(ctx, []types.Value{})
	if res.Type != types.TypeSimpleString || string(res.Str) != "PONG" {
		t.Errorf("Expected PONG, got %v", res)
	}

	// Test with arg
	res = cmd.Execute(ctx, []types.Value{types.NewString("hello")})
	if res.Type != types.TypeBulkString || string(res.Str) != "hello" {
		t.Errorf("Expected bulk string hello, got %v", res)
	}
}

func TestEchoCommand(t *testing.T) {
	cmd := EchoCommand{}
	ctx := &context.Context{}

	res := cmd.Execute(ctx, []types.Value{types.NewString("test")})
	if res.Type != types.TypeBulkString || string(res.Str) != "test" {
		t.Errorf("Expected test, got %v", res)
	}
}

func TestDelCommand(t *testing.T) {
	cmd := DelCommand{}
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	// Setup data
	engine.Set("k1", &object.Object{Value: "v1"})
	engine.Set("k2", &object.Object{Value: "v2"})

	res := cmd.Execute(ctx, []types.Value{types.NewString("k1"), types.NewString("k2"), types.NewString("k3")})
	if res.Type != types.TypeInteger || res.Num != 2 {
		t.Errorf("Expected to delete 2 keys, got %d", res.Num)
	}
}

func TestExpireCommand(t *testing.T) {
	cmd := ExpireCommand{}
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	engine.Set("k1", &object.Object{Value: "v1"})

	res := cmd.Execute(ctx, []types.Value{types.NewString("k1"), types.NewString("10")})
	if res.Type != types.TypeInteger || res.Num != 1 {
		t.Errorf("Expected 1, got %v", res)
	}

	obj, _ := engine.Get("k1")
	if obj.ExpireAt <= time.Now().UnixMilli() {
		t.Errorf("Expected ExpireAt to be in the future")
	}
}

func TestInfoCommand(t *testing.T) {
	cmd := InfoCommand{}
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	res := cmd.Execute(ctx, []types.Value{})
	if res.Type != types.TypeBulkString {
		t.Fatalf("Expected bulk string, got %v", res.Type)
	}
	if string(res.Str) == "" {
		t.Fatalf("Expected non-empty INFO response")
	}
}

func TestScanCommand(t *testing.T) {
	cmd := ScanCommand{}
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	res := cmd.Execute(ctx, []types.Value{types.NewString("0")})
	if res.Type != types.TypeArray || len(res.Array) != 2 {
		t.Fatalf("Expected two-element array, got %v", res)
	}
	if string(res.Array[0].Str) != "0" {
		t.Fatalf("Expected cursor 0, got %q", string(res.Array[0].Str))
	}
	if res.Array[1].Type != types.TypeArray {
		t.Fatalf("Expected key list array, got %v", res.Array[1].Type)
	}
}

func TestClientSetInfoCommand(t *testing.T) {
	cmd := ClientCommand{}
	ctx := &context.Context{}

	res := cmd.Execute(ctx, []types.Value{
		types.NewString("SETINFO"),
		types.NewString("LIB-NAME"),
		types.NewString("redis-py"),
	})
	if res.Type != types.TypeSimpleString || string(res.Str) != "OK" {
		t.Fatalf("Expected OK, got %v", res)
	}
}
