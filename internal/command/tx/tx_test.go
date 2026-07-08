package tx

import (
	"testing"

	"frostdb/internal/command/registry"
	strcmd "frostdb/internal/command/string"
	"frostdb/internal/engine/core"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func init() {
	registry.Register(MultiCommand{})
	registry.Register(ExecCommand{})
	registry.Register(DiscardCommand{})
	registry.Register(strcmd.GetCommand{})
	registry.Register(strcmd.SetCommand{})
}

func TestMultiExec(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	multi := MultiCommand{}
	exec := ExecCommand{}

	// Start transaction
	res := multi.Execute(ctx, nil)
	if string(res.Str) != "OK" {
		t.Fatalf("expected OK, got %v", res)
	}
	if !ctx.InMulti {
		t.Fatal("expected InMulti to be true")
	}

	// Queue SET command
	queued := registry.Dispatch(ctx, []types.Value{
		types.NewString("SET"),
		types.NewString("key1"),
		types.NewString("value1"),
	})
	if string(queued.Str) != "QUEUED" {
		t.Fatalf("expected QUEUED, got %v", queued)
	}

	// Queue GET command
	queued = registry.Dispatch(ctx, []types.Value{
		types.NewString("GET"),
		types.NewString("key1"),
	})
	if string(queued.Str) != "QUEUED" {
		t.Fatalf("expected QUEUED, got %v", queued)
	}

	// Execute
	result := exec.Execute(ctx, nil)
	if result.Type != types.TypeArray {
		t.Fatalf("expected array, got %v", result)
	}
	if len(result.Array) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result.Array))
	}
	// SET returns OK
	if string(result.Array[0].Str) != "OK" {
		t.Fatalf("expected OK from SET, got %v", result.Array[0])
	}
	// GET returns value1
	if string(result.Array[1].Str) != "value1" {
		t.Fatalf("expected value1 from GET, got %v", result.Array[1])
	}

	// Verify not in multi anymore
	if ctx.InMulti {
		t.Fatal("expected InMulti to be false after EXEC")
	}
}

func TestDiscard(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	multi := MultiCommand{}
	discard := DiscardCommand{}

	multi.Execute(ctx, nil)

	// Queue a SET
	registry.Dispatch(ctx, []types.Value{
		types.NewString("SET"),
		types.NewString("key1"),
		types.NewString("value1"),
	})

	// Discard
	res := discard.Execute(ctx, nil)
	if string(res.Str) != "OK" {
		t.Fatalf("expected OK, got %v", res)
	}
	if ctx.InMulti {
		t.Fatal("expected InMulti to be false after DISCARD")
	}

	// Verify key was not set
	_, ok := engine.Get("key1")
	if ok {
		t.Fatal("expected key1 to not exist after DISCARD")
	}
}

func TestExecWithoutMulti(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	exec := ExecCommand{}
	res := exec.Execute(ctx, nil)
	if res.Type != types.TypeError {
		t.Fatalf("expected error, got %v", res)
	}
}

func TestDiscardWithoutMulti(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	discard := DiscardCommand{}
	res := discard.Execute(ctx, nil)
	if res.Type != types.TypeError {
		t.Fatalf("expected error, got %v", res)
	}
}

func TestNestedMulti(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	multi := MultiCommand{}
	multi.Execute(ctx, nil)

	res := multi.Execute(ctx, nil)
	if res.Type != types.TypeError {
		t.Fatalf("expected error for nested MULTI, got %v", res)
	}
}

func TestTxErrorAbort(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	multi := MultiCommand{}
	exec := ExecCommand{}

	multi.Execute(ctx, nil)

	// Send unknown command - should mark tx as errored
	res := registry.Dispatch(ctx, []types.Value{
		types.NewString("BADCOMMAND"),
		types.NewString("arg"),
	})
	if res.Type != types.TypeError {
		t.Fatalf("expected error for unknown command, got %v", res)
	}

	// EXEC should abort
	result := exec.Execute(ctx, nil)
	if result.Type != types.TypeError {
		t.Fatalf("expected EXECABORT error, got %v", result)
	}
}
