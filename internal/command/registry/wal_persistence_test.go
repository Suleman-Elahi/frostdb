package registry

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"frostdb/internal/command/hashcmd"
	"frostdb/internal/command/setcmd"
	"frostdb/internal/command/zsetcmd"
	"frostdb/internal/engine/core"
	"frostdb/internal/engine/object"
	"frostdb/internal/engine/persistence/wal"
	kernelcontext "frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func TestDispatchCanonicalizesNondeterministicWALWrites(t *testing.T) {
	dir := t.TempDir()
	engine, err := core.NewWithPersistence(core.PersistOptions{
		DataDir:      dir,
		WALFsyncMode: wal.FsyncAlways,
	})
	if err != nil {
		t.Fatalf("failed to create persistent engine: %v", err)
	}
	view := engine.(*core.DBView)
	parent := view.ParentEngine()

	seedSet := func(key string, members ...string) {
		t.Helper()
		setObj := object.NewSet()
		setObj.Add(members...)
		view.Set(key, &object.Object{Type: object.TypeSet, Value: setObj})
		frame := make([][]byte, 0, len(members)+2)
		frame = append(frame, []byte("SADD"), []byte(key))
		for _, member := range members {
			frame = append(frame, []byte(member))
		}
		if err := view.LogCommand(frame); err != nil {
			t.Fatalf("failed to seed set %q: %v", key, err)
		}
	}
	seedZSet := func(key string) {
		t.Helper()
		zset := object.NewZSet()
		zset.Add(1, "member")
		view.Set(key, &object.Object{Type: object.TypeZSet, Value: zset})
		if err := view.LogCommand([][]byte{[]byte("ZADD"), []byte(key), []byte("1"), []byte("member")}); err != nil {
			t.Fatalf("failed to seed sorted set %q: %v", key, err)
		}
	}

	seedSet("spop-partial", "a", "b")
	seedSet("spop-all", "a", "b")
	seedZSet("zpopmin-final")
	seedZSet("zpopmax-final")

	Register(setcmd.SPopCommand{})
	Register(zsetcmd.ZPopMinCommand{})
	Register(zsetcmd.ZPopMaxCommand{})
	ctx := &kernelcontext.Context{Engine: view, Protocol: 2, AllowOOM: true}
	dispatch := func(argv ...string) types.Value {
		t.Helper()
		args := make([]types.Value, len(argv))
		for i, arg := range argv {
			args[i] = types.NewString(arg)
		}
		result := Dispatch(ctx, args)
		if result.Type == types.TypeError {
			t.Fatalf("dispatch %v failed: %s", argv, result.Str)
		}
		return result
	}

	dispatch("SPOP", "spop-partial")
	dispatch("SPOP", "spop-all", "2")
	dispatch("ZPOPMIN", "zpopmin-final")
	dispatch("ZPOPMAX", "zpopmax-final")
	dispatch("SPOP", "missing")

	if err := parent.Close(); err != nil {
		t.Fatalf("failed to close engine: %v", err)
	}

	entries, err := wal.ReadAll(filepath.Join(dir, "wal", "current.wal"))
	if err != nil {
		t.Fatalf("failed to read WAL: %v", err)
	}
	if len(entries) != 8 {
		t.Fatalf("expected 8 WAL entries (four seeds and four mutations), got %d", len(entries))
	}

	partial := decodeTestWALCommand(t, entries[4].Command)
	if len(partial) != 3 || string(partial[0]) != "SREM" || string(partial[1]) != "spop-partial" || (string(partial[2]) != "a" && string(partial[2]) != "b") {
		t.Fatalf("expected partial SPOP to log deterministic SREM, got %q", partial)
	}
	for i, want := range [][]byte{[]byte("spop-all"), []byte("zpopmin-final"), []byte("zpopmax-final")} {
		frame := decodeTestWALCommand(t, entries[5+i].Command)
		if len(frame) != 2 || string(frame[0]) != "DEL" || string(frame[1]) != string(want) {
			t.Fatalf("expected canonical DEL for final pop %q, got %q", want, frame)
		}
	}

	recovered, err := core.NewWithPersistence(core.PersistOptions{DataDir: dir, WALFsyncMode: wal.FsyncAlways})
	if err != nil {
		t.Fatalf("failed to recover engine: %v", err)
	}
	recoveredView := recovered.(*core.DBView)
	defer recoveredView.ParentEngine().Close()

	partialObj, ok := recoveredView.Get("spop-partial")
	if !ok || partialObj.Type != object.TypeSet || partialObj.Value.(*object.Set).Len() != 1 {
		t.Fatal("expected partial SPOP recovery to retain exactly one set member")
	}
	for _, key := range []string{"spop-all", "zpopmin-final", "zpopmax-final"} {
		if _, ok := recoveredView.Get(key); ok {
			t.Fatalf("expected %q to be absent after canonical DEL recovery", key)
		}
	}
}

func decodeTestWALCommand(t *testing.T, frame []byte) [][]byte {
	t.Helper()
	if len(frame) < 8 || string(frame[:4]) != "FDBW" || frame[4] != 1 {
		t.Fatalf("unexpected WAL command frame: %x", frame)
	}
	frame = frame[6:]
	argumentCount := int(binary.LittleEndian.Uint16(frame[:2]))
	frame = frame[2:]
	args := make([][]byte, 0, argumentCount)
	for i := 0; i < argumentCount; i++ {
		if len(frame) < 4 {
			t.Fatalf("truncated WAL argument header in frame: %x", frame)
		}
		length := int(binary.LittleEndian.Uint32(frame[:4]))
		frame = frame[4:]
		if len(frame) < length {
			t.Fatalf("truncated WAL argument body in frame: %x", frame)
		}
		args = append(args, frame[:length])
		frame = frame[length:]
	}
	if len(frame) != 0 {
		t.Fatalf("unexpected trailing WAL command data: %x", frame)
	}
	return args
}

func TestHMSetWALPersistenceAndReplay(t *testing.T) {
	dir := t.TempDir()
	engine, err := core.NewWithPersistence(core.PersistOptions{
		DataDir:      dir,
		WALFsyncMode: wal.FsyncAlways,
	})
	if err != nil {
		t.Fatalf("failed to create persistent engine: %v", err)
	}
	view := engine.(*core.DBView)
	parent := view.ParentEngine()

	Register(hashcmd.HMSetCommand{})
	ctx := &kernelcontext.Context{Engine: view, Protocol: 2, AllowOOM: true}

	args := []types.Value{
		types.NewString("HMSET"),
		types.NewString("myhash"),
		types.NewString("field1"),
		types.NewString("val1"),
		types.NewString("field2"),
		types.NewString("val2"),
	}

	result := Dispatch(ctx, args)
	if result.Type == types.TypeError {
		t.Fatalf("dispatch HMSET failed: %s", result.Str)
	}

	if err := parent.Close(); err != nil {
		t.Fatalf("failed to close engine: %v", err)
	}

	recovered, err := core.NewWithPersistence(core.PersistOptions{DataDir: dir, WALFsyncMode: wal.FsyncAlways})
	if err != nil {
		t.Fatalf("failed to recover engine: %v", err)
	}
	recoveredView := recovered.(*core.DBView)
	defer recoveredView.ParentEngine().Close()

	hashObj, ok := recoveredView.Get("myhash")
	if !ok || hashObj.Type != object.TypeHash {
		t.Fatalf("expected recovered hash object, got ok=%v", ok)
	}
	h := hashObj.Value.(*object.Hash)
	val1, ok1 := h.Get("field1")
	val2, ok2 := h.Get("field2")
	if !ok1 || val1 != "val1" || !ok2 || val2 != "val2" {
		t.Fatalf("recovered hash fields mismatch: field1=(%v, %s), field2=(%v, %s)", ok1, val1, ok2, val2)
	}
}

