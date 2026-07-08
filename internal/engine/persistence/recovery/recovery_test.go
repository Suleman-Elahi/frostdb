package recovery

import (
	"path/filepath"
	"testing"

	"frostdb/internal/engine/persistence/checkpoint"
	"frostdb/internal/engine/persistence/wal"
)

func TestRecoverEmpty(t *testing.T) {
	dir := t.TempDir()

	state, err := Recover(Options{DataDir: dir})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(state.Records) != 0 {
		t.Fatalf("expected no records, got %d", len(state.Records))
	}
	if len(state.WALEntries) != 0 {
		t.Fatalf("expected no WAL entries, got %d", len(state.WALEntries))
	}
}

func TestRecoverCheckpointOnly(t *testing.T) {
	dir := t.TempDir()
	cpDir := filepath.Join(dir, "checkpoint")

	w, err := checkpoint.NewWriter(cpDir)
	if err != nil {
		t.Fatalf("failed to create checkpoint: %v", err)
	}
	w.WriteRecord(checkpoint.Record{Key: "k1", Data: []byte("v1")})
	w.WriteRecord(checkpoint.Record{Key: "k2", Data: []byte("v2")})
	w.Close()

	state, err := Recover(Options{DataDir: dir})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(state.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(state.Records))
	}
	if state.Records[0].Key != "k1" || state.Records[1].Key != "k2" {
		t.Fatalf("unexpected records: %+v", state.Records)
	}
}

func TestRecoverWALOnly(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	w, err := wal.Open(wal.DefaultOptions(walDir))
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	w.Append([]byte("SET k1 v1"))
	w.Append([]byte("SET k2 v2"))
	w.Close()

	state, err := Recover(Options{DataDir: dir})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(state.WALEntries) != 2 {
		t.Fatalf("expected 2 WAL entries, got %d", len(state.WALEntries))
	}
	if string(state.WALEntries[0].Command) != "SET k1 v1" {
		t.Fatalf("unexpected entry: %s", state.WALEntries[0].Command)
	}
}

func TestRecoverCheckpointPlusWAL(t *testing.T) {
	dir := t.TempDir()
	cpDir := filepath.Join(dir, "checkpoint")
	walDir := filepath.Join(dir, "wal")

	// Write checkpoint
	cw, _ := checkpoint.NewWriter(cpDir)
	cw.WriteRecord(checkpoint.Record{Key: "k1", Data: []byte("v1")})
	cw.Close()

	// Write WAL
	ww, _ := wal.Open(wal.DefaultOptions(walDir))
	ww.Append([]byte("SET k2 v2"))
	ww.Close()

	state, err := Recover(Options{DataDir: dir})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(state.Records) != 1 {
		t.Fatalf("expected 1 checkpoint record, got %d", len(state.Records))
	}
	if len(state.WALEntries) != 1 {
		t.Fatalf("expected 1 WAL entry, got %d", len(state.WALEntries))
	}
}
