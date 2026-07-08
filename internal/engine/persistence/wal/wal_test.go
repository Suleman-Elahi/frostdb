package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)

	w, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	commands := []string{
		"SET key1 value1",
		"SET key2 value2",
		"DEL key1",
		"LPUSH mylist item1",
	}

	for _, cmd := range commands {
		if err := w.Append([]byte(cmd)); err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Read back
	entries, err := ReadAll(filepath.Join(dir, "current.wal"))
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if len(entries) != len(commands) {
		t.Fatalf("expected %d entries, got %d", len(commands), len(entries))
	}

	for i, entry := range entries {
		if string(entry.Command) != commands[i] {
			t.Errorf("entry %d: expected %q, got %q", i, commands[i], string(entry.Command))
		}
		if entry.Timestamp <= 0 {
			t.Errorf("entry %d: invalid timestamp %d", i, entry.Timestamp)
		}
	}
}

func TestWALRotation(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:         dir,
		MaxFileSize: 100, // tiny size to force rotation
		SyncOnWrite: false,
	}

	w, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	// Write enough to trigger rotation
	for i := 0; i < 20; i++ {
		if err := w.Append([]byte("SET key value_that_is_long_enough")); err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Should have rotated files
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}

	walCount := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			walCount++
		}
	}

	if walCount < 2 {
		t.Fatalf("expected at least 2 WAL files after rotation, got %d", walCount)
	}
}

func TestWALChecksumIntegrity(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)

	w, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	w.Append([]byte("SET key value"))
	w.Close()

	// Corrupt the file
	path := filepath.Join(dir, "current.wal")
	data, _ := os.ReadFile(path)
	if len(data) > 10 {
		data[10] ^= 0xFF // flip a byte in the timestamp/data area
		os.WriteFile(path, data, 0644)
	}

	_, err = ReadAll(path)
	if err == nil {
		t.Fatal("expected checksum error on corrupted WAL")
	}
}
