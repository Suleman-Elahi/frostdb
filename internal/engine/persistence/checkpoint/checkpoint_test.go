package checkpoint

import (
	"io"
	"os"
	"testing"
)

func TestCheckpointWriteAndRead(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	records := []Record{
		{Key: "key1", Type: 0, Data: []byte("value1")},
		{Key: "key2", Type: 0, Data: []byte("value2"), ExpireAt: 9999999},
		{Key: "list1", Type: 1, Data: []byte(`["a","b","c"]`)},
	}

	for _, rec := range records {
		if err := w.WriteRecord(rec); err != nil {
			t.Fatalf("failed to write record: %v", err)
		}
	}

	path := w.Path()
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read back
	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer r.Close()

	var read []Record
	for {
		rec, err := r.ReadRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		read = append(read, rec)
	}

	if len(read) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(read))
	}

	for i, rec := range read {
		if rec.Key != records[i].Key {
			t.Errorf("record %d: expected key %q, got %q", i, records[i].Key, rec.Key)
		}
		if string(rec.Data) != string(records[i].Data) {
			t.Errorf("record %d: data mismatch", i)
		}
		if rec.ExpireAt != records[i].ExpireAt {
			t.Errorf("record %d: expire mismatch", i)
		}
	}
}

func TestCheckpointLatest(t *testing.T) {
	dir := t.TempDir()

	// No checkpoints yet
	path, err := Latest(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if path != "" {
		t.Fatalf("expected empty path, got %q", path)
	}

	// Create one
	w, _ := NewWriter(dir)
	w.WriteRecord(Record{Key: "k", Data: []byte("v")})
	w.Close()

	path, err = Latest(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if path == "" {
		t.Fatal("expected a checkpoint path")
	}
}

func TestCheckpointInvalidMagic(t *testing.T) {
	dir := t.TempDir()

	// Write a valid checkpoint first to get a path
	w, _ := NewWriter(dir)
	path := w.Path()
	w.Close()

	// Overwrite with garbage magic
	os.WriteFile(path, []byte("BADMAGIC"), 0644)

	_, err := OpenReader(path)
	if err == nil {
		t.Fatal("expected error for invalid magic header")
	}
}
