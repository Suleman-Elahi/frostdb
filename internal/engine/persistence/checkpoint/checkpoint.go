package checkpoint

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Record represents a single key-value pair in a checkpoint
type Record struct {
	Key      string `json:"k"`
	Type     uint8  `json:"t"`
	Encoding uint8  `json:"e"`
	ExpireAt int64  `json:"x,omitempty"`
	Data     []byte `json:"d"`
}

// Writer writes checkpoint files
type Writer struct {
	dir  string
	file *os.File
	buf  *bufio.Writer
}

// NewWriter creates a checkpoint writer
func NewWriter(dir string) (*Writer, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("checkpoint: mkdir: %w", err)
	}

	path := filepath.Join(dir, fmt.Sprintf("checkpoint-%d.fdb", time.Now().UnixMilli()))
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: create: %w", err)
	}

	w := &Writer{
		dir:  dir,
		file: f,
		buf:  bufio.NewWriterSize(f, 64*1024),
	}

	// Write magic header
	w.buf.Write([]byte("FROSTCK1"))
	return w, nil
}

// WriteRecord writes a single record to the checkpoint
func (w *Writer) WriteRecord(rec Record) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	// Length-prefixed record
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := w.buf.Write(lenBuf); err != nil {
		return err
	}
	_, err = w.buf.Write(data)
	return err
}

// Close finalizes and closes the checkpoint file
func (w *Writer) Close() error {
	// Write EOF marker
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, 0)
	w.buf.Write(lenBuf)

	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Path returns the path of the checkpoint file
func (w *Writer) Path() string {
	return w.file.Name()
}

// Reader reads checkpoint files
type Reader struct {
	file *os.File
	buf  *bufio.Reader
}

// OpenReader opens a checkpoint file for reading
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		file: f,
		buf:  bufio.NewReader(f),
	}

	// Verify magic header
	magic := make([]byte, 8)
	if _, err := io.ReadFull(r.buf, magic); err != nil {
		f.Close()
		return nil, fmt.Errorf("checkpoint: read header: %w", err)
	}
	if string(magic) != "FROSTCK1" {
		f.Close()
		return nil, fmt.Errorf("checkpoint: invalid magic header")
	}

	return r, nil
}

// ReadRecord reads the next record. Returns io.EOF when done.
func (r *Reader) ReadRecord() (Record, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r.buf, lenBuf); err != nil {
		return Record{}, err
	}

	dataLen := binary.LittleEndian.Uint32(lenBuf)
	if dataLen == 0 {
		return Record{}, io.EOF
	}

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r.buf, data); err != nil {
		return Record{}, err
	}

	var rec Record
	if err := json.Unmarshal(data, &rec); err != nil {
		return Record{}, fmt.Errorf("checkpoint: unmarshal: %w", err)
	}

	return rec, nil
}

// Close closes the reader
func (r *Reader) Close() error {
	return r.file.Close()
}

// Latest finds the most recent checkpoint file in a directory
func Latest(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}

	var latest string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".fdb" {
			latest = filepath.Join(dir, e.Name())
		}
	}
	return latest, nil
}

// CleanupOldCheckpoints removes all checkpoint files except the most recent one.
// Returns the number of files removed.
func CleanupOldCheckpoints(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	var checkpoints []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".fdb" {
			checkpoints = append(checkpoints, filepath.Join(dir, e.Name()))
		}
	}

	if len(checkpoints) <= 1 {
		return 0, nil
	}

	// Keep the last one (most recent by name, which includes timestamp)
	removed := 0
	for i := 0; i < len(checkpoints)-1; i++ {
		if err := os.Remove(checkpoints[i]); err != nil {
			continue // best effort
		}
		removed++
	}
	return removed, nil
}
