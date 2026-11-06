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

const (
	magicV1 = "FROSTCK1"
	magicV2 = "FROSTCK2"
)

// Metadata describes the recovery boundary represented by a checkpoint.
// WALCutoff is the highest sealed WAL segment already included in this
// snapshot. A zero value is used for legacy FROSTCK1 snapshots.
type Metadata struct {
	WALCutoff uint64
}

// Record represents a single key-value pair in a checkpoint.
type Record struct {
	DB       uint8  `json:"b,omitempty"`
	Key      string `json:"k"`
	Type     uint8  `json:"t"`
	Encoding uint8  `json:"e"`
	ExpireAt int64  `json:"x,omitempty"`
	Data     []byte `json:"d"`
}

// Writer writes checkpoint files.
type Writer struct {
	dir      string
	path     string
	tempPath string
	file     *os.File
	buf      *bufio.Writer
	metadata Metadata
}

// NewWriter creates a versioned checkpoint with no WAL cutoff. It is retained
// for callers that do not use persistence rotation.
func NewWriter(dir string) (*Writer, error) {
	return NewWriterWithMetadata(dir, Metadata{})
}

// NewWriterWithMetadata creates a FROSTCK2 checkpoint. Records are written to
// a temporary file and become visible to recovery only after Close atomically
// publishes the final file.
func NewWriterWithMetadata(dir string, metadata Metadata) (*Writer, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("checkpoint: mkdir: %w", err)
	}

	path := filepath.Join(dir, fmt.Sprintf("checkpoint-%d.fdb", time.Now().UnixNano()))
	f, err := os.CreateTemp(dir, ".checkpoint-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("checkpoint: create temporary file: %w", err)
	}

	w := &Writer{
		dir:      dir,
		path:     path,
		tempPath: f.Name(),
		file:     f,
		buf:      bufio.NewWriterSize(f, 64*1024),
		metadata: metadata,
	}
	if _, err := w.buf.WriteString(magicV2); err != nil {
		_ = w.Abort()
		return nil, fmt.Errorf("checkpoint: write header: %w", err)
	}
	var metadataBuf [8]byte
	binary.LittleEndian.PutUint64(metadataBuf[:], metadata.WALCutoff)
	if _, err := w.buf.Write(metadataBuf[:]); err != nil {
		_ = w.Abort()
		return nil, fmt.Errorf("checkpoint: write metadata: %w", err)
	}
	return w, nil
}

// WriteRecord writes a single record to the checkpoint.
func (w *Writer) WriteRecord(rec Record) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := w.buf.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = w.buf.Write(data)
	return err
}

// Close finalizes, fsyncs, and atomically publishes the checkpoint file.
func (w *Writer) Close() error {
	if w.file == nil {
		return fmt.Errorf("checkpoint: writer is already closed")
	}

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], 0)
	if _, err := w.buf.Write(lenBuf[:]); err != nil {
		_ = w.Abort()
		return fmt.Errorf("checkpoint: write eof: %w", err)
	}
	if err := w.buf.Flush(); err != nil {
		_ = w.Abort()
		return fmt.Errorf("checkpoint: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		_ = w.Abort()
		return fmt.Errorf("checkpoint: fsync: %w", err)
	}
	if err := w.file.Close(); err != nil {
		w.file = nil
		_ = os.Remove(w.tempPath)
		return fmt.Errorf("checkpoint: close: %w", err)
	}
	w.file = nil

	if err := os.Rename(w.tempPath, w.path); err != nil {
		_ = os.Remove(w.tempPath)
		return fmt.Errorf("checkpoint: publish: %w", err)
	}
	if err := syncDir(w.dir); err != nil {
		return fmt.Errorf("checkpoint: sync directory: %w", err)
	}
	return nil
}

// Abort discards a checkpoint that was not successfully published.
func (w *Writer) Abort() error {
	var firstErr error
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			firstErr = err
		}
		w.file = nil
	}
	if w.tempPath != "" {
		if err := os.Remove(w.tempPath); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Path returns the final, atomically-published checkpoint path.
func (w *Writer) Path() string {
	return w.path
}

// Reader reads checkpoint files.
type Reader struct {
	file     *os.File
	buf      *bufio.Reader
	metadata Metadata
}

// OpenReader opens either a legacy FROSTCK1 checkpoint or a versioned
// FROSTCK2 checkpoint with WAL cutoff metadata.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r := &Reader{file: f, buf: bufio.NewReader(f)}
	var magic [8]byte
	if _, err := io.ReadFull(r.buf, magic[:]); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("checkpoint: read header: %w", err)
	}
	switch string(magic[:]) {
	case magicV1:
		// Legacy snapshots do not carry a WAL segment boundary.
	case magicV2:
		var metadataBuf [8]byte
		if _, err := io.ReadFull(r.buf, metadataBuf[:]); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("checkpoint: read metadata: %w", err)
		}
		r.metadata.WALCutoff = binary.LittleEndian.Uint64(metadataBuf[:])
	default:
		_ = f.Close()
		return nil, fmt.Errorf("checkpoint: invalid magic header")
	}
	return r, nil
}

// Metadata returns the recovery boundary recorded by this checkpoint.
func (r *Reader) Metadata() Metadata {
	return r.metadata
}

// ReadRecord reads the next record. Returns io.EOF when done.
func (r *Reader) ReadRecord() (Record, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r.buf, lenBuf[:]); err != nil {
		return Record{}, err
	}
	dataLen := binary.LittleEndian.Uint32(lenBuf[:])
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

// Close closes the reader.
func (r *Reader) Close() error {
	return r.file.Close()
}

// Latest finds the most recent checkpoint file in a directory.
func Latest(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}

	var latest string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".fdb" {
			latest = filepath.Join(dir, entry.Name())
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
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".fdb" {
			checkpoints = append(checkpoints, filepath.Join(dir, entry.Name()))
		}
	}
	if len(checkpoints) <= 1 {
		return 0, nil
	}

	removed := 0
	for i := 0; i < len(checkpoints)-1; i++ {
		if err := os.Remove(checkpoints[i]); err != nil {
			continue
		}
		removed++
	}
	return removed, nil
}
