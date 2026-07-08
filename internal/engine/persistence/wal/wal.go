package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Entry represents a single WAL entry
type Entry struct {
	Timestamp int64
	Command   []byte
}

// WAL implements a write-ahead log for forkless persistence
type WAL struct {
	mu          sync.Mutex
	dir         string
	file        *os.File
	writer      *bufio.Writer
	size        int64
	maxSize     int64
	sequence    uint64
	syncOnWrite bool
}

// Options configures the WAL
type Options struct {
	Dir         string
	MaxFileSize int64 // rotate after this size (default 64MB)
	SyncOnWrite bool
}

func DefaultOptions(dir string) Options {
	return Options{
		Dir:         dir,
		MaxFileSize: 64 * 1024 * 1024,
		SyncOnWrite: false,
	}
}

// Open creates or opens a WAL in the given directory
func Open(opts Options) (*WAL, error) {
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}

	w := &WAL{
		dir:         opts.Dir,
		maxSize:     opts.MaxFileSize,
		syncOnWrite: opts.SyncOnWrite,
	}

	if err := w.openCurrent(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *WAL) openCurrent() error {
	path := filepath.Join(w.dir, "current.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("wal: open: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("wal: stat: %w", err)
	}

	w.file = f
	w.writer = bufio.NewWriterSize(f, 32*1024)
	w.size = info.Size()
	return nil
}

// Append writes a command to the WAL
func (w *WAL) Append(command []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := Entry{
		Timestamp: time.Now().UnixMilli(),
		Command:   command,
	}

	n, err := w.writeEntry(entry)
	if err != nil {
		return err
	}
	w.size += int64(n)
	w.sequence++

	if w.maxSize > 0 && w.size >= w.maxSize {
		return w.rotateLocked()
	}

	if w.syncOnWrite {
		if err := w.writer.Flush(); err != nil {
			return err
		}
		if err := w.file.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// Sync flushes buffered data to disk
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close flushes and closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Truncate flushes pending writes and truncates the current WAL file.
// This is used after a checkpoint to discard already-persisted entries.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}

	// Remove the current WAL file and create a fresh one
	path := filepath.Join(w.dir, "current.wal")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("wal: truncate remove: %w", err)
	}

	w.size = 0
	w.sequence = 0
	return w.openCurrent()
}

// Size returns the current WAL file size in bytes.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

var walEntryBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 1024)
		return &b
	},
}

// writeEntry encodes and writes a single entry
// Format: [crc32:4][timestamp:8][len:4][data:len]
func (w *WAL) writeEntry(entry Entry) (int, error) {
	dataLen := len(entry.Command)
	totalLen := 16 + dataLen

	bp := walEntryBufPool.Get().(*[]byte)
	if cap(*bp) < totalLen {
		b := make([]byte, totalLen)
		bp = &b
	}
	buf := (*bp)[:totalLen]

	binary.LittleEndian.PutUint64(buf[4:12], uint64(entry.Timestamp))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(dataLen))
	copy(buf[16:], entry.Command)

	checksum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	n, err := w.writer.Write(buf)

	walEntryBufPool.Put(bp)
	return n, err
}

func (w *WAL) rotateLocked() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	w.file.Close()

	ts := time.Now().UnixMilli()
	oldPath := filepath.Join(w.dir, "current.wal")
	newPath := filepath.Join(w.dir, fmt.Sprintf("%d.wal", ts))
	if err := os.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("wal: rotate: %w", err)
	}

	w.size = 0
	return w.openCurrent()
}

// ReadAll reads all entries from a WAL file for replay
func ReadAll(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var entries []Entry

	for {
		entry, err := readEntry(reader)
		if err == io.EOF {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// Last entry may be torn if process crashed during append.
			// Keep all previously validated entries.
			break
		}
		if err != nil {
			return entries, fmt.Errorf("wal: read entry: %w", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func readEntry(r io.Reader) (Entry, error) {
	header := make([]byte, 16)
	if _, err := io.ReadFull(r, header); err != nil {
		return Entry{}, err
	}

	storedCRC := binary.LittleEndian.Uint32(header[0:4])
	timestamp := int64(binary.LittleEndian.Uint64(header[4:12]))
	dataLen := binary.LittleEndian.Uint32(header[12:16])

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return Entry{}, err
	}

	// Verify checksum
	check := make([]byte, 12+dataLen)
	copy(check[0:8], header[4:12])
	copy(check[8:12], header[12:16])
	copy(check[12:], data)
	if crc32.ChecksumIEEE(check) != storedCRC {
		return Entry{}, fmt.Errorf("wal: checksum mismatch")
	}

	return Entry{Timestamp: timestamp, Command: data}, nil
}
