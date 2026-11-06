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
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	currentFileName = "current.wal"
	segmentPrefix   = "segment-"
	segmentSuffix   = ".wal"
)

// FsyncMode controls when accepted WAL entries are flushed to stable storage.
// Always provides per-write durability, EverySec bounds normal crash loss to
// approximately one second, and No relies on the operating system's writeback.
type FsyncMode string

const (
	FsyncAlways   FsyncMode = "always"
	FsyncEverySec FsyncMode = "everysec"
	FsyncNo       FsyncMode = "no"
)

// ParseFsyncMode validates a Redis-compatible appendfsync value.
func ParseFsyncMode(value string) (FsyncMode, error) {
	mode := FsyncMode(strings.ToLower(strings.TrimSpace(value)))
	switch mode {
	case FsyncAlways, FsyncEverySec, FsyncNo:
		return mode, nil
	default:
		return "", fmt.Errorf("wal: invalid fsync mode %q (expected always, everysec, or no)", value)
	}
}

// Status reports the WAL's durability policy and most recent I/O health.
type Status struct {
	Enabled           bool
	FsyncMode         FsyncMode
	CurrentSize       int64
	LastSyncUnixMilli int64
	LastError         string
	FailureCount      uint64
	Healthy           bool
}

// Entry represents a single WAL entry.
type Entry struct {
	Timestamp int64
	Command   []byte
}

// WAL implements a write-ahead log for forkless persistence.
type WAL struct {
	mu          sync.Mutex
	dir         string
	file        *os.File
	writer      *bufio.Writer
	size        int64
	maxSize     int64
	nextSegment uint64
	fsyncMode   FsyncMode

	lastSync     time.Time
	pendingSync  bool
	lastError    string
	failureCount uint64
	healthy      bool

	syncStop  chan struct{}
	syncWg    sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// Options configures the WAL.
type Options struct {
	Dir            string
	MaxFileSize    int64  // rotate after this size (default 64MB)
	InitialSegment uint64 // must exceed the newest recovered checkpoint cutoff

	// FsyncMode is the explicit durability policy. An empty value defaults to
	// everysec. SyncOnWrite is retained only for source compatibility and maps
	// to always when FsyncMode is omitted.
	FsyncMode   FsyncMode
	SyncOnWrite bool
}

func DefaultOptions(dir string) Options {
	return Options{
		Dir:         dir,
		MaxFileSize: 64 * 1024 * 1024,
		FsyncMode:   FsyncEverySec,
	}
}

// Open creates or opens a WAL in the given directory.
func Open(opts Options) (*WAL, error) {
	mode := opts.FsyncMode
	if mode == "" {
		if opts.SyncOnWrite {
			mode = FsyncAlways
		} else {
			mode = FsyncEverySec
		}
	}
	if _, err := ParseFsyncMode(string(mode)); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}

	nextSegment, err := nextSegmentSequence(opts.Dir)
	if err != nil {
		return nil, err
	}
	if opts.InitialSegment > nextSegment {
		nextSegment = opts.InitialSegment
	}
	w := &WAL{
		dir:         opts.Dir,
		maxSize:     opts.MaxFileSize,
		nextSegment: nextSegment,
		fsyncMode:   mode,
		healthy:     true,
	}
	if err := w.openCurrent(); err != nil {
		return nil, err
	}
	if mode == FsyncEverySec {
		w.startPeriodicSync()
	}
	return w, nil
}

func (w *WAL) startPeriodicSync() {
	w.syncStop = make(chan struct{})
	w.syncWg.Add(1)
	go func() {
		defer w.syncWg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = w.syncIfNeeded() // Status retains the failure for INFO and callers.
			case <-w.syncStop:
				return
			}
		}
	}()
}

func nextSegmentSequence(dir string) (uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("wal: list segments: %w", err)
	}

	var max uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if sequence, ok := SegmentSequence(entry.Name()); ok && sequence > max {
			max = sequence
		}
	}
	return max + 1, nil
}

func (w *WAL) openCurrent() error {
	path := filepath.Join(w.dir, currentFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("wal: open: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("wal: stat: %w", err)
	}
	w.file = f
	w.writer = bufio.NewWriterSize(f, 32*1024)
	w.size = info.Size()
	return nil
}

// Append writes a command to the WAL.
func (w *WAL) Append(command []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil || w.writer == nil {
		return w.recordFailureLocked(fmt.Errorf("wal: append closed WAL"))
	}
	entry := Entry{Timestamp: time.Now().UnixMilli(), Command: command}
	n, err := w.writeEntry(entry)
	if err != nil {
		return w.recordFailureLocked(fmt.Errorf("wal: append: %w", err))
	}
	w.size += int64(n)
	w.pendingSync = true

	if w.maxSize > 0 && w.size >= w.maxSize {
		if _, err := w.rotateLocked(); err != nil {
			return err
		}
	}
	if w.fsyncMode == FsyncAlways {
		return w.syncLocked()
	}
	return nil
}

// Rotate seals the current WAL into an immutable, monotonic segment and opens
// a fresh current.wal. The returned sequence is a durable recovery cutoff once
// a checkpoint that records it has been atomically published.
func (w *WAL) Rotate() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateLocked()
}

func (w *WAL) rotateLocked() (uint64, error) {
	if w.file == nil || w.writer == nil {
		return 0, w.recordFailureLocked(fmt.Errorf("wal: rotate closed WAL"))
	}
	if err := w.syncLocked(); err != nil {
		return 0, fmt.Errorf("wal: rotate sync: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return 0, w.recordFailureLocked(fmt.Errorf("wal: rotate close: %w", err))
	}
	w.file = nil
	w.writer = nil

	sequence := w.nextSegment
	oldPath := filepath.Join(w.dir, currentFileName)
	newPath := filepath.Join(w.dir, segmentName(sequence))
	if err := os.Rename(oldPath, newPath); err != nil {
		// Restore a usable current WAL when publication of the sealed segment
		// fails. Recovery will continue to see the original current.wal.
		if reopenErr := w.openCurrent(); reopenErr != nil {
			return 0, w.recordFailureLocked(fmt.Errorf("wal: rotate rename: %w (reopen failed: %v)", err, reopenErr))
		}
		return 0, w.recordFailureLocked(fmt.Errorf("wal: rotate rename: %w", err))
	}
	w.nextSegment++
	w.size = 0
	if err := w.openCurrent(); err != nil {
		return 0, w.recordFailureLocked(fmt.Errorf("wal: rotate open current: %w", err))
	}
	return sequence, nil
}

// Sync flushes buffered data to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

// syncIfNeeded avoids issuing empty fsyncs from the everysec worker.
func (w *WAL) syncIfNeeded() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.pendingSync {
		return nil
	}
	return w.syncLocked()
}

func (w *WAL) syncLocked() error {
	if w.file == nil || w.writer == nil {
		return w.recordFailureLocked(fmt.Errorf("wal: sync closed WAL"))
	}
	if err := w.writer.Flush(); err != nil {
		return w.recordFailureLocked(fmt.Errorf("wal: flush: %w", err))
	}
	if err := w.file.Sync(); err != nil {
		return w.recordFailureLocked(fmt.Errorf("wal: fsync: %w", err))
	}
	w.lastSync = time.Now()
	w.pendingSync = false
	w.healthy = true
	return nil
}

func (w *WAL) recordFailureLocked(err error) error {
	w.lastError = err.Error()
	w.failureCount++
	w.healthy = false
	return err
}

// Status returns a consistent snapshot of WAL durability and I/O health.
func (w *WAL) Status() Status {
	w.mu.Lock()
	defer w.mu.Unlock()
	status := Status{
		Enabled:      true,
		FsyncMode:    w.fsyncMode,
		CurrentSize:  w.size,
		LastError:    w.lastError,
		FailureCount: w.failureCount,
		Healthy:      w.healthy,
	}
	if !w.lastSync.IsZero() {
		status.LastSyncUnixMilli = w.lastSync.UnixMilli()
	}
	return status
}

// Close stops background synchronization, performs a final durable sync, and
// closes the WAL. A failed final sync is returned to the shutdown caller.
func (w *WAL) Close() error {
	w.closeOnce.Do(func() {
		if w.syncStop != nil {
			close(w.syncStop)
			w.syncWg.Wait()
		}

		w.mu.Lock()
		defer w.mu.Unlock()
		if w.file == nil || w.writer == nil {
			return
		}
		if err := w.syncLocked(); err != nil {
			w.closeErr = err
		}
		if err := w.file.Close(); err != nil {
			err = w.recordFailureLocked(fmt.Errorf("wal: close: %w", err))
			if w.closeErr == nil {
				w.closeErr = err
			}
		}
		w.file = nil
		w.writer = nil
	})
	return w.closeErr
}

// Truncate replaces current.wal with an empty file. It is retained for callers
// that explicitly need it; normal checkpointing uses Rotate plus a persisted
// cutoff so that writes after a snapshot can never be lost.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil || w.writer == nil {
		return w.recordFailureLocked(fmt.Errorf("wal: truncate closed WAL"))
	}
	if err := w.writer.Flush(); err != nil {
		return w.recordFailureLocked(fmt.Errorf("wal: truncate flush: %w", err))
	}
	if err := w.file.Close(); err != nil {
		return w.recordFailureLocked(fmt.Errorf("wal: truncate close: %w", err))
	}
	w.file = nil
	w.writer = nil

	path := filepath.Join(w.dir, currentFileName)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return w.recordFailureLocked(fmt.Errorf("wal: truncate remove: %w", err))
	}
	w.size = 0
	if err := w.openCurrent(); err != nil {
		return w.recordFailureLocked(err)
	}
	return nil
}

// PruneThrough removes sealed WAL segments already represented by a published
// checkpoint. Legacy rotated files are also safe to remove once a versioned
// checkpoint has a nonzero cutoff.
func (w *WAL) PruneThrough(cutoff uint64) error {
	if cutoff == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return fmt.Errorf("wal: prune list: %w", err)
	}
	removed := false
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == currentFileName || filepath.Ext(entry.Name()) != ".wal" {
			continue
		}
		sequence, isSegment := SegmentSequence(entry.Name())
		if isSegment && sequence > cutoff {
			continue
		}
		if err := os.Remove(filepath.Join(w.dir, entry.Name())); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("wal: prune %s: %w", entry.Name(), err)
		}
		removed = true
	}
	if !removed {
		return nil
	}
	dir, err := os.Open(w.dir)
	if err != nil {
		return fmt.Errorf("wal: prune open directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("wal: prune sync directory: %w", err)
	}
	return nil
}

// Size returns the current WAL file size in bytes.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// SegmentSequence returns the sequence encoded in a sealed WAL segment name.
func SegmentSequence(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(name, segmentPrefix), segmentSuffix)
	if raw == "" {
		return 0, false
	}
	sequence, err := strconv.ParseUint(raw, 10, 64)
	return sequence, err == nil
}

func segmentName(sequence uint64) string {
	return fmt.Sprintf("%s%020d%s", segmentPrefix, sequence, segmentSuffix)
}

var walEntryBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 1024)
		return &b
	},
}

// writeEntry encodes and writes a single entry.
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

// ReadAll reads all entries from a WAL file for replay.
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
			// The final append may have been torn by a crash; retain every prior
			// complete, checksum-verified entry.
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
	var header [16]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Entry{}, err
	}

	storedCRC := binary.LittleEndian.Uint32(header[0:4])
	timestamp := int64(binary.LittleEndian.Uint64(header[4:12]))
	dataLen := binary.LittleEndian.Uint32(header[12:16])

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return Entry{}, err
	}

	check := make([]byte, 12+dataLen)
	copy(check[0:8], header[4:12])
	copy(check[8:12], header[12:16])
	copy(check[12:], data)
	if crc32.ChecksumIEEE(check) != storedCRC {
		return Entry{}, fmt.Errorf("wal: checksum mismatch")
	}
	return Entry{Timestamp: timestamp, Command: data}, nil
}
